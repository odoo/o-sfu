//! Offer/answer ownership for worker-local RTC users.
//!
//! This module keeps the one-outstanding-offer rule local to the worker that
//! owns the user's `str0m::Rtc`. It creates the initial server-authored offer,
//! builds each follow-up offer from the session's queue of media changes,
//! accepts remote answers and refreshes the worker-local state that answer
//! application invalidates. [`state`] documents the queue contract.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    mem,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use o_sfu_rfc::webrtc::MediaKind as ProtocolMediaKind;
use o_sfu_telemetry::metrics::RuntimeMetrics;
use str0m::{
    change::SdpAnswer,
    media::{Media, MediaKind, Mid},
};
use tracing::debug;

use super::{
    RtpProfile, bootstrap, codec,
    commands::{ParsedSessionAnswer, RtcSessionOffer},
    control::{
        publication::{answer_producer_projection, refresh_negotiated_producer_parameters},
        recv_stream::{RecvStreamRepair, StaleSsrcPolicy, apply_recv_stream},
    },
    state::{PacketLoopState, bitrate::BitrateRegistry},
};
use crate::{
    Bitrate, RtcTcpConfig, VideoBitrateLimits,
    engine::media_transport::{
        AppliedProducer, AppliedSessionAnswer, SessionUploadEncoding, TransportAdapterError,
        TransportMediaId, TransportSessionKey, rtc::state::demux::RemoteTransportAddr,
    },
};

mod media;
mod send_streams;
mod state;
pub(super) use media::{declare_send_stream, recv_encoding_identities, remove_send_streams};
pub(super) use send_streams::SyncGroup;
use state::{PendingRecvStream, TakenOffer};
pub(super) use state::{QueuedChange, SessionSdpNegotiationState};

#[derive(Clone, Copy)]
pub(super) struct OfferBootstrapConfig<'a> {
    pub(super) candidate_addr: SocketAddr,
    pub(super) max_bitrate_out: Bitrate,
    pub(super) video_bitrate_limits: VideoBitrateLimits,
    pub(super) profile: &'a RtpProfile,
    pub(super) media_quality_interval: Option<Duration>,
    pub(super) metrics: &'a RuntimeMetrics,
    pub(super) rtc_tcp_config: Option<RtcTcpConfig>,
}

/// Create the first local offer for a user after ensuring the worker has
/// packet-loop state and the user still has no negotiated media.
///
/// The initial offer is reserved for the transport bootstrap and capability
/// probe flow. Once media has been registered or an earlier initial offer is in
/// flight, callers must use the renegotiation path instead.
pub(super) fn worker_create_initial_session_offer(
    state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    config: OfferBootstrapConfig<'_>,
    room_id: Arc<str>,
    session_key: &TransportSessionKey,
    mid_reuse: bool,
) -> Result<RtcSessionOffer, TransportAdapterError> {
    ensure_session_ready_for_offer(state, bitrate_registry, config, room_id, session_key)?;
    if state.session_has_registered_media(session_key) {
        return Err(TransportAdapterError::UnsupportedFeature);
    }
    let Some(session_state) = state.users.get_mut(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    session_state.sdp_negotiation.create_initial_offer(
        &mut session_state.rtc,
        config.profile,
        config.video_bitrate_limits,
        mid_reuse,
    )
}

/// Builds the follow-up offer for `session_key` from its queued media changes.
///
/// A queued consumer joins the offer only when `committed_consumers` lists it.
/// The offer binds a MID to each addition it makes and names every consumer
/// MID of the session, so its caller needs no earlier list.
///
/// # Errors
///
/// Returns [`TransportAdapterError::TransportUnavailable`] when the session is
/// absent. Returns [`TransportAdapterError::InvalidInput`] before the initial
/// answer or while another answer is pending. Returns
/// [`TransportAdapterError::UnsupportedFeature`] when no queued change needs an
/// offer.
pub(super) fn worker_create_session_renegotiation_offer(
    state: &mut PacketLoopState,
    profile: &RtpProfile,
    video_bitrate_limits: VideoBitrateLimits,
    session_key: &TransportSessionKey,
    committed_consumers: &[TransportMediaId],
) -> Result<RtcSessionOffer, TransportAdapterError> {
    let Some(session_state) = state.users.get_mut(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let TakenOffer {
        offer,
        upload_slots,
        bound_mids,
    } = session_state.sdp_negotiation.take_offer(
        &mut session_state.rtc,
        profile,
        video_bitrate_limits,
        committed_consumers,
    )?;
    for (media, mid) in bound_mids {
        state.bind_mid(media, mid);
    }
    let consumer_mids = state
        .consumer_media_snapshot(session_key)
        .into_iter()
        .map(|(consumer_media, mid, _src_media)| (consumer_media, mid.to_string()))
        .collect();
    Ok(RtcSessionOffer::new(offer, upload_slots, consumer_mids))
}

/// Accept the currently pending local offer and reconcile every worker-local
/// structure that depends on the answer.
///
/// Applying an answer can recreate recv bindings inside `str0m`, so this path
/// must rebuild pending recv expectations, refresh negotiated producer
/// parameters and index the remote candidate addresses that later packet-loop
/// recovery depends on. Changes queued meanwhile wait for the next offer.
pub(super) fn worker_apply_session_answer(
    state: &mut PacketLoopState,
    max_bitrate_in: Bitrate,
    session_key: &TransportSessionKey,
    parsed_answer: ParsedSessionAnswer,
) -> Result<AppliedSessionAnswer, TransportAdapterError> {
    let producer_media_snapshot = state.producer_media_snapshot(session_key);
    let consumer_media_snapshot = state.consumer_media_snapshot(session_key);
    let producer_mids = producer_media_snapshot
        .iter()
        .map(|(_transport_media_id, mid)| *mid)
        .collect::<Vec<_>>();
    let ParsedSessionAnswer {
        answer,
        rids,
        repair,
    } = parsed_answer;
    let offered_repair = state
        .users
        .get(session_key)
        .and_then(|session| session.sdp_negotiation.offered_repair())
        .ok_or(TransportAdapterError::InvalidInput)?;
    // An answer can only retain repair mappings from the offer. The primary and RTX
    // payload types form one mapping.
    // https://www.rfc-editor.org/rfc/rfc3264.html#section-6.1
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1
    if !offered_repair.accepts(&repair) {
        return Err(TransportAdapterError::InvalidInput);
    }
    // Derive every fallible answer view available before `accept_answer`.
    // str0m 0.21 mutates ICE, DTLS and session state in place, so a later RID
    // or router-projection rejection could not restore the pending offer.
    // https://github.com/algesten/str0m/blob/0.21.0/src/change/sdp.rs#L158-L208
    // https://github.com/algesten/str0m/blob/0.21.0/src/change/sdp.rs#L961-L981
    let remote_candidate_addrs = answer_remote_candidate_addrs(&answer);
    let client_capabilities = codec::client_rtp_capabilities_from_sdp_answer(&answer)?;
    let producer_answer_projection = answer_producer_projection(&answer, &producer_mids)?;
    let offer_encodings = offer_encodings_by_mid(state, session_key, &producer_mids)?;
    let mut encodings_by_mid = producer_mids
        .iter()
        .map(|mid| {
            let encodings = offer_encodings
                .get(mid)
                .map(Vec::as_slice)
                .unwrap_or_default();
            rids.negotiate(*mid, encodings)
                .map(|encodings| (*mid, encodings))
                .map_err(|_error| TransportAdapterError::InvalidInput)
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    {
        let Some(session_state) = state.users.get_mut(session_key) else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        session_state
            .sdp_negotiation
            .accept_answer(&mut session_state.rtc, answer)?;
        session_state.purge_removed_rtx_streams();
        apply_pending_recv_streams(session_state, max_bitrate_in);
        session_state.dtls_started = true;
    }
    let declined_consumers =
        remove_declined_consumer_media(state, session_key, consumer_media_snapshot);
    let mut refreshed_by_mid = refresh_negotiated_producer_parameters(
        state,
        session_key,
        &producer_mids,
        producer_answer_projection,
        &encodings_by_mid,
        max_bitrate_in,
    )?
    .into_iter()
    .collect::<BTreeMap<_, _>>();
    let queued_producers = state
        .users
        .get(session_key)
        .map(|session| session.sdp_negotiation.queued_producers().collect())
        .unwrap_or_default();
    state.mark_session_dirty(session_key);
    state
        .remote_addr_demux
        .replace_remote_candidates(session_key, remote_candidate_addrs.iter().copied());
    Ok(AppliedSessionAnswer::from_negotiated_producer_details(
        producer_media_snapshot
            .into_iter()
            .filter_map(|(transport_media_id, mid)| {
                refreshed_by_mid.remove(&mid).map(|parameters| {
                    (
                        transport_media_id,
                        AppliedProducer::new(
                            parameters,
                            encodings_by_mid.remove(&mid).unwrap_or_default(),
                        ),
                    )
                })
            }),
    )
    .with_declined_consumers(declined_consumers)
    .with_queued_producers(queued_producers)
    .with_client_capabilities(client_capabilities))
}

fn remove_declined_consumer_media(
    state: &mut PacketLoopState,
    session_key: &TransportSessionKey,
    consumers: Vec<(TransportMediaId, Mid, TransportMediaId)>,
) -> Vec<TransportMediaId> {
    let mut declined = Vec::new();
    for (consumer_media, mid, src_media) in consumers {
        // A recvonly answer preserves the offerer's sendonly direction. If the
        // applied local direction is not sending, the answer declined the stream.
        // https://www.rfc-editor.org/rfc/rfc3264.html#section-6.1
        if state
            .users
            .get(session_key)
            .and_then(|session| session.rtc.media(mid))
            .is_some_and(|media| media.direction().is_sending())
        {
            continue;
        }
        declined.push(consumer_media);
        state.remove_consumer_route(session_key, consumer_media, src_media);
        let Some(session_state) = state.users.get_mut(session_key) else {
            continue;
        };
        session_state.remove_consumer_stream_tx(mid);
    }
    declined
}

fn offer_encodings_by_mid(
    state: &PacketLoopState,
    session_key: &TransportSessionKey,
    producer_mids: &[Mid],
) -> Result<BTreeMap<Mid, Vec<SessionUploadEncoding>>, TransportAdapterError> {
    let session_state = state
        .users
        .get(session_key)
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    let mut encodings = session_state
        .sdp_negotiation
        .upload_slots()
        .iter()
        .map(|slot| {
            (
                Mid::from(slot.mid.as_str()),
                slot.simulcast_encodings.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for mid in producer_mids {
        let Entry::Vacant(entry) = encodings.entry(*mid) else {
            continue;
        };
        let Some(rtp_parameters) = session_state
            .sdp_negotiation
            .negotiated_producer_parameters
            .get(mid)
        else {
            continue;
        };
        let Some(media_kind) = session_state.rtc.media(*mid).map(Media::kind) else {
            continue;
        };
        let simulcast_encodings = codec::publish_upload_encodings(media_kind, rtp_parameters);
        if simulcast_encodings.is_empty() {
            continue;
        }
        entry.insert(simulcast_encodings);
    }
    Ok(encodings)
}

fn answer_remote_candidate_addrs(answer: &SdpAnswer) -> Vec<RemoteTransportAddr> {
    answer
        .session
        .ice_candidates()
        .map(RemoteTransportAddr::from_candidate)
        .chain(answer.media_lines.iter().flat_map(|media_line| {
            media_line
                .ice_candidates()
                .map(RemoteTransportAddr::from_candidate)
        }))
        .collect()
}

fn apply_pending_recv_streams(
    session_state: &mut super::state::RtcSessionState,
    max_bitrate_in: Bitrate,
) {
    // Answer-time recv refresh can recreate `StreamRx` bindings, so REMB must
    // be re-set here to keep the inbound user cap alive across renegotiation.
    if session_state
        .sdp_negotiation
        .pending_recv_streams
        .is_empty()
    {
        return;
    }
    let pending_recv_streams = mem::take(&mut session_state.sdp_negotiation.pending_recv_streams);
    {
        let mut api = session_state.rtc.direct_api();
        for (mid, streams) in pending_recv_streams {
            for stream in streams {
                let (ssrc, repair_ssrc) = api
                    .stream_rx_by_mid(mid, stream.rid)
                    .map_or((stream.ssrc, stream.repair_ssrc), |accepted| {
                        (accepted.ssrc(), accepted.rtx())
                    });
                apply_recv_stream(
                    &mut api,
                    mid,
                    stream.rid,
                    ssrc,
                    RecvStreamRepair {
                        ssrc: repair_ssrc,
                        nack_enabled: repair_ssrc.is_some(),
                    },
                    max_bitrate_in,
                    StaleSsrcPolicy::ReplaceStale,
                );
            }
        }
    }
    #[cfg(test)]
    {
        session_state.max_bitrate_in = Some(max_bitrate_in);
    }
}

fn upload_kind(media_kind: MediaKind) -> ProtocolMediaKind {
    if media_kind.is_video() {
        ProtocolMediaKind::Video
    } else {
        ProtocolMediaKind::Audio
    }
}

fn ensure_session_ready_for_offer(
    state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    config: OfferBootstrapConfig<'_>,
    room_id: Arc<str>,
    session_key: &TransportSessionKey,
) -> Result<(), TransportAdapterError> {
    let created_session = bootstrap::ensure_session_rtc_state(
        &mut state.users,
        room_id,
        session_key,
        config.candidate_addr,
        config.max_bitrate_out,
        config.profile,
        config.media_quality_interval,
        config.rtc_tcp_config,
    )?;
    if let Some(session_state) = state.users.get(session_key) {
        if created_session && let Ok(mut bitrate) = bitrate_registry.lock() {
            bitrate.register_session_egress(session_key, Arc::clone(&session_state.egress_bitrate));
        }
        let local_ice_ufrag_changed = state
            .ufrag_registry
            .remember(&session_state.local_ice_ufrag, session_key);
        if created_session || local_ice_ufrag_changed {
            state.ufrag_worker_map.insert(
                &session_state.local_ice_ufrag,
                session_key.media_worker_id(),
            );
            debug!(
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id().as_usize(),
                candidate_addr = %config.candidate_addr,
                local_ice_ufrag = %session_state.local_ice_ufrag,
                created_session,
                "prepared rtc user for offer generation"
            );
        }
    }
    if created_session {
        config.metrics.add_active_transport_users(1);
    }
    Ok(())
}
