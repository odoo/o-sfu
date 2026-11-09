//! Worker-local media lifecycle for one RTC worker.
//!
//! Producer and consumer declarations stage SDP changes before committing media
//! registration or removal through [`PacketLoopState`]. Offer and answer transitions
//! remain in [`negotiation`].
//!
//! The helpers here rely on two invariants:
//! - worker commands are serialized through one mutable `PacketLoopState`
//! - the signaling edge obeys the one-outstanding-offer rule and does not try
//!   to hand out a second local offer while the previous one still awaits an
//!   answer

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use str0m::{
    bwe::Bitrate as Str0mBitrate,
    media::{MediaKind, Mid},
};
use tracing::{debug, warn};

use super::AddSendMediaRequest;
use crate::{
    Bitrate, VideoBitrateLimits,
    engine::{
        media_transport::{
            TransportAdapterError, TransportMediaId, TransportResult, TransportSessionKey,
            rtc::{
                RtpProfile, codec, negotiation,
                state::{
                    ConsumerRouteRegistration, PacketLoopState, RtcSessionState,
                    bitrate::BitrateRegistry, media_registry::RegisteredMediaHandle,
                    slots::ConsumerStreamHandle,
                },
            },
        },
        source_model::SourceSyncPolicy,
    },
};

#[derive(Clone, Copy)]
pub struct RecvMediaPolicy<'a> {
    pub max_bitrate_in: Bitrate,
    pub video_bitrate_limits: VideoBitrateLimits,
    pub profile: &'a RtpProfile,
}

/// Removes one registered transport media and its dependent routes.
///
/// Missing media is already removed and succeeds.
///
/// # Errors
///
/// Returns [`TransportAdapterError::InvalidInput`] when the media belongs to a
/// different session or its negotiated MID removal cannot be staged or queued.
pub fn worker_remove_media(
    state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
) -> Result<(), TransportAdapterError> {
    let Some(handle) = state.media_handle(transport_media_id).cloned() else {
        return Ok(());
    };
    if handle.session_key() != session_key {
        return Err(TransportAdapterError::InvalidInput);
    }
    if can_unregister_unnegotiated_producer(state, &handle) {
        debug!(
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id().as_usize(),
            ?transport_media_id,
            mid = ?handle.mid(),
            "released unnegotiated producer media without staging sdp removal"
        );
        return state.unregister_media_handle(bitrate_registry, transport_media_id);
    }
    // Record the MID transition before unregistering the handle and routes. An
    // in-flight offer queues the transition. Otherwise it is staged now.
    stage_last_mid_removal_before_unregistering_handle(state, transport_media_id, &handle)?;
    state.unregister_media_handle(bitrate_registry, transport_media_id)
}

fn stage_last_mid_removal_before_unregistering_handle(
    state: &mut PacketLoopState,
    transport_media_id: TransportMediaId,
    handle: &RegisteredMediaHandle,
) -> Result<(), TransportAdapterError> {
    let mid_is_shared = state.mid_is_shared(handle.session_key(), handle.mid(), transport_media_id);
    let session_state = state
        .users
        .get_mut(handle.session_key())
        .ok_or(TransportAdapterError::InvalidInput)?;
    if mid_is_shared {
        // Several handles may share one m-section. Disabling it while a sibling
        // remains would invalidate that sibling's negotiated media.
        return Ok(());
    }
    if session_state.sdp_negotiation.initial_offer_applied() {
        negotiation::queue_removal(session_state, handle.mid())
    } else {
        session_state.rtc.direct_api().remove_media(handle.mid());
        Ok(())
    }
}

fn can_unregister_unnegotiated_producer(
    state: &PacketLoopState,
    handle: &RegisteredMediaHandle,
) -> bool {
    match handle {
        RegisteredMediaHandle::Producer { session_key, mid }
            if let Some(session_state) = state.users.get(session_key) =>
        {
            session_state.sdp_negotiation.initial_offer_applied()
                && !session_state.sdp_negotiation.is_awaiting_answer()
                && !session_state
                    .sdp_negotiation
                    .negotiated_producer_parameters
                    .contains_key(mid)
        }
        RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. } => false,
    }
}

/// Declares one recv-only producer media for a publishing session.
///
/// # Errors
///
/// Returns [`TransportAdapterError::TransportUnavailable`] when the session is
/// missing or str0m cannot stage the offer. Returns
/// [`TransportAdapterError::InvalidInput`] while another offer awaits its answer.
pub fn worker_add_recv_media(
    state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    policy: RecvMediaPolicy<'_>,
    session_key: &TransportSessionKey,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
) -> TransportResult<TransportMediaId> {
    let Some(session_state) = state.users.get_mut(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let mid = if session_state.sdp_negotiation.initial_offer_applied() {
        // A negotiated description must change through an offer. Before the
        // first answer there is no remote description to keep in sync.
        negotiation::stage_recv_media(
            session_state,
            media_kind,
            rtp_parameters,
            policy.profile,
            policy.video_bitrate_limits,
        )?
    } else {
        let mid = transport_mid(rtp_parameters).unwrap_or_default();
        let has_media = session_state.rtc.media(mid).is_some();
        let recv_streams = negotiation::recv_encoding_identities(rtp_parameters);
        {
            let mut api = session_state.rtc.direct_api();
            if !has_media {
                api.declare_media(mid, media_kind);
            }
            for stream in recv_streams {
                let stream_rx =
                    api.expect_stream_rx(stream.ssrc, stream.repair_ssrc, mid, stream.rid);
                stream_rx.suppress_nack(stream.repair_ssrc.is_none());
                stream_rx.request_remb(Str0mBitrate::bps(policy.max_bitrate_in.as_bps()));
                #[cfg(test)]
                {
                    session_state.max_bitrate_in = Some(policy.max_bitrate_in);
                }
            }
        }
        state.mark_session_dirty(session_key);
        mid
    };
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid,
    });
    if let Ok(mut bitrate) = bitrate_registry.lock() {
        let counter =
            bitrate.register_incoming_media(session_key, transport_media_id, Instant::now());
        state.register_incoming_bitrate_counter(transport_media_id, counter);
    }
    debug!(
        user_id = ?session_key.user_id(),
        media_worker_id = session_key.media_worker_id().as_usize(),
        ?transport_media_id,
        ?media_kind,
        "declared recv-only media on rtc user for incoming producer RTP"
    );
    Ok(transport_media_id)
}

/// Declares one send-only consumer media and installs its source route.
///
/// # Errors
///
/// Returns [`TransportAdapterError::InvalidInput`] for conflicting source
/// ownership or an offer already awaiting its answer. Returns
/// [`TransportAdapterError::TransportUnavailable`] when required session or
/// source state is missing or str0m cannot stage the offer.
pub fn worker_add_send_media(
    state: &mut PacketLoopState,
    request: AddSendMediaRequest<'_>,
) -> TransportResult<(TransportMediaId, String)> {
    let AddSendMediaRequest {
        consumer_key,
        media_kind,
        source,
        sync,
        remote_source_control,
        consumer_rtp_parameters,
        active,
    } = request;
    let src_key = source.session_key();
    let src_media = source.transport_media_id();
    // Registration precedes route creation so its control path is available.
    // Every later failure must return the transaction to the registry.
    let (route_source, remote_transaction) =
        match state.ensure_route_src_registered(consumer_key, source, remote_source_control) {
            Ok(route_source) => route_source,
            Err(error) => {
                warn!(
                    consumer_user_id = ?consumer_key.user_id(),
                    consumer_media_worker_id = consumer_key.media_worker_id().as_usize(),
                    source_user_id = ?src_key.user_id(),
                    source_media_worker_id = src_key.media_worker_id().as_usize(),
                    ?src_media,
                    error = ?error,
                    "failed to register route source for consumer media"
                );
                return Err(error);
            }
        };
    let declared = state
        .users
        .get_mut(consumer_key)
        .ok_or(TransportAdapterError::TransportUnavailable)
        .and_then(|session_state| {
            declare_consumer_stream(
                session_state,
                src_key,
                media_kind,
                sync,
                consumer_rtp_parameters,
            )
        });
    let (mid, consumer_stream, should_mark_dirty) = match declared {
        Ok(consumer_stream) => consumer_stream,
        Err(error) => {
            if let Some(transaction) = remote_transaction {
                state.routes.rollback_remote_source(transaction);
            }
            return Err(error);
        }
    };
    if should_mark_dirty {
        state.mark_session_dirty(consumer_key);
    }
    let mid_string = mid.to_string();
    let transport_media_id = state.register_consumer_route(ConsumerRouteRegistration {
        consumer_key,
        consumer_stream,
        consumer_mid: mid,
        src_media,
        consumer_rtp: consumer_rtp_parameters,
        active,
    });
    debug!(
        consumer_user_id = ?consumer_key.user_id(),
        consumer_media_worker_id = consumer_key.media_worker_id().as_usize(),
        source_user_id = ?src_key.user_id(),
        source_media_worker_id = src_key.media_worker_id().as_usize(),
        ?src_media,
        source_route_kind = route_source.label(),
        ?transport_media_id,
        ?media_kind,
        consumer_payload_type = ?codec::primary_payload_type(consumer_rtp_parameters),
        active,
        downstream_rid_policy = "single_ridless_stream",
        "declared send-only media and registered media route for consumer"
    );
    Ok((transport_media_id, mid_string))
}

fn declare_consumer_stream(
    session_state: &mut RtcSessionState,
    source: &TransportSessionKey,
    media_kind: MediaKind,
    sync: SourceSyncPolicy,
    consumer_rtp_parameters: &RouterRtpParameters,
) -> TransportResult<(Mid, ConsumerStreamHandle, bool)> {
    let mid = if session_state.sdp_negotiation.initial_offer_applied() {
        negotiation::stage_send_media(session_state, source, media_kind, sync)?
    } else {
        let mid = transport_mid(consumer_rtp_parameters).unwrap_or_default();
        declare_direct_send_media(session_state, mid, media_kind, consumer_rtp_parameters);
        mid
    };
    Ok((
        mid,
        session_state.consumer_streams.allocate(mid),
        !session_state.sdp_negotiation.initial_offer_applied(),
    ))
}

/// Declares one RID-less downstream stream for the consumer.
///
/// Simulcast selection remains in `packet_gate`. Every selected producer RID is
/// rewritten onto this one browser-visible RTP identity.
fn declare_direct_send_media(
    session_state: &mut RtcSessionState,
    mid: Mid,
    media_kind: MediaKind,
    consumer_rtp_parameters: &RouterRtpParameters,
) {
    let has_media = session_state.rtc.media(mid).is_some();
    let mut api = session_state.rtc.direct_api();
    if !has_media {
        api.declare_media(mid, media_kind);
    }
    let ssrc = api.new_ssrc();
    // SSRC-multiplexed RTX uses a different SSRC from the primary stream.
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-4
    let repair_ssrc = codec::repair_enabled(consumer_rtp_parameters).then(|| {
        loop {
            let repair_ssrc = api.new_ssrc();
            if repair_ssrc != ssrc {
                break repair_ssrc;
            }
        }
    });
    api.declare_stream_tx(ssrc, repair_ssrc, mid, None);
    debug!(
        ?mid,
        ?media_kind,
        ?ssrc,
        ?repair_ssrc,
        downstream_rid_policy = "single_ridless_stream",
        "declared browser consumer RTP stream"
    );
}

fn transport_mid(rtp_parameters: &RouterRtpParameters) -> Option<Mid> {
    rtp_parameters.mid().map(Into::into)
}
