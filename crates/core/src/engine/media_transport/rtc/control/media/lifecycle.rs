//! Worker-local media lifecycle for one RTC worker.
//!
//! Before a session's first answer, declarations change str0m directly. After
//! it they queue an SDP change for the next offer. Media registers when it is
//! declared in both cases, because route controls address it before that offer
//! exists. The offer then binds its MID. Offer and answer transitions remain in
//! [`negotiation`].
//!
//! Worker commands are serialized through one mutable `PacketLoopState`.

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
    Bitrate,
    engine::media_transport::{
        TransportAdapterError, TransportMediaId, TransportResult, TransportSessionKey,
        rtc::{
            codec,
            negotiation::{self, QueuedChange},
            state::{
                ConsumerRouteRegistration, PacketLoopState, RtcSessionState,
                bitrate::BitrateRegistry, media_registry::RegisteredMediaHandle,
                slots::ConsumerStreamHandle,
            },
        },
    },
};

/// Removes one transport media and its dependent routes.
///
/// Media no offer has bound leaves the negotiation queue. A negotiated MID
/// queues its removal for the next offer, which frees a consumer's MID for
/// another consumer. Missing media is already removed and succeeds.
///
/// # Errors
///
/// Returns [`TransportAdapterError::InvalidInput`] when the media belongs to a
/// different session or its session no longer exists.
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
    let mid_is_shared = handle
        .mid()
        .is_some_and(|mid| state.mid_is_shared(session_key, mid, transport_media_id));
    let session_state = state
        .users
        .get_mut(session_key)
        .ok_or(TransportAdapterError::InvalidInput)?;
    match handle.mid() {
        // Media no offer has bound only leaves the queue.
        None => session_state
            .sdp_negotiation
            .cancel_addition(transport_media_id),
        // Several handles may share one m-section. Disabling it while a sibling
        // remains would invalidate that sibling's negotiated media.
        Some(_) if mid_is_shared => {}
        Some(mid) if session_state.sdp_negotiation.initial_offer_applied() => {
            session_state
                .sdp_negotiation
                .queue
                .push(QueuedChange::Remove(mid));
        }
        Some(mid) => session_state.rtc.direct_api().remove_media(mid),
    }
    state.unregister_media_handle(bitrate_registry, transport_media_id)
}

/// Declares one recv-only producer media for a publishing session.
///
/// # Errors
///
/// Returns [`TransportAdapterError::TransportUnavailable`] when the session is
/// missing.
pub fn worker_add_recv_media(
    state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    max_bitrate_in: Bitrate,
    session_key: &TransportSessionKey,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
) -> TransportResult<TransportMediaId> {
    let Some(session_state) = state.users.get_mut(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    // A negotiated description must change through an offer. Before the
    // first answer there is no remote description to keep in sync.
    let mid = if session_state.sdp_negotiation.initial_offer_applied() {
        None
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
                stream_rx.request_remb(Str0mBitrate::bps(max_bitrate_in.as_bps()));
                #[cfg(test)]
                {
                    session_state.max_bitrate_in = Some(max_bitrate_in);
                }
            }
        }
        state.mark_session_dirty(session_key);
        Some(mid)
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
    if mid.is_none()
        && let Some(session_state) = state.users.get_mut(session_key)
    {
        session_state
            .sdp_negotiation
            .queue
            .push(QueuedChange::AddRecv {
                media: transport_media_id,
                kind: media_kind,
                rtp_parameters: rtp_parameters.clone(),
            });
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
/// A negotiated session gets the MID from its next offer.
///
/// # Errors
///
/// Returns [`TransportAdapterError::InvalidInput`] for conflicting source
/// ownership. Returns [`TransportAdapterError::TransportUnavailable`] when
/// required session or source state is missing.
pub fn worker_add_send_media(
    state: &mut PacketLoopState,
    request: AddSendMediaRequest<'_>,
) -> TransportResult<TransportMediaId> {
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
    let Some(session_state) = state.users.get_mut(consumer_key) else {
        if let Some(transaction) = remote_transaction {
            state.routes.rollback_remote_source(transaction);
        }
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let (consumer_stream, mid) = if session_state.sdp_negotiation.initial_offer_applied() {
        (ConsumerStreamHandle::default(), None)
    } else {
        let mid = transport_mid(consumer_rtp_parameters).unwrap_or_default();
        declare_direct_send_media(session_state, mid, media_kind, consumer_rtp_parameters);
        (session_state.consumer_streams.allocate(mid), Some(mid))
    };
    let transport_media_id = state.register_consumer_route(ConsumerRouteRegistration {
        consumer_key,
        consumer_stream,
        consumer_mid: mid,
        src_media,
        consumer_rtp: consumer_rtp_parameters,
        active,
    });
    if mid.is_some() {
        state.mark_session_dirty(consumer_key);
    } else if let Some(session_state) = state.users.get_mut(consumer_key) {
        session_state
            .sdp_negotiation
            .queue
            .push(QueuedChange::AddSend {
                media: transport_media_id,
                kind: media_kind,
                sync_group: negotiation::SyncGroup::of(src_key, sync),
            });
    }
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
    Ok(transport_media_id)
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
    let (ssrc, repair_ssrc) = negotiation::declare_send_stream(
        &mut api,
        mid,
        codec::repair_enabled(consumer_rtp_parameters),
    );
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
