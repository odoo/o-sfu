//! Keyframe dispatch for local and relayed sources.
//!
//! Semantic feedback and recovery intents resolve current source ownership here.
//! Consumer workers retain retry ownership for relayed requests.

use std::time::Instant;

use str0m::media::{KeyframeRequestKind, MediaKind, Mid, Rid};
use tracing::debug;

use super::super::{
    commands::{RemoteControlSendOutcome, RemoteSourceControl},
    state::{
        PacketLoopState,
        keyframe_tracker::{KeyframeRequestDecision, KeyframeRequestOrigin, SourceKeyframeRequest},
        media_registry::RegisteredMediaHandle,
        relay_registry::RelayTargetId,
        source_route::{MediaRouteDestination, RemoteSourceRegistration},
    },
};
use crate::engine::{
    media_transport::{
        TransportAdapterError, TransportConsumerRoute, TransportMediaId, TransportSessionKey,
        TransportSourceKey,
    },
    metrics::{RtcKeyframeRequestOutcome, RtcMetricsRecorder, RtcRouteControlOutcome},
};

/// Forwards relay feedback to its producer without arming another retry loop.
pub fn worker_request_remote_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    src: &TransportSourceKey,
    target_id: RelayTargetId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
) {
    let src_media = src.transport_media_id();
    // The consumer worker owns coalescing and retries. Revalidate the relay
    // target because its command can race route teardown.
    if !state
        .routes
        .source_relay_target_is_active(src_media, target_id)
    {
        metrics.record_rtc_route_control(RtcRouteControlOutcome::RouteGatedRelayDrop);
        return;
    }
    // A source-wide request can match several RID streams. Resolve the target
    // against current producer bindings rather than the consumer snapshot.
    request_local_kf(
        state,
        metrics,
        src.session_key(),
        LocalKeyframeRequest {
            source: SourceKeyframeRequest {
                src_media,
                rid,
                kind,
            },
        },
        KeyframeRequestMode::Forward,
    );
}

pub fn worker_request_resumed_video_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    source: &TransportSourceKey,
    now: Instant,
) {
    let src_media = source.transport_media_id();
    let Ok(mid) = state.ensure_local_producer_mid(source.session_key(), src_media) else {
        return;
    };
    let is_video = state
        .users
        .get(source.session_key())
        .and_then(|session| session.rtc.media(mid))
        .is_some_and(|media| matches!(media.kind(), MediaKind::Video));
    if !is_video {
        return;
    }
    request_source_recovery_kf(state, metrics, source.session_key(), src_media, now);
}

/// Requests source-wide recovery for an active producer and its route demand.
pub fn request_source_recovery_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    observed_source: &TransportSessionKey,
    src_media: TransportMediaId,
    now: Instant,
) -> bool {
    request_recovery_kf(state, metrics, observed_source, src_media, None, now)
}

/// Resolves a current source and requests decoder recovery for its selected RID.
pub fn request_recovery_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    observed_source: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Option<Rid>,
    now: Instant,
) -> bool {
    let mode = KeyframeRequestMode::for_recovery(
        now,
        state.routes.decoder_refresh_is_observable(src_media),
    );
    request_source_kf(
        state,
        metrics,
        SourceKeyframeRequest {
            src_media,
            rid,
            kind: KeyframeRequestKind::Pli,
        },
        Some(observed_source),
        mode,
    )
}

/// Tracks a selected RID until a packet-proven decoder refresh satisfies its demand.
pub(super) fn request_decoder_readiness_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    observed_source: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Rid,
    now: Instant,
) -> bool {
    request_source_kf(
        state,
        metrics,
        SourceKeyframeRequest {
            src_media,
            rid: Some(rid),
            kind: KeyframeRequestKind::Pli,
        },
        Some(observed_source),
        KeyframeRequestMode::for_recovery(now, true),
    )
}

pub fn request_consumer_feedback_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    request: SourceKeyframeRequest,
    now: Instant,
) {
    request_source_kf(
        state,
        metrics,
        request,
        None,
        KeyframeRequestMode::Track {
            now,
            origin: KeyframeRequestOrigin::ConsumerFeedback,
        },
    );
}

pub fn retry_source_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    request: SourceKeyframeRequest,
    _now: Instant,
) {
    if !state.routes.has_kf_demand(request.src_media, request.rid)
        || !request_source_kf(state, metrics, request, None, KeyframeRequestMode::Retry)
    {
        state.routes.forget_kf_req(request.src_media, request.rid);
    }
}

fn request_source_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    request: SourceKeyframeRequest,
    observed_source: Option<&TransportSessionKey>,
    mode: KeyframeRequestMode,
) -> bool {
    let SourceKeyframeRequest {
        src_media,
        rid,
        kind,
    } = request;
    if let Some(RegisteredMediaHandle::Producer { session_key, .. }) = state.media_handle(src_media)
    {
        if observed_source.is_some_and(|observed| observed != session_key) {
            return false;
        }
        let src_key = session_key.clone();
        request_local_kf(
            state,
            metrics,
            &src_key,
            LocalKeyframeRequest { source: request },
            mode,
        );
        return true;
    }
    let Some((src, src_control)) = state
        .routes
        .remote_source(src_media)
        .map(RemoteSourceRegistration::cloned_control_path)
    else {
        return false;
    };
    if observed_source.is_some_and(|observed| observed != src.session_key()) {
        return false;
    }
    request_remote_kf(state, metrics, &src, &src_control, rid, kind, mode);
    true
}

#[derive(Debug, Clone, Copy)]
enum KeyframeRequestMode {
    Track {
        now: Instant,
        origin: KeyframeRequestOrigin,
    },
    Retry,
    Forward,
}

impl KeyframeRequestMode {
    /// Chooses recovery tracking from packet observability.
    ///
    /// `DecoderTransition` is used only when ingress can prove its completion.
    /// Otherwise `RecoveryHint` bounds retries because no packet can clear the
    /// request as a proven decoder refresh.
    pub const fn for_recovery(now: Instant, observable: bool) -> Self {
        Self::Track {
            now,
            origin: if observable {
                KeyframeRequestOrigin::DecoderTransition
            } else {
                KeyframeRequestOrigin::RecoveryHint
            },
        }
    }

    fn tracked(self) -> Option<(Instant, KeyframeRequestOrigin)> {
        match self {
            Self::Track { now, origin } => Some((now, origin)),
            Self::Retry | Self::Forward => None,
        }
    }

    fn origin(self) -> Option<KeyframeRequestOrigin> {
        self.tracked().map(|(_now, origin)| origin)
    }

    fn outcome(self) -> RtcKeyframeRequestOutcome {
        match self {
            Self::Track { .. } | Self::Forward => RtcKeyframeRequestOutcome::Forwarded,
            Self::Retry => RtcKeyframeRequestOutcome::Retry,
        }
    }
}

#[derive(Clone, Copy)]
struct LocalKeyframeRequest {
    source: SourceKeyframeRequest,
}

fn request_local_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    src_key: &TransportSessionKey,
    request: LocalKeyframeRequest,
    mode: KeyframeRequestMode,
) {
    let SourceKeyframeRequest {
        src_media,
        rid,
        kind,
    } = request.source;
    let Some(mid) = local_kf_req_mid(state, src_key, src_media, rid, kind) else {
        return;
    };
    let target_rids = producer_kf_target_rids(state, src_key, src_media, mid, rid, kind);
    if target_rids.is_empty() {
        return;
    }
    // A source-wide transition can target several simulcast streams. Track each
    // RID separately so the first keyframe does not clear recovery for the rest.
    if rid.is_none() && mode.origin() == Some(KeyframeRequestOrigin::DecoderTransition) {
        for target_rid in target_rids {
            if !track_kf_req(state, metrics, src_media, target_rid, kind, mode) {
                continue;
            }
            if request_kf_from_producer(
                state,
                metrics,
                src_key,
                src_media,
                mid,
                &[target_rid],
                kind,
            ) {
                metrics.record_rtc_keyframe_request(mode.outcome());
            } else {
                state.routes.forget_kf_req(src_media, target_rid);
            }
        }
        return;
    }
    if !track_kf_req(state, metrics, src_media, rid, kind, mode) {
        return;
    }
    if request_kf_from_producer(state, metrics, src_key, src_media, mid, &target_rids, kind) {
        metrics.record_rtc_keyframe_request(mode.outcome());
    } else {
        state.routes.forget_kf_req(src_media, rid);
    }
}

fn request_remote_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    src: &TransportSourceKey,
    src_control: &RemoteSourceControl,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    mode: KeyframeRequestMode,
) {
    if !track_kf_req(state, metrics, src.transport_media_id(), rid, kind, mode) {
        return;
    }
    match src_control.request_kf(src, rid, kind) {
        RemoteControlSendOutcome::Forwarded => {
            metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
            metrics.record_rtc_keyframe_request(mode.outcome());
        }
        // `Full` does not prove source teardown. Leave any retry state already
        // rescheduled by the tracker.
        RemoteControlSendOutcome::Full => {}
        RemoteControlSendOutcome::Closed => {
            state.routes.forget_kf_req(src.transport_media_id(), rid);
        }
    }
}

fn track_kf_req(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    src_media: TransportMediaId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    mode: KeyframeRequestMode,
) -> bool {
    let Some((now, origin)) = mode.tracked() else {
        return true;
    };
    match state.routes.track_kf_req(src_media, rid, kind, origin, now) {
        KeyframeRequestDecision::Forward => true,
        KeyframeRequestDecision::Absorb => {
            metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
            metrics.record_rtc_keyframe_request(RtcKeyframeRequestOutcome::Absorbed);
            false
        }
    }
}

/// Requests a refresh frame for an active, already-declared consumer route.
///
/// # Errors
///
/// Returns [`TransportAdapterError::InvalidInput`] when a registered media handle
/// does not belong to the declared source or consumer. Returns
/// [`TransportAdapterError::TransportUnavailable`] when the source, consumer or
/// destination route no longer exists.
pub fn worker_request_consumer_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    route: &TransportConsumerRoute,
) -> Result<(), TransportAdapterError> {
    let consumer_key = route.consumer_session_key();
    let consumer_media = route.consumer_transport_media_id();
    let src_key = route.source_session_key();
    let src_media = route.source_transport_media_id();
    state.ensure_existing_route_src(consumer_key, route.source())?;
    match state.media_handle(consumer_media) {
        Some(RegisteredMediaHandle::Consumer {
            session_key,
            src_media: consumer_src_media,
            ..
        }) if session_key == consumer_key && *consumer_src_media == src_media => {}
        Some(RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. }) => {
            return Err(TransportAdapterError::InvalidInput);
        }
        None => return Err(TransportAdapterError::TransportUnavailable),
    }
    let (route_entry, source_active) = state
        .routes
        .local_route_and_activity(src_media)
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    let destination = route_entry
        .destinations
        .iter()
        .find(|destination| {
            destination.dest_session == *consumer_key
                && destination.dest_transport_media_id == consumer_media
        })
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    if !source_active || !destination.active {
        return Ok(());
    }
    let dst_rid = kf_req_rid(destination);
    let now = Instant::now();
    if !request_recovery_kf(state, metrics, src_key, src_media, dst_rid, now) {
        return Err(TransportAdapterError::TransportUnavailable);
    }
    Ok(())
}

/// Resolves the producer RID that consumer feedback must refresh.
fn kf_req_rid(dst: &MediaRouteDestination) -> Option<Rid> {
    dst.delivery.requested_keyframe_rid()
}

fn local_kf_req_mid(
    state: &PacketLoopState,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
) -> Option<Mid> {
    let mid = state.ensure_local_producer_mid(src_key, src_media).ok();
    if mid.is_none() {
        log_ignored_kf_req(
            src_key,
            src_media,
            None,
            rid,
            kind,
            "ignored keyframe request for unknown local producer",
        );
    }
    mid
}

fn producer_kf_target_rids(
    state: &mut PacketLoopState,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    mid: Mid,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
) -> Vec<Option<Rid>> {
    let Some(session_state) = state.users.get_mut(src_key) else {
        log_ignored_kf_req(
            src_key,
            src_media,
            Some(mid),
            rid,
            kind,
            "ignored keyframe request for missing source session",
        );
        return Vec::new();
    };
    let mut candidate_rids = Vec::new();
    if rid.is_none() {
        for candidate_rid in session_state
            .sdp_negotiation
            .negotiated_producer_parameters
            .get(&mid)
            .into_iter()
            .flat_map(|parameters| {
                parameters
                    .bindings()
                    .filter_map(|binding| binding.rid().map(Rid::from))
            })
        {
            push_unique_rid(&mut candidate_rids, candidate_rid);
        }
        if let Some(pending_streams) = session_state.sdp_negotiation.pending_recv_streams.get(&mid)
        {
            for candidate_rid in pending_streams.iter().filter_map(|stream| stream.rid) {
                push_unique_rid(&mut candidate_rids, candidate_rid);
            }
        }
    }
    let mut direct_api = session_state.rtc.direct_api();
    let mut target_rids = Vec::new();
    if let Some(rid) = rid {
        if direct_api.stream_rx_by_mid(mid, Some(rid)).is_some() {
            target_rids.push(Some(rid));
        }
    } else {
        for candidate_rid in candidate_rids {
            if direct_api
                .stream_rx_by_mid(mid, Some(candidate_rid))
                .is_some()
            {
                target_rids.push(Some(candidate_rid));
            }
        }
        if target_rids.is_empty() && direct_api.stream_rx_by_mid(mid, None).is_some() {
            target_rids.push(None);
        }
    }
    if target_rids.is_empty() {
        log_ignored_kf_req(
            src_key,
            src_media,
            Some(mid),
            rid,
            kind,
            "ignored keyframe request for missing producer stream",
        );
    }
    target_rids
}

fn push_unique_rid(rids: &mut Vec<Rid>, rid: Rid) {
    if !rids.contains(&rid) {
        rids.push(rid);
    }
}

fn request_kf_from_producer(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    mid: Mid,
    target_rids: &[Option<Rid>],
    kind: KeyframeRequestKind,
) -> bool {
    let Some(session_state) = state.users.get_mut(src_key) else {
        log_ignored_kf_req(
            src_key,
            src_media,
            Some(mid),
            None,
            kind,
            "ignored keyframe request for missing source session",
        );
        return false;
    };
    let mut direct_api = session_state.rtc.direct_api();
    let mut requested_rids = Vec::with_capacity(target_rids.len());
    for target_rid in target_rids {
        if let Some(stream_rx) = direct_api.stream_rx_by_mid(mid, *target_rid) {
            stream_rx.request_keyframe(kind);
            requested_rids.push(*target_rid);
        }
    }
    if requested_rids.is_empty() {
        log_ignored_kf_req(
            src_key,
            src_media,
            Some(mid),
            None,
            kind,
            "ignored keyframe request for missing producer stream",
        );
        return false;
    }
    state.mark_session_dirty(src_key);
    metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
    debug!(
        source_session_key = ?src_key,
        source_transport_media_id = ?src_media,
        ?mid,
        ?requested_rids,
        ?kind,
        "requested local producer keyframe"
    );
    true
}

fn log_ignored_kf_req(
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    mid: Option<Mid>,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    message: &'static str,
) {
    debug!(
        source_session_key = ?src_key,
        source_transport_media_id = ?src_media,
        ?mid,
        ?rid,
        ?kind,
        message
    );
}
