//! route-level keyframe feedback handling
//!
//! consumer feedback arrives as MID/RID terms local to the receiving session
//! this module resolves it to the current producer source/RID target before
//! dispatching through the shared keyframe tracker

use std::{cmp::Ordering, time::Instant};

use itertools::Itertools;
use str0m::media::{KeyframeRequest as RtcKeyframeRequest, KeyframeRequestKind, Mid, Rid};

use super::{
    super::{
        commands::RemoteSourceControl,
        state::{
            PacketLoopState,
            keyframe_tracker::{KeyframeRequestOrigin, SourceKeyframeRequest, coalesce_kf_kind},
            media_registry::RegisteredMediaHandle,
        },
    },
    KeyframeRequestMode, KeyframeRequestTarget, request_kf_for_target,
};
use crate::engine::{
    media_transport::{TransportMediaId, TransportSessionKey, TransportSourceKey},
    metrics::RtcMetricsRecorder,
};

/// keyframe feedback emitted by one consumer session before producer lookup
#[derive(Debug, Clone, Copy)]
pub struct PendingKeyframeRequest {
    pub consumer_mid: Mid,
    pub consumer_rid: Option<Rid>,
    pub kind: KeyframeRequestKind,
}

impl PendingKeyframeRequest {
    pub fn new(request: RtcKeyframeRequest) -> Self {
        Self {
            consumer_mid: request.mid,
            consumer_rid: request.rid,
            kind: request.kind,
        }
    }

    #[cfg(feature = "internal-benchmarks")]
    pub const fn benchmark_request(mid: Mid, rid: Option<Rid>, kind: KeyframeRequestKind) -> Self {
        Self {
            consumer_mid: mid,
            consumer_rid: rid,
            kind,
        }
    }
}

enum ResolvedKeyframeRoute {
    Local {
        src_key: TransportSessionKey,
    },
    Remote {
        src: TransportSourceKey,
        src_control: RemoteSourceControl,
    },
}

impl ResolvedKeyframeRoute {
    fn target(&self, src_media: TransportMediaId) -> KeyframeRequestTarget<'_> {
        match self {
            Self::Local { src_key } => KeyframeRequestTarget::Local(src_key, src_media),
            Self::Remote { src, src_control } => KeyframeRequestTarget::Remote(src, src_control),
        }
    }
}

/// drains turn-local feedback into producer-scoped keyframe requests
///
/// `now` is the caller's turn clock, so the retry deadlines armed here sit on
/// the same timeline as the drain that later reads them
///
/// duplicate feedback for one `(src_media, rid)` sends the strongest request once
/// distinct rids stay separate so simulcast feedback is not widened
///
/// ```text
/// incoming feedback
///   [(M1, lo, PLI), (M1, lo, FIR), (M1, hi, PLI), (M2, lo, PLI)]
///                            |
///                            v  sort_unstable_by(src_media, rid)
///   [(M1, lo, PLI), (M1, lo, FIR), (M1, hi, PLI), (M2, lo, PLI)]
///                            |
///                            v  .coalesce(|current, next| ...)
///   +-----------------------------------------------------------+
///   | (M1, lo, PLI) + (M1, lo, FIR) --> Ok((M1, lo, FIR))       |
///   | (M1, lo, FIR) + (M1, hi, PLI) --> Err --> yield (M1, lo)  |
///   | (M1, hi, PLI) + (M2, lo, PLI) --> Err --> yield (M1, hi)  |
///   | stream drain                  --> End --> yield (M2, lo)  |
///   +-----------------------------------------------------------+
///                            |
///                            v
///             flush_coalesced_kf_req(...) (single flush sink)
/// ```
pub fn flush_pending_kf_reqs_at(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    pending_reqs: &mut Vec<(TransportSessionKey, PendingKeyframeRequest)>,
    coalesced_reqs: &mut Vec<SourceKeyframeRequest>,
    now: Instant,
) {
    coalesced_reqs.clear();
    let mut has_rid = false;
    let mut same_request: Option<SourceKeyframeRequest> = None;
    for (consumer_key, request) in pending_reqs.drain(..) {
        let Some(target) = state.active_consumer_kf_target(
            &consumer_key,
            request.consumer_mid,
            request.consumer_rid,
        ) else {
            continue;
        };
        let resolved_request = SourceKeyframeRequest {
            src_media: target.src_media,
            rid: target.rid,
            kind: request.kind,
        };
        has_rid |= resolved_request.rid.is_some();
        // most turns carry repeated feedback for one target
        // keep that path out of sorting
        if coalesced_reqs.is_empty() {
            match &mut same_request {
                Some(current)
                    if current.src_media == resolved_request.src_media
                        && current.rid == resolved_request.rid =>
                {
                    current.kind = coalesce_kf_kind(current.kind, resolved_request.kind);
                }
                Some(_) => {
                    if let Some(current) = same_request.take() {
                        coalesced_reqs.push(current);
                    }
                    coalesced_reqs.push(resolved_request);
                }
                None => {
                    same_request = Some(resolved_request);
                }
            }
        } else {
            coalesced_reqs.push(resolved_request);
        }
    }
    if coalesced_reqs.is_empty() {
        if let Some(request) = same_request {
            flush_coalesced_kf_req(state, metrics, request, now);
        }
        return;
    }
    if has_rid {
        // rid-scoped batches need the full source/RID key to avoid widening
        // simulcast feedback
        coalesced_reqs.sort_unstable_by(|left, right| {
            left.src_media
                .cmp(&right.src_media)
                .then_with(|| compare_kf_rids(left.rid, right.rid))
        });
    } else {
        coalesced_reqs.sort_unstable_by_key(|request| request.src_media);
    }
    for request in coalesced_reqs.drain(..).coalesce(|mut current, next| {
        if current.src_media == next.src_media && current.rid == next.rid {
            current.kind = coalesce_kf_kind(current.kind, next.kind);
            Ok(current)
        } else {
            Err((current, next))
        }
    }) {
        flush_coalesced_kf_req(state, metrics, request, now);
    }
}

/// drain retry deadlines after new feedback has had a chance to arm them
pub fn drain_due_kf_retries(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    retries: &mut Vec<SourceKeyframeRequest>,
    now: Instant,
) {
    state.routes.drain_due_kf_reqs(now, retries);
    for retry in retries.drain(..) {
        flush_kf_retry(state, metrics, retry);
    }
}

fn compare_kf_rids(left: Option<Rid>, right: Option<Rid>) -> Ordering {
    left.as_deref().cmp(&right.as_deref())
}

fn flush_coalesced_kf_req(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    coalesced_request: SourceKeyframeRequest,
    now: Instant,
) {
    let Some(route) = resolve_kf_route(state, coalesced_request.src_media) else {
        return;
    };
    request_kf_for_target(
        state,
        metrics,
        route.target(coalesced_request.src_media),
        coalesced_request.rid,
        coalesced_request.kind,
        KeyframeRequestMode::Track {
            now,
            origin: KeyframeRequestOrigin::ConsumerFeedback,
        },
    );
}

fn flush_kf_retry(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    retry: SourceKeyframeRequest,
) {
    let src_media = retry.src_media;
    let rid = retry.rid;
    let kind = retry.kind;
    if !state.routes.has_kf_demand(src_media, rid) {
        state.routes.forget_kf_req(src_media, rid);
        return;
    }
    let Some(route) = resolve_kf_route(state, src_media) else {
        state.routes.forget_kf_req(src_media, rid);
        return;
    };
    request_kf_for_target(
        state,
        metrics,
        route.target(src_media),
        rid,
        kind,
        KeyframeRequestMode::Retry,
    );
}

/// resolve a producer media id to its local or relayed control path
fn resolve_kf_route(
    state: &PacketLoopState,
    src_media: TransportMediaId,
) -> Option<ResolvedKeyframeRoute> {
    if let Some(RegisteredMediaHandle::Producer { session_key, .. }) = state.media_handle(src_media)
    {
        return Some(ResolvedKeyframeRoute::Local {
            src_key: session_key.clone(),
        });
    }
    state.routes.remote_source(src_media).map(|remote_source| {
        let (src, src_control) = remote_source.cloned_control_path();
        ResolvedKeyframeRoute::Remote { src, src_control }
    })
}
