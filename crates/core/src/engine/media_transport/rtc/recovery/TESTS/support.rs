use std::time::Instant;

use str0m::media::Rid;
use tracing::debug;

use super::apply_src_decoder_ready;
use crate::engine::{
    media_transport::{TransportMediaId, TransportSessionKey, rtc::state::PacketLoopState},
    metrics::RtcMetricsRecorder,
};

/// updates packet-path readiness for one incoming producer rid
///
/// this test helper mirrors the packet-loop sequence by recording liveness
/// before applying readiness work
///
/// returns `true` when an effective packet gate changed
pub fn observe_src_rid_ready(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Rid,
    is_keyframe: bool,
    now: Instant,
) -> bool {
    let first_observed = state
        .routes
        .observe_producer_packet(src_media, Some(rid), false, now);
    if first_observed {
        debug!(
            user_id = ?src_key.user_id(),
            media_worker_id = src_key.media_worker_id().as_usize(),
            source_transport_media_id = ?src_media,
            ?rid,
            is_keyframe,
            "observed first live RTP for producer RID"
        );
    }
    // Readiness uses the freshness set to detect stale RIDs. Include the packet
    // that triggered this transition before querying that set.
    apply_src_decoder_ready(
        state,
        metrics,
        src_key,
        src_media,
        Some(rid),
        is_keyframe,
        now,
    )
}
