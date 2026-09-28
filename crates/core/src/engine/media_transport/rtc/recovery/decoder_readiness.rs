//! Decoder-safe packet-gate transitions.
//!
//! Room policy chooses a requested gate. Worker-local readiness state decides
//! when that gate can become effective without stranding the decoder. Until a
//! keyframe arrives, the route retains the request in `pending_gate` and
//! enforces `Block`. A RID gate may use another recently decodable RID as a
//! temporary fallback while refreshing the selected RID.
//!
//! Packet liveness and decoder readiness are distinct. Delta packets refresh
//! RID liveness but do not activate a pending gate. A RID-less keyframe may
//! activate a pending `Open` gate.

use std::{mem::take, time::Instant};

use str0m::media::Rid;
use tracing::debug;

use super::{
    super::state::{PacketLoopState, route_table::RidReadinessSelectedGateUpdate},
    keyframe::request_decoder_readiness_kf,
};
use crate::engine::{
    media_transport::{TransportMediaId, TransportSessionKey},
    metrics::RtcMetricsRecorder,
};

/// Applies one producer packet's decoder readiness to its consumer routes.
///
/// Packet observation must update liveness first. Gate transitions invalidate
/// affected consumer RTX state before dispatching any decoder refresh feedback.
/// Returns whether an effective packet gate changed.
pub fn apply_src_decoder_ready(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Option<Rid>,
    is_keyframe: bool,
    now: Instant,
) -> bool {
    let mut scratch = take(&mut state.rid_readiness_scratch);
    let route_update =
        state.update_decoder_readiness(src_media, rid, is_keyframe, now, &mut scratch);
    for stale_rid in scratch.stale.iter().copied() {
        request_live_rid_kf(state, metrics, src_key, src_media, stale_rid, now);
    }
    match route_update.selected_gate {
        RidReadinessSelectedGateUpdate::BootstrapFallback if rid.is_some() => {
            for pending_rid in scratch.pending_selected.iter().copied() {
                request_live_rid_kf(state, metrics, src_key, src_media, pending_rid, now);
            }
        }
        RidReadinessSelectedGateUpdate::Pending if let Some(rid) = rid => {
            request_live_rid_kf(state, metrics, src_key, src_media, rid, now);
        }
        RidReadinessSelectedGateUpdate::Activated
        | RidReadinessSelectedGateUpdate::BootstrapFallback
        | RidReadinessSelectedGateUpdate::Pending
        | RidReadinessSelectedGateUpdate::None => {}
    }
    scratch.clear();
    state.rid_readiness_scratch = scratch;
    route_update.changed_gate()
}

/// requests a keyframe for a live rid on either a local or remote source
///
/// local sources can be refreshed directly through their registered producer
/// remote sources are refreshed through the relay source control after the
/// observed ownership is checked against the current registration
fn request_live_rid_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Rid,
    now: Instant,
) {
    debug!(
        user_id = ?src_key.user_id(),
        media_worker_id = src_key.media_worker_id().as_usize(),
        source_transport_media_id = ?src_media,
        ?rid,
        "requesting selected RID producer keyframe"
    );
    request_decoder_readiness_kf(state, metrics, src_key, src_media, rid, now);
}
