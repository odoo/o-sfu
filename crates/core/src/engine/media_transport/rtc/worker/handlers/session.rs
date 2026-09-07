//! worker teardown at user level and auxiliary user bookkeeping.
//!
//! Closing a user is more than removing `RtcSessionState`: the worker also
//! has to clear demux indexes, media registries, route ownership, snapshot
//! state, bitrate tracking, and lifetime metrics. A drained worker keeps the
//! shared UDP socket idle so a new session can reuse the packet loop without
//! racing socket teardown.

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use super::{
    super::super::{
        bitrate::BitrateRegistry,
        state::{PacketLoopState, RtcSnapshotState},
    },
    media::remove_source_route,
};
use crate::engine::{
    media_transport::{TransportSessionHealth, TransportSessionKey},
    metrics::{self, RuntimeMetrics},
};

#[derive(Clone, Copy)]
pub(in crate::engine::media_transport::rtc) enum SessionCloseDisposition {
    OwnerClose,
    OutputBudgetExhausted,
}

/// Retires one worker RTC session according to `disposition`.
///
/// Missing `RtcSessionState` is not an error. Scheduler, demux, media, route and
/// bitrate cleanup still runs so repeated close cannot retain stale indexes.
/// Output-budget retirement keeps disconnected health until the owner closes.
pub(in crate::engine::media_transport::rtc) fn worker_close_session(
    state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    session_key: &TransportSessionKey,
    disposition: SessionCloseDisposition,
    metrics: &RuntimeMetrics,
) {
    state.clear_session_schedule(session_key);
    let removed_session = state.users.remove(session_key);
    state
        .remote_addr_demux
        .forget_user_remote_addrs(session_key);
    state
        .remote_addr_demux
        .forget_user_local_ice_ufrag(session_key);
    state
        .remote_addr_demux
        .forget_user_remote_candidates(session_key);
    for src_media in state.remove_session_media_handles(session_key) {
        remove_source_route(state, src_media);
    }
    state.routes.remove_dsts_for_session(session_key);
    let mid_registry = &state.mid_registry;
    state
        .routes
        .prune_unrouted_remote_srcs(|src_media| mid_registry.contains_key(src_media));
    if let Ok(mut snapshot) = snapshot_state.lock() {
        let previous = snapshot.remove_session(session_key);
        let next = match disposition {
            SessionCloseDisposition::OwnerClose => None,
            SessionCloseDisposition::OutputBudgetExhausted => {
                snapshot.set_transport_health(session_key, TransportSessionHealth::Disconnected);
                Some(TransportSessionHealth::Disconnected)
            }
        };
        metrics.record_transport_health_transition(
            previous.map(metrics::transport_health_state),
            next.map(metrics::transport_health_state),
        );
    }
    if let Ok(mut bitrate) = bitrate_registry.lock() {
        bitrate.remove_session(session_key);
    }
    if let Some(removed_session) = removed_session {
        metrics.record_transport_user_lifetime(
            Instant::now().saturating_duration_since(removed_session.started_at),
        );
        metrics.add_active_transport_users(-1);
    }
}
