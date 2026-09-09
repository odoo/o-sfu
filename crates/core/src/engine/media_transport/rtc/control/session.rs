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

use super::super::state::{PacketLoopState, RtcSnapshotState, bitrate::BitrateRegistry};
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
    let removed_session = state.remove_session(session_key);
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
