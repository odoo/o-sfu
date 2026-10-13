//! worker teardown at user level and auxiliary user bookkeeping.
//!
//! Closing a user is more than removing `RtcSessionState`: the worker also
//! has to clear demux indexes, media registries, route ownership, snapshot
//! state, bitrate tracking, and lifetime metrics. A drained worker keeps the
//! shared UDP socket idle so a new session can reuse the packet loop without
//! racing socket teardown.

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Instant,
};

use o_sfu_rfc::tcp_framing::TcpFrame;
use tokio::sync::mpsc;

use super::super::{
    egress::TcpEgress,
    state::{PacketLoopState, RtcSnapshotState, bitrate::BitrateRegistry},
};
use crate::engine::{
    media_transport::{
        TransportAdapterError, TransportResult, TransportSessionHealth, TransportSessionKey,
    },
    metrics::{self, RuntimeMetrics},
};

#[derive(Clone, Copy)]
pub(in crate::engine::media_transport::rtc) enum SessionCloseDisposition {
    OwnerClose,
    TerminalFailure,
}

/// Retires one worker RTC session according to `disposition`.
///
/// Missing `RtcSessionState` is not an error. Scheduler, demux, media, route and
/// bitrate cleanup still runs so repeated close cannot retain stale indexes.
/// Terminal worker retirement keeps disconnected health until the owner closes.
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
            SessionCloseDisposition::TerminalFailure => {
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

/// Hands the outbox of a TCP stream to the session identified by `ufrag`. Dropping the
/// session, drops the sender thus ends the stream.
///
/// # Errors
///
/// Returns [`TransportAdapterError::TransportUnavailable`] when no session uses `ufrag`.
pub(in crate::engine::media_transport::rtc) fn worker_register_tcp_outbox(
    state: &mut PacketLoopState,
    tcp_egress: &mut TcpEgress,
    peer_addr: SocketAddr,
    ufrag: &str,
    outbox_tx: mpsc::Sender<TcpFrame>,
) -> TransportResult<()> {
    let session = state
        .ufrag_registry
        .session_for(ufrag)
        .and_then(|session_key| state.users.get_mut(session_key))
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    tcp_egress.register(peer_addr, &outbox_tx);
    session.attach_tcp_outbox(peer_addr, outbox_tx);
    Ok(())
}
