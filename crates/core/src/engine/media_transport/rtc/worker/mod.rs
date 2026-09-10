//! Thread lifecycle and turn scheduling for one [`RtcWorker`].
//!
//! Each worker runs its sessions on a dedicated OS thread. Mailboxes and
//! completed UDP receives feed one loop that mutates [`PacketLoopState`](super::state::PacketLoopState):
//!
//! ```text
//! RtcWorker senders       shared UDP socket
//!        |                       |
//!        v                       v
//! input mailboxes            UdpIngress
//!        |                       |
//!        +----> loop_driver <----+
//!                    |
//!             PacketLoopState
//! ```
//!
//! [`lifecycle`] defines startup and shutdown. [`loop_driver`] defines input
//! priority and packet ordering across [`super::control`], [`super::packet_loop`]
//! and [`super::recovery`]. [`session_drain`] stages session output at the turn's
//! timestamp and rolls it back before closing a session that exceeds its budget.
//!
//! Read-side snapshots may race processing or teardown and cannot authorize
//! room state.

#[cfg(feature = "internal-benchmarks")]
pub(crate) use loop_driver::{BenchmarkTurnInput, PacketLoopTurn};

#[cfg(feature = "internal-benchmarks")]
pub use self::{
    buffers::PacketLoopBuffers,
    loop_driver::route_queued_ingress_datagrams_for_benchmark,
    session_drain::{SessionDrainContext, drain_ready_sessions},
};
pub use self::{
    delay::PacketLoopDelaySnapshot,
    input::PacketLoopInputReceivers,
    loop_driver::{PacketLoopConfig, run_packet_loop},
};

pub(super) mod buffers;
pub(super) mod delay;
pub(super) mod input;
mod lifecycle;
pub(super) mod loop_driver;
pub(super) mod session_drain;

#[cfg(test)]
#[expect(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;

#[cfg(any(test, feature = "internal-benchmarks"))]
#[path = "TESTS/commands.rs"]
mod command_support;

#[cfg(any(test, feature = "testing-transport"))]
#[path = "TESTS/support.rs"]
mod test_support;

use std::{
    fmt,
    sync::{Arc, Mutex, atomic::AtomicU64},
    thread,
};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    commands::{RemoteSourceControl, RouteControlRequest, RtcWorkerCommand},
    state::{
        RtcSnapshotState,
        bitrate::BitrateRegistry,
        relay_registry::{RelayPacketMailbox, RelayTargetId},
    },
};
#[cfg(test)]
use crate::engine::media_transport::SourcePolicySignal;
#[cfg(any(test, feature = "testing-transport"))]
use crate::engine::metrics::RuntimeMetrics;
use crate::engine::{
    media_transport::{SourceActivityUpdate, TransportRelayRouteAction, TransportSourceKey},
    metrics::RtcMetricsRecorder,
};

static NEXT_RELAY_TARGET_ID: AtomicU64 = AtomicU64::new(1);

pub(super) struct RtcWorkerHandle {
    pub(super) command_tx: mpsc::Sender<RtcWorkerCommand>,
    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) debug_handle: super::test_support::RtcWorkerDebugHandle,
    pub(super) relay_mailbox: RelayPacketMailbox,
    pub(super) bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    pub(super) snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    pub(super) packet_loop_delay: Arc<PacketLoopDelaySnapshot>,
}

impl fmt::Debug for RtcWorkerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("RtcWorkerHandle");
        debug.field("command_tx", &self.command_tx);
        #[cfg(any(test, feature = "testing-transport"))]
        debug.field("debug_handle", &self.debug_handle);
        debug.field("relay_mailbox", &self.relay_mailbox);
        debug.finish_non_exhaustive()
    }
}

/// Worker-local RTC transport API.
pub struct RtcWorker {
    relay_target_id: RelayTargetId,
    handle: RtcWorkerHandle,
    shutdown: CancellationToken,
    thread: Option<thread::JoinHandle<()>>,
    #[cfg(any(test, feature = "testing-transport"))]
    pub metrics: Arc<RuntimeMetrics>,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    #[cfg(test)]
    pub(super) source_policy_signal: SourcePolicySignal,
}

impl RtcWorker {
    /// Builds the handle `consumer` stores for a producer owned by `self`.
    ///
    /// The returned handle lets the consumer worker send best-effort keyframe
    /// and packet-gate updates back to the worker that owns the producer.
    pub fn remote_source_control(&self, consumer: &Self) -> RemoteSourceControl {
        RemoteSourceControl::new(
            self.handle.command_tx.clone(),
            consumer.relay_target_id,
            Arc::clone(&consumer.rtc_metrics),
        )
    }

    /// Builds source-worker route control for `self` as the relay target.
    ///
    /// Dispatch the request to the worker that owns `source`. Install requests
    /// carry `self`'s relay mailbox and every action carries `self`'s target ID.
    pub fn relay_route_request(
        &self,
        source: TransportSourceKey,
        action: TransportRelayRouteAction,
    ) -> RouteControlRequest {
        match action {
            TransportRelayRouteAction::Install => RouteControlRequest::AddRelayTarget {
                source,
                target_id: self.relay_target_id,
                target: self.handle.relay_mailbox.clone(),
            },
            TransportRelayRouteAction::Release => RouteControlRequest::RemoveRelayTarget {
                source,
                target_id: self.relay_target_id,
            },
            TransportRelayRouteAction::SetActivity(activity) => {
                RouteControlRequest::SetRelayTargetActive {
                    source,
                    target_id: self.relay_target_id,
                    active: activity.is_active(),
                }
            }
        }
    }

    pub(crate) fn remote_source_activity_request(
        source: TransportSourceKey,
        update: SourceActivityUpdate,
    ) -> RouteControlRequest {
        RouteControlRequest::SetRemoteSourceActivity { source, update }
    }
}

impl fmt::Debug for RtcWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RtcWorker")
            .field("relay_target_id", &self.relay_target_id)
            .finish_non_exhaustive()
    }
}
