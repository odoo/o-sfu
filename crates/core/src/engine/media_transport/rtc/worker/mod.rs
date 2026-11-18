//! Thread lifecycle and turn scheduling for one [`RtcWorker`].
//!
//! Each worker runs its sessions on a dedicated OS thread. Mailboxes and
//! completed UDP receives feed one loop that mutates [`PacketLoopState`](super::state::PacketLoopState):
//!
//! ```text
//! RtcWorker senders       shared UDP socket
//!        |                       |
//!        v                       v
//! input mailboxes            UdpReceiveTask
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

pub use self::{
    delay::PacketLoopDelaySnapshot,
    input::PacketLoopInputReceivers,
    lifecycle::WorkerAssignment,
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
mod benchmark_support;

#[cfg(test)]
#[path = "TESTS/session_drain_peer.rs"]
pub(super) mod session_drain_peer;

#[cfg(any(test, feature = "testing-transport"))]
pub(super) mod test_support;

use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64},
    },
    thread,
};

use o_sfu_telemetry::metrics::RtcMetricsRecorder;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use super::{
    commands::{RelayTarget, RemoteSourceControl, RtcWorkerCommand},
    state::{
        RtcSnapshotState,
        bitrate::BitrateRegistry,
        relay_registry::{RelayPacketMailbox, RelayTargetId},
    },
};
use crate::engine::media_transport::rtc::packet_loop::IngressPacket;

static NEXT_RELAY_TARGET_ID: AtomicU64 = AtomicU64::new(1);

pub(super) struct RtcWorkerHandle {
    pub(super) command_tx: mpsc::Sender<RtcWorkerCommand>,
    pub(super) relay_mailbox: RelayPacketMailbox,
    pub(super) bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    pub(super) snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    pub(super) packet_loop_delay: Arc<PacketLoopDelaySnapshot>,
    /// Set before explicit cancellation or when the thread exits unexpectedly.
    pub(super) terminal: Arc<AtomicBool>,
    /// Producer end of the worker's ingress queue.
    pub(super) packet_tx: mpsc::Sender<IngressPacket>,
}

impl fmt::Debug for RtcWorkerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("RtcWorkerHandle");
        debug.field("command_tx", &self.command_tx);
        debug.field("relay_mailbox", &self.relay_mailbox);
        debug.finish_non_exhaustive()
    }
}

/// Worker-local RTC transport API.
pub struct RtcWorker {
    relay_target_id: RelayTargetId,
    handle: RtcWorkerHandle,
    shutdown: CancellationToken,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
    join_completion: CancellationToken,
    observation_permits: Semaphore,
    rtc_metrics: Arc<RtcMetricsRecorder>,
}

impl RtcWorker {
    /// Builds the handle `consumer` stores for a producer owned by `self`.
    ///
    /// The handle lets the consumer worker register itself as a relay target
    /// and send keyframe and packet-gate updates back to the worker that owns
    /// the producer, which answers with source activity.
    pub fn remote_source_control(&self, consumer: &Self) -> RemoteSourceControl {
        RemoteSourceControl::new(
            self.handle.command_tx.clone(),
            RelayTarget {
                id: consumer.relay_target_id,
                mailbox: consumer.handle.relay_mailbox.clone(),
                control: consumer.handle.command_tx.clone(),
            },
            Arc::clone(&consumer.rtc_metrics),
        )
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
