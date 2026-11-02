//! Packet-loop worker input
//!
//! This module keeps receive-side mailbox wiring

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::super::{commands::RtcWorkerCommand, packet_loop::forwarded_packet::ForwardedPacket};

/// Receive-side input bundle for one packet-loop worker.
///
/// `RtcWorker` builds this bundle during startup. The packet loop is then the
/// only receiver owner, while worker methods and relay-control handles retain
/// sender-side handles.
///
/// Keeping these receivers together makes the loop driver depend on a single
/// worker-input contract.
pub struct PacketLoopInputReceivers {
    /// Worker-authored commands that mutate the authoritative RTC worker state.
    command_rx: mpsc::Receiver<RtcWorkerCommand>,
    /// Cross-worker relay packets drained during the pump phase.
    ///
    /// Relay input stays separate from control input because it is
    /// bounded by the packet-loop turn budget and must not starve lifecycle
    /// commands or shutdown.
    relay_rx: mpsc::Receiver<ForwardedPacket>,
    /// First relay packet that woke the worker from the wait phase.
    woken_relay_packet: Option<ForwardedPacket>,
    /// Cancellation signal for the worker task.
    ///
    /// [`RtcWorker::drop`](super::RtcWorker) cancels this token
    /// before joining the packet-loop thread
    shutdown_token: CancellationToken,
}

/// Mailbox input that can wake the worker while it waits for external work.
#[expect(
    clippy::large_enum_variant,
    reason = "control input is cold and boxing would allocate every command wake"
)]
pub(super) enum PacketLoopMailboxInput {
    Control(RtcWorkerCommand),
    Relay,
}

impl PacketLoopInputReceivers {
    /// Build the receiver bundle for a newly booted packet loop.
    pub fn new(
        command_rx: mpsc::Receiver<RtcWorkerCommand>,
        relay_rx: mpsc::Receiver<ForwardedPacket>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            command_rx,
            relay_rx,
            woken_relay_packet: None,
            shutdown_token,
        }
    }

    /// Borrow relay input for bounded pump-phase draining.
    ///
    /// Callers should only use this while they are already in the packet-loop
    /// turn that drains relay packets. Control waits use [`Self::try_recv_control`]
    /// and [`Self::recv_control_or_relay`] so relay bursts cannot become
    /// lifecycle work.
    pub(super) fn relay_rx(&mut self) -> &mut mpsc::Receiver<ForwardedPacket> {
        &mut self.relay_rx
    }

    pub(super) fn shutdown_cancelled(&self) -> bool {
        self.shutdown_token.is_cancelled()
    }

    /// Transfers the relay wake into the next pump before ready-session output.
    pub(super) fn take_woken_relay_packet(&mut self) -> Option<ForwardedPacket> {
        self.woken_relay_packet.take()
    }

    /// Take one already queued command without awaiting.
    ///
    /// The loop calls this at the wait boundary so queued lifecycle work becomes
    /// one explicit input for the next packet-loop turn.
    pub(super) fn try_recv_control(&mut self) -> Option<RtcWorkerCommand> {
        self.command_rx.try_recv().ok()
    }

    /// Wait for shutdown, control input or one relay packet.
    ///
    /// Shutdown wins ready command or relay input so teardown can interrupt an
    /// idle worker. `None` means the worker should stop because shutdown fired
    /// or the owned command receiver closed. Relay receiver closure is ignored
    /// because relay handles are optional side inputs, not the worker lifetime
    /// owner.
    pub(super) async fn recv_control_or_relay(&mut self) -> Option<PacketLoopMailboxInput> {
        loop {
            tokio::select! {
                biased;
                () = self.shutdown_token.cancelled() => return None,
                maybe_command = self.command_rx.recv() => {
                    return maybe_command.map(PacketLoopMailboxInput::Control);
                }
                maybe_packet = self.relay_rx.recv() => {
                    if let Some(packet) = maybe_packet {
                        self.woken_relay_packet = Some(packet);
                        return Some(PacketLoopMailboxInput::Relay);
                    }
                }
            }
        }
    }
}
