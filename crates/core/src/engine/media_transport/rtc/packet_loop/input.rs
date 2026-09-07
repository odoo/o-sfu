//! Packet-loop worker input
//!
//! This module keeps receive-side mailbox wiring
//!
//! Production workers install only normal RTC commands. Test and
//! `testing-transport` builds may attach debug probes through
//! `rtc::test_support`, but the driver still treats them as control
//! input. That keeps probe-only state mutation out of the production worker
//! contract while preserving deterministic inspection tests.

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[cfg(any(test, feature = "testing-transport"))]
use super::super::test_support::{DebugProbeRequest, handle_debug_probe};
use super::super::{
    commands::RtcWorkerCommand,
    forwarded_packet::ForwardedPacket,
    state::PacketLoopState,
    worker::{WorkerCommandContext, handle_worker_command},
};

/// Receive-side input bundle for one packet-loop worker.
///
/// `RtcWorker` builds this bundle during startup. The packet loop is then the
/// only receiver owner, while worker methods,
/// relay-control handles and test-support helpers retain sender-side handles.
///
/// Keeping these receivers together makes the loop driver depend on a single
/// worker-input contract. Production construction stays limited to commands,
/// relay packets and shutdown. Test construction can extend the bundle without
/// adding probe-channel branches to the driver.
pub struct PacketLoopInputReceivers {
    /// Worker-authored commands that mutate the authoritative RTC worker state.
    ///
    /// These commands are always checked before test probes so
    /// lifecycle work keeps priority when `testing-transport` is enabled.
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
    /// [`RtcWorker::drop`](super::super::worker::RtcWorker) cancels this token
    /// before joining the packet-loop thread
    shutdown_token: CancellationToken,
    /// Test-only worker probes for deterministic inspection and state setup.
    ///
    /// The receiver is optional so production construction has no probe
    /// mailbox. When present, probe input is treated as control input because it
    /// can mutate the same worker indexes as normal commands.
    #[cfg(any(test, feature = "testing-transport"))]
    probe_rx: Option<mpsc::Receiver<DebugProbeRequest>>,
}

/// Input that may mutate authoritative worker state.
///
/// The packet loop handles all control variants through one path because both
/// normal commands and cfg-gated probes can change transport topology,
/// source ownership or demux state. Socket datagrams and timeout wakes stay
/// outside this enum because they follow different routing rules.
pub(crate) enum PacketLoopControlInput {
    /// Production worker command sent by the RTC transport API.
    Command(RtcWorkerCommand),
    /// Test-support probe used for deterministic route inspection or setup.
    #[cfg(any(test, feature = "testing-transport"))]
    Probe(DebugProbeRequest),
}

/// Mailbox input that can wake the worker while it waits for external work.
#[expect(
    clippy::large_enum_variant,
    reason = "control input is cold and boxing would allocate every command wake"
)]
pub(super) enum PacketLoopMailboxInput {
    Control(PacketLoopControlInput),
    Relay,
}

impl PacketLoopInputReceivers {
    /// Build the production receiver bundle for a newly booted packet loop.
    ///
    /// The returned bundle has no probe receiver. Test-support construction
    /// should attach one with `Self::with_probe_receiver` before passing the
    /// bundle to the loop.
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
            #[cfg(any(test, feature = "testing-transport"))]
            probe_rx: None,
        }
    }

    /// Attach the probe receiver used by deterministic RTC engine tests.
    ///
    /// This is a test-support extension point, not a second production control
    /// plane. The probe receiver is consumed into the same bundle so the loop
    /// driver does not need a separate probe wait or drain branch.
    #[cfg(any(test, feature = "testing-transport"))]
    pub fn with_probe_receiver(mut self, probe_rx: mpsc::Receiver<DebugProbeRequest>) -> Self {
        self.probe_rx = Some(probe_rx);
        self
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

    /// Take one already queued control input without awaiting.
    ///
    /// The loop calls this at the wait boundary so queued lifecycle work becomes
    /// one explicit input for the next packet-loop turn. Normal commands are
    /// checked before probe input to preserve production ordering even in test
    /// builds.
    pub(super) fn try_recv_control(&mut self) -> Option<PacketLoopControlInput> {
        if let Ok(command) = self.command_rx.try_recv() {
            return Some(PacketLoopControlInput::Command(command));
        }
        #[cfg(any(test, feature = "testing-transport"))]
        if let Some(probe_rx) = self.probe_rx.as_mut()
            && let Ok(probe) = probe_rx.try_recv()
        {
            return Some(PacketLoopControlInput::Probe(probe));
        }
        None
    }

    /// Wait for shutdown, control input or one relay packet.
    ///
    /// `None` means the worker should stop because shutdown fired or the owned
    /// command receiver closed. Relay receiver closure is ignored because relay
    /// handles are optional side inputs, not the worker lifetime owner.
    pub(super) async fn recv_control_or_relay(&mut self) -> Option<PacketLoopMailboxInput> {
        #[cfg(any(test, feature = "testing-transport"))]
        {
            if let Some(probe_rx) = self.probe_rx.as_mut() {
                return recv_mailbox_with_probe(
                    &mut self.command_rx,
                    &mut self.relay_rx,
                    &mut self.woken_relay_packet,
                    probe_rx,
                    &self.shutdown_token,
                )
                .await;
            }
        }
        recv_production_mailbox(
            &mut self.command_rx,
            &mut self.relay_rx,
            &mut self.woken_relay_packet,
            &self.shutdown_token,
        )
        .await
    }
}

impl PacketLoopControlInput {
    /// Apply this control input to authoritative worker state.
    ///
    /// The caller remains responsible for invalidating demux recovery hints
    /// after dispatch. Both variants may change ownership indexes that demux
    /// recovery relies on.
    pub(super) fn dispatch(self, state: &mut PacketLoopState, context: &WorkerCommandContext<'_>) {
        match self {
            Self::Command(command) => handle_worker_command(state, context, command),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Probe(probe) => {
                handle_debug_probe(state, context, probe);
            }
        }
    }
}

/// Shutdown wins ready command or relay input so teardown can interrupt an idle
/// worker. Command closure is terminal. Relay closure is ignored because relay
/// traffic does not define worker lifetime.
async fn recv_production_mailbox(
    command_rx: &mut mpsc::Receiver<RtcWorkerCommand>,
    relay_rx: &mut mpsc::Receiver<ForwardedPacket>,
    woken_relay_packet: &mut Option<ForwardedPacket>,
    shutdown_token: &CancellationToken,
) -> Option<PacketLoopMailboxInput> {
    loop {
        tokio::select! {
            biased;
            () = shutdown_token.cancelled() => return None,
            maybe_command = command_rx.recv() => {
                return maybe_command
                    .map(PacketLoopControlInput::Command)
                    .map(PacketLoopMailboxInput::Control);
            }
            maybe_packet = relay_rx.recv() => {
                if let Some(packet) = maybe_packet {
                    *woken_relay_packet = Some(packet);
                    return Some(PacketLoopMailboxInput::Relay);
                }
            }
        }
    }
}

/// Commands precede probes in the biased wait so testing preserves production
/// lifecycle and cleanup ordering.
#[cfg(any(test, feature = "testing-transport"))]
async fn recv_mailbox_with_probe(
    command_rx: &mut mpsc::Receiver<RtcWorkerCommand>,
    relay_rx: &mut mpsc::Receiver<ForwardedPacket>,
    woken_relay_packet: &mut Option<ForwardedPacket>,
    probe_rx: &mut mpsc::Receiver<DebugProbeRequest>,
    shutdown_token: &CancellationToken,
) -> Option<PacketLoopMailboxInput> {
    loop {
        tokio::select! {
            biased;
            () = shutdown_token.cancelled() => return None,
            maybe_command = command_rx.recv() => {
                return maybe_command
                    .map(PacketLoopControlInput::Command)
                    .map(PacketLoopMailboxInput::Control);
            }
            maybe_probe = probe_rx.recv() => {
                return maybe_probe
                    .map(PacketLoopControlInput::Probe)
                    .map(PacketLoopMailboxInput::Control);
            }
            maybe_packet = relay_rx.recv() => {
                if let Some(packet) = maybe_packet {
                    *woken_relay_packet = Some(packet);
                    return Some(PacketLoopMailboxInput::Relay);
                }
            }
        }
    }
}
