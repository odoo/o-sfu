//! Complete RTC output dispatched to worker-local network adapters.
//!
//! [`RtcEgress::flush`] dispatches each turn after session rollback and mutable
//! RTC state access finish. UDP transmits retain staging order per destination
//! without a cross-protocol completion order. A successful send means local socket
//! acceptance, not remote delivery.
//!
//! Tuple lookup during flush cannot distinguish output for a replaced connection.

use std::{net::SocketAddr, sync::Arc};

use str0m::net::{Protocol, Transmit};
use tracing::warn;

pub(crate) use self::tcp::TcpEgress;
use self::udp::UdpEgress;
use super::packet_loop::RtcUdpSocket;
use crate::engine::metrics::RtcMetricsRecorder;

mod tcp;
mod udp;

#[cfg(test)]
#[path = "TESTS/mod.rs"]
mod tests;

/// Network output for one RTC worker.
///
/// UDP and TCP adapters share this dispatch boundary without moving session
/// state or protocol-specific send mechanics into the worker.
pub(super) struct RtcEgress {
    udp: UdpEgress,
    pub(super) tcp: TcpEgress,
}

impl RtcEgress {
    pub fn new(
        socket: RtcUdpSocket,
        candidate_addr: SocketAddr,
        rtc_metrics: Arc<RtcMetricsRecorder>,
    ) -> Self {
        Self {
            udp: UdpEgress::new(socket, candidate_addr, rtc_metrics),
            tcp: TcpEgress::default(),
        }
    }

    /// Dispatches staged instructions and retains the vector capacity.
    ///
    /// TCP instructions are handed to their connection's outbox and other non-UDP
    /// instructions are rejected, both before any UDP socket wait. The
    /// remaining UDP instructions retain their relative order per destination
    /// across windows. `io_uring` can send independent destinations concurrently.
    /// UDP sources must match the announced candidate, including its port.
    /// Rejected instructions and socket failures do not suppress later output.
    /// UDP payloads remain owned until socket I/O completes. No UDP output queue
    /// survives a completed flush.
    ///
    /// # Failure reporting
    ///
    /// Rejections log `reason = "unsupported_protocol"` or
    /// `reason = "source_mismatch"`. Send failures increment worker transport
    /// I/O counters and emit sampled warnings with `category`, `suppressed`
    /// and `destination`. Warnings include `raw_os_error` when available.
    /// Unexpected UDP send lengths use the same reporting without retrying
    /// potentially accepted bytes.
    ///
    /// The worker awaits the entire flush before applying its next input or
    /// handling shutdown. Once draining begins, cancellation discards unsubmitted
    /// UDP output. Dropping an unfinished flush is terminal for this adapter.
    /// `tokio-uring` retains outstanding send buffers while its runtime runs.
    /// This retention does not extend to runtime teardown.
    pub async fn flush(&mut self, transmits: &mut Vec<Transmit>) {
        for transmit in transmits.extract_if(.., |transmit| transmit.proto != Protocol::Udp) {
            match transmit.proto {
                Protocol::Tcp => self.tcp.enqueue(transmit),
                _ => report_rejection(&transmit, TransmitRejection::UnsupportedProtocol),
            }
        }
        self.udp.flush(transmits.drain(..)).await;
    }
}

#[derive(Clone, Copy)]
enum TransmitRejection {
    UnsupportedProtocol,
    SourceMismatch,
}

fn report_rejection(transmit: &Transmit, rejection: TransmitRejection) {
    let reason = match rejection {
        TransmitRejection::UnsupportedProtocol => "unsupported_protocol",
        TransmitRejection::SourceMismatch => "source_mismatch",
    };
    warn!(
        reason,
        proto = %transmit.proto,
        source = %transmit.source,
        destination = %transmit.destination,
        "rejected packet-loop transport instruction"
    );
}
