//! Complete RTC output dispatched to worker-local network adapters.
//!
//! [`RtcEgress::flush`] dispatches each turn after session rollback and mutable
//! RTC state access finish. UDP transmits retain staging order without a
//! cross-protocol completion order. A successful send means local socket
//! acceptance, not remote delivery.
//!
//! TODO(rtc-over-tcp): capture session and path generations during staging,
//! then enqueue extracted TCP output into bounded per-path FIFO writers before
//! awaiting UDP. Queue admission must not wait for socket writability or
//! another path. Reject full or closed paths individually. Tuple lookup during
//! flush cannot distinguish output for a replaced connection.

use std::{net::SocketAddr, sync::Arc};

use str0m::net::{Protocol, Transmit};
use tracing::warn;

use self::udp::UdpEgress;
use super::packet_loop::RtcUdpSocket;
use crate::engine::metrics::RtcMetricsRecorder;

mod udp;

#[cfg(test)]
#[path = "TESTS/mod.rs"]
mod tests;

/// Network output for one RTC worker.
///
/// UDP and future TCP adapters share this dispatch boundary without moving
/// session state or protocol-specific send mechanics into the worker.
pub(super) struct RtcEgress {
    udp: UdpEgress,
}

impl RtcEgress {
    pub fn new(
        socket: RtcUdpSocket,
        candidate_addr: SocketAddr,
        rtc_metrics: Arc<RtcMetricsRecorder>,
    ) -> Self {
        Self {
            udp: UdpEgress::new(socket, candidate_addr, rtc_metrics),
        }
    }

    /// Dispatches staged instructions and retains the vector capacity.
    ///
    /// Non-UDP instructions are rejected before any UDP socket wait. The
    /// remaining UDP instructions retain their relative order across windows.
    /// UDP sources must match the announced candidate, including its port.
    /// Rejected instructions and socket failures do not suppress later output.
    /// Payloads remain owned until socket I/O completes. No output queue
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
    /// The worker awaits the entire flush before applying its next input.
    /// Once draining begins, cancellation discards unsubmitted UDP output.
    pub async fn flush(&mut self, transmits: &mut Vec<Transmit>) {
        for transmit in transmits.extract_if(.., |transmit| match transmit.proto {
            Protocol::Udp => false,
            Protocol::Tcp | Protocol::SslTcp | Protocol::Tls => true,
        }) {
            report_rejection(&transmit, TransmitRejection::UnsupportedProtocol);
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
