//! UDP execution selected by the existing worker socket backend.
//!
//! The planner and original buffers remain independent of kernel descriptors.
//! Invalid count coverage falls back to individual sends before any submission.
//! Linux Tokio batching retires only the accepted prefix. `ENOSYS` disables
//! batching for the socket without retiring any unaccepted datagram.
//!
//! TODO(gro-gso-sendmmsg): implement batched `io_uring` completion accounting
//! here. Keep its progress rules separate from synchronous `sendmmsg`.
//! Never resubmit accepted datagrams or release kernel-referenced storage
//! before terminal completion.

#[cfg(target_os = "linux")]
use std::sync::Arc;
use std::{io, vec::Drain};

use str0m::net::Transmit;
#[cfg(target_os = "linux")]
use tracing::field;
use tracing::warn;

use super::{RtcUdpSocket, plan::UdpMessage};
#[cfg(target_os = "linux")]
use crate::RtcUdpIoBackend;
use crate::engine::{
    media_transport::rtc::packet_loop::io_failures::report_udp_send_failure,
    metrics::RtcMetricsRecorder,
};

#[cfg(target_os = "linux")]
mod batch;

pub(super) async fn submit(
    socket: &mut RtcUdpSocket,
    rtc_metrics: &RtcMetricsRecorder,
    messages: &[UdpMessage],
    transmits: &mut Drain<'_, Transmit>,
    count: usize,
) {
    let valid_plan = messages.iter().try_fold(count, |remaining, message| {
        remaining.checked_sub(message.datagrams.get())
    }) == Some(0);
    if !valid_plan {
        warn!(
            messages = messages.len(),
            datagrams = count,
            "invalid UDP message plan, sending individual datagrams"
        );
    }
    let unsent = match socket {
        #[cfg(target_os = "linux")]
        RtcUdpSocket::TokioBatch(sender) if valid_plan => {
            let (window, _) = transmits.as_slice().split_at(count);
            let retired = batch::submit(sender, rtc_metrics, window).await;
            // Only ENOSYS leaves an unaccepted suffix. The receive task keeps the
            // same socket while all later flushes use individual sends.
            if retired < count {
                warn!(
                    backend = RtcUdpIoBackend::TokioBatch.wire_name(),
                    local_addr = sender.local_addr().ok().map(field::display),
                    "sendmmsg unavailable (ENOSYS), falling back to individual UDP sends"
                );
                *socket = RtcUdpSocket::Tokio(Arc::clone(sender));
            }
            transmits.by_ref().take(retired).for_each(drop);
            count - retired
        }
        _ => count,
    };
    for transmit in transmits.by_ref().take(unsent) {
        send_datagram(socket, rtc_metrics, transmit).await;
    }
}

async fn send_datagram(
    socket: &RtcUdpSocket,
    rtc_metrics: &RtcMetricsRecorder,
    transmit: Transmit,
) {
    let expected_bytes = transmit.contents.len();
    let packet = Vec::<u8>::from(transmit.contents);
    let result = match socket {
        RtcUdpSocket::Tokio(socket) => {
            socket
                .send_to(packet.as_slice(), transmit.destination)
                .await
        }
        #[cfg(target_os = "linux")]
        RtcUdpSocket::TokioBatch(socket) => {
            socket
                .send_to(packet.as_slice(), transmit.destination)
                .await
        }
        #[cfg(target_os = "linux")]
        RtcUdpSocket::IoUring(socket) => {
            let (result, _packet) = socket.send_to(packet, transmit.destination).await;
            result
        }
    };
    if let Err(error) = result.and_then(|sent| validate_length(sent, expected_bytes)) {
        report_udp_send_failure(rtc_metrics, transmit.destination, &error);
    }
}

/// UDP completion represents the entire datagram, including an empty one.
fn validate_length(sent: usize, expected: usize) -> io::Result<()> {
    if sent == expected {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "UDP send completed with an unexpected byte count",
        ))
    }
}

#[cfg(test)]
#[path = "TESTS/submit.rs"]
mod tests;
