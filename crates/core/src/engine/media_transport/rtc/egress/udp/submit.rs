//! UDP execution selected by the existing worker socket backend.
//!
//! Both executors currently complete individual sends in plan order. The
//! planner and original buffers remain independent of kernel descriptors.
//! Invalid count coverage falls back to individual sends before any submission.
//!
//! TODO(gro-gso-sendmmsg): implement sendmmsg prefix accounting and batched
//! `io_uring` completion accounting here. Keep their progress rules separate.
//! Never resubmit accepted datagrams or release kernel-referenced storage
//! before terminal completion.

use std::{io, iter::Take, vec::Drain};

use str0m::net::Transmit;
use tracing::warn;

use super::{RtcUdpSocket, plan::UdpMessage};
use crate::engine::{
    media_transport::rtc::packet_loop::io_failures::report_udp_send_failure,
    metrics::RtcMetricsRecorder,
};

pub(super) async fn submit(
    socket: &RtcUdpSocket,
    rtc_metrics: &RtcMetricsRecorder,
    messages: &[UdpMessage],
    mut transmits: Take<&mut Drain<'_, Transmit>>,
) {
    let remaining = messages
        .iter()
        .try_fold(transmits.len(), |remaining, message| {
            remaining.checked_sub(message.datagrams.get())
        });
    if remaining != Some(0) {
        warn!(
            messages = messages.len(),
            datagrams = transmits.len(),
            "invalid UDP message plan, sending individual datagrams"
        );
        for transmit in transmits {
            send_datagram(socket, rtc_metrics, transmit).await;
        }
        return;
    }
    for message in messages {
        for transmit in transmits.by_ref().take(message.datagrams.get()) {
            send_datagram(socket, rtc_metrics, transmit).await;
        }
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
