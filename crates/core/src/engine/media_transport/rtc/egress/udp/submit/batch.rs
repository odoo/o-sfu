//! Linux datagram submission with borrowed payloads and stack-backed descriptors.

use std::{
    array,
    io::{self, IoSlice},
};

use rustix::{
    io::Errno,
    net::{MMsgHdr, SendAncillaryBuffer, SendFlags, addr::SocketAddrArg, sendmmsg},
};
use str0m::net::Transmit;
use tokio::{io::Interest, net::UdpSocket, task::coop::consume_budget};

use super::{report_udp_send_failure, validate_length};
use crate::engine::metrics::RtcMetricsRecorder;

const BATCH_SIZE: usize = 32;

struct Completion {
    accepted: usize,
    lengths: [usize; BATCH_SIZE],
}

#[derive(Debug, PartialEq, Eq)]
enum Progress {
    Retired(usize),
    Retry,
    WaitWritable,
    Unsupported,
}

/// Returns the retired prefix, leaving a suffix only when `sendmmsg` is absent.
pub(super) async fn submit(
    socket: &UdpSocket,
    metrics: &RtcMetricsRecorder,
    transmits: &[Transmit],
) -> usize {
    let mut retired = 0;
    while retired < transmits.len() {
        let (_, remaining) = transmits.split_at(retired);
        let (batch, _) = remaining.split_at(remaining.len().min(BATCH_SIZE));
        // try_io does not consume Tokio's cooperative budget. Charge each
        // attempt so repeated EINTR yields too. Budget units cover batches,
        // allowing up to BATCH_SIZE times more datagrams between budget yields
        // than individual sends.
        consume_budget().await;
        match complete(metrics, batch, send(socket, batch)) {
            Progress::Retired(count) => retired += count,
            Progress::Retry => {}
            Progress::WaitWritable => {
                if let Err(error) = socket.writable().await {
                    if let Some(first) = batch.first() {
                        report_udp_send_failure(metrics, first.destination, &error);
                    }
                    retired += 1;
                }
            }
            Progress::Unsupported => break,
        }
    }
    retired
}

fn send(socket: &UdpSocket, transmits: &[Transmit]) -> io::Result<Completion> {
    let Some(first) = transmits.first() else {
        return Ok(Completion {
            accepted: 0,
            lengths: [0; BATCH_SIZE],
        });
    };
    // Descriptors borrow these arrays only during the syscall. Payloads stay
    // in the staging drain and no kernel-referenced storage crosses an await.
    let mut storage = array::from_fn::<_, BATCH_SIZE, _>(|index| {
        let transmit = transmits.get(index).unwrap_or(first);
        (
            transmit.destination.as_any(),
            [IoSlice::new(&transmit.contents)],
            SendAncillaryBuffer::default(),
        )
    });
    let mut headers = storage
        .each_mut()
        .map(|(address, payload, control)| MMsgHdr::new_with_addr(address, payload, control));
    let (headers, _) = headers.split_at_mut(transmits.len());
    let accepted = socket.try_io(Interest::WRITABLE, || {
        sendmmsg(socket, headers, SendFlags::DONTWAIT).map_err(io::Error::from)
    })?;
    let mut completion = Completion {
        accepted,
        lengths: [0; BATCH_SIZE],
    };
    for (length, header) in completion.lengths.iter_mut().zip(headers).take(accepted) {
        *length = header.bytes_sent();
    }
    Ok(completion)
}

fn complete(
    metrics: &RtcMetricsRecorder,
    transmits: &[Transmit],
    result: io::Result<Completion>,
) -> Progress {
    let error = match result {
        Ok(completion) if completion.accepted > 0 => {
            // sendmmsg loses an error after partial acceptance. Retire every
            // accepted message, including a bad length, before retrying the rest.
            for (transmit, sent) in transmits
                .iter()
                .zip(completion.lengths)
                .take(completion.accepted)
            {
                if let Err(error) = validate_length(sent, transmit.contents.len()) {
                    report_udp_send_failure(metrics, transmit.destination, &error);
                }
            }
            return Progress::Retired(completion.accepted);
        }
        Ok(_) => io::Error::new(
            io::ErrorKind::InvalidData,
            "UDP batch send completed without accepting a datagram",
        ),
        Err(error) => error,
    };
    match error.kind() {
        io::ErrorKind::WouldBlock => return Progress::WaitWritable,
        io::ErrorKind::Interrupted => return Progress::Retry,
        _ => {}
    }
    if error.raw_os_error() == Some(Errno::NOSYS.raw_os_error()) {
        return Progress::Unsupported;
    }
    if let Some(first) = transmits.first() {
        report_udp_send_failure(metrics, first.destination, &error);
    }
    Progress::Retired(1)
}

#[cfg(test)]
#[path = "TESTS/batch.rs"]
mod tests;
