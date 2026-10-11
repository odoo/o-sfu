//! Bounded `io_uring` fanout with at most one outstanding send per destination.
//!
//! Unlinked SQEs can execute out of order. Each contiguous destination run sends
//! serially and independent runs progress concurrently. A destination appearing
//! in a later run ends the group. Every completion settles before the next group.
//! If this future is dropped, tokio-uring retains submitted buffers while its
//! runtime remains active. Runtime teardown does not provide the same guarantee.

use std::{array, io, mem, net::SocketAddr, vec::Drain};

use futures_util::future::{join, join4};
use str0m::net::Transmit;
use tokio::task::coop::consume_budget;
use tokio_uring::net::UdpSocket;

use super::{super::UDP_TX_WINDOW, report_udp_send_failure, validate_length};
use crate::engine::metrics::RtcMetricsRecorder;

const MAX_GROUP_RUNS: usize = 32;

// Exclude validated source/protocol and retain failures in these slots so the
// joined future fits inline without allocating per flush.
struct Datagram {
    destination: SocketAddr,
    contents: Vec<u8>,
    error: Option<io::Error>,
}

pub(super) async fn submit(
    socket: &UdpSocket,
    metrics: &RtcMetricsRecorder,
    transmits: &mut Drain<'_, Transmit>,
    mut remaining: usize,
) {
    while remaining > 0 {
        // Preserve cooperative yielding if sends ever complete on their first poll.
        consume_budget().await;
        let (window, _) = transmits.as_slice().split_at(remaining);
        let (datagrams, runs) = group_size(window);
        let mut batch = transmits.by_ref().take(datagrams);
        let mut packets = array::from_fn::<_, UDP_TX_WINDOW, _>(|_| {
            batch.next().map(|transmit| Datagram {
                destination: transmit.destination,
                contents: Vec::from(transmit.contents),
                error: None,
            })
        });
        let (packets, _) = packets.split_at_mut(datagrams);
        if runs == 1 {
            send_run(socket, packets).await;
        } else {
            let mut runs = packets.chunk_by_mut(|left, right| {
                left.as_ref().map(|packet| packet.destination)
                    == right.as_ref().map(|packet| packet.destination)
            });
            // Session-major output needs concurrent runs. Each packet keeps its
            // error so diagnostics retain staging order after the runs settle.
            // Fixed-arity joins keep all 32 runs inline without pin projection.
            const { assert!(MAX_GROUP_RUNS == 2 * 4 * 4) };
            let [left, right] = array::from_fn(|_| {
                let [a, b, c, d] = array::from_fn(|_| {
                    let [a, b, c, d] =
                        array::from_fn(|_| send_run(socket, runs.next().unwrap_or_default()));
                    join4(a, b, c, d)
                });
                join4(a, b, c, d)
            });
            debug_assert!(runs.next().is_none());
            join(left, right).await;
        }
        for packet in packets.iter_mut().flatten() {
            if let Some(error) = packet.error.take() {
                report_udp_send_failure(metrics, packet.destination, &error);
            }
        }
        remaining -= datagrams;
    }
}

fn group_size(transmits: &[Transmit]) -> (usize, usize) {
    let mut datagrams = 0;
    let mut runs = 0;
    for run in transmits
        .chunk_by(|left, right| left.destination == right.destination)
        .take(MAX_GROUP_RUNS)
    {
        let [first, ..] = run else { break };
        let (prefix, _) = transmits.split_at(datagrams);
        if prefix
            .iter()
            .any(|earlier| earlier.destination == first.destination)
        {
            break;
        }
        datagrams += run.len();
        runs += 1;
    }
    (datagrams, runs)
}

async fn send_run(socket: &UdpSocket, packets: &mut [Option<Datagram>]) {
    // A plain slice iterator keeps the 32 concurrent futures small.
    for packet in packets {
        let Some(packet) = packet else { continue };
        let expected_bytes = packet.contents.len();
        let (result, _packet) = socket
            .send_to(mem::take(&mut packet.contents), packet.destination)
            .await;
        packet.error = result
            .and_then(|sent| validate_length(sent, expected_bytes))
            .err();
    }
}

#[cfg(test)]
#[path = "TESTS/uring.rs"]
mod tests;
