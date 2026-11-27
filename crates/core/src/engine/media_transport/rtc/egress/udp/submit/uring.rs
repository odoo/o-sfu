//! Bounded `io_uring` fanout with at most one outstanding send per destination.
//!
//! Unlinked SQEs can execute out of order. Each contiguous destination run sends
//! serially and independent runs progress concurrently. A destination appearing
//! in a later run ends the group. Every completion settles before the next group.
//! If this future is dropped, tokio-uring retains submitted buffers while its
//! runtime remains active. Runtime teardown does not provide the same guarantee.

use std::{array, mem, net::SocketAddr, vec::Drain};

use futures_util::future;
use o_sfu_telemetry::metrics::RtcMetricsRecorder;
use str0m::net::Transmit;
use tokio::task::coop;
use tokio_uring::net::UdpSocket;

use super::super as udp;
use crate::engine::media_transport::rtc::packet_loop::io_failures;

const MAX_GROUP_RUNS: usize = 32;

// Exclude validated source/protocol so the joined future fits inline without
// allocating per flush.
struct Datagram {
    destination: SocketAddr,
    contents: Vec<u8>,
}

pub(super) async fn submit(
    socket: &UdpSocket,
    metrics: &RtcMetricsRecorder,
    transmits: &mut Drain<'_, Transmit>,
    mut remaining: usize,
) {
    while remaining > 0 {
        // Preserve cooperative yielding if sends ever complete on their first poll.
        coop::consume_budget().await;
        let (window, _) = transmits.as_slice().split_at(remaining);
        let datagrams = group_size(window);
        let mut batch = transmits.by_ref().take(datagrams);
        let mut packets = array::from_fn::<_, { udp::UDP_TX_WINDOW }, _>(|_| {
            batch.next().map(|transmit| Datagram {
                destination: transmit.destination,
                contents: Vec::from(transmit.contents),
            })
        });
        let (packets, _) = packets.split_at_mut(datagrams);
        let mut runs = packets.chunk_by_mut(|left, right| {
            left.as_ref().map(|packet| packet.destination)
                == right.as_ref().map(|packet| packet.destination)
        });
        // Session-major output needs concurrent runs. Fixed-arity joins keep
        // all 32 runs inline without pin projection.
        const { assert!(MAX_GROUP_RUNS == 2 * 4 * 4) };
        let [left, right] = array::from_fn(|_| {
            let [a, b, c, d] = array::from_fn(|_| {
                let [a, b, c, d] =
                    array::from_fn(|_| send_run(socket, metrics, runs.next().unwrap_or_default()));
                future::join4(a, b, c, d)
            });
            future::join4(a, b, c, d)
        });
        debug_assert!(runs.next().is_none());
        future::join(left, right).await;
        remaining -= datagrams;
    }
}

fn group_size(transmits: &[Transmit]) -> usize {
    let mut datagrams = 0;
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
    }
    datagrams
}

async fn send_run(
    socket: &UdpSocket,
    metrics: &RtcMetricsRecorder,
    packets: &mut [Option<Datagram>],
) {
    // A plain slice iterator keeps the 32 concurrent futures small.
    for packet in packets {
        let Some(packet) = packet else { continue };
        let expected_bytes = packet.contents.len();
        let (result, _packet) = socket
            .send_to(mem::take(&mut packet.contents), packet.destination)
            .await;
        if let Err(error) = result.and_then(|sent| super::validate_length(sent, expected_bytes)) {
            io_failures::report_udp_send_failure(metrics, packet.destination, &error);
        }
    }
}

#[cfg(test)]
#[path = "TESTS/uring.rs"]
mod tests;
