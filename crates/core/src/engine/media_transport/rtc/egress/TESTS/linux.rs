use std::{
    error::Error,
    future::Future,
    io,
    mem::ManuallyDrop,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket},
    num::TryFromIntError,
    sync::Arc,
    task::{Context, Waker},
};

use futures_util::future;
use o_sfu_telemetry::metrics::RuntimeMetrics;
use str0m::net::Protocol;
use tokio::{
    net::UdpSocket as TokioUdpSocket,
    task::{coop::has_budget_remaining, yield_now},
    time::{self, timeout},
};

use super::{
    RECEIVE_TIMEOUT, assert_receiver_empty, flush_preserves_order_and_capacity,
    flush_rejects_bad_instructions_and_continues, receive, transmit,
};
use crate::{
    RtcUdpIoBackend,
    engine::media_transport::rtc::{
        egress::RtcEgress,
        packet_loop::{RtcIngress, RtcUdpSocket, UdpReceiveTask},
    },
};

#[tokio::test(flavor = "current_thread")]
async fn tokio_batch_egress_preserves_order_and_capacity() -> Result<(), Box<dyn Error>> {
    flush_preserves_order_and_capacity(RtcUdpIoBackend::TokioBatch).await
}

async fn flush_preserves_destinations(backend: RtcUdpIoBackend) -> Result<(), Box<dyn Error>> {
    const DESTINATION_COUNT: usize = 35;
    const DATAGRAMS_PER_DESTINATION: usize = 70;
    for bind_addr in [
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        SocketAddr::from((Ipv6Addr::LOCALHOST, 0)),
    ] {
        let sender = UdpSocket::bind(bind_addr)?;
        let source = sender.local_addr()?;
        sender.set_nonblocking(true)?;
        let socket = RtcUdpSocket::from_std(sender, backend)?;
        let metrics = RuntimeMetrics::default();
        let mut egress = RtcEgress::new(socket, source, metrics.register_rtc_worker());
        let receivers = (0..DESTINATION_COUNT)
            .map(|_| UdpSocket::bind(bind_addr))
            .collect::<io::Result<Vec<_>>>()?;
        let mut staged = Vec::new();
        for receiver in &receivers {
            receiver.set_read_timeout(Some(RECEIVE_TIMEOUT))?;
        }
        for index in 0..DATAGRAMS_PER_DESTINATION {
            for receiver in &receivers {
                staged.push(transmit(
                    Protocol::Udp,
                    source,
                    receiver.local_addr()?,
                    destination_payload(index)?,
                ));
            }
        }
        egress.flush(&mut staged).await;
        assert!(staged.is_empty());
        for receiver in &receivers {
            for index in 0..DATAGRAMS_PER_DESTINATION {
                receive(receiver, source, &destination_payload(index)?)?;
            }
            assert_receiver_empty(receiver)?;
        }
    }
    Ok(())
}

// Indices 0 and 65 supply empty datagrams, with 65 reached after multiple windows.
// Other lengths cycle through 1..=31 to exercise length validation.
fn destination_payload(index: usize) -> Result<Vec<u8>, TryFromIntError> {
    let length = if index == 0 || index == 65 {
        0
    } else {
        index % 31 + 1
    };
    Ok(vec![u8::try_from(index)?; length])
}

#[tokio::test(flavor = "current_thread")]
async fn tokio_batch_egress_preserves_destinations() -> Result<(), Box<dyn Error>> {
    flush_preserves_destinations(RtcUdpIoBackend::TokioBatch).await
}

#[tokio::test(flavor = "current_thread")]
async fn tokio_batch_egress_rejects_bad_instructions_and_continues() -> Result<(), Box<dyn Error>> {
    flush_rejects_bad_instructions_and_continues(RtcUdpIoBackend::TokioBatch).await
}

async fn flush_allows_receive_progress(backend: RtcUdpIoBackend) -> Result<(), Box<dyn Error>> {
    const RECEIVER_COUNT: usize = 128;
    const DATAGRAMS_PER_RECEIVER: usize = 64;
    let sender = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
    let source = sender.local_addr()?;
    sender.set_nonblocking(true)?;
    let socket = RtcUdpSocket::from_std(sender, backend)?;
    let send_socket = match &socket {
        RtcUdpSocket::TokioBatch(socket) => Some(Arc::clone(socket)),
        RtcUdpSocket::IoUring(_) => None,
        RtcUdpSocket::Tokio(_) => return Err(io::Error::other("expected batched socket").into()),
    };
    let metrics = RuntimeMetrics::default();
    let recorder = metrics.register_rtc_worker();
    let mut egress = RtcEgress::new(socket.clone(), source, Arc::clone(&recorder));
    let probe_sender = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
    let probe_source = probe_sender.local_addr()?;
    let receivers = (0..RECEIVER_COUNT)
        .map(|_| {
            let receiver = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
            receiver.set_read_timeout(Some(RECEIVE_TIMEOUT))?;
            Ok(receiver)
        })
        .collect::<io::Result<Vec<_>>>()?;
    let mut staged = Vec::with_capacity(RECEIVER_COUNT * DATAGRAMS_PER_RECEIVER);
    let warm_receiver = receivers
        .first()
        .ok_or_else(|| io::Error::other("missing warmup receiver"))?;
    staged.push(transmit(
        Protocol::Udp,
        source,
        warm_receiver.local_addr()?,
        b"warm".to_vec(),
    ));
    egress.flush(&mut staged).await;
    receive(warm_receiver, source, b"warm")?;
    // Each destination holds only 64 datagrams, even if the flush never yields.
    for index in 0..DATAGRAMS_PER_RECEIVER {
        for receiver in &receivers {
            staged.push(transmit(
                Protocol::Udp,
                source,
                receiver.local_addr()?,
                vec![u8::try_from(index)?],
            ));
        }
    }
    probe_sender.send_to(b"during", source)?;
    if let Some(send_socket) = send_socket {
        // Warming readiness prevents an initial I/O wait from proving fairness.
        timeout(RECEIVE_TIMEOUT, send_socket.readable()).await??;
        yield_now().await;
        send_socket.writable().await?;
        assert!(has_budget_remaining());
        // The Tokio task starts runnable with cached read readiness. No reactor
        // polling or initial send wait substitutes for cooperation in this flush.
    }
    // Every io_uring send waits for its completion, so this backend cannot
    // isolate cooperative-budget yielding. It checks that the io_uring receive
    // task delivers while sends share its socket.
    let (mut ingress, packet_tx, recycle_rx) = RtcIngress::new();
    let _receive_task =
        UdpReceiveTask::new(socket, source, source, recorder, packet_tx, recycle_rx);
    egress.flush(&mut staged).await;
    let received = ingress
        .try_recv()
        .ok_or_else(|| io::Error::other("UDP receive task did not progress during flush"))?;
    assert_eq!(received.source_addr, probe_source);
    assert_eq!(received.packet, b"during");
    assert!(staged.is_empty());
    for receiver in &receivers {
        for index in 0..DATAGRAMS_PER_RECEIVER {
            receive(receiver, source, &[u8::try_from(index)?])?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tokio_batch_flush_allows_receive_progress() -> Result<(), Box<dyn Error>> {
    flush_allows_receive_progress(RtcUdpIoBackend::TokioBatch).await
}

#[test]
fn io_uring_egress_preserves_order_and_capacity() -> Result<(), Box<dyn Error>> {
    tokio_uring::start(flush_preserves_order_and_capacity(RtcUdpIoBackend::IoUring))
}

#[test]
fn io_uring_egress_preserves_destinations() -> Result<(), Box<dyn Error>> {
    tokio_uring::start(flush_preserves_destinations(RtcUdpIoBackend::IoUring))
}

#[test]
fn io_uring_egress_rejects_bad_instructions_and_continues() -> Result<(), Box<dyn Error>> {
    tokio_uring::start(flush_rejects_bad_instructions_and_continues(
        RtcUdpIoBackend::IoUring,
    ))
}

#[test]
fn io_uring_flush_allows_receive_progress() -> Result<(), Box<dyn Error>> {
    tokio_uring::start(flush_allows_receive_progress(RtcUdpIoBackend::IoUring))
}

#[test]
fn io_uring_dropped_flush_discards_unsubmitted_suffix() -> Result<(), Box<dyn Error>> {
    // Keep this independent from production to pin the approved destination limit.
    const SUBMISSION_LIMIT: usize = 32;
    const DESTINATION_COUNT: usize = SUBMISSION_LIMIT + 3;
    // tokio-uring 0.5 frees dropped sends before cancelling them during teardown.
    // Retain this runtime until process exit, including on assertion failures.
    let runtime = ManuallyDrop::new(tokio_uring::Runtime::new(&tokio_uring::builder())?);
    runtime.block_on(async {
        let mut receivers = Vec::with_capacity(DESTINATION_COUNT);
        for _ in 0..DESTINATION_COUNT {
            receivers.push(TokioUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?);
        }
        let sender = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
        let source = sender.local_addr()?;
        sender.set_nonblocking(true)?;
        let socket = RtcUdpSocket::from_std(sender, RtcUdpIoBackend::IoUring)?;
        let metrics = RuntimeMetrics::default();
        let mut egress = RtcEgress::new(socket, source, metrics.register_rtc_worker());
        let mut staged = Vec::new();
        for contents in [b"first".as_slice(), b"second".as_slice()] {
            for receiver in &receivers {
                staged.push(transmit(
                    Protocol::Udp,
                    source,
                    receiver.local_addr()?,
                    contents.to_vec(),
                ));
            }
        }
        {
            let mut flush = std::pin::pin!(egress.flush(&mut staged));
            let mut context = Context::from_waker(Waker::noop());
            assert!(flush.as_mut().poll(&mut context).is_pending());
        }
        assert!(staged.is_empty());
        drop(egress);
        for receiver in receivers.iter().take(SUBMISSION_LIMIT) {
            let mut buffer = [0; 256];
            let (length, received_source) =
                time::timeout(RECEIVE_TIMEOUT, receiver.recv_from(&mut buffer)).await??;
            assert_eq!(received_source, source);
            assert_eq!(buffer.get(..length), Some(b"first".as_slice()));
        }
        future::join_all(
            receivers
                .iter()
                .enumerate()
                .map(|(index, receiver)| async move {
                    let mut buffer = [0; 256];
                    let received =
                        time::timeout(RECEIVE_TIMEOUT, receiver.recv_from(&mut buffer)).await;
                    assert!(
                        received.is_err(),
                        "unexpected output to destination {index}: {received:?}"
                    );
                }),
        )
        .await;
        Ok(())
    })
}
