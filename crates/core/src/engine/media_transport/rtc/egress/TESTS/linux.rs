use std::{
    error::Error,
    io,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::Arc,
};

use o_sfu_telemetry::metrics::RuntimeMetrics;
use str0m::net::Protocol;
use tokio::{
    task::{coop::has_budget_remaining, yield_now},
    time::timeout,
};

use super::{
    RECEIVE_TIMEOUT, flush_preserves_order_and_capacity,
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

#[tokio::test(flavor = "current_thread")]
async fn tokio_batch_egress_preserves_destinations() -> Result<(), Box<dyn Error>> {
    use std::net::Ipv6Addr;
    for bind_addr in [
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        SocketAddr::from((Ipv6Addr::LOCALHOST, 0)),
    ] {
        let sender = UdpSocket::bind(bind_addr)?;
        let source = sender.local_addr()?;
        sender.set_nonblocking(true)?;
        let socket = RtcUdpSocket::from_std(sender, RtcUdpIoBackend::TokioBatch)?;
        let metrics = RuntimeMetrics::default();
        let mut egress = RtcEgress::new(socket, source, metrics.register_rtc_worker());
        let receivers = [UdpSocket::bind(bind_addr)?, UdpSocket::bind(bind_addr)?];
        let mut staged = Vec::new();
        for receiver in &receivers {
            receiver.set_read_timeout(Some(RECEIVE_TIMEOUT))?;
        }
        for index in 0..70 {
            for receiver in &receivers {
                staged.push(transmit(
                    Protocol::Udp,
                    source,
                    receiver.local_addr()?,
                    vec![u8::try_from(index)?],
                ));
            }
        }
        egress.flush(&mut staged).await;
        assert!(staged.is_empty());
        for receiver in &receivers {
            for index in 0..70 {
                receive(receiver, source, &[u8::try_from(index)?])?;
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tokio_batch_egress_rejects_bad_instructions_and_continues() -> Result<(), Box<dyn Error>> {
    flush_rejects_bad_instructions_and_continues(RtcUdpIoBackend::TokioBatch).await
}

#[tokio::test(flavor = "current_thread")]
async fn tokio_batch_flush_allows_receive_progress() -> Result<(), Box<dyn Error>> {
    const RECEIVER_COUNT: usize = 128;
    const DATAGRAMS_PER_RECEIVER: usize = 64;
    let sender = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
    let source = sender.local_addr()?;
    sender.set_nonblocking(true)?;
    let socket = RtcUdpSocket::from_std(sender, RtcUdpIoBackend::TokioBatch)?;
    let send_socket = match &socket {
        RtcUdpSocket::TokioBatch(socket) => Arc::clone(socket),
        _ => return Err(io::Error::other("expected tokio_batch socket").into()),
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
    // Warming send readiness prevents an initial I/O wait from proving fairness.
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
    timeout(RECEIVE_TIMEOUT, send_socket.readable()).await??;
    yield_now().await;
    send_socket.writable().await?;
    assert!(has_budget_remaining());
    // The task starts runnable with cached read readiness. No reactor polling
    // or initial send wait can substitute for cooperation during this flush.
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

#[test]
fn io_uring_egress_preserves_order_and_capacity() -> Result<(), Box<dyn Error>> {
    tokio_uring::start(flush_preserves_order_and_capacity(RtcUdpIoBackend::IoUring))
}

#[test]
fn io_uring_egress_rejects_bad_instructions_and_continues() -> Result<(), Box<dyn Error>> {
    tokio_uring::start(flush_rejects_bad_instructions_and_continues(
        RtcUdpIoBackend::IoUring,
    ))
}
