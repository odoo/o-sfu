use std::{
    error::Error,
    io,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::{Arc, Mutex},
    time::Duration,
};

use serde_json::Value;
use str0m::net::{Protocol, Transmit};
#[cfg(target_os = "linux")]
use tokio::{
    task::{coop::has_budget_remaining, yield_now},
    time::timeout,
};
use tracing::instrument::WithSubscriber;

use super::RtcEgress;
#[cfg(target_os = "linux")]
use crate::engine::media_transport::rtc::packet_loop::{RtcIngress, UdpReceiveTask};
use crate::{
    RtcUdpIoBackend,
    engine::{
        media_transport::rtc::packet_loop::RtcUdpSocket,
        metrics::{MetricName, RuntimeMetrics, test_support::RuntimeMetricsSnapshotLookup},
    },
};

const RECEIVE_TIMEOUT: Duration = Duration::from_secs(1);
const UDP_BOUNDARY_COUNTS: [usize; 7] = [31, 32, 33, 63, 64, 65, 70];
const MAX_IPV4_UDP_PAYLOAD: usize = 65_507;

struct LogWriter(Arc<Mutex<Vec<u8>>>);

impl io::Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Ok(mut log) = self.0.lock() else {
            return Err(io::Error::other("egress log poisoned"));
        };
        log.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn transmit(
    proto: Protocol,
    source: SocketAddr,
    destination: SocketAddr,
    contents: Vec<u8>,
) -> Transmit {
    Transmit {
        proto,
        source,
        destination,
        contents: contents.into(),
    }
}

fn sockets(
    backend: RtcUdpIoBackend,
) -> io::Result<(RtcEgress, UdpSocket, SocketAddr, SocketAddr, RuntimeMetrics)> {
    let sender = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
    let bound_addr = sender.local_addr()?;
    let candidate_addr = SocketAddr::from(([127, 0, 0, 2], bound_addr.port()));
    sender.set_nonblocking(true)?;
    let receiver = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
    receiver.set_read_timeout(Some(RECEIVE_TIMEOUT))?;
    let destination = receiver.local_addr()?;
    let socket = RtcUdpSocket::from_std(sender, backend)?;
    let metrics = RuntimeMetrics::default();
    let egress = RtcEgress::new(socket, candidate_addr, metrics.register_rtc_worker());
    assert_ne!(candidate_addr, bound_addr);
    Ok((egress, receiver, candidate_addr, destination, metrics))
}

fn receive(receiver: &UdpSocket, expected_source: SocketAddr, expected: &[u8]) -> io::Result<()> {
    let mut buffer = [0; 256];
    let (length, source) = receiver.recv_from(&mut buffer)?;
    assert_eq!(source, expected_source);
    assert_eq!(buffer.get(..length), Some(expected));
    Ok(())
}

async fn flush_preserves_order_and_capacity(
    backend: RtcUdpIoBackend,
) -> Result<(), Box<dyn Error>> {
    let (mut egress, receiver, candidate, destination, metrics) = sockets(backend)?;
    let bound_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, candidate.port()));
    let mut staged =
        Vec::with_capacity(UDP_BOUNDARY_COUNTS.into_iter().max().unwrap_or_default() + 1);
    let capacity = staged.capacity();
    egress.flush(&mut staged).await;
    assert_eq!(staged.capacity(), capacity);
    for count in UDP_BOUNDARY_COUNTS {
        for index in 0..count {
            staged.push(transmit(
                Protocol::Udp,
                candidate,
                destination,
                vec![u8::try_from(index)?],
            ));
        }
        staged.push(transmit(Protocol::Udp, candidate, destination, Vec::new()));
        egress.flush(&mut staged).await;
        assert!(staged.is_empty());
        assert_eq!(staged.capacity(), capacity);
        for index in 0..count {
            receive(&receiver, bound_addr, &[u8::try_from(index)?])?;
        }
        receive(&receiver, bound_addr, &[])?;
    }
    staged.push(transmit(
        Protocol::Udp,
        candidate,
        destination,
        b"next".to_vec(),
    ));
    egress.flush(&mut staged).await;
    assert_eq!(staged.capacity(), capacity);
    receive(&receiver, bound_addr, b"next")?;
    assert_eq!(
        metrics.snapshot().counter_value(
            MetricName::RtcTransportIoFailuresTotal,
            &[("direction", "send"), ("category", "other")],
        ),
        0
    );
    Ok(())
}

async fn flush_rejects_bad_instructions_and_continues(
    backend: RtcUdpIoBackend,
) -> Result<(), Box<dyn Error>> {
    let (mut egress, receiver, candidate, destination, metrics) = sockets(backend)?;
    let bound_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, candidate.port()));
    let wrong_port = SocketAddr::from(([127, 0, 0, 2], candidate.port().wrapping_add(1)));
    let wrong_ip = SocketAddr::from(([127, 0, 0, 3], candidate.port()));
    let mut staged = vec![
        transmit(
            Protocol::Udp,
            candidate,
            destination,
            vec![0; MAX_IPV4_UDP_PAYLOAD + 1],
        ),
        transmit(Protocol::Udp, candidate, destination, b"before".to_vec()),
        transmit(
            Protocol::Udp,
            candidate,
            destination,
            vec![0; MAX_IPV4_UDP_PAYLOAD + 1],
        ),
        transmit(Protocol::Tcp, candidate, destination, b"tcp".to_vec()),
        transmit(Protocol::SslTcp, candidate, destination, b"ssltcp".to_vec()),
        transmit(Protocol::Tls, candidate, destination, b"tls".to_vec()),
        transmit(Protocol::Udp, wrong_port, destination, b"port".to_vec()),
        transmit(Protocol::Udp, wrong_ip, destination, b"ip".to_vec()),
        transmit(
            Protocol::Udp,
            candidate,
            destination,
            vec![0; MAX_IPV4_UDP_PAYLOAD + 1],
        ),
        transmit(Protocol::Udp, candidate, destination, b"after".to_vec()),
    ];
    let captured = Arc::new(Mutex::new(Vec::new()));
    let writer_records = Arc::clone(&captured);
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(move || LogWriter(Arc::clone(&writer_records)))
        .finish();
    egress.flush(&mut staged).with_subscriber(subscriber).await;
    let log_bytes = captured
        .lock()
        .map_err(|_error| io::Error::other("egress log poisoned"))?
        .clone();
    let log_lines = String::from_utf8(log_bytes)?;
    let events = log_lines
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let expected_rejections = [
        (Protocol::Tcp, candidate, "unsupported_protocol"),
        (Protocol::SslTcp, candidate, "unsupported_protocol"),
        (Protocol::Tls, candidate, "unsupported_protocol"),
        (Protocol::Udp, wrong_port, "source_mismatch"),
        (Protocol::Udp, wrong_ip, "source_mismatch"),
    ];
    assert_eq!(events.len(), expected_rejections.len() + 1, "{log_lines}");
    let rejections = events
        .iter()
        .filter(|event| event.pointer("/fields/reason").is_some())
        .collect::<Vec<_>>();
    assert_eq!(rejections.len(), expected_rejections.len());
    for (event, (proto, source, reason)) in rejections.into_iter().zip(expected_rejections) {
        let proto_text = proto.to_string();
        assert_eq!(
            event.pointer("/fields/proto").and_then(Value::as_str),
            Some(proto_text.as_str())
        );
        let source_text = source.to_string();
        assert_eq!(
            event.pointer("/fields/source").and_then(Value::as_str),
            Some(source_text.as_str())
        );
        assert_eq!(
            event.pointer("/fields/reason").and_then(Value::as_str),
            Some(reason)
        );
        assert!(event.pointer("/fields/error").is_none(), "{event}");
    }
    assert_sampled_send_failures(&events, destination, &metrics)?;
    assert!(staged.is_empty());
    receive(&receiver, bound_addr, b"before")?;
    receive(&receiver, bound_addr, b"after")?;
    receiver.set_nonblocking(true)?;
    let mut buffer = [0; 256];
    match receiver.recv_from(&mut buffer) {
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
        Err(error) => return Err(error.into()),
        Ok((length, source)) => {
            return Err(io::Error::other(format!(
                "unexpected {length}-byte datagram from {source}"
            ))
            .into());
        }
    }
    Ok(())
}

fn assert_sampled_send_failures(
    events: &[Value],
    destination: SocketAddr,
    metrics: &RuntimeMetrics,
) -> io::Result<()> {
    let send_failure = events
        .iter()
        .find(|event| event.pointer("/fields/category").is_some())
        .ok_or_else(|| io::Error::other("missing send failure warning"))?;
    assert_eq!(
        send_failure
            .pointer("/fields/category")
            .and_then(Value::as_str),
        Some("SendOther")
    );
    assert_eq!(
        send_failure
            .pointer("/fields/suppressed")
            .and_then(Value::as_u64),
        Some(0)
    );
    assert_eq!(
        send_failure
            .pointer("/fields/destination")
            .and_then(Value::as_str),
        Some(destination.to_string().as_str())
    );
    assert!(
        send_failure
            .pointer("/fields/raw_os_error")
            .and_then(Value::as_i64)
            .is_some(),
        "{send_failure}"
    );
    for field in ["reason", "proto", "source"] {
        assert!(send_failure.pointer(&format!("/fields/{field}")).is_none());
    }
    assert_eq!(
        metrics.snapshot().counter_value(
            MetricName::RtcTransportIoFailuresTotal,
            &[("direction", "send"), ("category", "other")],
        ),
        3
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tokio_egress_preserves_order_and_capacity() -> Result<(), Box<dyn Error>> {
    flush_preserves_order_and_capacity(RtcUdpIoBackend::Tokio).await
}

#[tokio::test(flavor = "current_thread")]
async fn tokio_egress_rejects_bad_instructions_and_continues() -> Result<(), Box<dyn Error>> {
    flush_rejects_bad_instructions_and_continues(RtcUdpIoBackend::Tokio).await
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn tokio_batch_egress_preserves_order_and_capacity() -> Result<(), Box<dyn Error>> {
    flush_preserves_order_and_capacity(RtcUdpIoBackend::TokioBatch).await
}

#[cfg(target_os = "linux")]
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

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn tokio_batch_egress_rejects_bad_instructions_and_continues() -> Result<(), Box<dyn Error>> {
    flush_rejects_bad_instructions_and_continues(RtcUdpIoBackend::TokioBatch).await
}

#[cfg(target_os = "linux")]
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

#[cfg(target_os = "linux")]
#[test]
fn io_uring_egress_preserves_order_and_capacity() -> Result<(), Box<dyn Error>> {
    tokio_uring::start(flush_preserves_order_and_capacity(RtcUdpIoBackend::IoUring))
}

#[cfg(target_os = "linux")]
#[test]
fn io_uring_egress_rejects_bad_instructions_and_continues() -> Result<(), Box<dyn Error>> {
    tokio_uring::start(flush_rejects_bad_instructions_and_continues(
        RtcUdpIoBackend::IoUring,
    ))
}
