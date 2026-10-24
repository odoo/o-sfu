#[cfg(target_os = "linux")]
use std::num::TryFromIntError;
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
    let failed_receivers = [
        UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?,
        UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?,
    ];
    let [failed_receiver_a, failed_receiver_b] = &failed_receivers;
    let failed_destination_a = failed_receiver_a.local_addr()?;
    let failed_destination_b = failed_receiver_b.local_addr()?;
    let udp = |destination, contents| transmit(Protocol::Udp, candidate, destination, contents);
    let mut staged = vec![
        // A later run may fail first. Diagnostics must still report the first
        // failure in staging order and an error must not suppress its run's tail.
        udp(failed_destination_a, b"leading".to_vec()),
        udp(failed_destination_a, vec![0; MAX_IPV4_UDP_PAYLOAD + 1]),
        udp(failed_destination_a, b"trailing".to_vec()),
        udp(destination, b"before".to_vec()),
        udp(failed_destination_b, vec![0; MAX_IPV4_UDP_PAYLOAD + 1]),
        transmit(Protocol::Tcp, candidate, destination, b"tcp".to_vec()),
        transmit(Protocol::SslTcp, candidate, destination, b"ssltcp".to_vec()),
        transmit(Protocol::Tls, candidate, destination, b"tls".to_vec()),
        transmit(Protocol::Udp, wrong_port, destination, b"port".to_vec()),
        transmit(Protocol::Udp, wrong_ip, destination, b"ip".to_vec()),
        udp(destination, vec![0; MAX_IPV4_UDP_PAYLOAD + 1]),
        udp(destination, b"after".to_vec()),
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
    assert_sampled_send_failures(&events, failed_destination_a, &metrics)?;
    assert!(staged.is_empty());
    receive(&receiver, bound_addr, b"before")?;
    receive(&receiver, bound_addr, b"after")?;
    assert_receiver_empty(&receiver)?;
    for receiver in &failed_receivers {
        receiver.set_read_timeout(Some(RECEIVE_TIMEOUT))?;
    }
    receive(failed_receiver_a, bound_addr, b"leading")?;
    receive(failed_receiver_a, bound_addr, b"trailing")?;
    for receiver in &failed_receivers {
        assert_receiver_empty(receiver)?;
    }
    Ok(())
}

fn assert_receiver_empty(receiver: &UdpSocket) -> io::Result<()> {
    receiver.set_nonblocking(true)?;
    let mut buffer = [0; 256];
    match receiver.recv_from(&mut buffer) {
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(()),
        Err(error) => Err(error),
        Ok((length, source)) => Err(io::Error::other(format!(
            "unexpected {length}-byte datagram from {source}"
        ))),
    }
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
async fn flush_preserves_destinations(backend: RtcUdpIoBackend) -> Result<(), Box<dyn Error>> {
    use std::net::Ipv6Addr;
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

#[cfg(target_os = "linux")]
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

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn tokio_batch_egress_preserves_destinations() -> Result<(), Box<dyn Error>> {
    flush_preserves_destinations(RtcUdpIoBackend::TokioBatch).await
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn tokio_batch_egress_rejects_bad_instructions_and_continues() -> Result<(), Box<dyn Error>> {
    flush_rejects_bad_instructions_and_continues(RtcUdpIoBackend::TokioBatch).await
}

#[cfg(target_os = "linux")]
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
    // io_uring completion waits also let the receive task run, so that backend
    // proves receive progress without isolating cooperative-budget yielding.
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
#[tokio::test(flavor = "current_thread")]
async fn tokio_batch_flush_allows_receive_progress() -> Result<(), Box<dyn Error>> {
    flush_allows_receive_progress(RtcUdpIoBackend::TokioBatch).await
}

#[cfg(target_os = "linux")]
#[test]
fn io_uring_egress_preserves_order_and_capacity() -> Result<(), Box<dyn Error>> {
    tokio_uring::start(flush_preserves_order_and_capacity(RtcUdpIoBackend::IoUring))
}

#[cfg(target_os = "linux")]
#[test]
fn io_uring_egress_preserves_destinations() -> Result<(), Box<dyn Error>> {
    tokio_uring::start(flush_preserves_destinations(RtcUdpIoBackend::IoUring))
}

#[cfg(target_os = "linux")]
#[test]
fn io_uring_egress_preserves_session_major_fanout() -> Result<(), Box<dyn Error>> {
    tokio_uring::start(async {
        const DESTINATION_COUNT: usize = 10;
        const DATAGRAMS_PER_DESTINATION: usize = 4;
        let (mut egress, receiver, candidate, _destination, _metrics) =
            sockets(RtcUdpIoBackend::IoUring)?;
        let source = SocketAddr::from((Ipv4Addr::LOCALHOST, candidate.port()));
        let mut receivers = vec![receiver];
        for _ in 1..DESTINATION_COUNT {
            receivers.push(UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?);
        }
        let mut staged = Vec::with_capacity(DESTINATION_COUNT * DATAGRAMS_PER_DESTINATION);
        // Session draining appends each session's ready output contiguously.
        for receiver in &receivers {
            receiver.set_read_timeout(Some(RECEIVE_TIMEOUT))?;
            for index in 0..DATAGRAMS_PER_DESTINATION {
                staged.push(transmit(
                    Protocol::Udp,
                    candidate,
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
        Ok(())
    })
}

#[cfg(target_os = "linux")]
#[test]
fn io_uring_egress_preserves_repeated_destinations() -> Result<(), Box<dyn Error>> {
    tokio_uring::start(async {
        const REPEAT_COUNT: usize = 70;
        let (mut egress, receiver_a, candidate, destination_a, _metrics) =
            sockets(RtcUdpIoBackend::IoUring)?;
        let source = SocketAddr::from((Ipv4Addr::LOCALHOST, candidate.port()));
        let receiver_b = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
        let receiver_c = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
        receiver_b.set_read_timeout(Some(RECEIVE_TIMEOUT))?;
        receiver_c.set_read_timeout(Some(RECEIVE_TIMEOUT))?;
        let destinations = [
            destination_a,
            receiver_b.local_addr()?,
            destination_a,
            receiver_c.local_addr()?,
            receiver_b.local_addr()?,
        ];
        let mut staged = Vec::with_capacity(REPEAT_COUNT * destinations.len());
        for round in 0..REPEAT_COUNT {
            for (offset, destination) in destinations.into_iter().enumerate() {
                let index = round * destinations.len() + offset;
                staged.push(transmit(
                    Protocol::Udp,
                    candidate,
                    destination,
                    index.to_be_bytes().to_vec(),
                ));
            }
        }
        egress.flush(&mut staged).await;
        assert!(staged.is_empty());
        let receivers: [(&UdpSocket, &[usize]); 3] = [
            (&receiver_a, &[0, 2]),
            (&receiver_b, &[1, 4]),
            (&receiver_c, &[3]),
        ];
        for (receiver, offsets) in receivers {
            for round in 0..REPEAT_COUNT {
                for offset in offsets {
                    let index = round * destinations.len() + offset;
                    receive(receiver, source, &index.to_be_bytes())?;
                }
            }
            assert_receiver_empty(receiver)?;
        }
        Ok(())
    })
}

#[cfg(target_os = "linux")]
#[test]
fn io_uring_egress_rejects_bad_instructions_and_continues() -> Result<(), Box<dyn Error>> {
    tokio_uring::start(flush_rejects_bad_instructions_and_continues(
        RtcUdpIoBackend::IoUring,
    ))
}

#[cfg(target_os = "linux")]
#[test]
fn io_uring_flush_allows_receive_progress() -> Result<(), Box<dyn Error>> {
    tokio_uring::start(flush_allows_receive_progress(RtcUdpIoBackend::IoUring))
}

#[cfg(target_os = "linux")]
#[test]
fn io_uring_dropped_flush_discards_unsubmitted_suffix() -> Result<(), Box<dyn Error>> {
    use std::{
        future::Future,
        task::{Context, Waker},
    };
    // Keep this independent from production to pin the approved destination limit.
    const SUBMISSION_LIMIT: usize = 32;
    const DESTINATION_COUNT: usize = SUBMISSION_LIMIT + 3;
    let receivers = (0..DESTINATION_COUNT)
        .map(|_| UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)))
        .collect::<io::Result<Vec<_>>>()?;
    let source = tokio_uring::start(async {
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
        // Packet assertions cover suffix discard. Buffer lifetime during runtime
        // teardown requires separate verification.
        drop(egress);
        Ok::<_, Box<dyn Error>>(source)
    })?;
    for (index, receiver) in receivers.iter().enumerate() {
        receiver.set_nonblocking(true)?;
        let mut buffer = [0; 256];
        match receiver.recv_from(&mut buffer) {
            Ok((length, received_source)) => {
                assert!(
                    index < SUBMISSION_LIMIT,
                    "destination {index} belonged to the unsent suffix"
                );
                assert_eq!(received_source, source);
                assert_eq!(buffer.get(..length), Some(b"first".as_slice()));
                assert_receiver_empty(receiver)?;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
