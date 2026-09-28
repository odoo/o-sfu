use std::{
    io::{self, ErrorKind},
    net::UdpSocket,
    num::NonZeroUsize,
    time::Duration,
};

use str0m::net::{Protocol, Transmit};

use super::{RtcUdpSocket, UdpMessage, submit, validate_length};
use crate::{RtcUdpIoBackend, engine::metrics::RuntimeMetrics};

#[test]
fn udp_completion_requires_the_entire_datagram() {
    assert!(validate_length(0, 0).is_ok());
    assert!(validate_length(10, 10).is_ok());
    for (sent, expected) in [(0, 10), (9, 10), (11, 10)] {
        assert_eq!(
            validate_length(sent, expected)
                .err()
                .map(|error| error.kind()),
            Some(ErrorKind::InvalidData),
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn incomplete_plan_preserves_original_datagrams() -> io::Result<()> {
    let sender = UdpSocket::bind("127.0.0.1:0")?;
    let source = sender.local_addr()?;
    sender.set_nonblocking(true)?;
    let socket = RtcUdpSocket::from_std(sender, RtcUdpIoBackend::Tokio)?;
    let receiver = UdpSocket::bind("127.0.0.1:0")?;
    receiver.set_read_timeout(Some(Duration::from_secs(1)))?;
    let destination = receiver.local_addr()?;
    let metrics = RuntimeMetrics::default();
    let recorder = metrics.register_rtc_worker();
    for messages in [
        vec![],
        vec![UdpMessage {
            datagrams: NonZeroUsize::MIN,
        }],
        vec![UdpMessage {
            datagrams: NonZeroUsize::MAX,
        }],
    ] {
        let mut transmits: Vec<_> = [42, 43, 44]
            .into_iter()
            .map(|byte| Transmit {
                proto: Protocol::Udp,
                source,
                destination,
                contents: vec![byte].into(),
            })
            .collect();
        let capacity = transmits.capacity();
        {
            let mut remaining = transmits.drain(..);
            submit(&socket, &recorder, &messages, remaining.by_ref().take(2)).await;
            assert_eq!(remaining.len(), 1);
            let tail = [UdpMessage {
                datagrams: NonZeroUsize::MIN,
            }];
            submit(&socket, &recorder, &tail, remaining.by_ref().take(1)).await;
        }
        for byte in [42, 43, 44] {
            let mut packet = [0; 1];
            let (length, address) = receiver.recv_from(&mut packet)?;
            assert_eq!(length, packet.len());
            assert_eq!(address, source);
            assert_eq!(packet, [byte]);
        }
        assert!(transmits.is_empty());
        assert_eq!(transmits.capacity(), capacity);
    }
    Ok(())
}
