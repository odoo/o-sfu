use std::net::{Ipv4Addr, SocketAddr};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{INGRESS_QUEUE_CAPACITY, RECEIVE_BUFFER_POOL_CAPACITY, UdpDatagram, UdpIngress};

/// Builds the production completed-datagram queue without a socket receive task.
pub(in crate::engine::media_transport::rtc) fn completed_datagram_channel()
-> (mpsc::Sender<UdpDatagram>, UdpIngress) {
    let (tx, rx) = mpsc::channel(INGRESS_QUEUE_CAPACITY);
    let (recycle_tx, _recycle_rx) = mpsc::channel(RECEIVE_BUFFER_POOL_CAPACITY);
    (
        tx,
        UdpIngress {
            rx,
            recycle_tx,
            shutdown: CancellationToken::new(),
            wake_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        },
    )
}
