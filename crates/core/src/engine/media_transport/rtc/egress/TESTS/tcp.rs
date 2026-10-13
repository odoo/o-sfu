use std::net::SocketAddr;

use str0m::net::{Protocol, Transmit};
use tokio::sync::{mpsc, mpsc::error::TryRecvError};

use super::TcpEgress;

fn local_addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn tcp_transmit(destination: SocketAddr, payload: &[u8]) -> Transmit {
    Transmit {
        proto: Protocol::Tcp,
        source: local_addr(4242),
        destination,
        contents: payload.to_vec().into(),
    }
}

#[test]
fn enqueue_delivers_to_registered_peer() {
    let mut egress = TcpEgress::default();
    let (p1_tx, mut p1_rx) = mpsc::channel(1);
    let (p2_tx, mut p2_rx) = mpsc::channel(1);
    let p1_addr = local_addr(8888);
    let p2_addr = local_addr(9999);
    egress.register(p1_addr, &p1_tx);
    egress.register(p2_addr, &p2_tx);
    egress.enqueue(tcp_transmit(p1_addr, b"for_peer_1"));
    egress.enqueue(tcp_transmit(p2_addr, b"for_peer_2"));
    assert_eq!(
        p1_rx
            .try_recv()
            .expect("p1 message was enqueued and should thus be received")
            .payload(),
        b"for_peer_1"
    );
    assert_eq!(p1_rx.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(
        p2_rx
            .try_recv()
            .expect("p2 message was enqueued and should thus be received")
            .payload(),
        b"for_peer_2"
    );
    assert_eq!(p2_rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn enqueue_drops_transmit_when_outbox_is_full() {
    let mut egress = TcpEgress::default();
    let (tx, mut rx) = mpsc::channel(1);
    let peer_addr = local_addr(8888);
    egress.register(peer_addr, &tx);
    egress.enqueue(tcp_transmit(peer_addr, b"1"));
    egress.enqueue(tcp_transmit(peer_addr, b"2"));
    assert_eq!(
        rx.try_recv()
            .expect("message was enqueued and should thus be received")
            .payload(),
        b"1"
    );
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    assert!(egress.outbox_tx_by_peer.contains_key(&peer_addr));
}

#[test]
fn enqueue_reaps_peer_on_try_send_closed_error() {
    let mut egress = TcpEgress::default();
    let (tx, mut rx) = mpsc::channel(1);
    let peer_addr = local_addr(8888);
    egress.register(peer_addr, &tx);
    assert!(egress.outbox_tx_by_peer.contains_key(&peer_addr));
    rx.close();
    egress.enqueue(tcp_transmit(peer_addr, b"hello"));
    assert!(!egress.outbox_tx_by_peer.contains_key(&peer_addr));
}

#[test]
fn register_reaps_closed_peers() {
    let mut egress = TcpEgress::default();
    let (tx, mut rx) = mpsc::channel(1);
    let peer_addr = local_addr(8888);
    egress.register(peer_addr, &tx);
    assert!(egress.outbox_tx_by_peer.contains_key(&peer_addr));
    rx.close();
    let (other_tx, _other_rx) = mpsc::channel(1);
    egress.register(local_addr(9999), &other_tx);
    assert!(!egress.outbox_tx_by_peer.contains_key(&peer_addr));
}

#[test]
fn egress_does_not_keep_the_outbox_alive() {
    let mut egress = TcpEgress::default();
    let (tx, mut rx) = mpsc::channel(1);
    egress.register(local_addr(8888), &tx);
    drop(tx);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn enqueue_reaps_peer_whose_owner_dropped_the_sender() {
    let mut egress = TcpEgress::default();
    let (tx, _rx) = mpsc::channel(1);
    let peer_addr = local_addr(8888);
    egress.register(peer_addr, &tx);
    drop(tx);
    egress.enqueue(tcp_transmit(peer_addr, b"hello"));
    assert!(!egress.outbox_tx_by_peer.contains_key(&peer_addr));
}

#[test]
fn register_reaps_peers_whose_owner_dropped_the_sender() {
    let mut egress = TcpEgress::default();
    let (tx, _rx) = mpsc::channel(1);
    let peer_addr = local_addr(8888);
    egress.register(peer_addr, &tx);
    drop(tx);
    let (other_tx, _other_rx) = mpsc::channel(1);
    egress.register(local_addr(9999), &other_tx);
    assert!(!egress.outbox_tx_by_peer.contains_key(&peer_addr));
}
