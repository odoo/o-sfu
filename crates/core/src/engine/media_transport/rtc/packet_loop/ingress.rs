//! Worker-local ingress queue.
//!
//! One [`RtcIngress`] exists per worker. The packet loop owns the single receiver and
//! every transport holds a sender clone, so UDP and TCP deliver into the same queue
//! without the loop knowing which one produced a packet.
//!
//! [`RtcIngress::new`] hands the sending side out instead of keeping it, so the
//! queue closes once every producer is gone.

use std::{net::SocketAddr, time::Instant};

use str0m::net::Protocol;
use tokio::sync::mpsc;

use super::super::worker::buffers::RECEIVE_BUFFER_LEN;

const INGRESS_QUEUE_CAPACITY: usize = 32;
const RECEIVE_BUFFER_POOL_CAPACITY: usize = 32;

/// Completed receive annotated with the local address expected by str0m.
///
/// The backend receive APIs supply only the peer address. `candidate_addr`
/// preserves the local candidate identity needed by [`str0m::Input::Receive`].
pub(crate) struct IngressPacket {
    pub(in super::super) source_addr: SocketAddr,
    pub(in super::super) candidate_addr: SocketAddr,
    /// Socket-completion time captured before ingress-queue backpressure.
    ///
    /// str0m uses this clock for jitter and bandwidth timing.
    pub(in super::super) received_at: Instant,
    pub(in super::super) packet: Vec<u8>,
    /// Protocol of the socket the packet was received on, which str0m matches against
    /// its local candidates.
    pub(in super::super) protocol: Protocol,
}

/// Consumer side of one worker's ingress queue, read by the packet loop.
pub struct RtcIngress {
    packet_rx: mpsc::Receiver<IngressPacket>,
    recycle_tx: mpsc::Sender<Vec<u8>>,
}

impl RtcIngress {
    /// Builds the queue and returns its three ends.
    ///
    /// - the consumer, owned by the packet loop;
    /// - the packet sender, cloned once per producer;
    /// - the recycled-buffer receiver, owned by the one producer that refills
    ///   receive storage.
    pub(in super::super) fn new() -> (Self, mpsc::Sender<IngressPacket>, mpsc::Receiver<Vec<u8>>) {
        let (packet_tx, packet_rx) = mpsc::channel(INGRESS_QUEUE_CAPACITY);
        let (recycle_tx, recycle_rx) = mpsc::channel(RECEIVE_BUFFER_POOL_CAPACITY);
        (
            Self {
                packet_rx,
                recycle_tx,
            },
            packet_tx,
            recycle_rx,
        )
    }

    pub(in super::super) fn try_recv(&mut self) -> Option<IngressPacket> {
        self.packet_rx.try_recv().ok()
    }

    /// Waits until a producer delivers the next packet.
    ///
    /// Returns `None` once every packet sender has been dropped.
    pub(in super::super) async fn recv(&mut self) -> Option<IngressPacket> {
        self.packet_rx.recv().await
    }

    /// Returns reusable receive storage without backpressuring the packet loop.
    ///
    /// Only buffers retaining [`RECEIVE_BUFFER_LEN`] capacity enter the bounded
    /// pool. A full pool drops the buffer because reuse is opportunistic.
    pub(in super::super) fn recycle(&self, mut packet: Vec<u8>) {
        if packet.capacity() < RECEIVE_BUFFER_LEN {
            return;
        }
        packet.clear();
        let _ = self.recycle_tx.try_send(packet);
    }
}
