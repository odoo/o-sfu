//! TCP output handed to the per-stream writers of the TCP acceptor.
//!
//! The worker never touches TCP sockets. Each accepted connection registers an outbox,
//! and its own task is responsible to write on the stream.
//!
//! The egress onwly keeps weak handles to the outboxes (the session served by the stream
//! owns the strong reference) so that dropping a session cleans up the egress channel.

use std::{collections::HashMap, net::SocketAddr};

use o_sfu_rfc::tcp_framing::TcpFrame;
use str0m::net::Transmit;
use tokio::sync::mpsc::{self, error::TrySendError};
use tracing::{debug, warn};

/// Weak outboxes of the TCP streams served by one worker, keyed by peer address.
#[derive(Default)]
pub(crate) struct TcpEgress {
    outbox_tx_by_peer: HashMap<SocketAddr, mpsc::WeakSender<TcpFrame>>,
}

impl TcpEgress {
    /// Routes output for `peer_addr` to `outbox_tx` without keeping it alive. Reaps the
    /// outboxes of the streams that ended or whose owner dropped them.
    pub fn register(&mut self, peer_addr: SocketAddr, outbox_tx: &mpsc::Sender<TcpFrame>) {
        self.outbox_tx_by_peer.retain(|_, weak_tx| {
            weak_tx
                .upgrade()
                .is_some_and(|outbox_tx| !outbox_tx.is_closed())
        });
        self.outbox_tx_by_peer
            .insert(peer_addr, outbox_tx.downgrade());
    }

    /// Hands one transmit to the outbox of its destination without waiting.
    pub fn enqueue(&mut self, transmit: Transmit) {
        let destination = transmit.destination;
        let Some((outbox_tx, frame)) = self.prepare(transmit) else {
            return;
        };
        let result = outbox_tx.try_send(frame);
        let Err(error) = result else {
            return;
        };
        match error {
            TrySendError::Full(_frame) => {
                debug!(%destination, "dropped TCP transmit, outbox full");
            }
            TrySendError::Closed(_frame) => {
                self.outbox_tx_by_peer.remove(&destination);
                debug!(%destination, "dropped TCP transmit, connection closed");
            }
        }
    }

    /// Finds the outbox and frames the payload, logging why a transmit is dropped.
    fn prepare(&mut self, transmit: Transmit) -> Option<(mpsc::Sender<TcpFrame>, TcpFrame)> {
        let destination = transmit.destination;
        let Some(weak_tx) = self.outbox_tx_by_peer.get(&destination) else {
            debug!(%destination, "dropped TCP transmit without a connection");
            return None;
        };
        let Some(outbox_tx) = weak_tx.upgrade() else {
            self.outbox_tx_by_peer.remove(&destination);
            debug!(%destination, "dropped TCP transmit, session closed");
            return None;
        };
        TcpFrame::try_new(transmit.contents.into()).map_or_else(
            |_| {
                warn!(%destination, "dropped TCP transmit too large for a frame");
                None
            },
            |frame| Some((outbox_tx, frame)),
        )
    }
}

#[cfg(test)]
#[path = "TESTS/tcp.rs"]
mod tests;
