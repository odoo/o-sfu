//! Worker staging buffers for one packet-loop turn.
//!
//! Session output, relay intake and ready-session scheduling reuse these vectors.
//! [`PacketLoopBuffers::clear`] resets staged work while retaining capacity.
//! Packet observation and fanout scratch belong to
//! [`PacketForwarder`](super::super::packet_loop::PacketForwarder).
//!
//! The buffers do not own durable routing state. Durable state stays in
//! `PacketLoopState`, `RtcSnapshotState`, worker-local relay target maps or
//! packet sinks.
//! Values stored here are staged work that must either be flushed during the
//! current turn or dropped as part of clearing the turn.

use std::sync::Arc;

use str0m::{media::SenderFeedback, net::Transmit};

use super::super::{
    packet_loop::forwarded_packet::ForwardedPacket,
    recovery::PendingKeyframeRequest,
    state::{keyframe_tracker::SourceKeyframeRequest, slots::SessionHandle},
};
use crate::engine::media_transport::TransportSessionKey;

pub(in super::super) const RECEIVE_BUFFER_LEN: usize = 2000;
pub(in super::super) const MAX_RELAY_PACKETS_PER_ITERATION: usize = 64;

pub(in super::super) struct SessionDrainCheckpoint {
    transmits: usize,
    packets: usize,
    keyframe_requests: usize,
    sender_feedback: usize,
}

/// per-worker scratch buffers reused across packet-loop turns
///
/// # hot-path contract
///
/// the packet loop owns one instance for the lifetime of the worker task
/// calling code may push staged work during a turn, but no field is
/// authoritative after the turn is flushed
/// new reusable collections should be
/// added here only when they replace repeated hot-path allocation or preserve a
/// bounded batch between two packet-loop phases
pub struct PacketLoopBuffers {
    /// reusable complete transmit slots produced by `str0m::Output::Transmit`
    pub(in super::super) pending_transmits: Vec<Transmit>,
    /// media packets produced by local adapter sessions or inbound relays
    pub pending_packets: Vec<ForwardedPacket>,
    /// raw keyframe feedback emitted by consumer sessions before source lookup
    pub pending_keyframe_requests: Vec<(TransportSessionKey, PendingKeyframeRequest)>,
    /// Sender reports and SDES names staged until the session drain succeeds.
    pub(in super::super) pending_sender_feedback: Vec<(SenderFeedback, Arc<str>)>,
    /// sessions ready for polling after dirty and timeout scheduling is merged
    pub(in super::super) ready_sessions: Vec<SessionHandle>,
    /// source-keyed feedback after duplicate requests are merged
    pub coalesced_keyframe_requests: Vec<SourceKeyframeRequest>,
    /// due keyframe retries drained from the tracker
    pub keyframe_retries: Vec<SourceKeyframeRequest>,
}

impl PacketLoopBuffers {
    /// build the reusable buffer set with small initial capacities
    ///
    /// the capacities are only starting points
    /// dense rooms may grow them once,
    /// after which normal `.clear()` calls keep the larger allocation for later
    /// turns
    pub fn new() -> Self {
        Self {
            pending_transmits: Vec::with_capacity(64),
            pending_packets: Vec::with_capacity(32),
            pending_keyframe_requests: Vec::with_capacity(8),
            pending_sender_feedback: Vec::with_capacity(4),
            ready_sessions: Vec::with_capacity(32),
            coalesced_keyframe_requests: Vec::with_capacity(8),
            keyframe_retries: Vec::with_capacity(8),
        }
    }

    /// Reset all staged work while retaining allocation capacity.
    pub fn clear(&mut self) {
        self.pending_transmits.clear();
        self.pending_packets.clear();
        self.pending_keyframe_requests.clear();
        self.pending_sender_feedback.clear();
        self.ready_sessions.clear();
        self.coalesced_keyframe_requests.clear();
        self.keyframe_retries.clear();
    }

    #[must_use]
    pub(in super::super) fn checkpoint_session_drain(&self) -> SessionDrainCheckpoint {
        SessionDrainCheckpoint {
            transmits: self.pending_transmits.len(),
            packets: self.pending_packets.len(),
            keyframe_requests: self.pending_keyframe_requests.len(),
            sender_feedback: self.pending_sender_feedback.len(),
        }
    }

    pub(in super::super) fn rollback_session_drain(&mut self, checkpoint: &SessionDrainCheckpoint) {
        self.pending_transmits.truncate(checkpoint.transmits);
        self.pending_packets.truncate(checkpoint.packets);
        self.pending_keyframe_requests
            .truncate(checkpoint.keyframe_requests);
        self.pending_sender_feedback
            .truncate(checkpoint.sender_feedback);
    }
}
