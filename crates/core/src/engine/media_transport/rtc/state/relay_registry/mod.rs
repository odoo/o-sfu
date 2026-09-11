//! Source-scoped relay registrations and bounded packet mailboxes.
//!
//! [`RelaySourceRegistration`] exposes active targets to packet planning. New
//! targets remain inactive until route control enables them and packet gates
//! still apply to active targets.
//!
//! [`RelayPacketMailbox`] reserves capacity before sharing a packet with another
//! worker. Full or closed mailboxes report rejection without waiting, leaving
//! the caller free to continue forwarding to other destinations.

use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::mpsc;

use super::{super::packet_loop::forwarded_packet::ForwardedPacket, PacketLoopState};
use crate::engine::media_transport::TransportMediaId;

#[cfg(test)]
#[path = "TESTS/support.rs"]
mod test_support;

pub(in super::super) const RELAY_MAILBOX_CAPACITY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) enum RelayEnqueueOutcome {
    Enqueued,
    Overloaded,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) struct RelayEnqueueReport {
    pub(in super::super) outcome: RelayEnqueueOutcome,
    pub(in super::super) mailbox_depth: usize,
}

#[derive(Debug, Clone)]
pub struct RelayPacketMailbox {
    tx: mpsc::Sender<ForwardedPacket>,
}

impl RelayPacketMailbox {
    pub(in super::super) fn new(tx: mpsc::Sender<ForwardedPacket>) -> Self {
        Self { tx }
    }

    /// Reserves mailbox capacity before sharing the packet across workers.
    ///
    /// Already full or closed targets therefore reject without cloning RTP
    /// metadata, source identity or the shared payload handle.
    pub(in super::super) fn forward_packet(
        &self,
        state: &PacketLoopState,
        packet: &ForwardedPacket,
        src_media: TransportMediaId,
    ) -> Option<RelayEnqueueReport> {
        let outcome = match self.tx.try_reserve() {
            Ok(permit) => {
                permit.send(packet.share_for_relay(state, src_media)?);
                RelayEnqueueOutcome::Enqueued
            }
            Err(mpsc::error::TrySendError::Full(())) => RelayEnqueueOutcome::Overloaded,
            Err(mpsc::error::TrySendError::Closed(())) => RelayEnqueueOutcome::Closed,
        };
        Some(RelayEnqueueReport {
            outcome,
            mailbox_depth: self.backlog_depth(),
        })
    }

    pub(in super::super) fn backlog_depth(&self) -> usize {
        sender_backlog_depth(&self.tx)
    }
}

#[derive(Debug, Clone)]
pub(in super::super) struct ActiveRelayTarget {
    pub(in super::super) target_id: RelayTargetId,
    pub(in super::super) target: RelayPacketMailbox,
}

pub(in super::super) fn sender_backlog_depth<T>(tx: &mpsc::Sender<T>) -> usize {
    tx.max_capacity().saturating_sub(tx.capacity())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelayTargetId(u64);

impl RelayTargetId {
    pub(in super::super) const fn new(raw: u64) -> Self {
        Self(raw)
    }
}

#[derive(Debug, Clone)]
struct RelayTargetRegistration {
    target: RelayPacketMailbox,
    active: bool,
}

/// Source-worker cache for relay target handles and active bits.
#[derive(Debug, Clone, Default)]
pub(in super::super) struct RelaySourceRegistration {
    targets: BTreeMap<RelayTargetId, RelayTargetRegistration>,
    active_targets: Arc<[ActiveRelayTarget]>,
}

impl RelaySourceRegistration {
    pub(in super::super) fn add_target(
        &mut self,
        target_id: RelayTargetId,
        target: RelayPacketMailbox,
    ) {
        self.targets
            .entry(target_id)
            .or_insert(RelayTargetRegistration {
                target,
                active: false,
            });
    }

    pub(in super::super) fn remove_target(&mut self, target_id: RelayTargetId) -> Option<bool> {
        let removed = self.targets.remove(&target_id)?;
        if removed.active {
            self.rebuild_mailboxes();
        }
        Some(self.targets.is_empty())
    }

    pub(in super::super) fn set_target_active(&mut self, target_id: RelayTargetId, active: bool) {
        let Some(registration) = self.targets.get_mut(&target_id) else {
            return;
        };
        if registration.active != active {
            registration.active = active;
            self.rebuild_mailboxes();
        }
    }

    #[must_use]
    pub(in super::super) fn active_targets(&self) -> &[ActiveRelayTarget] {
        &self.active_targets
    }

    #[must_use]
    pub(in super::super) fn has_active_targets(&self) -> bool {
        !self.active_targets.is_empty()
    }

    pub(in super::super) fn is_target_active(&self, target_id: RelayTargetId) -> bool {
        self.targets
            .get(&target_id)
            .is_some_and(|registration| registration.active)
    }

    #[cfg(test)]
    #[must_use]
    pub(in super::super) fn target_count(&self) -> usize {
        self.targets.len()
    }

    #[cfg(test)]
    #[must_use]
    pub(in super::super) fn active_target_count(&self) -> usize {
        self.targets
            .values()
            .filter(|registration| registration.active)
            .count()
    }

    fn rebuild_mailboxes(&mut self) {
        self.active_targets = self
            .targets
            .iter()
            .filter(|(_target_id, registration)| registration.active)
            .map(|(target_id, registration)| ActiveRelayTarget {
                target_id: *target_id,
                target: registration.target.clone(),
            })
            .collect();
    }
}
