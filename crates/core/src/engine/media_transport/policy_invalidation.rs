//! Coalesces transport observations and absolute policy deadlines into room wakeups.

use std::{
    collections::{BTreeMap, BTreeSet},
    mem,
    sync::{Arc, Mutex},
    time::Instant,
};

use tokio::{
    sync::Notify,
    time::{Instant as TokioInstant, sleep_until},
};

use super::MediaTransport;
use crate::{RoomInstanceId, engine::sync::lock_unpoisoned};

#[derive(Debug, Default)]
struct PendingSourcePolicyUpdates {
    rooms: BTreeSet<RoomInstanceId>,
    scheduled_by_room: BTreeMap<RoomInstanceId, Instant>,
    deadlines: BTreeSet<(Instant, RoomInstanceId)>,
}

impl PendingSourcePolicyUpdates {
    fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.first().map(|(deadline, _)| *deadline)
    }

    fn take(&mut self, now: Instant) -> BTreeSet<RoomInstanceId> {
        while let Some(&(deadline, room)) = self.deadlines.first() {
            if deadline > now {
                break;
            }
            self.deadlines.pop_first();
            self.scheduled_by_room.remove(&room);
            self.rooms.insert(room);
        }
        mem::take(&mut self.rooms)
    }
}

#[derive(Debug, Default)]
struct SourcePolicyUpdates {
    pending: Mutex<PendingSourcePolicyUpdates>,
    notify: Notify,
}

/// Shared drain for coalesced room source-policy invalidations.
///
/// Clones share one drain. The runtime must assign all clones to a single
/// consumer task. Dropping a wait leaves room work and deadlines in the drain.
#[derive(Debug, Clone)]
pub struct SourcePolicyUpdateSubscription(Arc<SourcePolicyUpdates>);

impl SourcePolicyUpdateSubscription {
    /// Waits until immediate work or an absolute deadline requires a policy pass.
    pub async fn wait_for_update(&self) -> BTreeSet<RoomInstanceId> {
        loop {
            // Retain the notification across the timer race and drain again
            // before waiting. A timer winner must not consume an unseen dirty wake.
            let notified = self.0.notify.notified();
            let deadline = {
                let mut pending = lock_unpoisoned(&self.0.pending);
                let rooms = pending.take(TokioInstant::now().into_std());
                if !rooms.is_empty() {
                    return rooms;
                }
                pending.next_deadline()
            };
            if let Some(deadline) = deadline {
                tokio::select! {
                    () = notified => {},
                    () = sleep_until(deadline.into()) => {},
                }
            } else {
                notified.await;
            }
        }
    }

    /// Drains immediate updates and expired deadlines after the previous wait.
    #[must_use]
    pub fn take_pending_updates(&self) -> BTreeSet<RoomInstanceId> {
        lock_unpoisoned(&self.0.pending).take(TokioInstant::now().into_std())
    }
}

/// Sender for coalesced room source-policy updates.
#[derive(Debug, Clone, Default)]
pub struct SourcePolicySignal(Arc<SourcePolicyUpdates>);

impl SourcePolicySignal {
    /// Creates the runtime's single-consumer subscription.
    #[must_use]
    pub fn subscribe(&self) -> SourcePolicyUpdateSubscription {
        SourcePolicyUpdateSubscription(Arc::clone(&self.0))
    }

    /// Marks one room as needing a source-policy pass.
    pub fn mark_dirty(&self, room_instance_id: RoomInstanceId) {
        self.mark_dirty_rooms([room_instance_id]);
    }

    /// Marks rooms dirty without changing their independently scheduled deadlines.
    /// Empty input from packet-pump turns does not acquire the shared lock.
    pub fn mark_dirty_rooms(&self, room_instance_ids: impl IntoIterator<Item = RoomInstanceId>) {
        let mut room_instance_ids = room_instance_ids.into_iter();
        let Some(first) = room_instance_ids.next() else {
            return;
        };
        let mut pending = lock_unpoisoned(&self.0.pending);
        let notify = pending.rooms.is_empty();
        pending.rooms.insert(first);
        pending.rooms.extend(room_instance_ids);
        drop(pending);
        if notify {
            self.0.notify.notify_one();
        }
    }

    /// Replaces or cancels a room's earliest deadline. Equal deadlines are a no-op.
    fn set_deadline(&self, room: RoomInstanceId, deadline: Option<Instant>) {
        let mut pending = lock_unpoisoned(&self.0.pending);
        if pending.scheduled_by_room.get(&room).copied() == deadline {
            return;
        }
        let previous_earliest = pending.next_deadline();
        if let Some(previous) = pending.scheduled_by_room.remove(&room) {
            pending.deadlines.remove(&(previous, room));
        }
        if let Some(deadline) = deadline {
            pending.scheduled_by_room.insert(room, deadline);
            pending.deadlines.insert((deadline, room));
        }
        let notify = previous_earliest != pending.next_deadline();
        drop(pending);
        if notify {
            self.0.notify.notify_one();
        }
    }
}

impl MediaTransport {
    /// Replaces or cancels the room deadline recomputed by its ordered policy turn.
    pub(in crate::engine) fn set_source_policy_deadline(
        &self,
        room: RoomInstanceId,
        deadline: Option<Instant>,
    ) {
        self.source_policy_signal.set_deadline(room, deadline);
    }
}

#[cfg(test)]
#[path = "TESTS/policy_invalidation.rs"]
mod tests;
