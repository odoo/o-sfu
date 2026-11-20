//! Logical subscriptions and their transport realization.
//!
//! Explicit receiver intent survives while no publication is attached. A
//! current publication realizes as `Absent -> Pending -> Committed`. Current
//! setup failures, router rejection and receiver replacement return realization
//! to `Absent`. Reservation IDs reject async completion after source detach or
//! replacement.
//!
//! ```text
//! logical subscription realization:
//!      publication attached
//!              |
//!              v
//!   +--------------------+  reserve setup
//!   | ConsumerRealization| ------------------> +-------------------------------+
//!   |      Absent        |                     | ConsumerRealization::Pending  |
//!   +--------------------+ <------------------ | (RouteReservationId)          |
//!              ^       setup fails/rejected    +-------------------------------+
//!              |                                                  |
//!              |              decline/replace                     v setup accepted
//!              +---------------------------------- +---------------------------------+
//!                                                  | ConsumerRealization::Committed  |
//!                                                  +---------------------------------+
//! ```
//!
//! Intent without a current publication keeps `Subscription.current` at `None`.
//! Accepted setup requires a current identity, a successful transport declaration
//! and router dependency acceptance. Source detach returns to `None` while
//! retaining intent. `remove_receiver` deletes the record and its intent.
//!
//! Each receiver retains at most the room session limit of absent publisher
//! targets. Sparse updates keep the target's original age. A new target evicts
//! every stream preference of the oldest absent target. Present members do not
//! consume this allowance, including members with no publications. Joining
//! promotes existing intent and departure enters the allowance as a new target.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    mem,
    time::Instant,
};

use o_sfu_model::UserId;
use o_sfu_router::topology::RoutedConsumerId;

use super::{ConsumerSourceSelection, SubscriptionKey, remove_from_index_set};
use crate::{
    ConnectionId,
    engine::{
        media_transport::{
            ConsumerActivity, TransportConsumerRoute, TransportMediaId, TransportSessionKey,
        },
        source_model::{
            PolicyPauseReason, PublishedSourceId, SourceSelector, SourceSubscriptionIntent,
        },
    },
};

#[derive(Debug)]
pub(super) struct RouteGraph {
    entries: BTreeMap<SubscriptionKey, Subscription>,
    by_receiver: BTreeMap<UserId, BTreeSet<SubscriptionKey>>,
    by_source: BTreeMap<PublishedSourceId, BTreeSet<SubscriptionKey>>,
    next_reservation: RouteReservationId,
    pending_targets: BTreeMap<UserId, VecDeque<UserId>>,
    pending_target_limit: usize,
}

#[derive(Debug, Default)]
struct Subscription {
    intent: SourceSubscriptionIntent,
    current: Option<CurrentPublication>,
}

#[derive(Debug)]
pub(super) struct CurrentPublication {
    pub source_id: PublishedSourceId,
    pub selection: ConsumerSourceSelection,
    realization: ConsumerRealization,
}

#[derive(Debug, Default)]
enum ConsumerRealization {
    #[default]
    Absent,
    Pending(RouteReservationId),
    Committed(CommittedConsumerRoute),
}

/// Transport realization and adaptation hold share the exact route lifetime.
#[derive(Debug)]
pub(super) struct CommittedConsumerRoute {
    pub route: TransportConsumerRoute,
    routed: RoutedConsumerId,
    pub pending_upgrade: Option<PendingUpgrade>,
}

/// Eligibility must remain continuous for this exact deliverable post-fit target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine) struct PendingUpgrade {
    pub selector: SourceSelector,
    pub deadline: Instant,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RouteReservationId(u64);

#[derive(Debug)]
pub struct ConsumerRouteReservation {
    key: SubscriptionKey,
    source_id: PublishedSourceId,
    declared_activity: ConsumerActivity,
    id: RouteReservationId,
}

#[derive(Debug, Default)]
pub(super) struct RemovedRoutes {
    pub routes: Vec<TransportConsumerRoute>,
    pub consumers: Vec<RoutedConsumerId>,
}

impl RemovedRoutes {
    pub(super) fn extend(&mut self, mut other: Self) {
        self.routes.append(&mut other.routes);
        self.consumers.append(&mut other.consumers);
    }

    fn collect(&mut self, realization: ConsumerRealization) {
        if let ConsumerRealization::Committed(committed) = realization {
            self.routes.push(committed.route);
            self.consumers.push(committed.routed);
        }
    }
}

impl RouteGraph {
    pub(super) fn new(pending_target_limit: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            by_receiver: BTreeMap::new(),
            by_source: BTreeMap::new(),
            next_reservation: RouteReservationId::default(),
            pending_targets: BTreeMap::new(),
            pending_target_limit,
        }
    }

    /// Membership promotion preserves every stream preference and frees its
    /// absent-target allowance without duplicating the logical subscriptions.
    pub(super) fn publisher_joined(&mut self, publisher: &UserId) {
        self.pending_targets.retain(|_, pending| {
            pending.retain(|target| target != publisher);
            !pending.is_empty()
        });
    }

    /// Call after detaching every source and removing this user's receiver state.
    /// A departed member becomes the newest absent target for each receiver
    /// retaining explicit intent, even when it never published media.
    pub(super) fn publisher_left(&mut self, publisher: &UserId) -> usize {
        let receivers: Vec<_> = self
            .by_receiver
            .iter()
            .filter(|(_, keys)| keys.iter().any(|key| &key.publisher == publisher))
            .map(|(receiver, _)| receiver.clone())
            .collect();
        receivers
            .iter()
            .map(|receiver| self.retain_pending_target(receiver, publisher))
            .sum()
    }

    fn retain_pending_target(&mut self, receiver: &UserId, publisher: &UserId) -> usize {
        let pending = self.pending_targets.entry(receiver.clone()).or_default();
        if pending.contains(publisher) {
            return 0;
        }
        pending.push_back(publisher.clone());
        if pending.len() <= self.pending_target_limit {
            return 0;
        }
        let Some(evicted) = pending.pop_front() else {
            return 0;
        };
        if pending.is_empty() {
            self.pending_targets.remove(receiver);
        }
        // Only absent publishers enter this index. Their detached records have
        // no transport ownership, so eviction removes intent alone.
        if let Some(keys) = self.by_receiver.get_mut(receiver) {
            keys.retain(|key| {
                if key.publisher != evicted {
                    return true;
                }
                let removed = self.entries.remove(key);
                debug_assert!(removed.is_none_or(|entry| entry.current.is_none()));
                false
            });
            if keys.is_empty() {
                self.by_receiver.remove(receiver);
            }
        }
        1
    }

    pub(super) fn subscription_count(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.has_consumer_setup_or_route())
            .count()
    }

    pub(super) fn count(&self) -> usize {
        self.attached()
            .filter(|(_, current)| current.committed().is_some())
            .count()
    }

    #[cfg(test)]
    pub(super) fn record_count(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn merge_intent(
        &mut self,
        key: &SubscriptionKey,
        update: SourceSubscriptionIntent,
        publisher_present: bool,
    ) -> usize {
        if update.is_empty() {
            return 0;
        }
        let entry = self.entry(key.clone());
        entry.intent.merge(update);
        if let (Some(active), Some(current)) = (update.active(), entry.current.as_mut()) {
            current.set_active(active);
        }
        if publisher_present {
            0
        } else {
            self.retain_pending_target(&key.receiver, &key.publisher)
        }
    }

    pub(super) fn intent(&self, key: &SubscriptionKey) -> SourceSubscriptionIntent {
        self.entries
            .get(key)
            .map_or_else(SourceSubscriptionIntent::default, |entry| entry.intent)
    }

    pub(super) fn attach_for_setup(
        &mut self,
        key: SubscriptionKey,
        source_id: PublishedSourceId,
    ) -> bool {
        let entry = self.entry(key.clone());
        if let Some(current) = &entry.current {
            return current.source_id == source_id
                && matches!(current.realization, ConsumerRealization::Absent);
        }
        entry.current = Some(CurrentPublication {
            source_id,
            selection: ConsumerSourceSelection::open(entry.intent.active().unwrap_or(true)),
            realization: ConsumerRealization::Absent,
        });
        self.by_source.entry(source_id).or_default().insert(key);
        true
    }

    /// Returns whether the attachment matches `source_id` and `connection_id`.
    pub(super) fn set_activity(
        &mut self,
        key: &SubscriptionKey,
        source_id: PublishedSourceId,
        connection_id: ConnectionId,
        active: bool,
        policy_pause_reason: Option<PolicyPauseReason>,
    ) -> bool {
        let Some(current) = self.current_mut(key, source_id) else {
            return false;
        };
        if let ConsumerRealization::Committed(committed) = &current.realization
            && committed.route.consumer_session_key().connection_id() != connection_id
        {
            return false;
        }
        current.set_active(active);
        if let Some(reason) = policy_pause_reason {
            current.selection.set_policy_pause_reason(Some(reason));
        }
        true
    }

    pub(super) fn reserve_consumer_setup(
        &mut self,
        key: SubscriptionKey,
        source_id: PublishedSourceId,
        selection: ConsumerSourceSelection,
    ) -> Option<ConsumerRouteReservation> {
        let current = self.entries.get_mut(&key)?.current.as_mut()?;
        if current.source_id != source_id
            || !matches!(current.realization, ConsumerRealization::Absent)
        {
            return None;
        }
        self.next_reservation.0 += 1;
        let id = self.next_reservation;
        current.selection = selection;
        current.realization = ConsumerRealization::Pending(id);
        Some(ConsumerRouteReservation {
            key,
            source_id,
            declared_activity: ConsumerActivity::from_active(selection.delivery_active()),
            id,
        })
    }

    pub(super) fn release_consumer_setup(&mut self, reservation: &ConsumerRouteReservation) {
        self.take_pending(reservation);
    }

    /// Returns whether the route committed.
    #[must_use]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "commit consumes the reservation on both success and failure"
    )]
    pub(super) fn commit(
        &mut self,
        reservation: ConsumerRouteReservation,
        route: TransportConsumerRoute,
        selection: ConsumerSourceSelection,
        accept: impl FnOnce() -> Option<RoutedConsumerId>,
    ) -> bool {
        // Consume the reservation before router acceptance so every async
        // completion is terminal and cannot reuse its identity after failure.
        if !self.take_pending(&reservation) {
            return false;
        }
        let Some(current) = self.current_mut(&reservation.key, reservation.source_id) else {
            return false;
        };
        let Some(routed) = accept() else {
            return false;
        };
        current.selection = selection;
        current.realization = ConsumerRealization::Committed(CommittedConsumerRoute {
            route,
            routed,
            pending_upgrade: None,
        });
        true
    }

    pub(super) fn update_selection(
        &mut self,
        key: &SubscriptionKey,
        source_id: PublishedSourceId,
        route: &TransportConsumerRoute,
        update: impl FnOnce(&mut ConsumerSourceSelection),
    ) -> bool {
        let Some(current) = self.current_mut(key, source_id) else {
            return false;
        };
        let ConsumerRealization::Committed(committed) = &current.realization else {
            return false;
        };
        if &committed.route != route {
            return false;
        }
        update(&mut current.selection);
        if !current.selection.active() {
            current.clear_upgrade();
        }
        true
    }

    pub(super) fn update_upgrade(
        &mut self,
        key: &SubscriptionKey,
        source_id: PublishedSourceId,
        route: &TransportConsumerRoute,
        pending_upgrade: Option<PendingUpgrade>,
    ) -> bool {
        let Some(current) = self.current_mut(key, source_id) else {
            return false;
        };
        let ConsumerRealization::Committed(committed) = &mut current.realization else {
            return false;
        };
        if &committed.route != route || !current.selection.active() {
            return false;
        }
        committed.pending_upgrade = pending_upgrade;
        true
    }

    pub(super) fn clear_source_upgrades(&mut self, source_id: PublishedSourceId) {
        let (entries, by_source) = (&mut self.entries, &self.by_source);
        for key in by_source.get(&source_id).into_iter().flatten() {
            if let Some(current) = entries
                .get_mut(key)
                .and_then(|entry| entry.current.as_mut())
            {
                current.clear_upgrade();
            }
        }
    }

    pub(super) fn selection(
        &self,
        key: &SubscriptionKey,
        source_id: PublishedSourceId,
    ) -> Option<ConsumerSourceSelection> {
        let current = self.entries.get(key)?.current.as_ref()?;
        (current.source_id == source_id).then_some(current.selection)
    }

    pub(super) fn attached(&self) -> impl Iterator<Item = (&SubscriptionKey, &CurrentPublication)> {
        self.entries
            .iter()
            .filter_map(|(key, entry)| Some((key, entry.current.as_ref()?)))
    }

    pub(super) fn current(
        &self,
        key: &SubscriptionKey,
    ) -> Option<(&SubscriptionKey, &CurrentPublication)> {
        let (key, entry) = self.entries.get_key_value(key)?;
        Some((key, entry.current.as_ref()?))
    }

    pub(super) fn attached_for_receiver(
        &self,
        receiver: &UserId,
    ) -> impl Iterator<Item = (&SubscriptionKey, &CurrentPublication)> {
        self.by_receiver
            .get(receiver)
            .into_iter()
            .flat_map(BTreeSet::iter)
            .filter_map(|key| self.current(key))
    }

    pub(super) fn attached_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> impl Iterator<Item = (&SubscriptionKey, &CurrentPublication)> {
        self.by_source
            .get(&source_id)
            .into_iter()
            .flat_map(BTreeSet::iter)
            .filter_map(|key| self.current(key))
    }

    pub(super) fn detach_source(&mut self, source_id: PublishedSourceId) -> RemovedRoutes {
        let keys = self.by_source.remove(&source_id).unwrap_or_default();
        let mut removed = RemovedRoutes::default();
        for key in keys {
            let (realization, prune) = {
                let Some(entry) = self.entries.get_mut(&key) else {
                    continue;
                };
                let Some(current) = entry.current.take() else {
                    continue;
                };
                debug_assert_eq!(current.source_id, source_id);
                (current.realization, entry.intent.is_empty())
            };
            removed.collect(realization);
            if prune {
                self.entries.remove(&key);
                remove_from_index_set(&mut self.by_receiver, &key.receiver, &key);
            }
        }
        removed
    }

    pub(super) fn reset_receiver_for_replacement(&mut self, receiver: &UserId) {
        for key in self.by_receiver.get(receiver).into_iter().flatten() {
            let Some(entry) = self.entries.get_mut(key) else {
                continue;
            };
            let Some(current) = entry.current.as_mut() else {
                continue;
            };
            current.selection =
                ConsumerSourceSelection::open(entry.intent.active().unwrap_or(true));
            current.realization = ConsumerRealization::Absent;
        }
    }

    pub(super) fn remove_receiver(&mut self, receiver: &UserId) -> RemovedRoutes {
        self.pending_targets.remove(receiver);
        let keys = self.by_receiver.remove(receiver).unwrap_or_default();
        let mut removed = RemovedRoutes::default();
        for key in keys {
            let Some(entry) = self.entries.remove(&key) else {
                continue;
            };
            let Some(current) = entry.current else {
                continue;
            };
            remove_from_index_set(&mut self.by_source, &current.source_id, &key);
            removed.collect(current.realization);
        }
        removed
    }

    pub(super) fn detach_declined_consumers(
        &mut self,
        session: &TransportSessionKey,
        declined: &[TransportMediaId],
    ) -> RemovedRoutes {
        let keys = self
            .by_receiver
            .get(session.user_id())
            .cloned()
            .unwrap_or_default();
        let mut removed = RemovedRoutes::default();
        for key in keys {
            let realization = {
                let Some(current) = self
                    .entries
                    .get_mut(&key)
                    .and_then(|entry| entry.current.as_mut())
                else {
                    continue;
                };
                let ConsumerRealization::Committed(committed) = &current.realization else {
                    continue;
                };
                if committed.route.consumer_session_key() != session
                    || !declined.contains(&committed.route.consumer_transport_media_id())
                {
                    continue;
                }
                mem::take(&mut current.realization)
            };
            removed.collect(realization);
        }
        removed
    }

    fn entry(&mut self, key: SubscriptionKey) -> &mut Subscription {
        self.by_receiver
            .entry(key.receiver.clone())
            .or_default()
            .insert(key.clone());
        self.entries.entry(key).or_default()
    }

    fn current_mut(
        &mut self,
        key: &SubscriptionKey,
        source_id: PublishedSourceId,
    ) -> Option<&mut CurrentPublication> {
        let current = self.entries.get_mut(key)?.current.as_mut()?;
        (current.source_id == source_id).then_some(current)
    }

    /// Returns whether `reservation` was pending.
    fn take_pending(&mut self, reservation: &ConsumerRouteReservation) -> bool {
        let Some(current) = self.current_mut(&reservation.key, reservation.source_id) else {
            return false;
        };
        let pending =
            matches!(current.realization, ConsumerRealization::Pending(id) if id == reservation.id);
        if pending {
            current.realization = ConsumerRealization::Absent;
        }
        pending
    }
}

impl ConsumerRouteReservation {
    pub const fn declared_activity(&self) -> ConsumerActivity {
        self.declared_activity
    }
}

impl Subscription {
    fn has_consumer_setup_or_route(&self) -> bool {
        self.current
            .as_ref()
            .is_some_and(|current| !matches!(current.realization, ConsumerRealization::Absent))
    }
}

impl CurrentPublication {
    fn set_active(&mut self, active: bool) {
        self.selection.set_active(active);
        if !active {
            self.clear_upgrade();
        }
    }

    fn clear_upgrade(&mut self) {
        if let ConsumerRealization::Committed(committed) = &mut self.realization {
            committed.pending_upgrade = None;
        }
    }

    pub(super) const fn is_pending(&self) -> bool {
        matches!(self.realization, ConsumerRealization::Pending(..))
    }

    pub(super) fn committed(&self) -> Option<&CommittedConsumerRoute> {
        match &self.realization {
            ConsumerRealization::Committed(committed) => Some(committed),
            ConsumerRealization::Absent | ConsumerRealization::Pending(..) => None,
        }
    }
}
