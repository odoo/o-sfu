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
//!   | ConsumerRealization| ------------------> +-------------------------------------+
//!   |      Absent        |                     | ConsumerRealization::Pending        |
//!   +--------------------+ <------------------ | (RouteReservationId, Option<Relay>) |
//!              ^       setup fails/rejected    +-------------------------------------+
//!              |                                                  |
//!              |              decline/replace                     v setup accepted
//!              +---------------------------------- +---------------------------------+
//!                                                  | ConsumerRealization::Committed  |
//!                                                  +---------------------------------+
//!
//! cross-worker relay sharing:
//!   receiver 1 (worker B) \
//!   receiver 2 (worker B)  --> [ RelayRouteKey (source worker A -> worker B) ]
//!   receiver 3 (worker B) /               |
//!                                         v
//!                         shared relay channel (worker A -> B)
//!                                         |
//!                             +-----------+-----------+
//!                             v           v           v
//!                          recv 1      recv 2      recv 3 (local fanout on worker B)
//! ```
//!
//! Intent without a current publication keeps `Subscription.current` at `None`.
//! Accepted setup requires a current identity, a successful transport declaration
//! and router dependency acceptance. Source detach returns to `None` while
//! retaining intent. `remove_receiver` deletes the record and its intent.
//!
//! Cross-worker relays are shared by subscriptions with the same source and
//! target worker. A relay remains active while any owner has active receiver
//! intent. An active first owner emits `Install` followed by
//! `SetActivity(Active)`. Later aggregate activity changes emit `SetActivity`.
//! Removing the last owner emits `Release`.

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    mem,
    time::Instant,
};

use o_sfu_router::topology::RoutedConsumerId;

use super::{
    ConsumerSourceSelection, SubscriptionKey, consumer_setup::ConsumerSetupTarget,
    remove_from_index_set,
};
use crate::engine::{
    ConnectionId, MediaWorkerId, UserId,
    media_transport::{
        ConsumerActivity, RelayRouteActivity, TransportConsumerRoute, TransportMediaId,
        TransportRelayRouteAction, TransportRelayRouteEffect, TransportSessionKey,
        TransportSourceKey,
    },
    source_model::{
        PolicyPauseReason, PublishedSourceId, SourceSelector, SourceSubscriptionIntent,
    },
};

#[derive(Debug, Default)]
pub(super) struct RouteGraph {
    entries: BTreeMap<SubscriptionKey, Subscription>,
    by_receiver: BTreeMap<UserId, BTreeSet<SubscriptionKey>>,
    by_source: BTreeMap<PublishedSourceId, BTreeSet<SubscriptionKey>>,
    relays: BTreeMap<RelayRouteKey, RelayOwners>,
    next_reservation: RouteReservationId,
}

type RelayOwners = BTreeMap<SubscriptionKey, RelayRouteActivity>;

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
    Pending(RouteReservationId, Option<RouteRelay>),
    Committed(CommittedConsumerRoute),
}

/// Transport realization and adaptation hold share the exact route lifetime.
#[derive(Debug)]
pub(super) struct CommittedConsumerRoute {
    pub route: TransportConsumerRoute,
    pub mid: String,
    routed: RoutedConsumerId,
    relay: Option<RouteRelay>,
    pub pending_upgrade: Option<PendingUpgrade>,
}

/// Eligibility must remain continuous for this exact deliverable post-fit target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::room) struct PendingUpgrade {
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteRelay {
    route: RelayRouteKey,
    activity: RelayRouteActivity,
}

struct TakenPending {
    relay: Option<RouteRelay>,
}

/// Captured source placement survives replacement until its relay owners release it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RelayRouteKey {
    pub source: TransportSourceKey,
    pub target_worker: MediaWorkerId,
}

#[derive(Debug, Default)]
pub(super) struct RemovedRoutes {
    pub routes: Vec<TransportConsumerRoute>,
    pub consumers: Vec<RoutedConsumerId>,
    pub relays: Vec<TransportRelayRouteEffect>,
}

impl RemovedRoutes {
    pub(super) fn extend(&mut self, mut other: Self) {
        self.routes.append(&mut other.routes);
        self.consumers.append(&mut other.consumers);
        self.relays.append(&mut other.relays);
    }
}

impl RouteGraph {
    pub(super) fn subscription_count(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.has_consumer_setup_or_route())
            .count()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn count(&self) -> usize {
        self.attached()
            .filter(|(_, current)| current.committed().is_some())
            .count()
    }

    #[cfg(test)]
    pub(super) fn record_count(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn merge_intent(&mut self, key: SubscriptionKey, update: SourceSubscriptionIntent) {
        if update.is_empty() {
            return;
        }
        let entry = self.entry(key);
        entry.intent.merge(update);
        if let (Some(active), Some(current)) = (update.active(), entry.current.as_mut()) {
            current.set_active(active);
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

    pub(super) fn set_activity(
        &mut self,
        key: &SubscriptionKey,
        source_id: PublishedSourceId,
        connection_id: ConnectionId,
        active: bool,
        policy_pause_reason: Option<PolicyPauseReason>,
    ) -> Option<Vec<TransportRelayRouteEffect>> {
        let relay = {
            let current = self
                .entries
                .get_mut(key)
                .and_then(|entry| entry.current.as_mut())
                .filter(|current| current.source_id == source_id)?;
            if let ConsumerRealization::Committed(committed) = &current.realization
                && committed.route.consumer_session_key().connection_id() != connection_id
            {
                return None;
            }
            current.set_active(active);
            if let Some(reason) = policy_pause_reason {
                current.selection.set_policy_pause_reason(Some(reason));
            }
            current
                .realization
                .set_relay_activity(RelayRouteActivity::from_active(active))
        };
        Some(relay.map_or_else(Vec::new, |relay| self.set_relay_owner(key, &relay, false)))
    }

    pub(super) fn reserve_consumer_setup(
        &mut self,
        key: SubscriptionKey,
        source_id: PublishedSourceId,
        selection: ConsumerSourceSelection,
    ) -> Option<ConsumerRouteReservation> {
        let current = self.entries.get(&key)?.current.as_ref()?;
        if current.source_id != source_id
            || !matches!(current.realization, ConsumerRealization::Absent)
        {
            return None;
        }
        let id = self.next_reservation();
        let current = self.entries.get_mut(&key)?.current.as_mut()?;
        current.selection = selection;
        current.realization = ConsumerRealization::Pending(id, None);
        Some(ConsumerRouteReservation {
            key,
            source_id,
            declared_activity: ConsumerActivity::from_active(selection.delivery_active()),
            id,
        })
    }

    pub(super) fn release_consumer_setup(
        &mut self,
        reservation: ConsumerRouteReservation,
    ) -> Vec<TransportRelayRouteEffect> {
        let Some(TakenPending { relay }) = self.take_pending(&reservation) else {
            return Vec::new();
        };
        let key = reservation.key;
        relay.map_or_else(Vec::new, |relay| self.release_relay(&key, &relay))
    }

    pub(super) fn commit(
        &mut self,
        reservation: ConsumerRouteReservation,
        route: TransportConsumerRoute,
        mid: String,
        selection: ConsumerSourceSelection,
        accept: impl FnOnce() -> Option<RoutedConsumerId>,
    ) -> Result<(), Vec<TransportRelayRouteEffect>> {
        // Consume the reservation before router acceptance so every async
        // completion is terminal and cannot reuse its identity after failure.
        let pending = self.take_pending(&reservation);
        let ConsumerRouteReservation { key, source_id, .. } = reservation;
        let Some(TakenPending { relay }) = pending else {
            return Err(Vec::new());
        };
        let Some(current) = self.current_mut(&key, source_id) else {
            return Err(relay.map_or_else(Vec::new, |relay| self.release_relay(&key, &relay)));
        };
        let Some(routed) = accept() else {
            return Err(relay.map_or_else(Vec::new, |relay| self.release_relay(&key, &relay)));
        };
        current.selection = selection;
        current.realization = ConsumerRealization::Committed(CommittedConsumerRoute {
            route,
            mid,
            routed,
            relay,
            pending_upgrade: None,
        });
        Ok(())
    }

    pub(super) fn update_selection(
        &mut self,
        key: &SubscriptionKey,
        source_id: PublishedSourceId,
        route: &TransportConsumerRoute,
        update: impl FnOnce(&mut ConsumerSourceSelection),
    ) -> bool {
        let Some(current) = self
            .entries
            .get_mut(key)
            .and_then(|entry| entry.current.as_mut())
        else {
            return false;
        };
        let ConsumerRealization::Committed(committed) = &current.realization else {
            return false;
        };
        if current.source_id != source_id || &committed.route != route {
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
            self.collect_removed_realization(&key, realization, &mut removed);
            if prune {
                self.entries.remove(&key);
                remove_from_index_set(&mut self.by_receiver, &key.receiver, &key);
            }
        }
        removed
    }

    pub(super) fn reset_receiver_for_replacement(
        &mut self,
        receiver: &UserId,
    ) -> Vec<TransportRelayRouteEffect> {
        let keys = self.by_receiver.get(receiver).cloned().unwrap_or_default();
        let mut relays = Vec::new();
        for key in keys {
            let relay = {
                let Some(entry) = self.entries.get_mut(&key) else {
                    continue;
                };
                let Some(current) = entry.current.as_mut() else {
                    continue;
                };
                current.selection =
                    ConsumerSourceSelection::open(entry.intent.active().unwrap_or(true));
                match mem::take(&mut current.realization) {
                    ConsumerRealization::Absent => None,
                    ConsumerRealization::Pending(_, relay) => relay,
                    ConsumerRealization::Committed(committed) => committed.relay,
                }
            };
            if let Some(relay) = relay {
                relays.extend(self.release_relay(&key, &relay));
            }
        }
        relays
    }

    pub(super) fn remove_receiver(&mut self, receiver: &UserId) -> RemovedRoutes {
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
            self.collect_removed_realization(&key, current.realization, &mut removed);
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
            self.collect_removed_realization(&key, realization, &mut removed);
        }
        removed
    }

    pub(super) fn reserve_relay(
        &mut self,
        reservation: &ConsumerRouteReservation,
        target: &ConsumerSetupTarget,
        target_worker: MediaWorkerId,
        active: bool,
    ) -> Vec<TransportRelayRouteEffect> {
        let (previous, relay) = {
            let Some(current) = self.current_mut_for(reservation) else {
                return Vec::new();
            };
            let ConsumerRealization::Pending(id, relay) = &mut current.realization else {
                return Vec::new();
            };
            if *id != reservation.id {
                return Vec::new();
            }
            let next = RouteRelay {
                route: target.relay_route_key(target_worker),
                activity: RelayRouteActivity::from_active(active),
            };
            if relay.as_ref() == Some(&next) {
                return Vec::new();
            }
            (relay.replace(next.clone()), next)
        };
        self.replace_relay(&reservation.key, previous, &relay)
    }

    pub(super) fn source_activity_target_workers<'a>(
        &'a self,
        source: &'a TransportSourceKey,
    ) -> impl Iterator<Item = MediaWorkerId> + 'a {
        self.relays
            .keys()
            .filter(move |route| &route.source == source)
            .map(|route| route.target_worker)
    }

    fn entry(&mut self, key: SubscriptionKey) -> &mut Subscription {
        self.by_receiver
            .entry(key.receiver.clone())
            .or_default()
            .insert(key.clone());
        self.entries.entry(key).or_default()
    }

    fn next_reservation(&mut self) -> RouteReservationId {
        self.next_reservation.0 += 1;
        self.next_reservation
    }

    fn current_mut(
        &mut self,
        key: &SubscriptionKey,
        source_id: PublishedSourceId,
    ) -> Option<&mut CurrentPublication> {
        let current = self.entries.get_mut(key)?.current.as_mut()?;
        (current.source_id == source_id).then_some(current)
    }

    fn current_mut_for(
        &mut self,
        reservation: &ConsumerRouteReservation,
    ) -> Option<&mut CurrentPublication> {
        self.current_mut(&reservation.key, reservation.source_id)
    }

    fn take_pending(&mut self, reservation: &ConsumerRouteReservation) -> Option<TakenPending> {
        let current = self.current_mut_for(reservation)?;
        let pending = mem::take(&mut current.realization);
        match pending {
            ConsumerRealization::Pending(id, relay) if id == reservation.id => {
                Some(TakenPending { relay })
            }
            other => {
                current.realization = other;
                None
            }
        }
    }

    fn collect_removed_realization(
        &mut self,
        key: &SubscriptionKey,
        realization: ConsumerRealization,
        removed: &mut RemovedRoutes,
    ) {
        let relay = match realization {
            ConsumerRealization::Absent => None,
            ConsumerRealization::Pending(_, relay) => relay,
            ConsumerRealization::Committed(committed) => {
                removed.routes.push(committed.route);
                removed.consumers.push(committed.routed);
                committed.relay
            }
        };
        if let Some(relay) = relay {
            removed.relays.extend(self.release_relay(key, &relay));
        }
    }

    fn replace_relay(
        &mut self,
        key: &SubscriptionKey,
        previous: Option<RouteRelay>,
        relay: &RouteRelay,
    ) -> Vec<TransportRelayRouteEffect> {
        match previous {
            None => self.set_relay_owner(key, relay, true),
            Some(previous) if previous.route == relay.route => {
                self.set_relay_owner(key, relay, false)
            }
            Some(previous) => {
                let mut effects = self.release_relay(key, &previous);
                effects.extend(self.set_relay_owner(key, relay, true));
                effects
            }
        }
    }

    fn set_relay_owner(
        &mut self,
        key: &SubscriptionKey,
        relay: &RouteRelay,
        insert_missing: bool,
    ) -> Vec<TransportRelayRouteEffect> {
        let route = relay.route.clone();
        let owners = match self.relays.entry(route.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) if insert_missing => entry.insert(RelayOwners::default()),
            Entry::Vacant(_) => return Vec::new(),
        };
        let before = relay_aggregate(owners);
        owners.insert(key.clone(), relay.activity);
        relay_effects_for(route, before, relay_aggregate(owners))
    }

    fn release_relay(
        &mut self,
        key: &SubscriptionKey,
        relay: &RouteRelay,
    ) -> Vec<TransportRelayRouteEffect> {
        let route = relay.route.clone();
        let Some(owners) = self.relays.get_mut(&route) else {
            return Vec::new();
        };
        let before = relay_aggregate(owners);
        if owners.remove(key).is_none() {
            return Vec::new();
        }
        let after = relay_aggregate(owners);
        if after.is_none() {
            self.relays.remove(&route);
        }
        relay_effects_for(route, before, after)
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

impl ConsumerRealization {
    fn set_relay_activity(&mut self, activity: RelayRouteActivity) -> Option<RouteRelay> {
        let relay = match self {
            Self::Absent => return None,
            Self::Pending(_, relay) => relay.as_mut()?,
            Self::Committed(committed) => committed.relay.as_mut()?,
        };
        if relay.activity == activity {
            return None;
        }
        relay.activity = activity;
        Some(relay.clone())
    }
}

fn relay_aggregate(owners: &RelayOwners) -> Option<RelayRouteActivity> {
    (!owners.is_empty()).then(|| {
        RelayRouteActivity::from_active(owners.values().copied().any(RelayRouteActivity::is_active))
    })
}

fn relay_effects_for(
    route: RelayRouteKey,
    before: Option<RelayRouteActivity>,
    after: Option<RelayRouteActivity>,
) -> Vec<TransportRelayRouteEffect> {
    let Some(activity) = after else {
        return vec![TransportRelayRouteEffect {
            source: route.source,
            target_media_worker_id: route.target_worker,
            action: TransportRelayRouteAction::Release,
        }];
    };
    let mut effects = Vec::new();
    if before.is_none() {
        effects.push(TransportRelayRouteEffect {
            source: route.source.clone(),
            target_media_worker_id: route.target_worker,
            action: TransportRelayRouteAction::Install,
        });
    }
    if before.unwrap_or(RelayRouteActivity::Inactive) != activity {
        effects.push(TransportRelayRouteEffect {
            source: route.source,
            target_media_worker_id: route.target_worker,
            action: TransportRelayRouteAction::SetActivity(activity),
        });
    }
    effects
}
