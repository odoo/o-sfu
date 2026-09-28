//! Room-local routed topology and media placement lifetimes.
//!
//! [`Router`] coordinates user sessions across one or more routers. Each user
//! holds a **Home Session** on their assigned placement router where their published
//! producers live.
//!
//! # Cross-Router Routing & Foreign Shadows
//!
//! When User 2 (placed on Router 2) subscribes to User 1's producer (published on Router 1):
//! 1. Producer P1 lives on Router 1.
//! 2. Router 1 uses a **Foreign Session Shadow** for User 2 (using User 2's connection ID),
//!    creating it only when absent.
//! 3. Consumer C2 is created inside this shadow session on Router 1, attached directly to Producer P1.
//! 4. The router records the C2-to-P1 dependency and `o-sfu-core` forwards to User 2's media worker.
//!
//! ```text
//!   RouterId 1 (Publisher Placement)                RouterId 2 (Subscriber Placement)
//! +------------------------------------+          +------------------------------------+
//! | Home Session (User 1):             |          | Home Session (User 2):             |
//! |   Producer P1 dependent IDs:       |          |   Connection Conn_2                |
//! |     |                              |          |     ^                              |
//! |     +--> C1 (User 3 home session)  |          |     |                              |
//! |     |                              |          |     |                              |
//! |     +--> C2 dependency             |          |     |                              |
//! |          (Conn_2 foreign shadow)   | -------->|-----+ (Core forwards to worker)    |
//! +------------------------------------+          +------------------------------------+
//!
//! Lifecycle & Cascade Teardowns:
//!   - Producer P1 Removed (`Router::remove_producer`):
//!     -> Cascades down to remove local C1 and foreign C2 on Router 1.
//!     -> The shadow session for Conn_2 becomes empty and is automatically pruned.
//!   - User 2 Disconnects / Replaces Connection:
//!     -> Removes User 2's Home Session on Router 2.
//!     -> Removes C2 from P1's dependents and prunes the foreign shadow on Router 1.
//!     -> User 1's Producer P1 and local Consumer C1 remain active and unaffected.
//! ```

use std::{
    collections::{BTreeMap, BTreeSet},
    iter, mem,
};

use o_sfu_model::UserId;

use crate::model::{
    ConnectionId, ConsumerId, MediaCapabilities, MediaWorkerId, ProducerId, RouterError, RouterId,
};

#[cfg(test)]
#[path = "../TESTS/topology_support.rs"]
pub(crate) mod test_support;

/// producer identity plus its source router and connection
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RoutedProducerId(RouterId, ConnectionId, ProducerId);

impl RoutedProducerId {
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub const fn for_test(
        router: RouterId,
        connection: ConnectionId,
        producer: ProducerId,
    ) -> Self {
        Self(router, connection, producer)
    }

    #[must_use]
    pub const fn router_id(self) -> RouterId {
        self.0
    }

    #[must_use]
    pub const fn connection_id(self) -> ConnectionId {
        self.1
    }

    #[must_use]
    pub const fn producer_id(self) -> ProducerId {
        self.2
    }
}

/// consumer identity plus its source router and receiver connection
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RoutedConsumerId(RouterId, ConnectionId, ConsumerId);

impl RoutedConsumerId {
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub const fn for_test(
        router: RouterId,
        connection: ConnectionId,
        consumer: ConsumerId,
    ) -> Self {
        Self(router, connection, consumer)
    }

    #[must_use]
    pub const fn router_id(self) -> RouterId {
        self.0
    }

    #[must_use]
    pub const fn connection_id(self) -> ConnectionId {
        self.1
    }

    #[must_use]
    pub const fn consumer_id(self) -> ConsumerId {
        self.2
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouterPlacement {
    pub router: RouterId,
    pub media_worker: MediaWorkerId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterPlacements {
    primary: RouterPlacement,
    spillover: Vec<RouterPlacement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouterPlacementsError {
    Empty,
}

impl RouterPlacements {
    #[must_use]
    pub fn new(primary: RouterPlacement, spillover: Vec<RouterPlacement>) -> Self {
        let mut placements = Self {
            primary,
            spillover: Vec::new(),
        };
        for placement in spillover {
            if placement.router != primary.router {
                placements.upsert(placement);
            }
        }
        placements
    }

    /// # Errors
    ///
    /// returns [`RouterPlacementsError::Empty`] when `placements` is empty
    pub fn try_from_vec(placements: Vec<RouterPlacement>) -> Result<Self, RouterPlacementsError> {
        let mut placements = placements.into_iter();
        let Some(primary) = placements.next() else {
            return Err(RouterPlacementsError::Empty);
        };
        Ok(Self::new(primary, placements.collect()))
    }

    #[must_use]
    pub const fn primary(&self) -> RouterPlacement {
        self.primary
    }

    fn upsert(&mut self, placement: RouterPlacement) {
        if self.primary.router == placement.router {
            self.primary = placement;
            return;
        }
        if let Some(existing) = self
            .spillover
            .iter_mut()
            .find(|existing| existing.router == placement.router)
        {
            *existing = placement;
        } else {
            self.spillover.push(placement);
        }
    }

    fn iter(&self) -> impl Iterator<Item = RouterPlacement> + '_ {
        iter::once(self.primary).chain(self.spillover.iter().copied())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementSnapshot {
    primary: RouterId,
    placements: Option<Vec<RouterPlacement>>,
}

impl PlacementSnapshot {
    #[must_use]
    pub const fn primary(&self) -> RouterId {
        self.primary
    }

    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn next_router(&self) -> RouterId {
        let router = self
            .placements
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|placement| placement.router.0)
            .max()
            .map_or(self.primary.0, |router| router.saturating_add(1));
        RouterId(router)
    }

    #[must_use]
    pub fn assigned_placements(&self) -> &[RouterPlacement] {
        self.placements.as_deref().unwrap_or_default()
    }
}

/// pure room router for placement and routed media lifetimes
#[derive(Debug)]
pub struct Router {
    primary: RouterId,
    placements: Option<RouterPlacements>,
    capabilities: MediaCapabilities,
    routers: BTreeMap<RouterId, LocalRouter>,
    sessions: BTreeMap<UserId, CommittedSession>,
    users: BTreeMap<ConnectionId, UserId>,
}

#[derive(Debug, Clone, Copy)]
struct CommittedSession {
    connection: ConnectionId,
    placement: RouterPlacement,
}

#[derive(Debug, Default)]
struct LocalRouter {
    sessions: BTreeMap<ConnectionId, LocalSession>,
}

#[derive(Debug, Default)]
struct LocalSession {
    producers: BTreeMap<ProducerId, BTreeSet<RoutedConsumerId>>,
    consumers: BTreeMap<ConsumerId, RoutedProducerId>,
}

impl LocalSession {
    fn is_empty(&self) -> bool {
        self.producers.is_empty() && self.consumers.is_empty()
    }
}

impl Router {
    #[must_use]
    pub fn new(primary: RouterId, capabilities: MediaCapabilities) -> Self {
        let mut routers = BTreeMap::new();
        routers.insert(primary, LocalRouter::default());
        Self {
            primary,
            placements: None,
            capabilities,
            routers,
            sessions: BTreeMap::new(),
            users: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_placements(placements: RouterPlacements, capabilities: MediaCapabilities) -> Self {
        let mut router = Self::new(placements.primary().router, capabilities);
        router.placements = Some(placements);
        router
    }

    #[must_use]
    pub const fn rtp_capabilities(&self) -> &MediaCapabilities {
        &self.capabilities
    }

    #[must_use]
    pub fn committed_media_worker_id(
        &self,
        user: &UserId,
        connection: ConnectionId,
    ) -> Option<MediaWorkerId> {
        let session = self.sessions.get(user)?;
        (session.connection == connection).then_some(session.placement.media_worker)
    }

    /// commit one user connection to a router placement
    ///
    /// # Errors
    ///
    /// returns an error when the connection is occupied or when the placement
    /// contradicts an existing router-to-worker assignment
    pub fn commit_session_placement(
        &mut self,
        user: &UserId,
        connection: ConnectionId,
        placement: RouterPlacement,
    ) -> Result<MediaWorkerId, RouterError> {
        if self.users.contains_key(&connection) {
            return Err(RouterError::DuplicateConnection(connection));
        }
        self.validate_placement(placement)?;
        if self.sessions.contains_key(user) {
            self.remove_session(user)?;
        }
        self.attach_placement(placement);
        let local = self
            .routers
            .get_mut(&placement.router)
            .ok_or(RouterError::MissingRouter(placement.router))?;
        local.sessions.insert(connection, LocalSession::default());
        self.sessions.insert(
            user.clone(),
            CommittedSession {
                connection,
                placement,
            },
        );
        self.users.insert(connection, user.clone());
        Ok(placement.media_worker)
    }

    pub fn retire_committed_placement(
        &mut self,
        user: &UserId,
        connection: ConnectionId,
    ) -> Option<MediaWorkerId> {
        let worker = self.committed_media_worker_id(user, connection)?;
        self.remove_session(user).ok()?;
        Some(worker)
    }

    /// # Panics
    ///
    /// panics when `connection` has no committed placement
    #[expect(
        clippy::unreachable,
        reason = "route planning requires a committed connection placement"
    )]
    #[must_use]
    pub fn media_worker_id_for_connection(&self, connection: ConnectionId) -> MediaWorkerId {
        let Some(user) = self.users.get(&connection) else {
            unreachable!("media worker lookup requires committed connection placement");
        };
        let Some(session) = self.sessions.get(user) else {
            unreachable!("connection owner must have a committed session");
        };
        session.placement.media_worker
    }

    #[must_use]
    pub fn primary_worker(&self) -> Option<MediaWorkerId> {
        self.placements
            .as_ref()
            .map(|placements| placements.primary().media_worker)
    }

    pub fn assigned_media_workers(&self) -> impl Iterator<Item = MediaWorkerId> + '_ {
        self.placements
            .iter()
            .flat_map(RouterPlacements::iter)
            .map(|placement| placement.media_worker)
    }

    #[must_use]
    pub fn placement_snapshot(&self) -> PlacementSnapshot {
        let placements = self
            .placements
            .as_ref()
            .map(|placements| placements.iter().collect());
        PlacementSnapshot {
            primary: self.primary,
            placements,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn router_count(&self) -> usize {
        self.routers.len()
    }

    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn consumer_dependency_count(&self) -> usize {
        self.routers
            .values()
            .flat_map(|router| router.sessions.values())
            .flat_map(|session| session.producers.values())
            .map(BTreeSet::len)
            .sum()
    }

    /// create a producer on the user's home router
    ///
    /// # Errors
    ///
    /// returns a missing session or duplicate producer error
    pub fn add_producer(
        &mut self,
        user: &UserId,
        producer: ProducerId,
    ) -> Result<RoutedProducerId, RouterError> {
        let session = *self.require_session(user)?;
        let routed = RoutedProducerId(session.placement.router, session.connection, producer);
        let local = self
            .routers
            .get_mut(&routed.router_id())
            .ok_or(RouterError::MissingRouter(routed.router_id()))?
            .sessions
            .get_mut(&routed.connection_id())
            .ok_or_else(|| RouterError::MissingSession(user.clone()))?;
        if local.producers.contains_key(&producer) {
            return Err(RouterError::DuplicateProducer(producer));
        }
        local.producers.insert(producer, BTreeSet::new());
        Ok(routed)
    }

    /// create a consumer on its producer's source router
    ///
    /// # Errors
    ///
    /// returns a missing session, router, producer or duplicate consumer error
    pub fn add_consumer(
        &mut self,
        user: &UserId,
        consumer: ConsumerId,
        producer: RoutedProducerId,
    ) -> Result<RoutedConsumerId, RouterError> {
        let receiver = *self.require_session(user)?;
        let local = self
            .routers
            .get_mut(&producer.router_id())
            .ok_or(RouterError::MissingRouter(producer.router_id()))?;
        if local
            .sessions
            .get(&producer.connection_id())
            .and_then(|session| session.producers.get(&producer.producer_id()))
            .is_none()
        {
            return Err(RouterError::MissingProducer(producer));
        }
        if local
            .sessions
            .get(&receiver.connection)
            .is_some_and(|session| session.consumers.contains_key(&consumer))
        {
            return Err(RouterError::DuplicateConsumer(consumer));
        }

        let routed = RoutedConsumerId(producer.router_id(), receiver.connection, consumer);
        local
            .sessions
            .entry(receiver.connection)
            .or_default()
            .consumers
            .insert(consumer, producer);
        let producer_session = local
            .sessions
            .get_mut(&producer.connection_id())
            .ok_or(RouterError::MissingProducer(producer))?;
        let consumers = producer_session
            .producers
            .get_mut(&producer.producer_id())
            .ok_or(RouterError::MissingProducer(producer))?;
        consumers.insert(routed);
        Ok(routed)
    }

    /// remove one consumer and its empty foreign receiver session
    ///
    /// # Errors
    ///
    /// returns a missing router or consumer error
    pub fn remove_consumer(&mut self, consumer: RoutedConsumerId) -> Result<(), RouterError> {
        let local = self
            .routers
            .get_mut(&consumer.router_id())
            .ok_or(RouterError::MissingRouter(consumer.router_id()))?;
        let producer = local
            .sessions
            .get_mut(&consumer.connection_id())
            .and_then(|session| session.consumers.remove(&consumer.consumer_id()))
            .ok_or(RouterError::MissingConsumer(consumer))?;
        if let Some(dependents) = local
            .sessions
            .get_mut(&producer.connection_id())
            .and_then(|session| session.producers.get_mut(&producer.producer_id()))
        {
            dependents.remove(&consumer);
        }
        self.prune_foreign_session(consumer.router_id(), consumer.connection_id());
        Ok(())
    }

    /// remove one producer and every dependent consumer
    ///
    /// # Errors
    ///
    /// returns a missing router or producer error
    pub fn remove_producer(&mut self, producer: RoutedProducerId) -> Result<(), RouterError> {
        let local = self
            .routers
            .get_mut(&producer.router_id())
            .ok_or(RouterError::MissingRouter(producer.router_id()))?;
        let consumers = local
            .sessions
            .get_mut(&producer.connection_id())
            .and_then(|session| session.producers.remove(&producer.producer_id()))
            .ok_or(RouterError::MissingProducer(producer))?;
        self.remove_dependents(producer.router_id(), consumers);
        Ok(())
    }

    /// remove a user's home session, foreign sessions and routed media
    ///
    /// # Errors
    ///
    /// returns a missing session or router error
    pub fn remove_session(&mut self, user: &UserId) -> Result<(), RouterError> {
        let session = *self.require_session(user)?;
        let mut producers = mem::take(
            &mut self
                .routers
                .get_mut(&session.placement.router)
                .ok_or(RouterError::MissingRouter(session.placement.router))?
                .sessions
                .get_mut(&session.connection)
                .ok_or_else(|| RouterError::MissingSession(user.clone()))?
                .producers,
        );
        for consumers in producers.values_mut() {
            self.remove_dependents(session.placement.router, mem::take(consumers));
        }
        for local in self.routers.values_mut() {
            let Some(removed) = local.sessions.remove(&session.connection) else {
                continue;
            };
            for (consumer, producer) in &removed.consumers {
                let routed = RoutedConsumerId(producer.router_id(), session.connection, *consumer);
                if let Some(dependents) = local
                    .sessions
                    .get_mut(&producer.connection_id())
                    .and_then(|source| source.producers.get_mut(&producer.producer_id()))
                {
                    dependents.remove(&routed);
                }
            }
        }
        self.sessions.remove(user);
        self.users.remove(&session.connection);
        Ok(())
    }

    fn remove_dependents(&mut self, router: RouterId, consumers: BTreeSet<RoutedConsumerId>) {
        for consumer in consumers {
            if let Some(session) = self
                .routers
                .get_mut(&router)
                .and_then(|local| local.sessions.get_mut(&consumer.connection_id()))
            {
                session.consumers.remove(&consumer.consumer_id());
            }
            self.prune_foreign_session(router, consumer.connection_id());
        }
    }

    fn attach_placement(&mut self, placement: RouterPlacement) {
        match &mut self.placements {
            Some(placements) => placements.upsert(placement),
            None => self.placements = Some(RouterPlacements::new(placement, Vec::new())),
        }
        self.routers.entry(placement.router).or_default();
    }

    fn validate_placement(&self, placement: RouterPlacement) -> Result<(), RouterError> {
        let Some(placements) = &self.placements else {
            return if placement.router == self.primary {
                Ok(())
            } else {
                Err(RouterError::PrimaryRouterMismatch {
                    expected: self.primary,
                    actual: placement.router,
                })
            };
        };
        if let Some(existing) = placements
            .iter()
            .find(|existing| existing.router == placement.router)
            && existing.media_worker != placement.media_worker
        {
            return Err(RouterError::MediaWorkerMismatch {
                router: placement.router,
                expected: existing.media_worker,
                actual: placement.media_worker,
            });
        }
        Ok(())
    }

    fn require_session(&self, user: &UserId) -> Result<&CommittedSession, RouterError> {
        self.sessions
            .get(user)
            .ok_or_else(|| RouterError::MissingSession(user.clone()))
    }

    fn prune_foreign_session(&mut self, router: RouterId, connection: ConnectionId) {
        let home = self
            .users
            .get(&connection)
            .and_then(|user| self.sessions.get(user))
            .map(|session| session.placement.router);
        if home == Some(router) {
            return;
        }
        let Some(local) = self.routers.get_mut(&router) else {
            return;
        };
        if local
            .sessions
            .get(&connection)
            .is_some_and(LocalSession::is_empty)
        {
            local.sessions.remove(&connection);
        }
    }
}
