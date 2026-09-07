//! Room publications and subscriptions share transport-placement authority.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use o_sfu_router::{
    MediaKind, ProducerId, Router, RouterError,
    rtp::{MediaCapabilities, MediaStream as RouterRtpParameters},
};
use tracing::{error, warn};

use super::{
    CommittedConsumerSetup, ConsumerId, ConsumerRouteView, ConsumerSetupTarget,
    DeclaredConsumerSetup, PendingConsumerRouteView, PendingConsumerSetup, PublishedSource,
    ReceiverRouteActivity, SubscriptionKey, ValidatedPublish,
    producer::{PublicationCommitError, allocate_source_descriptor},
    route_graph::{CurrentPublication, PendingUpgrade, RemovedRoutes, RouteGraph},
    source_index::PublishedSources,
};
use crate::engine::{
    ConnectionId, MediaWorkerId, RoomInstanceId, UserId,
    media_transport::{
        SessionUploadEncoding, SourceActivityRevision, SourceActivityUpdate,
        TransportConsumerRoute, TransportMediaId, TransportRelayRouteEffect, TransportSessionKey,
        TransportSourceActivityEffect, TransportSourceKey, TransportTeardown,
    },
    room::{
        RoomMediaCounts, RoomRuntimeContext, RouterPlacement,
        effects::transport::RoomTransportPlan, outbound::OutboundSender,
    },
    source_model::{
        ActiveSpeakerSourceRole, ConsumerSourceSelection, PolicyPauseReason,
        PublishedSourceDescriptor, PublishedSourceId, SourceSubscriptionIntent, UserStreamId,
    },
};

#[cfg(test)]
#[path = "TESTS/topology_support.rs"]
mod topology_support;

/// Keeps source records and route realization beside [`Router`] so transport
/// effects resolve from the same committed placement.
///
/// Transport-facing mutations return resolved work for execution after the room
/// state lock is released.
#[derive(Debug)]
pub struct RoomTopology {
    instance: RoomInstanceId,
    sources: PublishedSources,
    route_graph: RouteGraph,
    router: Router,
    next_producer_id: u64,
    // Revision of committed video allocation inputs across transport awaits.
    video_allocation_revision: u64,
}

/// Receipt acknowledging that [`RoomTopology`] committed a session placement.
///
/// It snapshots the committed connection identity and worker-resolved transport
/// key across the room-state lock boundary. Placement lifetime remains controlled by
/// [`RoomTopology::commit_session_placement`], [`RoomTopology::remove_session`]
/// or [`RoomTopology::retire_committed_placement`].
///
/// # Admission handoff
///
/// Membership keeps the receipt while
/// [`RoomEffects`](crate::engine::room::effects::batch::RoomEffects) consumes the
/// join commit:
///
/// ```rust,ignore
/// let commit = admission.commit(self, joined_fanout).await?;
/// let receipt = commit.receipt.clone();
///
/// RoomEffects::from_join(commit).execute(self, context).await;
/// Ok(receipt)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedTransportReceipt {
    /// Transport identity resolved from the committed media worker placement.
    pub transport_session_key: TransportSessionKey,
}

/// The new placement is authoritative before displaced-session cleanup is returned.
///
/// Bundling the receipt with resolved cleanup lets membership release `room.state`
/// without looking up the displaced placement again.
#[derive(Debug)]
pub struct SessionPlacementCommit {
    pub receipt: CommittedTransportReceipt,
    /// Empty for a first placement.
    pub replacement_transport_plan: RoomTransportPlan,
}

#[derive(Debug)]
pub(in crate::engine::room) struct ConsumerActivityCommit {
    pub(super) update: Option<ReceiverRouteActivity>,
    pub(super) relay_effects: Vec<TransportRelayRouteEffect>,
}

#[derive(Debug)]
pub enum SessionPlacementRejection {
    MissingPreviousSession { previous_connection: ConnectionId },
    Router(RouterError),
}

impl RoomTopology {
    pub fn new(
        runtime_context: &RoomRuntimeContext,
        router_rtp_capabilities: MediaCapabilities,
    ) -> Self {
        let router = match runtime_context.initial_router_placements() {
            Some(placements) => {
                Router::with_placements(placements.clone(), router_rtp_capabilities)
            }
            None => Router::new(runtime_context.primary_router(), router_rtp_capabilities),
        };
        Self {
            instance: runtime_context.instance(),
            sources: PublishedSources::default(),
            route_graph: RouteGraph::default(),
            router,
            next_producer_id: 1,
            video_allocation_revision: 0,
        }
    }

    pub(in crate::engine::room) const fn video_allocation_revision(&self) -> u64 {
        self.video_allocation_revision
    }

    fn invalidate_video_allocation(&mut self) {
        self.video_allocation_revision = self.video_allocation_revision.wrapping_add(1);
    }

    pub(in crate::engine::room) fn router(&self) -> &Router {
        &self.router
    }

    /// Returns `None` unless the exact user connection remains committed.
    #[must_use]
    pub fn committed_transport_user_key(
        &self,
        user_id: impl Into<Arc<UserId>>,
        connection_id: ConnectionId,
    ) -> Option<TransportSessionKey> {
        let user_id = user_id.into();
        let worker = self
            .router
            .committed_media_worker_id(user_id.as_ref(), connection_id)?;
        Some(self.transport_session_key(user_id, connection_id, worker))
    }

    /// Requires the exact user connection to remain committed.
    ///
    /// Use [`Self::committed_transport_user_key`] for stale callbacks or teardown
    /// races where the placement may already be retired.
    ///
    /// # Panics
    ///
    /// Panics when no committed router placement exists.
    #[must_use]
    #[expect(
        clippy::unreachable,
        reason = "current room operations require committed connection placement and must not synthesize a transport worker"
    )]
    pub fn transport_user_key(
        &self,
        user_id: impl Into<Arc<UserId>>,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        let user_id = user_id.into();
        let Some(worker) = self
            .router
            .committed_media_worker_id(user_id.as_ref(), connection_id)
        else {
            unreachable!("transport session key lookup requires committed connection placement");
        };
        self.transport_session_key(user_id, connection_id, worker)
    }

    fn transport_session_key(
        &self,
        user_id: Arc<UserId>,
        connection_id: ConnectionId,
        media_worker_id: MediaWorkerId,
    ) -> TransportSessionKey {
        TransportSessionKey::new(self.instance, media_worker_id, connection_id, user_id)
    }

    /// Returns `None` when `connection_id` is not the user's committed placement.
    pub fn retire_committed_placement(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<TransportSessionKey> {
        let media_worker = self
            .router
            .retire_committed_placement(user_id, connection_id)?;
        Some(self.transport_session_key(user_id.clone().into(), connection_id, media_worker))
    }

    /// Counts each logical subscription once while pending or committed.
    #[must_use]
    pub fn media_counts(&self) -> RoomMediaCounts {
        RoomMediaCounts {
            publications: self.sources.publication_count(),
            subscriptions: self.route_graph.subscription_count(),
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::engine::room) fn consumer_count(&self) -> usize {
        self.route_graph.count()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::engine::room) fn first_published_transport_media_id(
        &self,
    ) -> Option<TransportMediaId> {
        self.sources.first_transport_media_id()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::engine::room) fn producer_transport_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<TransportMediaId> {
        self.sources
            .transport_media_id(user_id, connection_id, stream_id)
    }

    #[must_use]
    pub(in crate::engine::room) fn source_descriptor(
        &self,
        source_id: PublishedSourceId,
    ) -> Option<&PublishedSourceDescriptor> {
        self.sources
            .source(source_id)
            .map(|source| &source.descriptor)
    }

    #[must_use]
    pub(in crate::engine::room) fn source_for_transport_media(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&PublishedSource> {
        self.sources.source_for_transport(transport_media_id)
    }

    #[must_use]
    pub(in crate::engine::room) fn source_id_for_owner_stream(
        &self,
        owner_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<PublishedSourceId> {
        self.sources.id_for_owner_stream(owner_user_id, stream_id)
    }

    /// Returns the source ID only when owner, connection and stream remain current.
    #[must_use]
    pub(in crate::engine::room) fn published_source_id(
        &self,
        owner: &UserId,
        connection: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<PublishedSourceId> {
        let id = self.sources.id_for_owner_stream(owner, stream_id)?;
        let source = self.sources.source(id)?;
        (source.transport.session_key().connection_id() == connection).then_some(id)
    }

    /// Iterates committed sources in source-ID order.
    pub(in crate::engine::room) fn published_sources(
        &self,
    ) -> impl Iterator<Item = &PublishedSource> {
        self.sources.iter()
    }

    /// Counts distinct users with active publications for each logical stream.
    pub(in crate::engine::room) fn active_stream_user_counts(&self) -> BTreeMap<UserStreamId, u64> {
        let mut users_by_stream: BTreeMap<UserStreamId, BTreeSet<UserId>> = BTreeMap::new();
        for source in self.sources.iter().filter(|source| source.active) {
            users_by_stream
                .entry(source.descriptor.stream_id().clone())
                .or_default()
                .insert(source.descriptor.owner().user_id().clone());
        }
        users_by_stream
            .into_iter()
            .map(|(stream_id, users)| (stream_id, u64::try_from(users.len()).unwrap_or(u64::MAX)))
            .collect()
    }

    /// Returns a detector owner with an active promotable source in the same group.
    #[must_use]
    pub(in crate::engine::room) fn active_speaker_detector_owner(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<UserId> {
        let source = self.source_for_transport_media(transport_media_id)?;
        let detector_policy = source.descriptor.policy().active_speaker()?;
        if detector_policy.role() != ActiveSpeakerSourceRole::Detector {
            return None;
        }
        let owner = source.descriptor.owner().user_id();
        self.sources
            .owner_has_promotable_source_in_group(owner, detector_policy.group())
            .then(|| owner.clone())
    }

    pub(in crate::engine::room) fn committed_consumer_routes(
        &self,
    ) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.route_graph
            .attached()
            .filter_map(|(key, current)| self.consumer_route(key, current))
    }

    pub(in crate::engine::room) fn committed_consumer_routes_for_user(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.route_graph
            .attached_for_receiver(user_id)
            .filter_map(|(key, current)| self.consumer_route(key, current))
    }

    pub(super) fn detach_declined_consumers(
        &mut self,
        session: &TransportSessionKey,
        declined: &[TransportMediaId],
    ) -> (Vec<TransportRelayRouteEffect>, Vec<TransportTeardown>, bool) {
        let RemovedRoutes {
            routes,
            consumers,
            relays,
        } = self
            .route_graph
            .detach_declined_consumers(session, declined);
        let detached = !routes.is_empty();
        if detached {
            self.invalidate_video_allocation();
        }
        for consumer in consumers {
            if let Some(error) = self.router.remove_consumer(consumer).err() {
                error!(?consumer, ?error, "failed to remove declined room consumer");
            }
        }
        let teardown = Self::media_teardowns([], routes).collect();
        (relays, teardown, detached)
    }

    pub(in crate::engine::room) fn pending_consumer_routes_for_user(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = PendingConsumerRouteView<'_>> {
        self.route_graph
            .attached_for_receiver(user_id)
            .filter(|(_, current)| current.is_pending())
            .filter_map(|(_, current)| {
                let source = self.sources.source(current.source_id)?;
                Some(PendingConsumerRouteView {
                    source,
                    selection: current.selection,
                })
            })
    }

    /// Returns `None` when the attached source differs from `source_id`.
    #[must_use]
    pub(in crate::engine::room) fn consumer_source_selection(
        &self,
        key: &SubscriptionKey,
        source_id: PublishedSourceId,
    ) -> Option<ConsumerSourceSelection> {
        self.route_graph.selection(key, source_id)
    }

    /// Returns merged receiver intent or the default for a missing subscription.
    #[must_use]
    pub(in crate::engine::room) fn subscription_intent(
        &self,
        key: &SubscriptionKey,
    ) -> SourceSubscriptionIntent {
        self.route_graph.intent(key)
    }

    pub(in crate::engine::room) fn committed_consumer_user_ids_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> BTreeSet<UserId> {
        self.route_graph
            .attached_for_source(source_id)
            .filter(|(_, current)| current.committed().is_some())
            .map(|(key, _)| key.receiver.clone())
            .collect()
    }

    pub(in crate::engine::room) fn committed_consumer_user_ids_for_owner_sources(
        &self,
        user_id: &UserId,
    ) -> BTreeSet<UserId> {
        let route_graph = &self.route_graph;
        self.sources
            .ids_for_owner(user_id)
            .flat_map(|source_id| route_graph.attached_for_source(source_id))
            .filter(|(_, current)| current.committed().is_some())
            .map(|(key, _)| key.receiver.clone())
            .collect()
    }

    #[must_use]
    pub(in crate::engine::room) fn committed_consumer_route_for_key(
        &self,
        key: &SubscriptionKey,
    ) -> Option<ConsumerRouteView<'_>> {
        let (key, current) = self.route_graph.current(key)?;
        self.consumer_route(key, current)
    }

    #[must_use]
    pub(in crate::engine::room) fn published_source(
        &self,
        source_id: PublishedSourceId,
    ) -> Option<&PublishedSource> {
        self.sources.source(source_id)
    }

    /// Attaches `source_id` to each eligible receiver lacking a route realization.
    pub(super) fn missing_consumer_targets_for_source<'a>(
        &mut self,
        source_id: PublishedSourceId,
        receivers: impl IntoIterator<Item = (&'a UserId, ConnectionId)>,
    ) -> Vec<ConsumerSetupTarget> {
        receivers
            .into_iter()
            .filter_map(|(user, connection)| self.consumer_target(user, connection, source_id))
            .collect()
    }

    fn consumer_target(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        source_id: PublishedSourceId,
    ) -> Option<ConsumerSetupTarget> {
        let consumer_session = self.committed_transport_user_key(user_id.clone(), connection_id)?;
        let (sources, route_graph) = (&self.sources, &mut self.route_graph);
        Self::attach_consumer_target(route_graph, &consumer_session, sources.source(source_id)?)
    }

    /// Skips self-consumption and attaches only an absent realization.
    fn attach_consumer_target(
        route_graph: &mut RouteGraph,
        consumer_session: &TransportSessionKey,
        source: &PublishedSource,
    ) -> Option<ConsumerSetupTarget> {
        if source.descriptor.owner().user_id() == consumer_session.user_id() {
            return None;
        }
        let target = ConsumerSetupTarget::new(consumer_session.clone(), source);
        route_graph
            .attach_for_setup(target.subscription_key(), target.source_id)
            .then_some(target)
    }

    /// Attaches each matching source with no realization to the exact committed receiver.
    ///
    /// Returns an empty list when `connection_id` is stale.
    pub(super) fn missing_consumer_targets(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        include_source: impl Fn(&PublishedSource) -> bool,
    ) -> Vec<ConsumerSetupTarget> {
        let Some(consumer_session) =
            self.committed_transport_user_key(user_id.clone(), connection_id)
        else {
            return Vec::new();
        };
        let (sources, route_graph) = (&self.sources, &mut self.route_graph);
        sources
            .iter()
            .filter(|source| include_source(source))
            .filter_map(|source| {
                Self::attach_consumer_target(route_graph, &consumer_session, source)
            })
            .collect()
    }

    /// Updates selection while subscription, source and exact route still match.
    ///
    /// Returns `false` when async transport work refers to a displaced route.
    pub fn update_consumer_source_selection(
        &mut self,
        key: &SubscriptionKey,
        source_id: PublishedSourceId,
        route: &TransportConsumerRoute,
        update: impl FnOnce(&mut ConsumerSourceSelection),
    ) -> bool {
        let updated = self
            .route_graph
            .update_selection(key, source_id, route, update);
        if updated {
            self.invalidate_video_allocation();
        }
        updated
    }

    /// Commits a hold only for the active source, subscription and exact route.
    pub(in crate::engine::room) fn update_consumer_upgrade(
        &mut self,
        key: &SubscriptionKey,
        source_id: PublishedSourceId,
        route: &TransportConsumerRoute,
        pending_upgrade: Option<PendingUpgrade>,
    ) -> bool {
        if !self
            .sources
            .source(source_id)
            .is_some_and(|source| source.active)
        {
            return false;
        }
        self.route_graph
            .update_upgrade(key, source_id, route, pending_upgrade)
    }

    fn detach_user_sources(
        &mut self,
        user_id: &UserId,
    ) -> (Vec<TransportSourceKey>, RemovedRoutes) {
        let mut sources = Vec::new();
        let mut removed = RemovedRoutes::default();
        let source_ids = self.sources.ids_for_owner(user_id).collect::<Vec<_>>();
        for source_id in source_ids {
            if let Some((source, routes)) = self.remove_source(source_id) {
                sources.push(source.transport);
                removed.extend(routes);
            }
        }
        (sources, removed)
    }

    fn remove_source(
        &mut self,
        source_id: PublishedSourceId,
    ) -> Option<(PublishedSource, RemovedRoutes)> {
        let source = self.sources.remove(source_id)?;
        let routes = self.route_graph.detach_source(source_id);
        self.invalidate_video_allocation();
        Some((source, routes))
    }

    fn consumer_route<'a>(
        &'a self,
        key: &'a SubscriptionKey,
        current: &'a CurrentPublication,
    ) -> Option<ConsumerRouteView<'a>> {
        let committed = current.committed()?;
        let source = self.sources.source(current.source_id)?;
        Some(ConsumerRouteView {
            key,
            route: &committed.route,
            mid: &committed.mid,
            pending_upgrade: committed.pending_upgrade.as_ref(),
            source,
            selection: current.selection,
        })
    }

    /// Commits a negotiated source only after its router producer succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`PublicationCommitError::Source`] when descriptor allocation or
    /// validation fails. Returns [`PublicationCommitError::Router`] when the
    /// router rejects the producer dependency.
    pub(in crate::engine::room) fn commit_publication(
        &mut self,
        publish: ValidatedPublish,
        rtp: RouterRtpParameters,
        encodings: &[SessionUploadEncoding],
        media: TransportMediaId,
    ) -> Result<PublishedSourceId, PublicationCommitError> {
        let descriptor = allocate_source_descriptor(&mut self.sources, &publish, &rtp, encodings)?;
        let source_id = descriptor.source_id();
        let producer_id = ProducerId::allocate(&mut self.next_producer_id);
        let routed = self
            .router
            .add_producer(publish.session_key.user_id(), producer_id)?;
        self.sources.insert(PublishedSource {
            descriptor,
            transport: TransportSourceKey::new(publish.session_key, media),
            rtp,
            routed,
            active: true,
            activity_revision: SourceActivityRevision::default(),
        });
        Ok(source_id)
    }

    fn media_teardowns(
        sources: impl IntoIterator<Item = TransportSourceKey>,
        routes: impl IntoIterator<Item = TransportConsumerRoute>,
    ) -> impl Iterator<Item = TransportTeardown> {
        sources
            .into_iter()
            .map(|source| TransportTeardown::RemoveMedia {
                session_key: source.session_key().clone(),
                transport_media_id: source.transport_media_id(),
            })
            .chain(
                routes
                    .into_iter()
                    .map(|route| TransportTeardown::RemoveMedia {
                        session_key: route.consumer_session_key().clone(),
                        transport_media_id: route.consumer_transport_media_id(),
                    }),
            )
    }

    /// Advances the revision only when the exact source connection changes activity.
    ///
    /// Returns `None` when the source is missing, the connection is stale or the
    /// requested activity already matches.
    pub fn set_published_source_activity(
        &mut self,
        source_id: PublishedSourceId,
        connection_id: ConnectionId,
        active: bool,
    ) -> Option<SourceActivityRevision> {
        let source = self.sources.source_mut(source_id)?;
        if source.transport.session_key().connection_id() != connection_id
            || source.active == active
        {
            return None;
        }
        source.active = active;
        source.activity_revision = source.activity_revision.next();
        let revision = source.activity_revision;
        if !active {
            self.route_graph.clear_source_upgrades(source_id);
        }
        self.invalidate_video_allocation();
        Some(revision)
    }

    pub(super) fn source_activity_effects(
        &self,
        source: &TransportSourceKey,
        update: SourceActivityUpdate,
    ) -> Vec<TransportSourceActivityEffect> {
        self.route_graph
            .source_activity_target_workers(source)
            .map(|target_media_worker_id| TransportSourceActivityEffect {
                source: source.clone(),
                target_media_worker_id,
                update,
            })
            .collect()
    }

    /// Commits the new placement before returning cleanup for `previous_connection`.
    ///
    /// Replacement joins must pass the currently committed `previous_connection`.
    ///
    /// # Errors
    ///
    /// Returns [`SessionPlacementRejection::MissingPreviousSession`] when the
    /// expected replacement target is no longer committed. Returns
    /// [`SessionPlacementRejection::Router`] when the router rejects the new
    /// connection or placement.
    pub fn commit_session_placement(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        previous_connection: Option<ConnectionId>,
        home_placement: RouterPlacement,
    ) -> Result<SessionPlacementCommit, SessionPlacementRejection> {
        // Preserve the displaced key before the router replaces its user mapping.
        let previous_session_key = if let Some(previous_connection) = previous_connection {
            let Some(key) = self.committed_transport_user_key(user_id.clone(), previous_connection)
            else {
                return Err(SessionPlacementRejection::MissingPreviousSession {
                    previous_connection,
                });
            };
            Some(key)
        } else {
            None
        };
        let media_worker = self
            .router
            .commit_session_placement(user_id, connection_id, home_placement)
            .map_err(SessionPlacementRejection::Router)?;
        let session_key =
            self.transport_session_key(user_id.clone().into(), connection_id, media_worker);
        let receipt = CommittedTransportReceipt {
            transport_session_key: session_key,
        };
        let replacement_transport_plan = previous_session_key.as_ref().map_or_else(
            RoomTransportPlan::default,
            |replaced_session_key| {
                let close_session = TransportTeardown::CloseSession {
                    session_key: replaced_session_key.clone(),
                };
                // Session close removes source media while subscriber routes need
                // explicit teardown.
                let (_, removed_sources) = self.detach_user_sources(user_id);
                let receiver_relays = self.route_graph.reset_receiver_for_replacement(user_id);
                let RemovedRoutes {
                    routes, mut relays, ..
                } = removed_sources;
                relays.extend(receiver_relays);
                let teardown = Self::media_teardowns([], routes).chain([close_session]);
                RoomTransportPlan::from_relays_and_teardown(relays, teardown)
            },
        );
        if previous_session_key.is_some() {
            self.invalidate_video_allocation();
        }
        Ok(SessionPlacementCommit {
            receipt,
            replacement_transport_plan,
        })
    }

    /// Returns the cleanup plan even if router removal fails.
    pub fn remove_session(&mut self, user_id: &UserId) -> RoomTransportPlan {
        let (sources, mut removed) = self.detach_user_sources(user_id);
        removed.extend(self.route_graph.remove_receiver(user_id));
        self.invalidate_video_allocation();
        let teardown = Self::media_teardowns(sources, removed.routes);
        if let Some(error) = self.router.remove_session(user_id).err() {
            error!(?user_id, ?error, "failed to remove user from room router");
        }
        RoomTransportPlan::from_relays_and_teardown(removed.relays, teardown)
    }

    /// Selects MID from the transport declaration, negotiated RTP MID then the
    /// consumer identity.
    ///
    /// # Errors
    ///
    /// Returns the declared route and relay release effects when the reservation
    /// is stale or the router rejects the consumer dependency.
    pub(super) fn commit_consumer_setup(
        &mut self,
        setup: DeclaredConsumerSetup,
        selection: ConsumerSourceSelection,
    ) -> Result<CommittedConsumerSetup, (TransportConsumerRoute, Vec<TransportRelayRouteEffect>)>
    {
        let DeclaredConsumerSetup {
            pending:
                PendingConsumerSetup {
                    target,
                    consumer,
                    reservation,
                    sender,
                    rtp,
                    relays: _,
                },
            route,
            mid,
        } = setup;
        let active = selection.delivery_active();
        let declared_active = reservation.declared_activity().is_active();
        let committed_mid = mid.unwrap_or_else(|| {
            rtp.mid()
                .map_or_else(|| consumer.to_string(), ToOwned::to_owned)
        });
        let (route_graph, room_router) = (&mut self.route_graph, &mut self.router);
        // Remove pending state first so router rejection cannot strand a reservation.
        let result =
            route_graph.commit(reservation, route.clone(), committed_mid, selection, || {
                match room_router.add_consumer(target.session.user_id(), consumer, target.routed) {
                    Ok(routed_consumer) => Some(routed_consumer),
                    Err(error) => {
                        warn!(
                            consumer_user_id = ?target.session.user_id(),
                            source_id = ?target.source_id,
                            ?error,
                            "router rejected consumer creation"
                        );
                        None
                    }
                }
            });
        if let Err(relays) = result {
            return Err((route, relays));
        }
        self.invalidate_video_allocation();
        Ok(CommittedConsumerSetup {
            target,
            route,
            sender,
            transport_activity_update: (active != declared_active).then_some(active),
        })
    }

    /// Returns `None` unless the route realization remains absent.
    ///
    /// A cross-worker reservation also claims relay ownership.
    pub(super) fn reserve_consumer_setup(
        &mut self,
        target: ConsumerSetupTarget,
        consumer: ConsumerId,
        selection: ConsumerSourceSelection,
        sender: OutboundSender,
        rtp: RouterRtpParameters,
    ) -> Option<PendingConsumerSetup> {
        let key = target.subscription_key();
        // Relay activity follows receiver intent rather than policy-gated delivery.
        // A temporary policy pause must remain resumable without rebuilding the
        // shared cross-worker source path.
        let relay_active = selection.active();
        let reservation =
            self.route_graph
                .reserve_consumer_setup(key, target.source_id, selection)?;
        let source_worker = target.source.session_key().media_worker_id();
        let target_worker = target.session.media_worker_id();
        let relays = if source_worker == target_worker {
            Vec::new()
        } else {
            self.route_graph
                .reserve_relay(&reservation, &target, target_worker, relay_active)
        };
        Some(PendingConsumerSetup {
            target,
            consumer,
            reservation,
            sender,
            rtp,
            relays,
        })
    }

    /// A stale reservation releases no relay ownership.
    pub(super) fn release_consumer_setup(
        &mut self,
        setup: PendingConsumerSetup,
    ) -> Vec<TransportRelayRouteEffect> {
        self.route_graph.release_consumer_setup(setup.reservation)
    }

    /// Merges sparse intent before projecting activity to an attached route.
    ///
    /// Empty updates have no effect. Intent survives absent publications and
    /// unready receivers. Returns no activity work for layout-only updates,
    /// missing sources, changed attachments or displaced consumer connections.
    pub(in crate::engine::room) fn apply_subscription_intent(
        &mut self,
        key: &SubscriptionKey,
        connection_id: ConnectionId,
        intent: SourceSubscriptionIntent,
        receiver_deafened: bool,
    ) -> Option<ConsumerActivityCommit> {
        if intent.is_empty() {
            return None;
        }
        self.route_graph.merge_intent(key.clone(), intent);
        self.invalidate_video_allocation();
        let active = intent.active()?;
        let source_id = self.source_id_for_owner_stream(&key.publisher, &key.stream)?;
        // Deafening pauses audio delivery without replacing explicit receiver intent.
        let policy_pause_reason = (active
            && receiver_deafened
            && self
                .source_descriptor(source_id)
                .is_some_and(|source| source.media_kind() == MediaKind::Audio))
        .then_some(PolicyPauseReason::ReceiverDeafened);
        let relay_effects = self.route_graph.set_activity(
            key,
            source_id,
            connection_id,
            active,
            policy_pause_reason,
        )?;
        let update = self
            .committed_consumer_route_for_key(key)
            .filter(|route| route.route.consumer_session_key().connection_id() == connection_id)
            .map(|route| {
                ReceiverRouteActivity::new(route.target(), route.selection.delivery_active())
            });
        Some(ConsumerActivityCommit {
            update,
            relay_effects,
        })
    }
}
