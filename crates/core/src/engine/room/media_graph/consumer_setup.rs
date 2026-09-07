use std::mem;

use o_sfu_router::{
    MediaKind as RouterMediaKind, rtp::MediaStream as RouterRtpParameters,
    topology::RoutedProducerId,
};
use tracing::warn;

use super::{
    super::{
        outbound::{OutboundSender, VersionedRemoteTrackSnapshot},
        state::RoomState,
    },
    ConsumerId, ConsumerRouteTarget, PublishedSource, SubscriptionKey,
    route_graph::{ConsumerRouteReservation, RelayRouteKey},
};
use crate::engine::{
    MediaWorkerId,
    media_transport::{
        MediaTransport, ProducerActivity, SourceActivityUpdate, TransportConsumerRoute,
        TransportMediaId, TransportRelayRouteEffect, TransportSessionKey,
        TransportSourceActivityEffect, TransportSourceKey,
    },
    source_model::{PublishedSourceId, UserStreamId},
};

#[derive(Debug)]
pub struct ConsumerSetupTarget {
    pub session: TransportSessionKey,
    pub source: TransportSourceKey,
    pub source_id: PublishedSourceId,
    pub stream: UserStreamId,
    pub kind: RouterMediaKind,
    pub routed: RoutedProducerId,
}

/// Consumer-route reservation awaiting transport declaration.
///
/// It carries graph and optional relay ownership across work performed without
/// the room lock. Callers must commit or release it after declaration.
#[derive(Debug)]
#[must_use = "pending consumer setups reserve route graph state and must be committed or released"]
pub struct PendingConsumerSetup {
    pub(super) target: ConsumerSetupTarget,
    pub(super) consumer: ConsumerId,
    pub(super) reservation: ConsumerRouteReservation,
    pub(super) sender: OutboundSender,
    pub(super) rtp: RouterRtpParameters,
    pub(super) relays: Vec<TransportRelayRouteEffect>,
}

pub struct DeclaredConsumerSetup {
    pub(super) pending: PendingConsumerSetup,
    pub(super) route: TransportConsumerRoute,
    pub(super) mid: Option<String>,
}

pub struct CommittedConsumerSetup {
    pub(super) target: ConsumerSetupTarget,
    pub(super) route: TransportConsumerRoute,
    pub(super) sender: OutboundSender,
    pub(super) transport_activity_update: Option<bool>,
}

#[expect(
    clippy::large_enum_variant,
    reason = "consumer setup outcomes are returned and matched immediately so boxing the committed setup would allocate on every successful consumer setup"
)]
#[derive(Debug)]
pub enum ConsumerSetupOutcome {
    Committed {
        target: ConsumerSetupTarget,
        route: TransportConsumerRoute,
        sender: OutboundSender,
        track_snapshot: VersionedRemoteTrackSnapshot,
        remote_source_activity: Option<TransportSourceActivityEffect>,
        transport_activity_update: Option<bool>,
        readiness_keyframe: Option<ConsumerRouteTarget>,
    },
    Released(TransportConsumerRoute, Vec<TransportRelayRouteEffect>),
}

#[derive(Debug, Clone, Copy)]
pub enum ConsumerSetupOrigin {
    Readiness,
    Publish,
    Subscribe,
}

impl ConsumerSetupOrigin {
    pub const fn as_diagnostic_str(self) -> &'static str {
        match self {
            Self::Readiness => "readiness",
            Self::Publish => "publish",
            Self::Subscribe => "subscribe",
        }
    }
}

impl RoomState {
    /// Commits a transport-declared consumer route.
    ///
    /// Returns [`ConsumerSetupOutcome::Released`] when receiver or source
    /// identity changed during declaration or the router rejects the dependency.
    pub fn commit_declared_consumer_setup(
        &mut self,
        setup: DeclaredConsumerSetup,
        origin: ConsumerSetupOrigin,
    ) -> ConsumerSetupOutcome {
        let target = &setup.pending.target;
        let session = &target.session;
        if self
            .user_for_connection(session.user_id(), session.connection_id())
            .is_some_and(|user| user.parsed_client_rtp_capabilities.is_some())
            && let Some((source_active, source_activity_revision)) = self
                .topology
                .published_source(target.source_id)
                .filter(|source| target.matches_identity(source))
                .map(|source| (source.active, source.activity_revision))
        {
            // Transport declaration ran without the room lock. Recompute from
            // current intent and policy before committing the pending route.
            let selection = self.setup_selection(target, source_active);
            let delivery_active = selection.delivery_active();
            match self.topology.commit_consumer_setup(setup, selection) {
                Ok(commit) => {
                    // A consumer on another media worker must be told the source
                    // activity because that worker does not host the producer.
                    let remote_source_activity =
                        (commit.route.source().session_key().media_worker_id()
                            != commit.route.consumer_session_key().media_worker_id())
                        .then(|| TransportSourceActivityEffect {
                            source: commit.route.source().clone(),
                            target_media_worker_id: commit
                                .route
                                .consumer_session_key()
                                .media_worker_id(),
                            update: SourceActivityUpdate::new(
                                ProducerActivity::from_active(source_active),
                                source_activity_revision,
                            ),
                        });
                    let track_snapshot =
                        self.remote_track_snapshot_for_user(commit.target.session.user_id(), true);
                    // A pending activity correction to active already requests a
                    // keyframe so avoid a duplicate readiness refresh.
                    let readiness_keyframe = match origin {
                        ConsumerSetupOrigin::Readiness
                            if delivery_active
                                && commit.transport_activity_update != Some(true)
                                && commit.target.kind == RouterMediaKind::Video =>
                        {
                            Some(commit.target.route_target(commit.route.clone()))
                        }
                        ConsumerSetupOrigin::Readiness
                        | ConsumerSetupOrigin::Publish
                        | ConsumerSetupOrigin::Subscribe => None,
                    };
                    ConsumerSetupOutcome::Committed {
                        target: commit.target,
                        route: commit.route,
                        sender: commit.sender,
                        track_snapshot,
                        remote_source_activity,
                        transport_activity_update: commit.transport_activity_update,
                        readiness_keyframe,
                    }
                }
                Err((route, relays)) => ConsumerSetupOutcome::Released(route, relays),
            }
        } else {
            let DeclaredConsumerSetup { pending, route, .. } = setup;
            ConsumerSetupOutcome::Released(route, self.topology.release_consumer_setup(pending))
        }
    }

    pub fn release_pending_consumer_setup(
        &mut self,
        setup: PendingConsumerSetup,
    ) -> Vec<TransportRelayRouteEffect> {
        self.topology.release_consumer_setup(setup)
    }
}

impl PendingConsumerSetup {
    pub(in crate::engine::room) fn take_relays(&mut self) -> Vec<TransportRelayRouteEffect> {
        mem::take(&mut self.relays)
    }

    pub(in crate::engine::room) async fn declare(
        self,
        media_transport: &MediaTransport,
        origin: ConsumerSetupOrigin,
    ) -> Result<DeclaredConsumerSetup, Self> {
        match media_transport
            .consume_media(
                &self.target.session,
                self.target.kind,
                self.target.source.session_key(),
                self.target.source.transport_media_id(),
                &self.rtp,
                self.reservation.declared_activity(),
            )
            .await
        {
            Ok(media) => {
                let mid = media_transport
                    .transport_media_mid(&self.target.session, media)
                    .await;
                Ok(DeclaredConsumerSetup {
                    route: self.target.transport_consumer_route(media),
                    pending: self,
                    mid,
                })
            }
            Err(error) => {
                warn!(
                    consumer_user_id = ?self.target.session.user_id(),
                    consumer_connection_id = ?self.target.session.connection_id(),
                    producer_user_id = ?self.target.source.session_key().user_id(),
                    producer_connection_id = ?self.target.source.session_key().connection_id(),
                    source_transport_media_id = ?self.target.source.transport_media_id(),
                    error = ?error,
                    consumer_mid = self.rtp.mid(),
                    ?origin,
                    "media transport rejected consume media declaration"
                );
                Err(self)
            }
        }
    }
}

impl ConsumerSetupTarget {
    pub fn new(session: TransportSessionKey, source: &PublishedSource) -> Self {
        Self {
            session,
            source: source.transport.clone(),
            source_id: source.descriptor.source_id(),
            stream: source.descriptor.stream_id().clone(),
            kind: source.descriptor.media_kind(),
            routed: source.routed,
        }
    }

    pub(super) fn subscription_key(&self) -> SubscriptionKey {
        SubscriptionKey::new(
            self.session.user_id(),
            self.source.session_key().user_id(),
            &self.stream,
        )
    }

    pub(super) fn transport_consumer_route(
        &self,
        consumer_media: TransportMediaId,
    ) -> TransportConsumerRoute {
        TransportConsumerRoute::new(self.session.clone(), consumer_media, self.source.clone())
    }

    fn route_target(&self, route: TransportConsumerRoute) -> ConsumerRouteTarget {
        ConsumerRouteTarget::new(route, self.stream.clone(), self.kind)
    }

    pub(super) fn relay_route_key(&self, target_worker: MediaWorkerId) -> RelayRouteKey {
        RelayRouteKey {
            source: self.source.clone(),
            target_worker,
        }
    }

    pub(super) fn matches_identity(&self, source: &PublishedSource) -> bool {
        source.descriptor.source_id() == self.source_id
            && source.transport == self.source
            && source.descriptor.stream_id() == &self.stream
            && source.descriptor.media_kind() == self.kind
            && source.routed == self.routed
    }
}
