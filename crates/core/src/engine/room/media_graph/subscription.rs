use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::{MediaKind as RouterMediaKind, negotiation::negotiate_consumer_rtp_parameters};

use super::{
    super::state::{ActiveUser, RoomState},
    ConsumerId, ConsumerRouteTarget, SubscriptionKey,
    consumer_setup::{ConsumerSetupTarget, PendingConsumerSetup},
};
use crate::engine::{
    ConnectionId, UserId,
    media_transport::{TransportMediaId, TransportRelayRouteEffect, TransportTeardown},
    room::{
        outbound::{OutboundSender, VersionedRemoteTrackSnapshot},
        source_policy::VideoAdmissionRank,
    },
    source_model::{
        ConsumerSourceSelection, PolicyPauseReason, PublishedSourceId, SourceRoutePriority,
        SourceSubscriptionIntent, UserStreamId,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverRouteActivity {
    target: ConsumerRouteTarget,
    active: bool,
}

impl ReceiverRouteActivity {
    pub const fn new(target: ConsumerRouteTarget, active: bool) -> Self {
        Self { target, active }
    }

    pub const fn target(&self) -> &ConsumerRouteTarget {
        &self.target
    }

    pub const fn active(&self) -> bool {
        self.active
    }
}

/// observable receiver route state for room inspection
#[cfg(any(test, feature = "testing-transport"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerRouteState {
    Absent,
    Inactive,
    Active,
}

#[derive(Debug, Default)]
pub struct ReceiverRouteWork {
    pub(in crate::engine::room) activities: Vec<ReceiverRouteActivity>,
    pub(in crate::engine::room) setups: Vec<PendingConsumerSetup>,
    pub(in crate::engine::room) relays: Vec<TransportRelayRouteEffect>,
    pub(in crate::engine::room) teardown: Vec<TransportTeardown>,
}

#[derive(Debug)]
pub struct ReceiverRouteCommit {
    pub(in crate::engine::room) work: ReceiverRouteWork,
    pub(in crate::engine::room) track_snapshots:
        Vec<(OutboundSender, VersionedRemoteTrackSnapshot)>,
}

#[derive(Clone, Copy)]
pub(super) enum ReceiverRouteScope<'a> {
    Source(PublishedSourceId),
    Receiver(&'a UserId, ConnectionId),
    SourceUser(&'a UserId, ConnectionId, &'a UserId),
}

impl RoomState {
    pub fn apply_receiver_intent(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> Option<ReceiverRouteCommit> {
        self.user_for_connection(user_id, connection_id)?;
        let work =
            self.plan_receiver_intent_change(user_id, connection_id, target_user_id, intents);
        Some(ReceiverRouteCommit {
            work,
            track_snapshots: Vec::new(),
        })
    }

    #[cfg(test)]
    pub fn plan_receiver_route_work(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> ReceiverRouteWork {
        if self.user_for_connection(user_id, connection_id).is_none() {
            return ReceiverRouteWork::default();
        }
        self.plan_receiver_intent_change(user_id, connection_id, target_user_id, intents)
    }

    fn plan_receiver_intent_change(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> ReceiverRouteWork {
        let mut activities = Vec::new();
        let mut relays = Vec::new();
        let receiver_deafened = self
            .user_for_connection(user_id, connection_id)
            .is_some_and(ActiveUser::is_deaf);
        for (stream_id, intent) in intents {
            let Some(commit) = self.topology.apply_subscription_intent(
                &SubscriptionKey::new(user_id, target_user_id, stream_id),
                connection_id,
                *intent,
                receiver_deafened,
            ) else {
                continue;
            };
            relays.extend(commit.relay_effects);
            activities.extend(commit.update);
        }
        let ReceiverRouteWork { setups, .. } = self.plan_missing_receiver_routes(
            ReceiverRouteScope::SourceUser(user_id, connection_id, target_user_id),
        );
        ReceiverRouteWork {
            activities,
            setups,
            relays,
            ..Default::default()
        }
    }

    pub fn refresh_consumer_readiness(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        declined_consumers: &[TransportMediaId],
    ) -> Option<ReceiverRouteCommit> {
        let sender = {
            let user = self.user_for_connection(user_id, connection_id)?;
            user.parsed_client_rtp_capabilities.as_ref()?;
            (!declined_consumers.is_empty()).then(|| user.sender.clone())
        };
        let session = self
            .topology
            .transport_user_key(user_id.clone(), connection_id);
        // Keep declined routes committed while planning so the answer turn cannot
        // immediately recreate the same media lines.
        let mut work =
            self.plan_missing_receiver_routes(ReceiverRouteScope::Receiver(user_id, connection_id));
        let (relays, teardown, detached) = self
            .topology
            .detach_declined_consumers(&session, declined_consumers);
        work.relays.extend(relays);
        work.teardown.extend(teardown);
        let track_snapshots = sender
            .filter(|_| detached)
            .map(|sender| (sender, self.remote_track_snapshot_for_user(user_id, false)))
            .into_iter()
            .collect();
        Some(ReceiverRouteCommit {
            work,
            track_snapshots,
        })
    }

    #[cfg(test)]
    pub fn plan_missing_consumers(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<Vec<PendingConsumerSetup>> {
        let user = self.user_for_connection(user_id, connection_id)?;
        if user.parsed_client_rtp_capabilities.is_none() {
            return Some(Vec::new());
        }
        let ReceiverRouteWork { setups, .. } =
            self.plan_missing_receiver_routes(ReceiverRouteScope::Receiver(user_id, connection_id));
        Some(setups)
    }

    pub(super) fn plan_missing_receiver_routes(
        &mut self,
        scope: ReceiverRouteScope<'_>,
    ) -> ReceiverRouteWork {
        let targets = self.missing_receiver_route_targets(scope);
        ReceiverRouteWork {
            setups: self.plan_consumers(targets),
            ..Default::default()
        }
    }

    fn plan_consumers(
        &mut self,
        mut targets: Vec<ConsumerSetupTarget>,
    ) -> Vec<PendingConsumerSetup> {
        // Initial admission has no active-speaker snapshot. The following
        // source-policy turn applies the current speaker ranking.
        let active_speakers = BTreeSet::new();
        targets.sort_by_key(|target| self.setup_rank(target, &active_speakers));
        targets
            .into_iter()
            .filter_map(|target| self.plan_consumer(target))
            .collect()
    }

    fn setup_rank(
        &self,
        target: &ConsumerSetupTarget,
        active_speakers: &BTreeSet<UserId>,
    ) -> VideoAdmissionRank {
        if target.kind != RouterMediaKind::Video {
            return VideoAdmissionRank::new(
                SourceRoutePriority::PinnedOrFeatured,
                None,
                target.source_id,
            );
        }
        let Some(source) = self.topology.source_descriptor(target.source_id) else {
            return VideoAdmissionRank::new(
                SourceRoutePriority::HiddenOrOverflow,
                None,
                target.source_id,
            );
        };
        VideoAdmissionRank::new(
            self.receiver_video_layout_role(target.session.user_id(), source, active_speakers)
                .priority(),
            None,
            target.source_id,
        )
    }

    fn missing_receiver_route_targets(
        &mut self,
        scope: ReceiverRouteScope<'_>,
    ) -> Vec<ConsumerSetupTarget> {
        match scope {
            ReceiverRouteScope::Source(source_id) => {
                let users = &self.users;
                self.topology.missing_consumer_targets_for_source(
                    source_id,
                    users
                        .iter()
                        .map(|(user, state)| (user, state.connection_id)),
                )
            }
            ReceiverRouteScope::Receiver(user, connection) => self
                .topology
                .missing_consumer_targets(user, connection, |_| true),
            ReceiverRouteScope::SourceUser(user, connection, source_user_id) => self
                .topology
                .missing_consumer_targets(user, connection, |source| {
                    source.descriptor.owner().user_id() == source_user_id
                }),
        }
    }

    fn plan_consumer(&mut self, target: ConsumerSetupTarget) -> Option<PendingConsumerSetup> {
        let (sender, client_caps) = {
            let user = self.users.get(target.session.user_id())?;
            if user.connection_id != target.session.connection_id() {
                return None;
            }
            (
                user.sender.clone(),
                user.parsed_client_rtp_capabilities.as_ref()?,
            )
        };
        let source = self.topology.published_source(target.source_id)?;
        if !target.matches_identity(source) {
            return None;
        }
        let selection = self.setup_selection(&target, source.active);
        let rtp = negotiate_consumer_rtp_parameters(&source.rtp, client_caps).ok()?;
        let consumer = ConsumerId::allocate(&mut self.next_consumer_id);
        self.topology
            .reserve_consumer_setup(target, consumer, selection, sender, rtp)
    }

    pub(super) fn setup_selection(
        &self,
        target: &ConsumerSetupTarget,
        source_active: bool,
    ) -> ConsumerSourceSelection {
        let key = target.subscription_key();
        let selection = self
            .topology
            .consumer_source_selection(&key, target.source_id)
            .unwrap_or_else(|| {
                ConsumerSourceSelection::open(
                    self.topology
                        .subscription_intent(&key)
                        .active()
                        .unwrap_or(true),
                )
            });
        let selection = self.apply_initial_video_download_cap(target, source_active, selection);
        self.apply_initial_receiver_deafened(target, selection)
    }

    fn apply_initial_receiver_deafened(
        &self,
        target: &ConsumerSetupTarget,
        mut selection: ConsumerSourceSelection,
    ) -> ConsumerSourceSelection {
        if target.kind == RouterMediaKind::Audio
            && selection.delivery_active()
            && self
                .user_for_connection(target.session.user_id(), target.session.connection_id())
                .is_some_and(ActiveUser::is_deaf)
        {
            selection.set_policy_pause_reason(Some(PolicyPauseReason::ReceiverDeafened));
        }
        selection
    }

    fn apply_initial_video_download_cap(
        &self,
        target: &ConsumerSetupTarget,
        source_active: bool,
        mut selection: ConsumerSourceSelection,
    ) -> ConsumerSourceSelection {
        if target.kind != RouterMediaKind::Video
            || !source_active
            || !selection.delivery_active()
            || self.active_video_count(target.session.user_id())
                < self.media_limits.max_video_downloads_per_receiver()
        {
            return selection;
        }
        selection.set_policy_pause_reason(Some(PolicyPauseReason::VideoDownloadLimit));
        selection
    }

    fn active_video_count(&self, consumer_user_id: &UserId) -> usize {
        let committed = self
            .topology
            .committed_consumer_routes_for_user(consumer_user_id)
            .filter(|route| route.source.descriptor.media_kind() == RouterMediaKind::Video)
            .filter(|route| route.source.active)
            .filter(|route| route.selection.delivery_active())
            .count();
        // Delivery-active reservations already claim download capacity. Counting
        // them prevents concurrent setup from oversubscribing the receiver cap.
        let pending = self
            .topology
            .pending_consumer_routes_for_user(consumer_user_id)
            .filter(|route| route.source.descriptor.media_kind() == RouterMediaKind::Video)
            .filter(|route| route.source.active)
            .filter(|route| route.selection.delivery_active())
            .count();
        committed + pending
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn consumer_route_state(
        &self,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<ConsumerRouteState> {
        self.users.get(consumer_user_id)?;
        let key = SubscriptionKey::new(consumer_user_id, producer_user_id, stream_id);
        let Some(route) = self.topology.committed_consumer_route_for_key(&key) else {
            return Some(ConsumerRouteState::Absent);
        };
        let route_active = route.source.active && route.selection.delivery_active();
        Some(if route_active {
            ConsumerRouteState::Active
        } else {
            ConsumerRouteState::Inactive
        })
    }
}
