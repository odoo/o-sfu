use super::{output::RoomOutputPlan, transport::RoomTransportPlan};
use crate::engine::{
    media_transport::MediaTransport,
    room::{
        Room,
        media_graph::{
            ConsumerSetupOrigin, ProducerActivityCommit, PublishCommit, ReceiverRouteCommit,
            ReceiverRouteWork,
        },
        source_policy::SourcePolicyTurn,
        state::{
            ConnectionCloseCommit, DisconnectCommit, JoinCommit, PresenceCommit, UserJoinedFanout,
        },
    },
};

#[derive(Debug, Clone, Copy)]
pub struct RoomEffectContext<'a> {
    media_transport: Option<&'a MediaTransport>,
    route_effects: bool,
    joined_fanout: UserJoinedFanout,
}

impl<'a> RoomEffectContext<'a> {
    pub const fn runtime(media_transport: &'a MediaTransport) -> Self {
        Self {
            media_transport: Some(media_transport),
            route_effects: true,
            joined_fanout: UserJoinedFanout::Emit,
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub const fn state_only(media_transport: Option<&'a MediaTransport>) -> Self {
        Self {
            media_transport,
            route_effects: false,
            joined_fanout: UserJoinedFanout::Suppress,
        }
    }

    pub(in crate::engine::room) const fn user_joined_fanout(self) -> UserJoinedFanout {
        self.joined_fanout
    }

    fn media_transport(self) -> Option<&'a MediaTransport> {
        self.media_transport
    }

    fn route_transport(self) -> Option<&'a MediaTransport> {
        self.route_effects.then_some(self.media_transport).flatten()
    }
}

/// batches post-lock transport, signaling and policy side-effects for room state transitions
///
/// ```text
/// room state mutation (holds RoomState write lock)
///   - mutate in-memory graph
///   - return commit data
///             |
///             v  drop RoomState write lock
/// RoomEffects::from_*(commit) -> execute_inner
///             |
///             v  step 1: transport execution
///   +-----------------------------------------------------------+
///   | - create/remove local consumer routes on workers          |
///   | - register/remove cross-worker relay route targets        |
///   | - dispatch session teardowns                              |
///   +-----------------------------------------------------------+
///             |
///             v  step 2: RoomOutputPlan pre-policy fanout
///   +-----------------------------------------------------------+
///   | - send track snapshots and presence user-info fanout      |
///   +-----------------------------------------------------------+
///             |
///             v  step 3: source policy turn
///   +-----------------------------------------------------------+
///   | - re-evaluate audio admission and video bandwidth solver  |
///   | - commit packet gate and BWE target updates               |
///   +-----------------------------------------------------------+
///             |
///             v  step 4: RoomOutputPlan post-policy fanout
///   +-----------------------------------------------------------+
///   | - send user-info and lifecycle close/track/fanout output  |
///   +-----------------------------------------------------------+
/// ```
///
/// The diagram shows the normal batch order. Only an active `ProducerActivityCommit` from
/// `from_publication_activity` runs its pre-policy user-info output and source policy before transport.
/// Callers of `from_publish` and `from_publication_activity` use
/// `execute_with_source_policy_guard` while holding `source_policy_turn`.
#[derive(Debug, Default)]
#[must_use = "room effect batches must be executed after the state transition commits"]
pub struct RoomEffects {
    policy_before_transport: bool,
    transport: RoomTransportPlan,
    output: RoomOutputPlan,
    source_policy: SourcePolicyTurn,
}

impl RoomEffects {
    pub(in crate::engine::room) fn from_join(commit: JoinCommit) -> Self {
        let JoinCommit {
            effects,
            transport_plan,
            ..
        } = commit;
        let mut batch = Self {
            transport: transport_plan,
            ..Self::default()
        };
        batch.source_policy.request();
        batch.output.lifecycle = effects;
        batch
    }

    pub(in crate::engine::room) fn from_connection_close(commit: ConnectionCloseCommit) -> Self {
        let mut batch = Self::default();
        match commit {
            ConnectionCloseCommit::Current {
                session_teardown,
                effects,
                transport_plan,
            } => {
                batch.transport = transport_plan;
                batch.output.lifecycle = effects;
                batch.source_policy.request();
                batch.transport.extend_teardown(session_teardown);
            }
            ConnectionCloseCommit::StalePlacement { session_teardown } => {
                batch.transport.extend_teardown([session_teardown]);
            }
        }
        batch
    }

    pub(in crate::engine::room) fn from_disconnect(commit: DisconnectCommit) -> Self {
        let mut batch = Self {
            transport: commit.transport_plan,
            ..Self::default()
        };
        batch.source_policy.request();
        batch.output.lifecycle = commit.effects;
        batch.transport.extend_teardown(commit.session_teardowns);
        batch
    }

    pub(in crate::engine::room) fn from_presence(commit: PresenceCommit) -> Self {
        let mut batch = Self::default();
        batch.output.user_info = Some(commit.fanout);
        batch.source_policy.request();
        batch
    }

    pub(in crate::engine::room) fn from_publish(commit: PublishCommit) -> Self {
        let mut batch = Self::default();
        batch
            .transport
            .push_receiver_work(commit.receiver_route_work, ConsumerSetupOrigin::Publish);
        batch.output.user_info_before_policy = commit.presence.map(|presence| presence.fanout);
        batch.source_policy.request();
        batch
    }

    pub(in crate::engine::room) fn from_publication_activity(
        commit: ProducerActivityCommit,
    ) -> Self {
        let ProducerActivityCommit {
            source,
            stream_id,
            update,
            remote_activity_effects,
            track_snapshots,
            presence,
        } = commit;
        let mut batch = Self {
            policy_before_transport: update.activity().is_active(),
            ..Self::default()
        };
        batch
            .transport
            .extend_remote_source_activity(remote_activity_effects);
        batch.transport.push_producer(source, stream_id, update);
        batch.output.track_snapshots = track_snapshots;
        batch.output.user_info_before_policy = presence.map(|presence| presence.fanout);
        batch.source_policy.request();
        batch
    }

    pub(in crate::engine::room) fn from_receiver_intent(commit: ReceiverRouteCommit) -> Self {
        let mut batch = Self::from_receiver_route(commit.work, ConsumerSetupOrigin::Subscribe);
        batch.source_policy.request();
        batch
    }

    pub(in crate::engine::room) fn from_consumer_readiness(commit: ReceiverRouteCommit) -> Self {
        let ReceiverRouteCommit {
            work,
            track_snapshots,
        } = commit;
        let mut batch = Self::from_receiver_route(work, ConsumerSetupOrigin::Readiness);
        batch.output.track_snapshots = track_snapshots;
        batch.source_policy.request();
        batch
    }

    fn from_receiver_route(work: ReceiverRouteWork, origin: ConsumerSetupOrigin) -> Self {
        let mut batch = Self::default();
        batch.transport.push_receiver_work(work, origin);
        batch
    }

    /// preserves the room-wide side-effect order across transport and policy work
    pub async fn execute(self, room: &Room, context: RoomEffectContext<'_>) {
        if self.policy_before_transport {
            let _guard = room.source_policy_turn.lock().await;
            self.execute_inner(room, context, true).await;
        } else {
            self.execute_inner(room, context, false).await;
        }
    }

    pub(in crate::engine::room) async fn execute_with_source_policy_guard(
        self,
        room: &Room,
        context: RoomEffectContext<'_>,
    ) {
        self.execute_inner(room, context, true).await;
    }

    async fn execute_inner(
        self,
        room: &Room,
        context: RoomEffectContext<'_>,
        source_policy_guarded: bool,
    ) {
        let mut output = self.output;
        let mut source_policy = self.source_policy;
        if self.policy_before_transport {
            output.emit_user_info_before_policy();
            source_policy
                .execute_guarded(room, context.media_transport(), None)
                .await;
            source_policy = SourcePolicyTurn::default();
        }
        self.transport
            .execute(room, context.route_transport())
            .await;
        output.emit_before_policy();
        if source_policy_guarded {
            source_policy
                .execute_guarded(room, context.media_transport(), None)
                .await;
        } else {
            source_policy
                .execute(room, context.media_transport(), None)
                .await;
        }
        output.emit_after_policy();
    }
}
