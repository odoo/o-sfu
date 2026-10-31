use super::{output::RoomOutputPlan, transport::RoomTransportPlan};
use crate::engine::{
    media_transport::MediaTransport,
    obligation::{Execute, Obligation, Work},
    room::{
        Room, SourcePolicyGuard,
        media_graph::{
            ConsumerSetupOrigin, ProducerActivityCommit, PublishCommit, ReceiverRouteCommit,
            ReceiverRouteWork,
        },
        source_policy::SourcePolicyTurn,
        state::{
            ConnectionCloseCommit, DisconnectCommit, LifecycleEffects, PresenceCommit,
            UserJoinedFanout,
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
/// RoomEffects::from_*(commit) -> execute
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
/// Publication commits return [`PublicationEffects`] so their execution requires
/// the source-policy guard held since the state commit.
/// A nonempty batch retains its obligation until transport, policy and output
/// finish. Cancellation after transport still leaves the batch unfinished.
#[derive(Debug, Default)]
#[must_use = "room effect batches must be executed after the state transition commits"]
pub struct RoomEffects(Option<Obligation<RoomEffectWork>>);

#[derive(Debug, Default)]
struct RoomEffectWork {
    transport: RoomTransportPlan,
    output: RoomOutputPlan,
    source_policy: SourcePolicyTurn,
}

/// Requires the source-policy guard held continuously from the publication commit.
/// Activation emits user-info and applies policy before transport opens packet gates.
#[derive(Debug)]
#[must_use = "publication effects must execute with the guard held since their state commit"]
pub(in crate::engine::room) struct PublicationEffects {
    batch: Obligation<RoomEffectWork>,
    policy_before_transport: bool,
}

impl RoomEffects {
    pub(in crate::engine::room) fn from_join(
        effects: LifecycleEffects,
        transport_plan: RoomTransportPlan,
    ) -> Self {
        let mut batch = RoomEffectWork {
            transport: transport_plan,
            ..RoomEffectWork::default()
        };
        batch.source_policy.request();
        batch.output.lifecycle = effects;
        Self(Some(Obligation::new(batch)))
    }

    pub(in crate::engine::room) fn from_connection_close(commit: ConnectionCloseCommit) -> Self {
        let mut batch = RoomEffectWork::default();
        match commit {
            ConnectionCloseCommit::Current {
                session_teardown,
                effects,
                transport_plan,
                ..
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
        Self(Some(Obligation::new(batch)))
    }

    pub(in crate::engine::room) fn from_disconnect(commit: DisconnectCommit) -> Self {
        let mut batch = RoomEffectWork {
            transport: commit.transport_plan,
            ..RoomEffectWork::default()
        };
        batch.source_policy.request();
        batch.output.lifecycle = commit.effects;
        batch.transport.extend_teardown(commit.session_teardowns);
        Self(Some(Obligation::new(batch)))
    }

    pub(in crate::engine::room) fn from_presence(commit: PresenceCommit) -> Self {
        let mut batch = RoomEffectWork::default();
        batch.output.user_info = Some(commit.fanout);
        batch.source_policy.request();
        Self(Some(Obligation::new(batch)))
    }

    pub(in crate::engine::room) fn from_publish(commit: PublishCommit) -> PublicationEffects {
        let mut batch = RoomEffectWork::default();
        batch
            .transport
            .push_receiver_work(commit.receiver_route_work, ConsumerSetupOrigin::Publish);
        batch.output.user_info_before_policy = commit.presence.map(|presence| presence.fanout);
        batch.source_policy.request();
        PublicationEffects {
            batch: Obligation::new(batch),
            policy_before_transport: false,
        }
    }

    pub(in crate::engine::room) fn from_publication_activity(
        commit: ProducerActivityCommit,
    ) -> PublicationEffects {
        let ProducerActivityCommit {
            source,
            stream_id,
            update,
            remote_activity_effects,
            track_snapshots,
            presence,
        } = commit;
        let policy_before_transport = update.activity().is_active();
        let mut batch = RoomEffectWork::default();
        batch
            .transport
            .extend_remote_source_activity(remote_activity_effects);
        batch.transport.push_producer(source, stream_id, update);
        batch.output.track_snapshots = track_snapshots;
        batch.output.user_info_before_policy = presence.map(|presence| presence.fanout);
        batch.source_policy.request();
        PublicationEffects {
            batch: Obligation::new(batch),
            policy_before_transport,
        }
    }

    pub(in crate::engine::room) fn from_receiver_intent(commit: ReceiverRouteCommit) -> Self {
        let batch = Self::receiver_route_work(commit.work, ConsumerSetupOrigin::Subscribe);
        Self(Some(Obligation::new(batch)))
    }

    pub(in crate::engine::room) fn from_consumer_readiness(commit: ReceiverRouteCommit) -> Self {
        let ReceiverRouteCommit {
            work,
            track_snapshots,
        } = commit;
        let mut batch = Self::receiver_route_work(work, ConsumerSetupOrigin::Readiness);
        batch.output.track_snapshots = track_snapshots;
        Self(Some(Obligation::new(batch)))
    }

    fn receiver_route_work(work: ReceiverRouteWork, origin: ConsumerSetupOrigin) -> RoomEffectWork {
        let mut batch = RoomEffectWork::default();
        batch.transport.push_receiver_work(work, origin);
        batch.source_policy.request();
        batch
    }

    /// preserves the room-wide side-effect order across transport and policy work
    pub async fn execute(self, room: &Room, context: RoomEffectContext<'_>) {
        if let Some(batch) = self.0 {
            batch.complete((room, context)).await;
        }
    }
}

impl PublicationEffects {
    pub async fn execute(self, guard: &SourcePolicyGuard<'_>, context: RoomEffectContext<'_>) {
        self.batch
            .complete((guard, context, self.policy_before_transport))
            .await;
    }
}

impl Work for RoomEffectWork {
    fn label(&self) -> &'static str {
        "room effects"
    }
}

impl Execute<(&Room, RoomEffectContext<'_>)> for RoomEffectWork {
    type Output = ();

    async fn execute(self, (room, context): (&Room, RoomEffectContext<'_>)) {
        let mut output = self.output;
        self.transport
            .execute(room, context.route_transport())
            .await;
        output.emit_before_policy();
        self.source_policy
            .execute(room, context.media_transport(), None)
            .await;
        // The batch is not complete until this last output is sent.
        output.emit_after_policy();
    }
}

impl Execute<(&SourcePolicyGuard<'_>, RoomEffectContext<'_>, bool)> for RoomEffectWork {
    type Output = ();

    async fn execute(
        self,
        (guard, context, policy_before_transport): (
            &SourcePolicyGuard<'_>,
            RoomEffectContext<'_>,
            bool,
        ),
    ) {
        let room = guard.room();
        let mut output = self.output;
        let mut source_policy = self.source_policy;
        if policy_before_transport {
            output.emit_user_info_before_policy();
            source_policy
                .execute_guarded(guard, context.media_transport(), None)
                .await;
            source_policy = SourcePolicyTurn::default();
        }
        self.transport
            .execute(room, context.route_transport())
            .await;
        output.emit_before_policy();
        source_policy
            .execute_guarded(guard, context.media_transport(), None)
            .await;
        output.emit_after_policy();
    }
}

#[cfg(test)]
#[path = "TESTS/batch.rs"]
mod tests;
