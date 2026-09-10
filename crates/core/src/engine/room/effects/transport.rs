use o_sfu_router::MediaKind;
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::{info, warn};

use super::receiver_route::ReceiverSetupTurn;
use crate::engine::{
    media_transport::{
        ConsumerActivity, ConsumerRouteControl, ConsumerRouteControlOutcome, MediaControlPlan,
        MediaTransport, ProducerActivity, ReceiverBweTargetUpdate, SourceActivityUpdate,
        TransportConsumerRoute, TransportRelayRouteAction, TransportRelayRouteEffect,
        TransportSourceActivityEffect, TransportSourceKey, TransportTeardown,
    },
    room::{
        Room,
        media_graph::{
            ConsumerRouteTarget, ConsumerSetupOrigin, ReceiverRouteActivity, ReceiverRouteWork,
        },
        source_policy::ConsumerPacketSelectionUpdate,
    },
    source_model::UserStreamId,
};

#[derive(Debug, Default)]
pub(in crate::engine::room) struct RoomTransportPlan {
    relays: Vec<TransportRelayRouteEffect>,
    remote_source_activity: Vec<TransportSourceActivityEffect>,
    teardown: Vec<TransportTeardown>,
    route_control: RoomRouteEffects,
    setup_turns: Vec<ReceiverSetupTurn>,
}

impl RoomTransportPlan {
    pub(in crate::engine::room) fn from_relays_and_teardown(
        mut relays: Vec<TransportRelayRouteEffect>,
        additional_teardown: impl IntoIterator<Item = TransportTeardown>,
    ) -> Self {
        let mut teardown = Vec::new();
        extract_relay_teardown(&mut relays, &mut teardown);
        teardown.extend(additional_teardown);
        Self {
            relays,
            teardown,
            ..Self::default()
        }
    }

    pub(in crate::engine::room) fn extend(&mut self, other: Self) {
        self.relays.extend(other.relays);
        self.remote_source_activity
            .extend(other.remote_source_activity);
        self.teardown.extend(other.teardown);
        self.route_control.append(other.route_control);
        self.setup_turns.extend(other.setup_turns);
    }

    pub(in crate::engine::room) fn extend_teardown(
        &mut self,
        teardown: impl IntoIterator<Item = TransportTeardown>,
    ) {
        self.teardown.extend(teardown);
    }

    pub(super) fn push_producer(
        &mut self,
        source: TransportSourceKey,
        stream_id: UserStreamId,
        update: SourceActivityUpdate,
    ) {
        self.route_control
            .producer_activity(source, stream_id, update);
    }

    pub(super) fn extend_remote_source_activity(
        &mut self,
        effects: impl IntoIterator<Item = TransportSourceActivityEffect>,
    ) {
        self.remote_source_activity.extend(effects);
    }

    pub(super) fn push_receiver_work(
        &mut self,
        mut work: ReceiverRouteWork,
        origin: ConsumerSetupOrigin,
    ) {
        extract_relay_teardown(&mut work.relays, &mut self.teardown);
        self.relays.extend(work.relays);
        self.teardown.extend(work.teardown);
        for activity in work.activities {
            self.route_control.receiver_activity(activity);
        }
        for setup in work.setups {
            self.setup_turns.push(ReceiverSetupTurn::new(setup, origin));
        }
    }

    pub(super) async fn execute(self, room: &Room, media_transport: Option<&MediaTransport>) {
        let Some(media_transport) = media_transport else {
            return;
        };
        execute_relay_route_effects(media_transport, self.relays).await;
        execute_remote_source_activity_effects(media_transport, self.remote_source_activity).await;
        self.route_control
            .execute(room.uuid(), media_transport)
            .await;
        media_transport.teardown(self.teardown).await;
        for turn in self.setup_turns {
            turn.execute(room, media_transport).await;
        }
    }

    #[cfg(test)]
    pub(in crate::engine::room) fn relays_and_teardown(
        &self,
    ) -> (&[TransportRelayRouteEffect], &[TransportTeardown]) {
        (&self.relays, &self.teardown)
    }
}

#[derive(Debug, Default)]
#[must_use = "room route effects must be executed or intentionally dropped"]
pub(in crate::engine::room) struct RoomRouteEffects(
    MediaControlPlan<ProducerRouteFinish, ConsumerRouteFinish>,
);

impl RoomRouteEffects {
    fn producer_activity(
        &mut self,
        source: TransportSourceKey,
        stream_id: UserStreamId,
        update: SourceActivityUpdate,
    ) {
        self.0.push_producer(
            source.clone(),
            update,
            ProducerRouteFinish {
                source,
                stream_id,
                activity: update.activity(),
            },
        );
    }

    fn receiver_activity(&mut self, activity: ReceiverRouteActivity) {
        let target = activity.target();
        let active = activity.active();
        self.0.push_consumer(
            ConsumerRouteControl::new(target.transport_route().clone())
                .activity(ConsumerActivity::from_active(active))
                .request_keyframe(target.request_keyframe_after_activity(active)),
            ConsumerRouteFinish::Activity(activity),
        );
    }

    pub(super) fn setup_activity(
        &mut self,
        route: TransportConsumerRoute,
        kind: MediaKind,
        active: bool,
    ) {
        self.0.push_consumer(
            ConsumerRouteControl::new(route.clone())
                .activity(ConsumerActivity::from_active(active))
                .request_keyframe(active && kind == MediaKind::Video),
            ConsumerRouteFinish::SetupActivity(route, active),
        );
    }

    pub(super) fn keyframe(&mut self, target: ConsumerRouteTarget) {
        self.0.push_consumer(
            ConsumerRouteControl::new(target.transport_route().clone()).request_keyframe(true),
            ConsumerRouteFinish::Keyframe(target),
        );
    }

    pub(in crate::engine::room) fn source_policy_update(
        &mut self,
        update: ConsumerPacketSelectionUpdate,
    ) {
        self.0.push_consumer(
            update.route_control(),
            ConsumerRouteFinish::SourcePolicy(update),
        );
    }

    pub(in crate::engine::room) fn set_receiver_bwe_targets(
        &mut self,
        targets: Vec<ReceiverBweTargetUpdate>,
    ) {
        self.0.set_receiver_bwe_targets(targets);
    }

    fn append(&mut self, other: Self) {
        self.0.append(other.0);
    }

    pub(in crate::engine::room) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(in crate::engine::room) async fn execute(
        self,
        room_id: &str,
        media_transport: &MediaTransport,
    ) -> Vec<ConsumerPacketSelectionUpdate> {
        let transport_outcome = media_transport.apply_media_control(self.0).await;

        let mut accepted_policy_updates = Vec::new();
        for (completion, result) in transport_outcome.producers {
            if let Err(error) = result {
                warn!(
                    ?error,
                    source = ?completion.source,
                    active = completion.activity.is_active(),
                    "media transport failed to update producer route activity"
                );
            }
            completion.emit_activity_event(room_id);
        }
        for (completion, result) in transport_outcome.consumers {
            completion.finish(room_id, result, &mut accepted_policy_updates);
        }
        accepted_policy_updates
    }
}

#[derive(Debug)]
struct ProducerRouteFinish {
    source: TransportSourceKey,
    stream_id: UserStreamId,
    activity: ProducerActivity,
}

impl ProducerRouteFinish {
    fn emit_activity_event(&self, room_id: &str) {
        let session = self.source.session_key();
        info!(
            event = telemetry_event::PUBLICATION_ACTIVITY_CHANGED,
            room_id,
            user_id = %session.user_id().path_segment(),
            connection_id = session.connection_id().as_u64(),
            media_worker_id = session.media_worker_id().as_usize(),
            transport_media_id = self.source.transport_media_id().as_u64(),
            active = self.activity.is_active(),
            stream_id = %self.stream_id,
            "publication activity changed"
        );
    }
}

#[derive(Debug)]
enum ConsumerRouteFinish {
    Activity(ReceiverRouteActivity),
    Keyframe(ConsumerRouteTarget),
    SetupActivity(TransportConsumerRoute, bool),
    SourcePolicy(ConsumerPacketSelectionUpdate),
}

impl ConsumerRouteFinish {
    #[expect(
        clippy::cognitive_complexity,
        reason = "closed route completion policy is clearer than one use finish helpers"
    )]
    fn finish(
        self,
        room_id: &str,
        result: ConsumerRouteControlOutcome,
        accepted_policy_updates: &mut Vec<ConsumerPacketSelectionUpdate>,
    ) {
        match self {
            Self::Activity(activity) => {
                let target = activity.target();
                if result.activity_failed() {
                    warn!(
                        error = ?result.error(),
                        route = ?target.transport_route(),
                        stream_id = %target.stream_id(),
                        active = activity.active(),
                        "media transport failed to update consumer route activity"
                    );
                } else if result.keyframe_failed() {
                    warn!(
                        error = ?result.error(),
                        route = ?target.transport_route(),
                        stream_id = %target.stream_id(),
                        "media transport failed to request a consumer keyframe refresh"
                    );
                }
                let session = target.transport_route().consumer_session_key();
                info!(
                    event = telemetry_event::SUBSCRIPTION_ACTIVITY_CHANGED,
                    room_id,
                    user_id = %session.user_id().path_segment(),
                    connection_id = session.connection_id().as_u64(),
                    media_worker_id = session.media_worker_id().as_usize(),
                    transport_media_id = target.consumer_media_id().as_u64(),
                    active = activity.active(),
                    producer_user_id = %target.producer_user_id().path_segment(),
                    source_transport_media_id = target.source_media_id().as_u64(),
                    stream_id = %target.stream_id(),
                    "subscription activity changed"
                );
            }
            Self::Keyframe(target) => {
                if result.keyframe_failed() {
                    warn!(
                        error = ?result.error(),
                        route = ?target.transport_route(),
                        "media transport failed to request a refreshed consumer keyframe"
                    );
                }
            }
            Self::SetupActivity(route, active) => {
                if result.activity_failed() {
                    warn!(
                        error = ?result.error(),
                        ?route,
                        active,
                        "media transport failed to correct in-flight consumer setup activity"
                    );
                } else if result.keyframe_failed() {
                    warn!(
                        error = ?result.error(),
                        ?route,
                        "media transport failed to request keyframe after consumer setup activity correction"
                    );
                }
            }
            Self::SourcePolicy(update) => {
                if result.packet_gate_failed() || result.activity_failed() {
                    warn!(
                        route = ?update.route,
                        error = ?result.error(),
                        route_active = update.route_active(),
                        "media transport rejected the receiver-driven packet selection update"
                    );
                    return;
                }
                if result.keyframe_failed() {
                    warn!(
                        error = ?result.error(),
                        route = ?update.route,
                        "media transport failed to request an adaptation keyframe refresh"
                    );
                }
                accepted_policy_updates.push(update);
            }
        }
    }
}

pub(super) async fn execute_relays_and_teardown(
    media_transport: &MediaTransport,
    mut relays: Vec<TransportRelayRouteEffect>,
    additional_teardown: impl IntoIterator<Item = TransportTeardown>,
) -> bool {
    let mut teardown = Vec::new();
    extract_relay_teardown(&mut relays, &mut teardown);
    teardown.extend(additional_teardown);
    let relays_applied = execute_relay_route_effects(media_transport, relays).await;
    media_transport.teardown(teardown).await;
    relays_applied
}

fn extract_relay_teardown(
    relays: &mut Vec<TransportRelayRouteEffect>,
    teardown: &mut Vec<TransportTeardown>,
) {
    teardown.extend(
        relays
            .extract_if(.., |effect| {
                effect.action == TransportRelayRouteAction::Release
            })
            .map(|effect| TransportTeardown::ReleaseRelayRoute {
                source: effect.source,
                target_media_worker_id: effect.target_media_worker_id,
            }),
    );
}

async fn execute_relay_route_effects(
    media_transport: &MediaTransport,
    effects: impl IntoIterator<Item = TransportRelayRouteEffect>,
) -> bool {
    let mut applied = true;
    for effect in effects {
        if let Err(error) = media_transport.apply_relay_route_effect(&effect).await {
            applied = false;
            warn!(
                ?effect,
                ?error,
                "media transport failed to apply relay route effect"
            );
        }
    }
    applied
}

pub(super) async fn execute_remote_source_activity_effects(
    media_transport: &MediaTransport,
    effects: impl IntoIterator<Item = TransportSourceActivityEffect>,
) {
    for effect in effects {
        let _ = media_transport
            .apply_remote_source_activity_effect(&effect)
            .await;
    }
}

#[cfg(test)]
#[path = "TESTS/route.rs"]
mod route_tests;
