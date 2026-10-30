use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::info;

use super::transport::{
    RoomRouteEffects, execute_relays_and_teardown, execute_remote_source_activity_effects,
};
use crate::engine::{
    media_transport::{MediaTransport, TransportTeardown},
    room::{
        Room,
        media_graph::{ConsumerSetupOrigin, ConsumerSetupOutcome, PendingConsumerSetup},
    },
};

#[derive(Debug)]
pub(super) struct ReceiverSetupTurn {
    setup: PendingConsumerSetup,
    origin: ConsumerSetupOrigin,
}

impl ReceiverSetupTurn {
    pub(super) const fn new(setup: PendingConsumerSetup, origin: ConsumerSetupOrigin) -> Self {
        Self { setup, origin }
    }

    pub(super) async fn execute(self, room: &Room, media_transport: &MediaTransport) {
        let Self {
            setup: mut pending,
            origin,
        } = self;
        if !execute_relays_and_teardown(media_transport, pending.take_relays(), []).await {
            Self::release_pending_setup(room, pending, media_transport).await;
            return;
        }
        let setup = match pending.declare(media_transport, origin).await {
            Ok(setup) => setup,
            Err(pending) => {
                Self::release_pending_setup(room, pending, media_transport).await;
                return;
            }
        };
        let setup_outcome = {
            let mut state = room.state.write().await;
            state.commit_declared_consumer_setup(setup, origin)
        };
        match setup_outcome {
            ConsumerSetupOutcome::Committed {
                target,
                route,
                sender,
                track_snapshot,
                remote_source_activity,
                transport_activity_update,
                readiness_keyframe,
            } => {
                let consumer = route.consumer_session_key();
                let source = route.source();
                info!(
                    event = telemetry_event::SUBSCRIBE_SUCCEEDED,
                    room_id = room.uuid(),
                    user_id = %consumer.user_id().path_segment(),
                    connection_id = consumer.connection_id().as_u64(),
                    media_worker_id = consumer.media_worker_id().as_usize(),
                    transport_media_id = route.consumer_transport_media_id().as_u64(),
                    producer_user_id = %source.session_key().user_id().path_segment(),
                    source_transport_media_id = source.transport_media_id().as_u64(),
                    stream_id = %target.stream,
                    origin = origin.as_diagnostic_str(),
                    "subscription committed"
                );
                if let Some(effect) = remote_source_activity {
                    execute_remote_source_activity_effects(media_transport, [effect]).await;
                }
                let mut route_effects = RoomRouteEffects::default();
                if let Some(active) = transport_activity_update {
                    route_effects.setup_activity(route, target.kind, active);
                }
                if let Some(kf_target) = readiness_keyframe {
                    route_effects.keyframe(kf_target);
                }
                if !route_effects.is_empty() {
                    route_effects.execute(room.uuid(), media_transport).await;
                }
                let _ = sender.send_remote_tracks(track_snapshot);
            }
            ConsumerSetupOutcome::Released(route, relays) => {
                let teardown = [TransportTeardown::remove_media(
                    route.consumer_session_key().clone(),
                    route.consumer_transport_media_id(),
                )];
                execute_relays_and_teardown(media_transport, relays, teardown).await;
            }
        }
    }

    async fn release_pending_setup(
        room: &Room,
        setup: PendingConsumerSetup,
        media_transport: &MediaTransport,
    ) {
        let relays = {
            let mut state = room.state.write().await;
            state.release_pending_consumer_setup(setup)
        };
        execute_relays_and_teardown(media_transport, relays, []).await;
    }
}
