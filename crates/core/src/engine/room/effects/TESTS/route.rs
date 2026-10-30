use std::{
    error::Error,
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use o_sfu_router::{
    MediaKind,
    rtp::{MediaStream, StreamBinding},
    test_support::rtp_samples::{sample_client_rtp_capabilities, sample_video_rtp_parameters},
};
use o_sfu_telemetry::schema::event as telemetry_event;
use serde_json::Value;

use super::*;
use crate::{
    MediaWorkerId,
    engine::{
        ConnectionId, RoomInstanceId, TestSourceKind, UserId, UserPermissions,
        media_transport::{
            ConsumerActivity, ProducerActivity, RelayRouteActivity, SourceActivityRevision,
            SourceActivityUpdate, TransportAdapterError, TransportMediaId,
            TransportRelayRouteAction, TransportSessionKey, TransportTeardownAction,
            test_support::{
                DebugRouteEntry, test_media_transport, test_media_transport_config,
                test_media_transport_deps, test_rtc_port_range,
            },
        },
        metrics::{RuntimeMetrics, test_support::RuntimeMetricsSnapshotTestExt},
        room::{
            RoomConfig, RoomManager,
            TESTS::tracing::{assert_user_exact, capture},
            UserOutboundSender,
            media_graph::SubscriptionKey,
        },
        source_model::{
            ConsumerSourceSelection, PolicyPauseReason, PublishedSourceId, UserStreamId,
        },
    },
};

#[test]
fn room_transport_plan_moves_relay_release_to_teardown() {
    let source_session = session_key(3, UserId::Integer(3));
    let source = TransportSourceKey::new(source_session, TransportMediaId::new(31));
    let target_media_worker_id = MediaWorkerId::from_raw(2);
    let activity = TransportRelayRouteAction::SetActivity(RelayRouteActivity::Inactive);
    let plan = RoomTransportPlan::from_relays_and_teardown(
        vec![
            TransportRelayRouteEffect {
                source: source.clone(),
                target_media_worker_id,
                action: TransportRelayRouteAction::Release,
            },
            TransportRelayRouteEffect {
                source: source.clone(),
                target_media_worker_id,
                action: activity,
            },
        ],
        [],
    );
    let (relays, teardown) = plan.relays_and_teardown();
    assert_eq!(
        relays,
        [TransportRelayRouteEffect {
            source: source.clone(),
            target_media_worker_id,
            action: activity,
        }]
    );
    let [operation] = teardown else {
        panic!("the plan should contain exactly one relay teardown");
    };
    assert!(matches!(
        operation.action(),
        TransportTeardownAction::ReleaseRelayRoute {
            source: teardown_source,
            target_media_worker_id: teardown_target,
        } if teardown_source == &source && *teardown_target == target_media_worker_id
    ));
    plan.discard_for_test();
}

#[test]
fn empty_room_transport_plans_need_no_execution() {
    let mut plan = RoomTransportPlan::from_relays_and_teardown(Vec::new(), []);
    plan.extend(RoomTransportPlan::default());
    plan.extend_teardown([]);
    plan.extend_remote_source_activity([]);
    plan.push_receiver_work(ReceiverRouteWork::default(), ConsumerSetupOrigin::Subscribe);
    drop(plan);
}

#[test]
#[should_panic(expected = "unfinished lifecycle obligation: room transport plan")]
fn dropping_producer_activity_plan_diagnoses_unfinished_work() {
    drop(producer_activity_plan(TransportSourceKey::new(
        session_key(1, UserId::Integer(1)),
        TransportMediaId::new(31),
    )));
}

#[test]
fn merged_room_transport_plans_allow_explicit_simulation_discard() {
    let source = TransportSourceKey::new(
        session_key(1, UserId::Integer(1)),
        TransportMediaId::new(31),
    );
    let mut plan = producer_activity_plan(source.clone());
    plan.extend(producer_activity_plan(source));
    plan.discard_for_test();
}

#[tokio::test]
async fn cancelled_merged_transport_plan_diagnoses_unacknowledged_activity()
-> Result<(), Box<dyn Error>> {
    let fixture = RouteFixture::new().await?;
    let room = RoomManager::for_test()
        .serve_room(
            "room-transport-obligation",
            b"room".to_vec().into(),
            &RoomConfig::default(),
            None,
        )
        .await?;
    for receiver_has_work in [false, true] {
        let source = fixture.route.source().clone();
        let mut receiver = if receiver_has_work {
            producer_activity_plan(source.clone())
        } else {
            RoomTransportPlan::default()
        };
        receiver.extend(producer_activity_plan(source));
        let release_worker = fixture
            .media_transport
            .test_api()
            .pause_first_worker()
            .await
            .ok_or_else(|| io::Error::other("the activity worker should pause"))?;
        let mut execution = Box::pin(receiver.execute(&room, Some(&fixture.media_transport)));
        assert!(futures_util::poll!(execution.as_mut()).is_pending());
        assert!(
            fixture
                .media_transport
                .worker_pressure_snapshots()
                .iter()
                .any(|worker| worker.command_backlog_depth > 0),
            "activity should queue behind the paused worker"
        );
        let cancellation = catch_unwind(AssertUnwindSafe(|| drop(execution)));
        release_worker.send(()).map_err(io::Error::other)?;
        let panic = cancellation.err().ok_or_else(|| {
            io::Error::other("cancelled activity should diagnose unfinished work")
        })?;
        assert!(panic.downcast_ref::<String>().is_some_and(|message| {
            message.contains("unfinished lifecycle obligation: room transport plan")
        }));
        fixture.assert_route_state(1, true).await?;
    }
    Ok(())
}

fn producer_activity_plan(source: TransportSourceKey) -> RoomTransportPlan {
    let mut plan = RoomTransportPlan::default();
    plan.push_producer(
        source,
        UserStreamId::from("camera"),
        SourceActivityUpdate::new(
            ProducerActivity::Inactive,
            SourceActivityRevision::default().next(),
        ),
    );
    plan
}

#[tokio::test]
async fn cancelled_final_consumer_setup_diagnoses_unfinished_transport_plan()
-> Result<(), Box<dyn Error>> {
    let media_transport = test_media_transport(1, test_rtc_port_range())?;
    let room = RoomManager::for_test()
        .serve_room(
            "room-setup-obligation",
            b"room".to_vec().into(),
            &RoomConfig::default(),
            None,
        )
        .await?;
    let publisher = UserId::Integer(1);
    let receiver = UserId::Integer(2);
    for user_id in [&publisher, &receiver] {
        let (sender, _outbound) =
            UserOutboundSender::channel(128, Arc::new(RuntimeMetrics::default()));
        room.test_api()
            .join_user(user_id.clone(), None, UserPermissions::default(), sender)
            .await?;
    }
    room.test_api()
        .make_session_ready(&publisher, &media_transport)
        .await?;
    room.test_api()
        .publish_track(
            &publisher,
            TestSourceKind::ScalableVideo,
            sample_video_rtp_parameters(Some("camera"), 71_000),
            &media_transport,
        )
        .await
        .ok_or_else(|| io::Error::other("the source should publish before receiver readiness"))?;
    let receiver_connection = room
        .test_api()
        .user_connection_id(&receiver)
        .await
        .ok_or_else(|| io::Error::other("the receiver should have joined"))?;
    let receiver_session = room
        .transport_user_key(&receiver, receiver_connection)
        .await;
    media_transport
        .create_initial_session_offer("room-setup-obligation", &receiver_session)
        .await?;
    let setups = {
        let mut state = room.state.write().await;
        assert_eq!(
            state.set_user_negotiated(
                &receiver,
                receiver_connection,
                sample_client_rtp_capabilities(),
            ),
            Some(true)
        );
        state
            .plan_missing_consumers(&receiver, receiver_connection)
            .ok_or_else(|| io::Error::other("the negotiated receiver should reserve setup"))?
    };
    assert_eq!(setups.len(), 1);
    let mut plan = RoomTransportPlan::default();
    plan.push_receiver_work(
        ReceiverRouteWork {
            setups,
            ..ReceiverRouteWork::default()
        },
        ConsumerSetupOrigin::Readiness,
    );
    assert_eq!(plan.setup_turns.len(), 1);
    assert!(plan.relays.is_empty());
    assert!(plan.remote_source_activity.is_empty());
    assert!(plan.route_control.is_empty());
    assert!(plan.teardown.is_empty());
    let release_worker = media_transport
        .test_api()
        .pause_first_worker()
        .await
        .ok_or_else(|| io::Error::other("the declaration worker should pause"))?;
    let mut execution = Box::pin(plan.execute(&room, Some(&media_transport)));
    assert!(futures_util::poll!(execution.as_mut()).is_pending());
    // Only the final setup can enqueue work because every preceding phase is empty.
    assert!(
        media_transport
            .worker_pressure_snapshots()
            .iter()
            .any(|worker| worker.command_backlog_depth > 0),
        "consumer declaration should queue behind the paused worker"
    );
    let cancellation = catch_unwind(AssertUnwindSafe(|| drop(execution)));
    release_worker.send(()).map_err(io::Error::other)?;
    let panic = cancellation
        .err()
        .ok_or_else(|| io::Error::other("cancelled setup should diagnose unfinished work"))?;
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied());
    assert_eq!(
        message,
        Some("unfinished lifecycle obligation: room transport plan")
    );
    Ok(())
}

#[test]
fn keyframe_failure_keeps_source_policy_update() -> Result<(), io::Error> {
    let consumer = session_key(1, UserId::Integer(1));
    let source = session_key(2, UserId::Integer(2));
    let route = TransportConsumerRoute::new(
        consumer.clone(),
        TransportMediaId::new(1),
        TransportSourceKey::new(source.clone(), TransportMediaId::new(2)),
    );
    let update = ConsumerPacketSelectionUpdate::route_activity(
        SubscriptionKey::new(
            consumer.user_id(),
            source.user_id(),
            &UserStreamId::from("camera"),
        ),
        PublishedSourceId::from_raw(1),
        route,
        ConsumerSourceSelection::open(true),
        Some(PolicyPauseReason::HiddenTile),
    )
    .ok_or_else(|| io::Error::other("policy pause change should create a route update"))?;
    let mut accepted_policy_updates = Vec::new();
    ConsumerRouteFinish::SourcePolicy(update.clone()).finish(
        "room-route-effects",
        ConsumerRouteControlOutcome::keyframe_error(TransportAdapterError::TransportUnavailable),
        &mut accepted_policy_updates,
    );
    assert_eq!(accepted_policy_updates, [update]);
    Ok(())
}

#[tokio::test]
async fn room_route_effects_execute_finish_work() -> Result<(), Box<dyn Error>> {
    let fixture = RouteFixture::new().await?;
    fixture.request_standalone_keyframe().await;
    fixture.pause_route_activity().await?;
    fixture.apply_setup_activity_correction(false).await?;
    fixture.apply_setup_activity_correction(true).await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn route_activity_events_survive_transport_failure() -> Result<(), Box<dyn Error>> {
    let _guard = capture().await;
    let fixture = RouteFixture::new().await?;
    let producer = TransportSessionKey::new(
        RoomInstanceId::from_raw(70),
        MediaWorkerId::from_raw(99),
        ConnectionId::from_raw(41),
        UserId::Integer(7),
    );
    let consumer = TransportSessionKey::new(
        RoomInstanceId::from_raw(70),
        MediaWorkerId::from_raw(99),
        ConnectionId::from_raw(42),
        UserId::Integer(8),
    );
    let source = TransportSourceKey::new(producer, TransportMediaId::new(71));
    let route = TransportConsumerRoute::new(consumer, TransportMediaId::new(81), source.clone());
    let target =
        ConsumerRouteTarget::for_test(route, UserStreamId::from("camera"), MediaKind::Video);
    let mut routes = RoomRouteEffects::default();
    routes.producer_activity(
        source,
        UserStreamId::from("camera"),
        SourceActivityUpdate::new(
            ProducerActivity::Inactive,
            SourceActivityRevision::default().next(),
        ),
    );
    routes.receiver_activity(ReceiverRouteActivity::new(target, false));

    routes
        .execute("room-route-failure", &fixture.media_transport)
        .await;

    assert_user_exact(
        telemetry_event::PUBLICATION_ACTIVITY_CHANGED,
        "room-route-failure",
        "7",
        41,
        99,
        &[
            ("transport_media_id", Value::from(71)),
            ("active", Value::from(false)),
            ("stream_id", Value::from("camera")),
        ],
    );
    assert_user_exact(
        telemetry_event::SUBSCRIPTION_ACTIVITY_CHANGED,
        "room-route-failure",
        "8",
        42,
        99,
        &[
            ("transport_media_id", Value::from(81)),
            ("active", Value::from(false)),
            ("producer_user_id", Value::from("7")),
            ("source_transport_media_id", Value::from(71)),
            ("stream_id", Value::from("camera")),
        ],
    );
    Ok(())
}

struct RouteFixture {
    media_transport: MediaTransport,
    metrics: Arc<RuntimeMetrics>,
    route: TransportConsumerRoute,
    target: ConsumerRouteTarget,
}

impl RouteFixture {
    async fn new() -> Result<Self, Box<dyn Error>> {
        let metrics = Arc::new(RuntimeMetrics::default());
        let mut deps = test_media_transport_deps();
        deps.metrics = Arc::clone(&metrics);
        let media_transport =
            MediaTransport::build(test_media_transport_config(1, test_rtc_port_range()), deps)?;
        let source_session = session_key(1, UserId::Integer(1));
        let consumer_session = session_key(2, UserId::Integer(2));
        media_transport
            .create_initial_session_offer("test-room", &source_session)
            .await?;
        media_transport
            .create_initial_session_offer("test-room", &consumer_session)
            .await?;

        let source_media = media_transport
            .publish_media(
                &source_session,
                MediaKind::Video,
                &sample_rtp_parameters("cam-up", 71_000),
            )
            .await?;
        let consumer_media = media_transport
            .consume_media(
                &consumer_session,
                MediaKind::Video,
                &source_session,
                source_media,
                &sample_rtp_parameters("cam-down", 72_000),
                ConsumerActivity::Active,
            )
            .await?;
        let source = TransportSourceKey::new(source_session.clone(), source_media);
        let route = TransportConsumerRoute::new(consumer_session.clone(), consumer_media, source);
        let target = ConsumerRouteTarget::for_test(
            route.clone(),
            UserStreamId::from("camera"),
            MediaKind::Video,
        );
        Ok(Self {
            media_transport,
            metrics,
            route,
            target,
        })
    }

    async fn request_standalone_keyframe(&self) {
        let mut routes = RoomRouteEffects::default();
        routes.keyframe(self.target.clone());
        routes
            .execute("room-route-effects", &self.media_transport)
            .await;
        assert_eq!(self.metrics.snapshot().rtc_keyframe_requests_forwarded(), 1);
    }

    async fn pause_route_activity(&self) -> Result<(), io::Error> {
        let mut routes = RoomRouteEffects::default();
        routes.producer_activity(
            self.route.source().clone(),
            UserStreamId::from("camera"),
            SourceActivityUpdate::new(
                ProducerActivity::Inactive,
                SourceActivityRevision::default().next(),
            ),
        );
        routes.receiver_activity(ReceiverRouteActivity::new(self.target.clone(), false));

        routes
            .execute("room-route-effects", &self.media_transport)
            .await;
        self.assert_route_state(0, false).await?;
        Ok(())
    }

    async fn apply_setup_activity_correction(&self, active: bool) -> Result<(), io::Error> {
        let keyframes = self.keyframe_request_count();
        let mut routes = RoomRouteEffects::default();
        routes.setup_activity(self.route.clone(), MediaKind::Video, active);
        routes
            .execute("room-route-effects", &self.media_transport)
            .await;

        assert_eq!(self.keyframe_request_count(), keyframes);
        self.assert_route_state(usize::from(active), active).await
    }

    async fn assert_route_state(
        &self,
        active_destination_count: usize,
        destination_active: bool,
    ) -> Result<(), io::Error> {
        let route = self.route_entry().await?;
        assert!(!route.source_active);
        assert_eq!(route.active_destination_count, active_destination_count);
        assert!(route.destinations.iter().any(|destination| {
            destination.dest_transport_media_id == self.route.consumer_transport_media_id()
                && destination.active == destination_active
        }));
        Ok(())
    }

    async fn route_entry(&self) -> Result<DebugRouteEntry, io::Error> {
        self.media_transport
            .test_api()
            .route_entry_by_media_id(self.route.source_transport_media_id())
            .await
            .map_err(io::Error::other)?
            .ok_or_else(|| io::Error::other("video route should exist"))
    }

    fn keyframe_request_count(&self) -> u64 {
        let snapshot = self.metrics.snapshot();
        snapshot.rtc_keyframe_requests_forwarded() + snapshot.rtc_keyframe_requests_absorbed()
    }
}

fn session_key(connection_id: u64, user_id: UserId) -> TransportSessionKey {
    TransportSessionKey::new(
        RoomInstanceId::from_raw(70),
        MediaWorkerId::from_raw(0),
        ConnectionId::from_raw(connection_id),
        user_id,
    )
}

fn sample_rtp_parameters(mid: &str, ssrc: u32) -> MediaStream {
    MediaStream::new(vec![], vec![], vec![StreamBinding::new().with_ssrc(ssrc)])
        .with_mid(String::from(mid))
}
