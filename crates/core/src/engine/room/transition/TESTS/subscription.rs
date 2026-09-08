#![allow(
    clippy::expect_used,
    reason = "transition tests fail loudly when fixed room setup is invalid"
)]

use std::{
    collections::BTreeMap,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use o_sfu_router::test_support::rtp_samples::{
    sample_client_rtp_capabilities, sample_simulcast_video_rtp_parameters,
};
use o_sfu_telemetry::schema::event as telemetry_event;
use serde_json::Value;

use super::super::super::{
    Room, RoomAdmissionPolicy, RoomConfig, RoomManager, RoomRuntimePolicy,
    TESTS::tracing::{assert_user_exact, capture},
    UserOutbound, UserOutboundReceiver, UserOutboundSender,
    media_graph::ConsumerRouteState,
    transition::{PublishIntentOutcome, PublishStageOutcome},
};
use crate::{
    RoomWorkerPolicy, RuntimeFeatureFlags,
    engine::{
        ConnectionId, TestSourceKind, UserId, UserPermissions,
        media_transport::{
            AppliedSessionAnswer, MediaTransport, TransportMediaId, TransportRelayRouteAction,
            TransportTeardown,
            test_support::{test_media_transport, test_rtc_port_range},
        },
        metrics::RuntimeMetrics,
        source_model::{
            SourceSubscriptionIntent, UserStreamId,
            test_support::{source_publish_intent_for_source, stream_id_for_source},
        },
    },
};

fn media_transport() -> MediaTransport {
    test_media_transport(4, test_rtc_port_range())
        .expect("test media transport config should be valid")
}

fn test_sender() -> UserOutboundSender {
    test_outbound().0
}

fn test_outbound() -> (UserOutboundSender, UserOutboundReceiver) {
    UserOutboundSender::channel(1024, Arc::new(RuntimeMetrics::default()))
}

fn pause_scalable_video_intents() -> BTreeMap<UserStreamId, SourceSubscriptionIntent> {
    BTreeMap::from([(
        stream_id_for_source(TestSourceKind::ScalableVideo),
        SourceSubscriptionIntent::new(Some(false), None),
    )])
}

fn active_scalable_video_intents() -> BTreeMap<UserStreamId, SourceSubscriptionIntent> {
    BTreeMap::from([(
        stream_id_for_source(TestSourceKind::ScalableVideo),
        SourceSubscriptionIntent::new(Some(true), None),
    )])
}

async fn join_negotiated_user(
    room: &Arc<Room>,
    media_transport: &MediaTransport,
    user_id: &UserId,
    create_transport_session: bool,
) -> ConnectionId {
    join_negotiated_user_with_sender(
        room,
        media_transport,
        user_id,
        create_transport_session,
        test_sender(),
        None,
    )
    .await
}

async fn join_negotiated_user_with_sender(
    room: &Arc<Room>,
    media_transport: &MediaTransport,
    user_id: &UserId,
    create_transport_session: bool,
    sender: UserOutboundSender,
    packet_loop_delays_ms: Option<Vec<Option<u64>>>,
) -> ConnectionId {
    let api = room.test_api();
    let connection_id = if let Some(delays_ms) = packet_loop_delays_ms {
        api.join_user_with_packet_loop_delays(
            user_id.clone(),
            None,
            UserPermissions::default(),
            sender,
            delays_ms,
        )
        .await
    } else {
        api.join_user(user_id.clone(), None, UserPermissions::default(), sender)
            .await
    }
    .expect("test user should join");
    if create_transport_session {
        let session_key = room.transport_user_key(user_id, connection_id).await;
        media_transport
            .create_initial_session_offer("test-room", &session_key)
            .await
            .expect("test session should create an initial offer");
    }
    assert_eq!(
        room.apply_session_negotiated(
            user_id,
            connection_id,
            sample_client_rtp_capabilities(),
            media_transport,
        )
        .await,
        Some(())
    );
    connection_id
}

fn drain_setup_track(rx: &mut UserOutboundReceiver) -> bool {
    let mut found = false;
    while let Ok(message) = rx.try_recv() {
        found |= matches!(message, UserOutbound::RemoteTracks(_));
    }
    found
}

async fn setup_subscription_room(
    create_subscriber_transport_session: bool,
) -> (
    Arc<Room>,
    MediaTransport,
    UserId,
    ConnectionId,
    UserId,
    ConnectionId,
) {
    setup_subscription_room_with_manager(
        RoomManager::for_test(),
        "issuer-transition-subscription",
        create_subscriber_transport_session,
    )
    .await
}

async fn setup_spillover_subscription_room() -> (
    Arc<Room>,
    MediaTransport,
    UserId,
    ConnectionId,
    UserId,
    ConnectionId,
) {
    let setup = setup_subscription_room_with_manager(
        RoomManager::for_test_with_runtime_policy(
            RoomRuntimePolicy::new(
                RoomAdmissionPolicy::new(100),
                RuntimeFeatureFlags::default(),
                sample_client_rtp_capabilities(),
            )
            .with_room_worker_policy(RoomWorkerPolicy::new(
                NonZeroUsize::new(2).expect("test router cap should be positive"),
                NonZeroU64::new(RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS)
                    .expect("default delay threshold should be positive"),
            )),
        ),
        "issuer-transition-subscription-spillover",
        true,
    )
    .await;
    let (
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        subscriber_id,
        subscriber_connection_id,
    ) = setup;
    assert_ne!(
        room.transport_user_key(&publisher_id, publisher_connection_id)
            .await
            .media_worker_id(),
        room.transport_user_key(&subscriber_id, subscriber_connection_id)
            .await
            .media_worker_id()
    );
    (
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        subscriber_id,
        subscriber_connection_id,
    )
}

async fn setup_subscription_room_with_manager(
    manager: RoomManager,
    issuer: &str,
    create_subscriber_transport_session: bool,
) -> (
    Arc<Room>,
    MediaTransport,
    UserId,
    ConnectionId,
    UserId,
    ConnectionId,
) {
    setup_subscription_room_with_sender(
        manager,
        issuer,
        create_subscriber_transport_session,
        test_sender(),
    )
    .await
}

async fn setup_subscription_room_with_sender(
    manager: RoomManager,
    issuer: &str,
    create_subscriber_transport_session: bool,
    subscriber_sender: UserOutboundSender,
) -> (
    Arc<Room>,
    MediaTransport,
    UserId,
    ConnectionId,
    UserId,
    ConnectionId,
) {
    let room = manager
        .serve_room(issuer, "room".into(), &RoomConfig::default(), None)
        .await
        .expect("test room should be served");
    let media_transport = media_transport();
    let publisher_id = UserId::Integer(1);
    let subscriber_id = UserId::Integer(2);
    let publisher_connection_id =
        join_negotiated_user(&room, &media_transport, &publisher_id, true).await;
    let packet_loop_delays_ms = if room.room_worker_policy().max_local_routers() > 1 {
        let primary_worker = room
            .transport_user_key(&publisher_id, publisher_connection_id)
            .await
            .media_worker_id();
        let delays = (0..2)
            .map(|worker| {
                Some(if worker == primary_worker.as_usize() {
                    RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS
                } else {
                    0
                })
            })
            .collect();
        Some(delays)
    } else {
        None
    };
    let subscriber_connection_id = join_negotiated_user_with_sender(
        &room,
        &media_transport,
        &subscriber_id,
        create_subscriber_transport_session,
        subscriber_sender,
        packet_loop_delays_ms,
    )
    .await;
    (
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        subscriber_id,
        subscriber_connection_id,
    )
}

async fn publish_scalable_video(
    room: &Room,
    media_transport: &MediaTransport,
    publisher_id: &UserId,
    publisher_connection_id: ConnectionId,
) -> TransportMediaId {
    let transport_media_id =
        stage_scalable_video(room, media_transport, publisher_id, publisher_connection_id).await;
    commit_scalable_video(
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        transport_media_id,
    )
    .await;
    transport_media_id
}

async fn stage_scalable_video(
    room: &Room,
    media_transport: &MediaTransport,
    publisher_id: &UserId,
    publisher_connection_id: ConnectionId,
) -> TransportMediaId {
    assert_eq!(
        room.user_operation(publisher_id, publisher_connection_id, media_transport)
            .stage_negotiated_publish(&source_publish_intent_for_source(
                TestSourceKind::ScalableVideo,
            ))
            .await
            .expect("stage publish should not fail"),
        PublishStageOutcome::Staged
    );
    room.staged_media_id(
        publisher_id,
        publisher_connection_id,
        TestSourceKind::ScalableVideo,
    )
    .await
    .expect("test publish should be staged")
}

async fn commit_scalable_video(
    room: &Room,
    media_transport: &MediaTransport,
    publisher_id: &UserId,
    publisher_connection_id: ConnectionId,
    transport_media_id: TransportMediaId,
) {
    room.user_operation(publisher_id, publisher_connection_id, media_transport)
        .commit_staged_publishes(&AppliedSessionAnswer::from_negotiated_producers([(
            transport_media_id,
            sample_simulcast_video_rtp_parameters(None),
        )]))
        .await;
    assert_eq!(room.test_api().producer_count().await, 1);
}

async fn destination_state(
    media_transport: &MediaTransport,
    source_media_id: TransportMediaId,
    user_id: &UserId,
) -> Option<(TransportMediaId, bool)> {
    media_transport
        .test_api()
        .route_entry_by_media_id(source_media_id)
        .await?
        .destinations
        .into_iter()
        .find(|destination| destination.dest_session.user_id() == user_id)
        .map(|destination| (destination.dest_transport_media_id, destination.active))
}

async fn destination_active(
    media_transport: &MediaTransport,
    source_media_id: TransportMediaId,
    user_id: &UserId,
) -> Option<bool> {
    destination_state(media_transport, source_media_id, user_id)
        .await
        .map(|(_media_id, active)| active)
}

#[tokio::test]
async fn stored_receiver_intent_applies_before_publish_and_across_activity() {
    let room = RoomManager::for_test()
        .serve_room(
            "issuer-transition-subscription-intent",
            "room".into(),
            &RoomConfig::default(),
            None,
        )
        .await
        .expect("test room should be served");
    let media_transport = media_transport();
    let publisher_id = UserId::Integer(1);
    let subscriber_id = UserId::Integer(2);
    let subscriber_connection_id =
        join_negotiated_user(&room, &media_transport, &subscriber_id, true).await;
    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);

    assert_eq!(
        room.user_operation(&subscriber_id, subscriber_connection_id, &media_transport)
            .apply_receiver_intent(&publisher_id, &pause_scalable_video_intents())
            .await,
        Some(())
    );
    let publisher_connection_id =
        join_negotiated_user(&room, &media_transport, &publisher_id, true).await;
    publish_scalable_video(
        &room,
        &media_transport,
        &publisher_id,
        publisher_connection_id,
    )
    .await;

    assert_eq!(
        room.test_api()
            .consumer_route_state(&subscriber_id, &publisher_id, &stream_id)
            .await,
        Some(ConsumerRouteState::Inactive)
    );

    assert!(
        room.test_api()
            .deactivate_publication(&publisher_id, &stream_id, &media_transport)
            .await
    );
    assert_eq!(
        room.test_api()
            .consumer_route_state(&subscriber_id, &publisher_id, &stream_id)
            .await,
        Some(ConsumerRouteState::Inactive)
    );
    assert!(matches!(
        room.user_operation(&publisher_id, publisher_connection_id, &media_transport)
            .start_publish(
                &source_publish_intent_for_source(TestSourceKind::ScalableVideo),
                true,
            )
            .await,
        Ok(PublishIntentOutcome::Activated)
    ));
    assert_eq!(
        room.test_api()
            .consumer_route_state(&subscriber_id, &publisher_id, &stream_id)
            .await,
        Some(ConsumerRouteState::Inactive)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn receiver_intent_updates_transport_route_activity() {
    let (
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        subscriber_id,
        subscriber_connection_id,
    ) = setup_spillover_subscription_room().await;
    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);
    let _guard = capture().await;
    let source_media_id = publish_scalable_video(
        &room,
        &media_transport,
        &publisher_id,
        publisher_connection_id,
    )
    .await;

    let (consumer_media_id, active) =
        destination_state(&media_transport, source_media_id, &subscriber_id)
            .await
            .expect("subscriber route should exist");
    assert!(active);
    let subscriber_worker = room
        .transport_user_key(&subscriber_id, subscriber_connection_id)
        .await
        .media_worker_id()
        .as_usize();
    let route_fields = [
        (
            "transport_media_id",
            Value::from(consumer_media_id.as_u64()),
        ),
        (
            "producer_user_id",
            Value::from(publisher_id.path_segment().as_ref()),
        ),
        (
            "source_transport_media_id",
            Value::from(source_media_id.as_u64()),
        ),
        ("stream_id", Value::from(stream_id.to_string())),
    ];
    let mut subscribe_fields = route_fields.to_vec();
    subscribe_fields.push(("origin", Value::from("publish")));
    assert_user_exact(
        telemetry_event::SUBSCRIBE_SUCCEEDED,
        room.uuid(),
        subscriber_id.path_segment().as_ref(),
        subscriber_connection_id.as_u64(),
        subscriber_worker,
        &subscribe_fields,
    );
    assert_eq!(
        room.user_operation(&subscriber_id, subscriber_connection_id, &media_transport)
            .apply_receiver_intent(&publisher_id, &pause_scalable_video_intents())
            .await,
        Some(())
    );
    assert_eq!(
        room.test_api()
            .consumer_route_state(&subscriber_id, &publisher_id, &stream_id)
            .await,
        Some(ConsumerRouteState::Inactive)
    );
    let (paused_consumer_media_id, active) =
        destination_state(&media_transport, source_media_id, &subscriber_id)
            .await
            .expect("subscriber route should still exist");
    assert!(!active);
    assert_eq!(paused_consumer_media_id, consumer_media_id);
    let mut activity_fields = route_fields.to_vec();
    activity_fields.push(("active", Value::from(false)));
    assert_user_exact(
        telemetry_event::SUBSCRIPTION_ACTIVITY_CHANGED,
        room.uuid(),
        subscriber_id.path_segment().as_ref(),
        subscriber_connection_id.as_u64(),
        subscriber_worker,
        &activity_fields,
    );

    assert_eq!(
        room.user_operation(&subscriber_id, subscriber_connection_id, &media_transport)
            .apply_receiver_intent(&publisher_id, &active_scalable_video_intents())
            .await,
        Some(())
    );
    assert_eq!(
        room.test_api()
            .consumer_route_state(&subscriber_id, &publisher_id, &stream_id)
            .await,
        Some(ConsumerRouteState::Active)
    );
    assert_eq!(
        destination_active(&media_transport, source_media_id, &subscriber_id).await,
        Some(true)
    );
}

#[tokio::test]
async fn transport_consume_failure_releases_pending_setup_for_retry() {
    let (subscriber_sender, mut subscriber_rx) = test_outbound();
    let (
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        subscriber_id,
        subscriber_connection_id,
    ) = setup_subscription_room_with_sender(
        RoomManager::for_test(),
        "issuer-transition-subscription-outbound",
        false,
        subscriber_sender,
    )
    .await;
    assert!(!drain_setup_track(&mut subscriber_rx));
    let source_media_id = publish_scalable_video(
        &room,
        &media_transport,
        &publisher_id,
        publisher_connection_id,
    )
    .await;

    assert_eq!(
        room.test_api()
            .consumer_route_state(
                &subscriber_id,
                &publisher_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
            )
            .await,
        Some(ConsumerRouteState::Absent)
    );
    assert!(!drain_setup_track(&mut subscriber_rx));
    let subscriber_session_key = room
        .transport_user_key(&subscriber_id, subscriber_connection_id)
        .await;
    media_transport
        .create_initial_session_offer("test-room", &subscriber_session_key)
        .await
        .expect("retry session should create an initial offer");

    assert_eq!(
        room.user_operation(&subscriber_id, subscriber_connection_id, &media_transport)
            .apply_session_refreshed(&[])
            .await,
        Some(())
    );
    assert_eq!(room.test_api().consumer_count().await, 1);
    assert!(drain_setup_track(&mut subscriber_rx));
    assert!(
        media_transport
            .test_api()
            .route_entry_by_media_id(source_media_id)
            .await
            .is_some_and(|entry| !entry.destinations.is_empty())
    );
}

#[tokio::test]
async fn relay_setup_failure_releases_pending_setup_for_retry() {
    let (
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        subscriber_id,
        subscriber_connection_id,
    ) = setup_spillover_subscription_room().await;
    let source_media_id = stage_scalable_video(
        &room,
        &media_transport,
        &publisher_id,
        publisher_connection_id,
    )
    .await;
    let publisher_session_key = room
        .transport_user_key(&publisher_id, publisher_connection_id)
        .await;
    media_transport
        .teardown([TransportTeardown::CloseSession {
            session_key: publisher_session_key,
        }])
        .await;

    commit_scalable_video(
        &room,
        &media_transport,
        &publisher_id,
        publisher_connection_id,
        source_media_id,
    )
    .await;

    assert_eq!(room.test_api().consumer_count().await, 0);
    let release_relays = {
        let mut state = room.state.write().await;
        let mut setups = state
            .refresh_consumer_readiness(&subscriber_id, subscriber_connection_id, &[])
            .expect("subscriber session should still be current")
            .work
            .setups;
        let setup = setups.pop().expect("retry setup should be planned");
        assert!(setups.is_empty());
        state.release_pending_consumer_setup(setup)
    };
    assert_eq!(release_relays.len(), 1);
    assert_eq!(
        release_relays.first().map(|effect| effect.action),
        Some(TransportRelayRouteAction::Release)
    );
}

#[tokio::test]
async fn stale_receiver_subscription_update_is_rejected() {
    let (room, media_transport, publisher_id, _, subscriber_id, stale_connection_id) =
        setup_subscription_room(true).await;
    let _current_connection_id =
        join_negotiated_user(&room, &media_transport, &subscriber_id, true).await;

    assert_eq!(
        room.user_operation(&subscriber_id, stale_connection_id, &media_transport)
            .apply_receiver_intent(&publisher_id, &pause_scalable_video_intents())
            .await,
        None
    );
}

#[tokio::test]
async fn committed_consumer_reaches_graph_topology_and_transport() {
    let (
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        subscriber_id,
        _subscriber_connection_id,
    ) = setup_subscription_room(true).await;
    let source_media_id = publish_scalable_video(
        &room,
        &media_transport,
        &publisher_id,
        publisher_connection_id,
    )
    .await;

    assert_eq!(room.test_api().consumer_count().await, 1);
    assert_eq!(
        room.test_api()
            .consumer_route_state(
                &subscriber_id,
                &publisher_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
            )
            .await,
        Some(ConsumerRouteState::Active)
    );
    assert!(
        media_transport
            .test_api()
            .route_entry_by_media_id(source_media_id)
            .await
            .is_some_and(|entry| !entry.destinations.is_empty())
    );
}
