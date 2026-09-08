#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "transition tests fail loudly when fixed room setup is invalid"
)]

use std::sync::Arc;

use o_sfu_router::{
    rtp::MediaCapabilities, test_support::rtp_samples::sample_simulcast_video_rtp_parameters,
};

use super::PublishStageOutcome;
use crate::engine::{
    ConnectionId, TestSourceKind, UserId, UserPermissions,
    media_transport::{
        AppliedSessionAnswer, MediaTransport, TransportMediaId,
        test_support::{test_media_transport, test_rtc_port_range},
    },
    metrics::RuntimeMetrics,
    room::{Room, RoomConfig, RoomManager, UserOutboundSender},
    source_model::test_support::{source_publish_intent_for_source, stream_id_for_source},
};

fn media_transport() -> MediaTransport {
    test_media_transport(4, test_rtc_port_range())
        .expect("test media transport config should be valid")
}

fn test_sender() -> UserOutboundSender {
    UserOutboundSender::channel(1024, Arc::new(RuntimeMetrics::default())).0
}

async fn join_user(room: &Arc<Room>, user_id: &UserId) -> ConnectionId {
    room.test_api()
        .join_user(
            user_id.clone(),
            None,
            UserPermissions::default(),
            test_sender(),
        )
        .await
        .expect("test user should join")
}

async fn prepare_publish_session(
    room: &Arc<Room>,
    media_transport: &MediaTransport,
    user_id: &UserId,
) -> ConnectionId {
    let connection_id = join_user(room, user_id).await;
    let session_key = room.transport_user_key(user_id, connection_id).await;
    media_transport
        .create_initial_session_offer("test-room", &session_key)
        .await
        .expect("test session should create an initial offer");
    assert_eq!(
        room.apply_session_negotiated(
            user_id,
            connection_id,
            MediaCapabilities::default(),
            media_transport,
        )
        .await,
        Some(())
    );
    connection_id
}

async fn staged_room() -> (Arc<Room>, MediaTransport, UserId, ConnectionId) {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room(
            "issuer-transition-publication",
            "room".into(),
            &RoomConfig::default(),
            None,
        )
        .await
        .expect("test room should be served");
    let media_transport = media_transport();
    let user_id = UserId::Integer(1);
    let connection_id = prepare_publish_session(&room, &media_transport, &user_id).await;
    assert_eq!(
        room.user_operation(&user_id, connection_id, &media_transport)
            .stage_negotiated_publish(&source_publish_intent_for_source(
                TestSourceKind::ScalableVideo,
            ))
            .await
            .expect("stage publish should not fail"),
        PublishStageOutcome::Staged
    );
    (room, media_transport, user_id, connection_id)
}

async fn staged_media_id(
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
) -> TransportMediaId {
    room.staged_media_id(user_id, connection_id, TestSourceKind::ScalableVideo)
        .await
        .expect("test publish should be staged")
}

#[tokio::test]
async fn staged_publish_is_not_visible_in_room_graph_before_answer() {
    let (room, media_transport, user_id, connection_id) = staged_room().await;
    let transport_media_id = staged_media_id(&room, &user_id, connection_id).await;
    let session_key = room.transport_user_key(&user_id, connection_id).await;

    assert_eq!(room.test_api().producer_count().await, 0);
    assert!(
        room.has_staged_publish(
            &user_id,
            connection_id,
            &stream_id_for_source(TestSourceKind::ScalableVideo)
        )
        .await
    );
    assert!(
        media_transport
            .transport_media_mid(&session_key, transport_media_id)
            .await
            .is_some()
    );
    assert!(
        room.user_operation(&user_id, connection_id, &media_transport)
            .rollback_staged_publish(&stream_id_for_source(TestSourceKind::ScalableVideo))
            .await
    );
}

#[tokio::test]
async fn missing_answered_producer_parameters_release_reserved_publish() {
    let (room, media_transport, user_id, connection_id) = staged_room().await;
    let transport_media_id = staged_media_id(&room, &user_id, connection_id).await;

    room.user_operation(&user_id, connection_id, &media_transport)
        .commit_staged_publishes(&AppliedSessionAnswer::default())
        .await;

    assert_eq!(room.test_api().producer_count().await, 0);
    assert_eq!(room.staged_count(&user_id, connection_id).await, 0);
    assert!(
        media_transport
            .test_api()
            .route_entry_by_media_id(transport_media_id)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn stale_connection_commit_rejects_and_releases_reserved_publish() {
    let (room, media_transport, user_id, stale_connection_id) = staged_room().await;
    let transport_media_id = staged_media_id(&room, &user_id, stale_connection_id).await;
    let _new_connection_id = prepare_publish_session(&room, &media_transport, &user_id).await;
    let applied_answer = AppliedSessionAnswer::from_negotiated_producers([(
        transport_media_id,
        sample_simulcast_video_rtp_parameters(None),
    )]);

    room.user_operation(&user_id, stale_connection_id, &media_transport)
        .commit_staged_publishes(&applied_answer)
        .await;

    assert_eq!(room.test_api().producer_count().await, 0);
    assert!(
        media_transport
            .test_api()
            .route_entry_by_media_id(transport_media_id)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn rollback_before_answer_consumes_reserved_publish_once() {
    let (room, media_transport, user_id, connection_id) = staged_room().await;
    let transport_media_id = staged_media_id(&room, &user_id, connection_id).await;

    assert!(
        room.user_operation(&user_id, connection_id, &media_transport)
            .rollback_staged_publish(&stream_id_for_source(TestSourceKind::ScalableVideo))
            .await
    );

    assert!(
        !room
            .user_operation(&user_id, connection_id, &media_transport)
            .rollback_staged_publish(&stream_id_for_source(TestSourceKind::ScalableVideo))
            .await
    );
    assert!(
        media_transport
            .test_api()
            .route_entry_by_media_id(transport_media_id)
            .await
            .is_none()
    );
}
