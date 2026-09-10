use std::time::Duration;

use tokio::time::timeout;

use super::support::*;
use crate::engine::{
    UserInfo,
    room::DeactivateIntentOutcome,
    source_model::{SourceDeactivateIntent, SourcePolicy, SourcePublishIntent, UserStreamId},
};

#[tokio::test]
async fn publication_activity_pauses_and_resumes_committed_source() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;

    assert_remote_track_snapshot_for_stream(
        &drain_outbound(&mut rx2),
        TestSourceKind::ScalableVideo,
    );
    assert!(drain_outbound(&mut rx1).is_empty());

    let publisher_id = UserId::Integer(1);
    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);
    let connection_id = user_connection_id(&room, &publisher_id).await;
    let source_policy_guard = room.lock_source_policy().await;
    let pause = room
        .test_api()
        .deactivate_publication(&publisher_id, &stream_id, &adapter);
    tokio::pin!(pause);
    assert!(
        timeout(Duration::from_millis(10), &mut pause)
            .await
            .is_err()
    );
    let state = room.state.read().await;
    let source_id = state
        .published_source_id(&publisher_id, connection_id, &stream_id)
        .expect("publication should remain active while source policy is busy");
    assert!(
        state
            .topology
            .published_source(source_id)
            .expect("publication should remain in the room graph")
            .active
    );
    drop(state);
    drop(source_policy_guard);
    assert!(pause.await);

    let msgs1 = drain_outbound(&mut rx1);
    let msgs2 = drain_outbound(&mut rx2);
    assert!(
        msgs1.is_empty(),
        "publisher should not get a remote track snapshot"
    );
    assert_eq!(msgs2.len(), 1, "subscriber should get a track snapshot");
    assert_remote_track_activity_snapshot(
        &msgs2[0],
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        false,
        false,
    );

    assert_eq!(
        room.user_operation(&publisher_id, connection_id, &adapter)
            .deactivate_publication(&SourceDeactivateIntent::new(stream_id_for_source(
                TestSourceKind::ScalableVideo,
            )))
            .await,
        DeactivateIntentOutcome::Noop
    );
    assert!(drain_outbound(&mut rx2).is_empty());
    assert!(matches!(
        room.user_operation(&publisher_id, connection_id, &adapter)
            .start_publish(
                &source_publish_intent_for_source(TestSourceKind::ScalableVideo),
                true,
            )
            .await,
        Ok(PublishIntentOutcome::Activated)
    ));

    let msgs2 = drain_outbound(&mut rx2);
    assert_remote_track_activity_snapshot(
        &msgs2[0],
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        true,
        false,
    );
    assert!(matches!(
        room.user_operation(&publisher_id, connection_id, &adapter)
            .start_publish(
                &source_publish_intent_for_source(TestSourceKind::ScalableVideo),
                true,
            )
            .await,
        Ok(PublishIntentOutcome::Noop)
    ));
    assert!(drain_outbound(&mut rx2).is_empty());
}

#[tokio::test]
async fn duplicate_camera_publish_intent_updates_presence_and_remote_activity() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;
    let publisher_id = UserId::Integer(1);
    let camera_stream = UserStreamId::new("camera");
    let camera_off_intent = SourcePublishIntent::new(
        camera_stream.clone(),
        MediaKind::Video,
        SourcePolicy::hidden(),
    )
    .with_presence(Some(UserInfo {
        is_camera_on: Some(false),
        ..UserInfo::default()
    }));

    assert!(
        room.test_api()
            .publish_intent(
                &publisher_id,
                &camera_off_intent,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    assert!(
        room.test_api()
            .deactivate_publication(&publisher_id, &camera_stream, &adapter)
            .await
    );
    drain_outbound(&mut rx2);

    let connection_id = user_connection_id(&room, &publisher_id).await;
    let camera_on_intent = SourcePublishIntent::new(
        camera_stream.clone(),
        MediaKind::Video,
        SourcePolicy::hidden(),
    )
    .with_presence(Some(UserInfo {
        is_camera_on: Some(true),
        ..UserInfo::default()
    }));
    assert!(matches!(
        room.user_operation(&publisher_id, connection_id, &adapter)
            .start_publish(&camera_on_intent, true)
            .await,
        Ok(PublishIntentOutcome::Activated)
    ));

    let track_snapshots = drain_remote_track_snapshots(&mut rx2);
    let [track_snapshot] = track_snapshots.as_slice() else {
        panic!("duplicate publish should emit one final track snapshot");
    };
    let [projection] = track_snapshot.tracks.as_slice() else {
        panic!("subscriber should receive the camera track");
    };
    assert_eq!(projection.stream_id, camera_stream);
    assert!(projection.producer_active);
}

#[tokio::test]
async fn late_join_receives_remote_track_snapshot_from_route_state() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await
        .expect("test room should be served");
    let adapter = real_adapter();
    let publisher_id = UserId::Integer(1);
    let subscriber_id = UserId::Integer(2);
    let (publisher_tx, mut publisher_rx) = test_sender();
    let (subscriber_tx, mut subscriber_rx) = test_sender();

    join_user_with_sender(&room, publisher_id.clone(), publisher_tx).await;
    make_session_ready_with_transport(&room, &publisher_id, &adapter).await;
    publish_track(
        &room,
        &publisher_id,
        TestSourceKind::ScalableVideo,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
    assert!(drain_outbound(&mut publisher_rx).is_empty());

    join_user_with_sender(&room, subscriber_id, subscriber_tx).await;
    make_session_ready_with_transport(&room, &UserId::Integer(2), &adapter).await;

    let snapshot = drain_remote_track_snapshots(&mut subscriber_rx)
        .into_iter()
        .next()
        .expect("late subscriber should receive a track snapshot");
    assert!(snapshot.requires_negotiation);
    assert_eq!(snapshot.tracks.len(), 1);
    assert_eq!(snapshot.tracks[0].user_id, publisher_id);
    assert_eq!(
        snapshot.tracks[0].stream_id,
        stream_id_for_source(TestSourceKind::ScalableVideo)
    );
}

#[tokio::test]
async fn publish_track_emits_remote_track_snapshot_with_committed_mid() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;

    assert!(drain_outbound(&mut rx1).is_empty());
    let snapshot = drain_remote_track_snapshots(&mut rx2)
        .into_iter()
        .next()
        .expect("subscriber should receive a remote track snapshot");
    let [projection] = snapshot.tracks.as_slice() else {
        panic!("subscriber should receive one remote track");
    };
    assert!(!projection.consumer_mid.is_empty());
    assert_eq!(
        projection.stream_id,
        stream_id_for_source(TestSourceKind::ScalableVideo)
    );
}

#[tokio::test]
async fn generic_camera_info_cannot_override_publication_presence() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;
    let publisher_id = UserId::Integer(1);
    let camera_stream = UserStreamId::new("camera");
    let camera_intent = SourcePublishIntent::new(
        camera_stream.clone(),
        MediaKind::Video,
        SourcePolicy::hidden(),
    )
    .with_presence(Some(UserInfo {
        is_camera_on: Some(true),
        ..UserInfo::default()
    }));

    assert!(
        room.test_api()
            .publish_intent(
                &publisher_id,
                &camera_intent,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    let connection_id = user_connection_id(&room, &publisher_id).await;
    room.update_user_info(
        &publisher_id,
        connection_id,
        &adapter,
        UserInfo {
            is_camera_on: Some(false),
            ..UserInfo::default()
        },
    )
    .await;

    assert!(drain_remote_track_snapshots(&mut rx2).is_empty());
    let Some((_, info)) = room.test_api().user_info_snapshot(&publisher_id).await else {
        panic!("publisher user should still be present");
    };
    assert_eq!(info.is_camera_on, Some(true));
}

#[tokio::test]
async fn user_replacement_purges_all_published_stream_mappings() {
    let (room, adapter, mut publisher_rx, mut subscriber_rx) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::AudioDetector,
        test_audio_rtp_parameters(),
        &adapter,
    )
    .await;
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert_eq!(
        drain_outbound(&mut subscriber_rx)
            .into_iter()
            .filter(|message| remote_track_snapshot(message).is_some())
            .count(),
        2,
        "subscriber should receive one setup request per published stream"
    );

    let camera_transport_media_id = room
        .test_api()
        .producer_transport_media_id(
            &UserId::Integer(1),
            test_connection_id(0),
            TestSourceKind::ScalableVideo,
        )
        .await;
    let audio_transport_media_id = room
        .test_api()
        .producer_transport_media_id(
            &UserId::Integer(1),
            test_connection_id(0),
            TestSourceKind::AudioDetector,
        )
        .await;
    assert!(camera_transport_media_id.is_some());
    assert!(audio_transport_media_id.is_some());
    assert!(
        room.test_api()
            .deactivate_publication(
                &UserId::Integer(1),
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                &adapter,
            )
            .await
    );
    drain_outbound(&mut subscriber_rx);

    let (replacement_tx, _replacement_rx) = test_sender();
    assert!(
        room.test_api()
            .join_user(
                UserId::Integer(1),
                None,
                UserPermissions::default(),
                replacement_tx,
            )
            .await
            .is_ok()
    );

    assert_eq!(room.test_api().producer_count().await, 0);
    assert_eq!(room.test_api().consumer_count().await, 0);
    assert_transport_media_mapping_is_missing(
        &room,
        camera_transport_media_id.expect("camera producer should expose a transport id"),
    )
    .await;
    assert_transport_media_mapping_is_missing(
        &room,
        audio_transport_media_id.expect("audio producer should expose a transport id"),
    )
    .await;
    assert_user_has_no_published_source(
        &room,
        &UserId::Integer(1),
        test_connection_id(0),
        TestSourceKind::ScalableVideo,
    )
    .await;
    assert_user_has_no_published_source(
        &room,
        &UserId::Integer(1),
        test_connection_id(0),
        TestSourceKind::AudioDetector,
    )
    .await;
}

#[tokio::test]
async fn screen_deactivation_updates_remote_track_activity() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ReadableVideo,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;

    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    let publisher_id = UserId::Integer(1);
    assert!(
        room.test_api()
            .deactivate_publication(
                &publisher_id,
                &stream_id_for_source(TestSourceKind::ReadableVideo),
                &adapter,
            )
            .await
    );

    let msgs = drain_outbound(&mut rx2);
    assert_remote_track_activity_snapshot(
        &msgs[0],
        &UserId::Integer(1),
        TestSourceKind::ReadableVideo,
        false,
        false,
    );
}

#[tokio::test]
async fn publication_deactivation_updates_transport_route_activity() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    let publisher_id = UserId::Integer(1);
    assert!(
        room.test_api()
            .deactivate_publication(
                &publisher_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                &adapter,
            )
            .await
    );
    let transport_media_id = room
        .test_api()
        .producer_transport_media_id(
            &publisher_id,
            user_connection_id(&room, &publisher_id).await,
            TestSourceKind::ScalableVideo,
        )
        .await
        .expect("published camera should expose a transport media id");
    let route = adapter
        .test_api()
        .route_entry_by_media_id(transport_media_id)
        .await
        .expect("published camera should still have a route entry");
    assert!(!route.source_active);
}

#[tokio::test]
async fn publication_deactivation_updates_presence() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    let user_id = UserId::Integer(1);
    let connection_id = user_connection_id(&room, &user_id).await;
    let intent = SourceDeactivateIntent::new(stream_id_for_source(TestSourceKind::ScalableVideo))
        .with_presence(Some(UserInfo {
            is_camera_on: Some(false),
            ..UserInfo::default()
        }));
    assert_eq!(
        room.user_operation(&user_id, connection_id, &adapter)
            .deactivate_publication(&intent)
            .await,
        DeactivateIntentOutcome::Deactivated
    );

    let Some((_, info)) = room.test_api().user_info_snapshot(&user_id).await else {
        panic!("publisher user should still be present");
    };
    assert_eq!(info.is_camera_on, Some(false));
}

#[tokio::test]
async fn unknown_publication_deactivation_is_a_noop() {
    let (room, adapter, mut rx1, mut _rx2) = setup_two_ready_users().await;

    let publisher_id = UserId::Integer(1);
    assert!(
        !room
            .test_api()
            .deactivate_publication(
                &publisher_id,
                &stream_id_for_source(TestSourceKind::AudioDetector),
                &adapter,
            )
            .await
    );

    assert!(
        drain_outbound(&mut rx1).is_empty(),
        "no broadcast expected when no producer exists for the stream type"
    );
}
