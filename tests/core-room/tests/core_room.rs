#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]

use std::collections::BTreeMap;

use anyhow::{Result, anyhow};
use o_sfu_core::{
    prelude::{SourceSubscriptionIntent, UserStreamId},
    server::{
        room::{
            JoinUserRequest, RoomAdmissionPolicy, RoomConfig, RoomManager, RoomManagerJoinError,
            test_support::{
                TestSourceKind, TestSubscriptionStates, stream_id_for_source,
                subscription_intents_from_test_states,
            },
        },
        session::{UserId, UserPermissions},
    },
};
mod support;

use support::{
    ReadyRoom, TEST_ROOM_KEY, close_user, home_worker, join_ready_users, join_user,
    manager_with_policy, media_transport, publish_audio_and_camera, publish_track, router_count,
    serve_room, spillover_policy, test_sender, test_video_rtp_parameters,
};

type SubscriptionIntents = BTreeMap<UserStreamId, SourceSubscriptionIntent>;

fn pause_scalable_video_intents() -> SubscriptionIntents {
    subscription_intents_from_test_states(&TestSubscriptionStates {
        scalable_video: Some(false),
        ..TestSubscriptionStates::default()
    })
}

fn resume_scalable_video_intents() -> SubscriptionIntents {
    subscription_intents_from_test_states(&TestSubscriptionStates {
        scalable_video: Some(true),
        ..TestSubscriptionStates::default()
    })
}

fn pause_audio_and_scalable_video_intents() -> SubscriptionIntents {
    subscription_intents_from_test_states(&TestSubscriptionStates {
        audio_detector: Some(false),
        scalable_video: Some(false),
        ..TestSubscriptionStates::default()
    })
}

async fn update_subscription(
    ready: &ReadyRoom,
    subscriber_id: i64,
    publisher_id: &UserId,
    intents: &SubscriptionIntents,
) -> Result<()> {
    let session = ready
        .sessions
        .get(&subscriber_id)
        .ok_or_else(|| anyhow!("ready room should include subscriber session"))?;
    session
        .subscribe(publisher_id, intents)
        .await
        .map_err(|error| anyhow!("subscription update should commit: {error}"))
}

#[tokio::test]
async fn room_manager_is_idempotent_by_issuer_key_and_config() -> Result<()> {
    let manager = RoomManager::for_test();
    let config = RoomConfig::default();
    let first = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &config, None)
        .await?;
    let second = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &config, None)
        .await?;
    let third = manager
        .serve_room("issuer-b", TEST_ROOM_KEY, &config, None)
        .await?;
    assert_eq!(
        first.uuid(),
        second.uuid(),
        "a repeat request with the same key and config should reuse the reservation"
    );
    assert_ne!(
        first.uuid(),
        third.uuid(),
        "a different issuer should get its own room"
    );
    Ok(())
}

#[tokio::test]
async fn room_manager_join_user_reports_missing_room() -> Result<()> {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let media_transport = media_transport()?;
    let (sender, _receiver) = test_sender();
    let result = manager
        .join_user(
            "missing-room",
            JoinUserRequest {
                user_id: UserId::Integer(1),
                label: None,
                permissions: UserPermissions::default(),
                sender,
            },
            &media_transport,
        )
        .await;
    assert!(matches!(result, Err(RoomManagerJoinError::MissingRoom)));
    Ok(())
}

#[tokio::test]
async fn manager_leave_user_removes_empty_room() -> Result<()> {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let media_transport = media_transport()?;
    let room = serve_room(&manager, "issuer-leave-empty").await?;
    let room_id = room.uuid();
    let connection_id = join_user(&manager, &room, 1, &media_transport).await?;
    let user_id = UserId::Integer(1);
    room.test_api()
        .make_session_ready(&user_id, &media_transport)
        .await?;
    publish_track(
        &room,
        &user_id,
        TestSourceKind::ReadableVideo,
        test_video_rtp_parameters(),
        &media_transport,
    )
    .await?;
    let media = room
        .test_api()
        .first_published_transport_media_id()
        .await
        .ok_or_else(|| anyhow!("published track should expose transport media"))?;

    close_user(&manager, &room, 1, connection_id, &media_transport).await?;

    assert!(
        media_transport
            .test_api()
            .route_entry_by_media_id(media)
            .await
            .is_none()
    );
    assert!(manager.get_by_uuid(room_id).await.is_none());
    Ok(())
}

#[tokio::test]
async fn manager_disconnect_users_removes_empty_room() -> Result<()> {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let media_transport = media_transport()?;
    let room = serve_room(&manager, "issuer-disconnect-empty").await?;
    let room_id = room.uuid();
    join_user(&manager, &room, 1, &media_transport).await?;

    manager
        .disconnect_users(room_id, &[UserId::Integer(1)], &media_transport)
        .await;

    assert!(manager.get_by_uuid(room_id).await.is_none());
    Ok(())
}

#[tokio::test]
async fn healthy_room_stays_on_its_primary_worker() -> Result<()> {
    let manager = manager_with_policy(spillover_policy(2)?);
    let media_transport = media_transport()?;
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(0); 4]);
    let room = serve_room(&manager, "issuer-healthy-room").await?;

    for user_id in 1..=64 {
        join_user(&manager, &room, user_id, &media_transport).await?;
    }

    let primary_worker = home_worker(&room, 1).await;
    for user_id in 2..=64 {
        assert_eq!(home_worker(&room, user_id).await, primary_worker);
    }
    assert_eq!(router_count(&room).await, 1);
    Ok(())
}

#[tokio::test]
async fn new_rooms_cycle_across_healthy_workers() -> Result<()> {
    let manager = manager_with_policy(spillover_policy(2)?);
    let media_transport = media_transport()?;
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(0); 4]);
    let mut room_count_by_worker = [0; 4];

    for room_index in 0..8 {
        let room = serve_room(&manager, &format!("issuer-cyclic-{room_index}")).await?;
        join_user(&manager, &room, 1, &media_transport).await?;
        let worker = home_worker(&room, 1)
            .await
            .ok_or_else(|| anyhow!("joined user should have a worker"))?;
        *room_count_by_worker
            .get_mut(worker)
            .ok_or_else(|| anyhow!("joined user worker should exist"))? += 1;
    }

    assert_eq!(room_count_by_worker, [2; 4]);
    Ok(())
}

#[tokio::test]
async fn overloaded_room_reaches_but_does_not_exceed_local_router_cap() -> Result<()> {
    const LOCAL_ROUTER_CAP: usize = 4;
    let manager = manager_with_policy(spillover_policy(LOCAL_ROUTER_CAP)?);
    let media_transport = media_transport()?;
    let room = serve_room(&manager, "issuer-overloaded-room-cap").await?;

    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(0), None, None, None]);
    join_user(&manager, &room, 1, &media_transport).await?;
    for worker_id in 1..LOCAL_ROUTER_CAP {
        let mut delays = vec![None; LOCAL_ROUTER_CAP];
        delays
            .get_mut(..worker_id)
            .ok_or_else(|| anyhow!("assigned workers should exist"))?
            .fill(Some(20));
        *delays
            .get_mut(worker_id)
            .ok_or_else(|| anyhow!("spillover worker should exist"))? = Some(0);
        media_transport.test_api().set_packet_loop_delays_ms(delays);
        join_user(
            &manager,
            &room,
            i64::try_from(worker_id + 1)?,
            &media_transport,
        )
        .await?;
        assert_eq!(
            home_worker(&room, i64::try_from(worker_id + 1)?).await,
            Some(worker_id)
        );
    }

    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(20); LOCAL_ROUTER_CAP]);
    for user_id in 5..=12 {
        join_user(&manager, &room, user_id, &media_transport).await?;
        assert_eq!(router_count(&room).await, LOCAL_ROUTER_CAP);
    }
    assert_eq!(router_count(&room).await, LOCAL_ROUTER_CAP);
    Ok(())
}

#[tokio::test]
async fn next_join_uses_spillover_worker_after_leave() -> Result<()> {
    let manager = manager_with_policy(spillover_policy(2)?);
    let media_transport = media_transport()?;
    let room = serve_room(&manager, "issuer-overload-reuse").await?;
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(0), None, None, None]);
    join_user(&manager, &room, 1, &media_transport).await?;
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(20), Some(0), None, None]);
    let second_connection = join_user(&manager, &room, 2, &media_transport).await?;
    assert_eq!(home_worker(&room, 2).await, Some(1));

    close_user(&manager, &room, 2, second_connection, &media_transport).await?;
    join_user(&manager, &room, 3, &media_transport).await?;
    assert_eq!(home_worker(&room, 3).await, Some(1));
    Ok(())
}

#[tokio::test]
async fn subscription_change_pauses_and_resumes_consumer_silently() -> Result<()> {
    let mut ready = join_ready_users(&[1, 2]).await?;
    publish_track(
        &ready.room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        test_video_rtp_parameters(),
        &ready.media_transport,
    )
    .await?;
    ready.drain_user(1)?;
    ready.drain_user(2)?;

    let subscriber_id = 2;
    let publisher_id = UserId::Integer(1);
    update_subscription(
        &ready,
        subscriber_id,
        &publisher_id,
        &pause_scalable_video_intents(),
    )
    .await?;
    ready.assert_no_outbound(1)?;
    ready.assert_no_outbound(2)?;
    assert_eq!(ready.room.test_api().consumer_count().await, 1);

    update_subscription(
        &ready,
        subscriber_id,
        &publisher_id,
        &resume_scalable_video_intents(),
    )
    .await?;
    ready.assert_no_outbound(1)?;
    ready.assert_no_outbound(2)?;
    assert_eq!(ready.room.test_api().consumer_count().await, 1);
    Ok(())
}

#[tokio::test]
async fn subscription_change_persists_preference_for_future_consumer_setup() -> Result<()> {
    let mut ready = join_ready_users(&[1, 2]).await?;
    ready.drain_user(1)?;
    ready.drain_user(2)?;
    let subscriber_id = 2;
    let publisher_id = UserId::Integer(1);
    update_subscription(
        &ready,
        subscriber_id,
        &publisher_id,
        &pause_audio_and_scalable_video_intents(),
    )
    .await?;
    ready.assert_no_outbound(1)?;
    ready.assert_no_outbound(2)?;

    publish_track(
        &ready.room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        test_video_rtp_parameters(),
        &ready.media_transport,
    )
    .await?;

    assert_eq!(ready.room.test_api().consumer_count().await, 1);
    Ok(())
}

#[tokio::test]
async fn subscription_change_handles_multiple_stream_types() -> Result<()> {
    let mut ready = join_ready_users(&[1, 2]).await?;
    publish_audio_and_camera(&ready.room, &UserId::Integer(1), &ready.media_transport).await?;
    ready.drain_user(1)?;
    ready.drain_user(2)?;

    let subscriber_id = 2;
    let publisher_id = UserId::Integer(1);
    update_subscription(
        &ready,
        subscriber_id,
        &publisher_id,
        &pause_audio_and_scalable_video_intents(),
    )
    .await?;

    ready.assert_no_outbound(1)?;
    ready.assert_no_outbound(2)?;
    assert_eq!(ready.room.test_api().consumer_count().await, 2);
    Ok(())
}

#[tokio::test]
async fn publication_activity_after_source_owner_leave_is_a_noop() -> Result<()> {
    let mut ready = join_ready_users(&[1, 2]).await?;
    publish_track(
        &ready.room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        test_video_rtp_parameters(),
        &ready.media_transport,
    )
    .await?;
    ready.drain_user(1)?;
    ready.drain_user(2)?;
    let publisher_id = UserId::Integer(1);
    let mut publisher_session = ready
        .sessions
        .remove(&1)
        .ok_or_else(|| anyhow!("ready room should include publisher session"))?;
    assert!(publisher_session.close().await);
    ready.drain_user(2)?;

    let subscriber_id = 2;
    update_subscription(
        &ready,
        subscriber_id,
        &publisher_id,
        &pause_scalable_video_intents(),
    )
    .await?;
    ready.assert_no_outbound(2)?;

    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);
    assert!(
        !ready
            .room
            .test_api()
            .deactivate_publication(&publisher_id, &stream_id, &ready.media_transport,)
            .await
    );
    Ok(())
}
