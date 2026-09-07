use std::time::Duration;

use tokio::time::timeout;

use super::support::*;

#[tokio::test]
async fn staged_negotiated_publish_rollback_cleans_transport_media_without_committing_state() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    let transport_media_id = scenario
        .staged_media_id(TestSourceKind::ScalableVideo)
        .await;

    assert!(scenario.rollback_scalable_video().await);

    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(scenario.room.test_api().producer_count().await, 0);
    assert!(
        !scenario
            .route_for_staged_media_exists(transport_media_id)
            .await
    );
    scenario.assert_no_outbound();
}

#[tokio::test]
async fn duplicate_staged_publish_is_ignored_before_transport_reservation() {
    let scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Duplicate
    );
    assert_eq!(scenario.staged_count().await, 1);
    assert!(scenario.rollback_scalable_video().await);
}

#[tokio::test]
async fn staged_negotiated_publish_commit_moves_through_room_owned_transaction() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    let transport_media_id = scenario
        .staged_media_id(TestSourceKind::ScalableVideo)
        .await;

    {
        let source_policy_guard = scenario.room.source_policy_turn.lock().await;
        let mut commit = Box::pin(scenario.commit());
        assert!(
            timeout(Duration::from_millis(10), commit.as_mut())
                .await
                .is_err()
        );
        assert_eq!(scenario.room.test_api().producer_count().await, 0);
        assert_eq!(scenario.staged_count().await, 1);
        drop(commit);
        drop(source_policy_guard);
    }
    scenario.commit().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert!(scenario.scalable_video_is_published().await);
    assert!(
        scenario
            .room
            .state
            .read()
            .await
            .inspect_source_encoding_ids_for_transport_media_id(transport_media_id)
            .is_some()
    );
    assert!(scenario.drain_publisher().is_empty());
    let subscriber_output = scenario.drain_subscriber();
    assert!(
        !subscriber_output.iter().any(|message| {
            remote_track_snapshot(message)
                .is_some_and(|snapshot| snapshot.requires_negotiation && snapshot.tracks.is_empty())
        }),
        "pending consumer routes must not emit empty remote track snapshots"
    );
    assert_remote_track_snapshot_for_stream(&subscriber_output, TestSourceKind::ScalableVideo);
}

#[tokio::test]
async fn staged_publish_connection_teardown_rolls_back_every_staged_stream() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    assert_eq!(
        scenario.stage_source(TestSourceKind::ReadableVideo).await,
        PublishStageOutcome::Staged
    );

    scenario.close_user().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(scenario.room.test_api().producer_count().await, 0);
    let publisher_output = scenario.drain_publisher();
    assert!(
        matches!(
            publisher_output.as_slice(),
            [UserOutbound::Close(UserCloseReason::RemovedByRuntime)]
        ),
        "publisher should only receive the normal close output: {publisher_output:?}"
    );
    let subscriber_output = scenario.drain_subscriber();
    assert!(
        matches!(
            subscriber_output.as_slice(),
            [UserOutbound::Message(RoomEventMessage::UserDeparted { user_id })]
                if user_id == &scenario.user_id
        ),
        "subscriber should only receive the normal departure output: {subscriber_output:?}"
    );
}
