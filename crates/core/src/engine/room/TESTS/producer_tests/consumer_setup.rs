use super::support::*;

#[tokio::test]
async fn initial_answer_sets_up_pending_consumers_once() {
    let (room, media_transport, mut publisher_rx, mut subscriber_rx) =
        setup_pending_consumer_readiness_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let user_id = UserId::Integer(2);

    assert!(
        room.test_api()
            .mark_session_ready(&user_id, test_client_rtp_capabilities(), &media_transport)
            .await
    );

    assert_remote_track_snapshot_for_stream(
        &drain_outbound(&mut subscriber_rx),
        TestSourceKind::ScalableVideo,
    );
    assert_eq!(room.test_api().consumer_count().await, 1);

    assert!(
        room.test_api()
            .mark_session_ready(&user_id, test_client_rtp_capabilities(), &media_transport)
            .await
    );
    assert_eq!(room.test_api().consumer_count().await, 1);
    assert!(drain_outbound(&mut subscriber_rx).is_empty());
}

#[tokio::test]
async fn refresh_retry_sets_up_only_missing_consumers_on_real_rtc() {
    let mut scenario = Box::pin(setup_real_rtc_refresh_scenario()).await;

    publish_track(
        &scenario.room,
        &scenario.publisher_user_id,
        TestSourceKind::ScalableVideo,
        video_rtp_parameters_with_mid("cam-refresh-retry", 22_222),
        &scenario.media_transport,
    )
    .await;
    assert!(drain_outbound(&mut scenario.publisher_rx).is_empty());
    assert_remote_track_snapshot_for_stream(
        &drain_outbound(&mut scenario.subscriber_rx),
        TestSourceKind::ScalableVideo,
    );
    assert_eq!(scenario.room.test_api().consumer_count().await, 1);

    let first_refresh_offer = scenario
        .media_transport
        .create_session_renegotiation_offer(&scenario.subscriber_session_key)
        .await
        .expect("first subscriber refresh should stage an rtc offer");

    publish_track(
        &scenario.room,
        &scenario.publisher_user_id,
        TestSourceKind::ReadableVideo,
        video_rtp_parameters_with_mid("screen-refresh-retry", 33_333),
        &scenario.media_transport,
    )
    .await;
    assert_eq!(
        scenario.room.test_api().consumer_count().await,
        1,
        "second consumer must stay pending while the first rtc offer awaits an answer"
    );
    assert!(
        drain_outbound(&mut scenario.subscriber_rx).is_empty(),
        "no second setup should be emitted before the first refresh answer lands"
    );

    let requests_before_refresh = keyframe_request_count(&scenario);
    settle_refresh_offer(&mut scenario, first_refresh_offer).await;
    assert_eq!(
        keyframe_request_count(&scenario),
        requests_before_refresh + 1,
        "refresh answer should keyframe only the newly committed active video route"
    );

    assert_eq!(scenario.room.test_api().consumer_count().await, 2);
    assert_remote_track_snapshot_for_stream(
        &drain_outbound(&mut scenario.subscriber_rx),
        TestSourceKind::ReadableVideo,
    );

    let second_refresh_offer = scenario
        .media_transport
        .create_session_renegotiation_offer(&scenario.subscriber_session_key)
        .await
        .expect("retry should stage the deferred rtc offer");
    settle_refresh_offer(&mut scenario, second_refresh_offer).await;

    assert_eq!(
        scenario.room.test_api().consumer_count().await,
        2,
        "retry pass must not duplicate already-committed consumers"
    );
    assert!(
        drain_outbound(&mut scenario.subscriber_rx).is_empty(),
        "no new setup should be emitted once every consumer already exists"
    );
}

#[tokio::test]
async fn stale_refresh_does_not_request_replaced_receiver_keyframes() {
    let scenario = Box::pin(setup_real_rtc_refresh_scenario()).await;

    publish_track(
        &scenario.room,
        &scenario.publisher_user_id,
        TestSourceKind::ScalableVideo,
        video_rtp_parameters_with_mid("cam-stale-refresh", 22_222),
        &scenario.media_transport,
    )
    .await;
    assert_eq!(scenario.room.test_api().consumer_count().await, 1);

    let stale_connection_id = scenario.subscriber_session_key.connection_id();
    let (replacement_tx, _replacement_rx) = test_sender();
    join_user_with_sender(
        &scenario.room,
        scenario.subscriber_user_id.clone(),
        replacement_tx,
    )
    .await;

    let requests_before_refresh = keyframe_request_count(&scenario);
    assert_eq!(
        scenario
            .room
            .user_operation(
                &scenario.subscriber_user_id,
                stale_connection_id,
                &scenario.media_transport,
            )
            .apply_session_refreshed(&[])
            .await,
        None
    );
    assert_eq!(
        keyframe_request_count(&scenario),
        requests_before_refresh,
        "stale refresh must not request keyframes for routes owned by the replaced receiver"
    );
}

fn keyframe_request_count(scenario: &RealRtcRefreshScenario) -> u64 {
    let snapshot = scenario.metrics.snapshot();
    snapshot.rtc_keyframe_requests_forwarded() + snapshot.rtc_keyframe_requests_absorbed()
}

#[tokio::test]
async fn negotiated_publish_commit_sets_up_consumers_on_real_rtc() {
    let mut scenario = Box::pin(setup_real_rtc_refresh_scenario()).await;
    let publisher_connection_id =
        user_connection_id(&scenario.room, &scenario.publisher_user_id).await;
    let publisher_session_key = scenario
        .room
        .transport_user_key(&scenario.publisher_user_id, publisher_connection_id)
        .await;
    let mut publisher_remote = build_remote_rtc(55_101);
    apply_offer_answer(
        &scenario.media_transport,
        &publisher_session_key,
        &mut publisher_remote,
        scenario.publisher_initial_offer.into_parts().0,
    )
    .await;

    let transport_media_id = scenario
        .media_transport
        .publish_media(
            &publisher_session_key,
            MediaKind::Video,
            &MediaStream::new(vec![], vec![], vec![]),
        )
        .await
        .expect("protocol publish intent should stage a recv-only media line");
    let publish_offer = scenario
        .media_transport
        .create_session_renegotiation_offer(&publisher_session_key)
        .await
        .expect("protocol publish should stage a follow-up offer");
    apply_offer_answer(
        &scenario.media_transport,
        &publisher_session_key,
        &mut publisher_remote,
        publish_offer.into_parts().0,
    )
    .await;
    let negotiated_parameters = scenario
        .media_transport
        .test_api()
        .negotiated_producer_parameters(&publisher_session_key, transport_media_id)
        .await
        .expect("answered protocol publish should expose negotiated producer parameters");

    assert!(
        scenario
            .room
            .test_api()
            .publish_negotiated_track(
                &scenario.publisher_user_id,
                NegotiatedPublish {
                    connection_id: publisher_connection_id,
                    stream_type: TestSourceKind::ScalableVideo,
                    transport_media_id,
                    consumable_rtp_parameters: negotiated_parameters,
                },
                &scenario.media_transport,
            )
            .await
            .is_some()
    );
    assert!(drain_outbound(&mut scenario.publisher_rx).is_empty());
    assert_remote_track_snapshot_for_stream(
        &drain_outbound(&mut scenario.subscriber_rx),
        TestSourceKind::ScalableVideo,
    );
    assert_eq!(scenario.room.test_api().consumer_count().await, 1);
}
