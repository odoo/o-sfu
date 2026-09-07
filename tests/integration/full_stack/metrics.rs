use super::support::{self as s, media as m, metrics as mt, setup as st};

#[tokio::test]
async fn fake_rtc_peer_room_stats_and_gauges_follow_lifecycle() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let (server, room) = st::room_parts("issuer-room-gauges").await?;
    let mut expected = mt::RoomGaugeValues {
        rooms: 1,
        ..mt::RoomGaugeValues::default()
    };
    assert!(mt::wait_for_room_gauges(&server, expected).await);
    let (mut publisher, mut subscriber) = s::require_some(
        s::connect_two_fake_peers(
            &server,
            &room,
            s::UserId::Integer(60),
            s::UserId::Integer(61),
        )
        .await,
        "peers should connect",
    )?;
    expected.users = 2;
    assert!(mt::wait_for_room_gauges(&server, expected).await);
    for peer in [&mut publisher, &mut subscriber] {
        s::require_some(
            peer.rtc()
                .wait_until_connected(s::Duration::from_secs(5))
                .await,
            "peer should reach ready state",
        )?;
    }

    let mut source = s::FakeMediaSource::audio();
    m::publish_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &s::UserId::Integer(60),
        &source,
    )
    .await;
    expected.publications = 1;
    expected.subscriptions = 1;
    assert!(mt::wait_for_room_gauges(&server, expected).await);
    let recording = s::RecordingOptions {
        audio: Some(true),
        ..s::RecordingOptions::default()
    };
    assert_eq!(
        publisher
            .request_recording(s::ClientRequest::StartRecording(recording))
            .await,
        Some(false)
    );
    assert_eq!(
        publisher
            .request_recording(s::ClientRequest::StopRecording)
            .await,
        Some(false)
    );
    assert!(mt::wait_for_room_gauges(&server, expected).await);

    let mut clock = s::FakeClock::default();
    let stats = mt::stream_until_audio_bitrate_is_observable(
        &server,
        &room,
        &mut publisher,
        &mut source,
        &mut clock,
    )
    .await;
    let stats = s::require_some(stats, "audio bitrate should become observable")?;
    assert!(stats.audio > 0);
    assert!(stats.total >= stats.audio);
    s::require_some(subscriber.close().await, "subscriber should close")?;
    expected.users = 1;
    expected.subscriptions = 0;
    assert!(mt::wait_for_room_gauges(&server, expected).await);
    s::require_some(publisher.close().await, "publisher should close")?;
    assert!(mt::wait_for_room_gauges(&server, mt::RoomGaugeValues::default()).await);
    Ok(())
}

#[tokio::test]
async fn fake_rtc_peers_export_longer_transport_lifetimes_after_steady_state_run() -> s::TestResult
{
    let _guard = st::full_stack_test_guard().await;
    let st::ReadyRoomFakePeers {
        server,
        publisher,
        subscriber,
        ..
    } = st::ready_room_fake_integer_peers("issuer-lifetime-metrics", 62, 63).await?;

    s::sleep(s::Duration::from_millis(1_200)).await;

    s::require_some(publisher.close().await, "publisher should close")?;
    s::require_some(subscriber.close().await, "subscriber should close")?;

    let lifetime_metrics = mt::wait_for_transport_lifetime_metrics(&server, 2).await;
    let lifetime_metrics = s::require_some(
        lifetime_metrics,
        "transport lifetime metrics should include both peers",
    )?;

    assert_eq!(lifetime_metrics.le_1_second, 0);
    assert_eq!(lifetime_metrics.le_10_seconds, 2);
    assert_eq!(lifetime_metrics.le_60_seconds, 2);
    assert_eq!(lifetime_metrics.le_300_seconds, 2);
    assert_eq!(lifetime_metrics.count, 2);
    assert!(lifetime_metrics.sum_seconds >= 2.0);
    Ok(())
}

#[tokio::test]
async fn fake_rtc_peers_export_transport_and_rtp_metrics_during_live_media() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let st::ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = st::ready_room_fake_integer_peers("issuer-live-metrics", 64, 65).await?;

    let mut source = s::FakeMediaSource::audio();
    m::publish_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &s::UserId::Integer(64),
        &source,
    )
    .await;

    let mut clock = s::FakeClock::default();
    let initial_forwarded_bytes =
        m::assert_packet_forwarded(&mut publisher, &mut subscriber, &mut source, &mut clock).await
            + m::assert_packet_forwarded(&mut publisher, &mut subscriber, &mut source, &mut clock)
                .await;

    let before_live_metrics = mt::wait_for_live_rtc_metrics(&server, 2).await;
    let before_live_metrics = s::require_some(
        before_live_metrics,
        "live metrics should include both peers",
    )?;
    mt::assert_initial_live_rtc_metrics(&before_live_metrics, initial_forwarded_bytes);

    let mut additional_forwarded_bytes = 0;
    for _ in 0..4 {
        additional_forwarded_bytes +=
            m::assert_packet_forwarded(&mut publisher, &mut subscriber, &mut source, &mut clock)
                .await;
    }

    let during_live_metrics = mt::wait_for_live_rtc_metrics(&server, 2).await;
    let during_live_metrics =
        s::require_some(during_live_metrics, "live metrics should retain both peers")?;

    mt::assert_steady_state_live_rtc_metrics(
        &before_live_metrics,
        &during_live_metrics,
        additional_forwarded_bytes,
    );

    s::require_some(publisher.close().await, "publisher should close")?;
    s::require_some(subscriber.close().await, "subscriber should close")?;

    let after_live_metrics = mt::wait_for_live_rtc_metrics(&server, 0).await;
    let after_live_metrics =
        s::require_some(after_live_metrics, "live metrics should drain after close")?;

    assert_eq!(after_live_metrics.connected_transport_users, 0);
    assert_eq!(after_live_metrics.disconnected_transport_users, 0);
    assert_eq!(
        after_live_metrics.transport_health_transitions_connected_to_unset
            - during_live_metrics.transport_health_transitions_connected_to_unset,
        2
    );
    Ok(())
}
