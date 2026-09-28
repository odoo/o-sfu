use super::support::{self as s, metrics as mt, protocol as p, setup as st};

#[tokio::test]
async fn fake_peers_publish_and_receive_track_snapshot_over_real_server_entries() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let st::RoomFakePeers {
        server: _server,
        room: _room,
        mut publisher,
        mut subscriber,
    } = st::room_fake_integer_peers("issuer-a", 1, 2).await?;

    assert!(publisher.welcome().features.rtc);
    assert!(subscriber.welcome().features.rtc);

    let source = s::FakeMediaSource::audio();
    s::require_some(
        publisher.publish_track(&source).await,
        "publisher should send audio publish intent",
    )?;
    s::require_some(
        publisher.complete_next_negotiation().await,
        "publisher should complete audio negotiation",
    )?;
    p::assert_track_snapshot(
        &mut subscriber,
        s::UserId::Integer(1),
        s::StreamType::Audio,
        true,
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn fake_peers_keep_room_topology_isolation_with_same_user_ids() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let config = s::test_config(1_000, 10);

    let server = s::spawn_test_server(config).await?;

    let peers = Box::pin(st::connect_two_isolated_audio_flows(&server)).await;
    let (mut publisher_a, mut subscriber_a, mut publisher_b, mut subscriber_b) =
        s::require_some(peers, "isolated audio flows should connect")?;

    let source = s::FakeMediaSource::audio();
    s::require_some(
        publisher_a.publish_track(&source).await,
        "room A publisher should send audio publish intent",
    )?;
    s::require_some(
        publisher_a.complete_next_negotiation().await,
        "room A publisher should complete audio negotiation",
    )?;
    p::assert_track_snapshot(
        &mut subscriber_a,
        s::UserId::Integer(90),
        s::StreamType::Audio,
        true,
    )
    .await;
    p::assert_no_server_message_protocol(&mut subscriber_b).await;
    mt::wait_for_room_gauges(
        &server,
        mt::RoomGaugeValues {
            rooms: 2,
            users: 4,
            publications: 1,
            subscriptions: 1,
            recording_rooms: 0,
        },
    )
    .await
    .expect("room gauges should reflect the replacement flow");

    s::require_some(
        publisher_b.publish_track(&source).await,
        "room B publisher should send audio publish intent",
    )?;
    s::require_some(
        publisher_b.complete_next_negotiation().await,
        "room B publisher should complete audio negotiation",
    )?;
    p::assert_track_snapshot(
        &mut subscriber_b,
        s::UserId::Integer(90),
        s::StreamType::Audio,
        true,
    )
    .await;

    s::require_some(publisher_a.close().await, "room A publisher should close")?;
    p::assert_departure_message_protocol(&mut subscriber_a, s::UserId::Integer(90)).await;
    p::assert_no_server_message_protocol(&mut subscriber_b).await;
    Ok(())
}

#[tokio::test]
async fn fake_peers_cover_user_replacement_and_republish_over_protocol_user_flow() -> s::TestResult
{
    let _guard = st::full_stack_test_guard().await;
    let st::RoomFakePeers {
        server,
        room,
        publisher: mut initial_publisher,
        mut subscriber,
    } = st::room_fake_integer_peers("issuer-c", 40, 50).await?;

    let replacement =
        s::connect_fake_peer(&server, &room, s::UserId::Integer(40), s::TEST_ROOM_KEY).await;
    let mut replacement = s::require_some(replacement, "replacement peer should connect")?;

    assert_eq!(
        initial_publisher.read_close_code().await,
        Some(s::CloseCode::Library(4108))
    );
    p::assert_departure_message_protocol(&mut subscriber, s::UserId::Integer(40)).await;
    p::assert_peer_joined_message_protocol(&mut subscriber, s::UserId::Integer(40)).await;

    let source = s::FakeMediaSource::audio();
    s::require_some(
        replacement.publish_track(&source).await,
        "replacement should send audio publish intent",
    )?;
    s::require_some(
        replacement.complete_next_negotiation().await,
        "replacement should complete audio negotiation",
    )?;
    p::assert_track_snapshot(
        &mut subscriber,
        s::UserId::Integer(40),
        s::StreamType::Audio,
        true,
    )
    .await;
    Ok(())
}
