use super::support::{self as s, flows as f, media as m, protocol as p, setup as st};

#[tokio::test]
async fn fake_rtc_peers_rebootstrap_user_replacement_without_stale_media_routes() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let st::ReadyRoomFakePeers {
        server,
        room,
        publisher: mut initial_publisher,
        mut subscriber,
    } = st::ready_room_fake_integer_peers("issuer-replacement-rtc", 80, 81).await?;

    let mut source = s::FakeMediaSource::audio();
    m::publish_source_and_ready_route(
        &server,
        &room,
        &mut initial_publisher,
        &mut subscriber,
        &s::UserId::Integer(80),
        &source,
    )
    .await;

    let mut clock = s::FakeClock::default();
    m::assert_packet_forwarded(
        &mut initial_publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;

    let replacement =
        s::connect_fake_peer(&server, &room, s::UserId::Integer(80), s::TEST_ROOM_KEY).await;
    let mut replacement = s::require_some(replacement, "replacement peer should connect")?;

    assert_eq!(
        initial_publisher.read_close_code().await,
        Some(s::CloseCode::Library(4108))
    );
    p::assert_departure_message_protocol(&mut subscriber, s::UserId::Integer(80)).await;
    p::assert_peer_joined_message_protocol(&mut subscriber, s::UserId::Integer(80)).await;

    m::assert_packet_dropped(
        &mut initial_publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;

    s::require_some(
        replacement
            .rtc()
            .wait_until_connected(s::Duration::from_secs(5))
            .await,
        "replacement peer should reach ready state",
    )?;
    m::publish_source_and_ready_route(
        &server,
        &room,
        &mut replacement,
        &mut subscriber,
        &s::UserId::Integer(80),
        &source,
    )
    .await;
    m::assert_packet_forwarded(&mut replacement, &mut subscriber, &mut source, &mut clock).await;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_subscriber_replacement_preserves_download_mute_after_renegotiation()
-> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let st::ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = st::ready_room_fake_integer_peers("issuer-subscriber-replacement-mute", 82, 83).await?;

    Box::pin(
        f::assert_subscriber_replacement_preserves_download_mute_after_renegotiation(
            &server,
            &room,
            &mut publisher,
            &mut subscriber,
        ),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_replaced_socket_cannot_emit_presence_updates_after_rejoin() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let st::RoomFakePeers {
        server,
        room,
        publisher: mut initial,
        subscriber: mut observer,
    } = st::room_fake_integer_peers("issuer-replacement-rtc-info", 84, 85).await?;

    p::assert_peer_joined_message_protocol(&mut initial, s::UserId::Integer(85)).await;

    let replacement =
        s::connect_fake_peer(&server, &room, s::UserId::Integer(84), s::TEST_ROOM_KEY).await;
    let replacement = s::require_some(replacement, "replacement peer should connect")?;

    let _ = initial
        .send_info(s::UserInfo {
            is_talking: Some(true),
            ..s::UserInfo::default()
        })
        .await;

    assert_eq!(
        initial.read_close_code().await,
        Some(s::CloseCode::Library(4108))
    );
    p::assert_departure_message_protocol(&mut observer, s::UserId::Integer(84)).await;
    p::assert_peer_joined_message_protocol(&mut observer, s::UserId::Integer(84)).await;
    p::assert_no_server_message_protocol(&mut observer).await;
    s::require_some(replacement.close().await, "replacement should close")?;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_replaced_socket_cannot_finish_a_queued_publish_negotiation() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let st::ReadyRoomFakePeers {
        server,
        room,
        publisher: mut initial_publisher,
        mut subscriber,
    } = st::ready_room_fake_integer_peers("issuer-replacement-rtc-queued-publish", 86, 87).await?;

    let mut source = s::FakeMediaSource::audio();
    s::require_some(
        initial_publisher.publish_track(&source).await,
        "initial publisher should send audio publish intent",
    )?;
    let request = initial_publisher.read_next_server_request().await;
    let (request_id, request) =
        s::require_some(request, "initial publisher should receive renegotiation")?;
    assert!(
        matches!(request, s::ServerRequest::Renegotiate(_)),
        "publish should leave a renegotiation answer pending on the original socket"
    );

    let replacement =
        s::connect_fake_peer(&server, &room, s::UserId::Integer(86), s::TEST_ROOM_KEY).await;
    let mut replacement = s::require_some(replacement, "replacement peer should connect")?;

    p::assert_departure_message_protocol(&mut subscriber, s::UserId::Integer(86)).await;
    p::assert_peer_joined_message_protocol(&mut subscriber, s::UserId::Integer(86)).await;

    s::require_some(
        initial_publisher
            .respond_to_server_request(request_id, request)
            .await,
        "stale publisher should send queued negotiation response",
    )?;
    p::assert_no_server_message_protocol(&mut subscriber).await;

    let mut clock = s::FakeClock::default();
    m::assert_packet_dropped(
        &mut initial_publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;
    assert_eq!(
        initial_publisher.read_close_code().await,
        Some(s::CloseCode::Library(4108))
    );

    s::require_some(
        replacement
            .rtc()
            .wait_until_connected(s::Duration::from_secs(5))
            .await,
        "replacement peer should reach ready state",
    )?;
    m::publish_source_and_ready_route(
        &server,
        &room,
        &mut replacement,
        &mut subscriber,
        &s::UserId::Integer(86),
        &source,
    )
    .await;
    m::assert_packet_forwarded(&mut replacement, &mut subscriber, &mut source, &mut clock).await;
    Ok(())
}
