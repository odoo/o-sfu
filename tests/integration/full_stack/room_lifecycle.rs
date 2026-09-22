use super::support::{self as s, media as m, setup as st};

/// grace long enough that it cannot lapse while this suite runs, so only the
/// rejoin below can keep the room alive
const TEST_DEPARTURE_GRACE: s::Duration = s::Duration::from_mins(10);

#[tokio::test]
async fn fake_rtc_peers_rejoin_the_room_they_left_during_its_departure_grace() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let publisher_user_id = s::UserId::Integer(60);
    let subscriber_user_id = s::UserId::Integer(61);
    let mut config = s::test_config(1_000, 10);
    config.user.departure_grace = TEST_DEPARTURE_GRACE;

    let st::ReadyRoomFakePeers {
        server,
        room,
        publisher,
        subscriber,
    } = Box::pin(st::ready_room_fake_peers_with_config(
        config,
        "issuer-departure-grace",
        publisher_user_id.clone(),
        subscriber_user_id.clone(),
    ))
    .await?;

    // every connection disappears at once, the way a shared network drop looks
    // from the server side
    s::require_some(publisher.close().await, "publisher should close")?;
    s::require_some(subscriber.close().await, "subscriber should close")?;
    assert!(
        server.wait_for_vacated_room(&room).await,
        "session and media cleanup should complete while the room waits for a rejoin"
    );

    let (mut publisher, mut subscriber) = s::require_some(
        Box::pin(s::connect_two_rtc_ready_fake_peers(
            &server,
            &room,
            publisher_user_id.clone(),
            subscriber_user_id,
            s::Duration::from_secs(5),
        ))
        .await,
        "peers should reconnect with the uuid they left",
    )?;

    let mut source = s::FakeMediaSource::audio();
    m::publish_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &publisher_user_id,
        &source,
    )
    .await;
    let mut clock = s::FakeClock::default();
    m::assert_packet_forwarded(&mut publisher, &mut subscriber, &mut source, &mut clock).await;
    Ok(())
}
