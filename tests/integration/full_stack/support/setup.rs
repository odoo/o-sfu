use super::*;

static FULL_STACK_TEST_LOCK: Mutex<()> = Mutex::const_new(());

pub(crate) async fn full_stack_test_guard() -> MutexGuard<'static, ()> {
    FULL_STACK_TEST_LOCK.lock().await
}

pub(crate) struct RoomFakePeers {
    pub(crate) server: TestServer,
    pub(crate) room: String,
    pub(crate) publisher: ProtocolFakePeer,
    pub(crate) subscriber: ProtocolFakePeer,
}

pub(crate) type ReadyRoomFakePeers = RoomFakePeers;

pub(crate) async fn room_fake_peers(
    issuer: &str,
    publisher_user_id: UserId,
    subscriber_user_id: UserId,
) -> TestResult<RoomFakePeers> {
    let (server, room) = room_parts(issuer).await?;
    let (publisher, subscriber) = require_some(
        Box::pin(connect_two_fake_peers(
            &server,
            &room,
            publisher_user_id,
            subscriber_user_id,
        ))
        .await,
        "fake peers should connect",
    )?;
    Ok(RoomFakePeers {
        server,
        room,
        publisher,
        subscriber,
    })
}

pub(crate) async fn room_fake_integer_peers(
    issuer: &str,
    publisher_user_id: i64,
    subscriber_user_id: i64,
) -> TestResult<RoomFakePeers> {
    Box::pin(room_fake_peers(
        issuer,
        publisher_user_id.into(),
        subscriber_user_id.into(),
    ))
    .await
}

pub(crate) async fn ready_room_fake_peers(
    issuer: &str,
    publisher_user_id: UserId,
    subscriber_user_id: UserId,
) -> TestResult<ReadyRoomFakePeers> {
    Box::pin(ready_room_fake_peers_with_config(
        test_config(1_000, 10),
        issuer,
        publisher_user_id,
        subscriber_user_id,
    ))
    .await
}

pub(crate) async fn ready_room_fake_integer_peers(
    issuer: &str,
    publisher_user_id: i64,
    subscriber_user_id: i64,
) -> TestResult<ReadyRoomFakePeers> {
    Box::pin(ready_room_fake_peers(
        issuer,
        publisher_user_id.into(),
        subscriber_user_id.into(),
    ))
    .await
}

pub(crate) async fn ready_room_fake_peers_with_config(
    config: Config,
    issuer: &str,
    publisher_user_id: UserId,
    subscriber_user_id: UserId,
) -> TestResult<ReadyRoomFakePeers> {
    let worker_count = config.transport.rtc_media_worker_count;
    let force_spillover = config.transport.room_worker_policy.max_local_routers() > 1;
    let (server, room) = room_parts_with_config(config, issuer).await?;
    let (publisher, subscriber) = if force_spillover {
        server.set_packet_loop_delays_ms(placement_delays_for_worker(worker_count, 0));
        let mut publisher = require_some(
            connect_fake_peer(&server, &room, publisher_user_id, TEST_ROOM_KEY).await,
            "publisher should connect",
        )?;
        require_some(
            publisher
                .rtc()
                .wait_until_connected(Duration::from_secs(5))
                .await,
            "publisher should reach ready state",
        )?;
        server.set_packet_loop_delays_ms(placement_delays_for_worker(worker_count, 1));
        let mut subscriber = require_some(
            connect_fake_peer(&server, &room, subscriber_user_id, TEST_ROOM_KEY).await,
            "subscriber should connect",
        )?;
        require_some(
            subscriber
                .rtc()
                .wait_until_connected(Duration::from_secs(5))
                .await,
            "subscriber should reach ready state",
        )?;
        (publisher, subscriber)
    } else {
        require_some(
            Box::pin(connect_two_rtc_ready_fake_peers(
                &server,
                &room,
                publisher_user_id,
                subscriber_user_id,
                Duration::from_secs(5),
            ))
            .await,
            "fake RTC peers should reach ready state",
        )?
    };
    Ok(ReadyRoomFakePeers {
        server,
        room,
        publisher,
        subscriber,
    })
}

pub(crate) async fn room_parts(issuer: &str) -> TestResult<(TestServer, String)> {
    room_parts_with_config(test_config(1_000, 10), issuer).await
}

pub(crate) async fn room_parts_with_config(
    config: Config,
    issuer: &str,
) -> TestResult<(TestServer, String)> {
    require_some(
        spawn_room_server_with_config(config, issuer, TEST_ROOM_KEY).await,
        "room server should start",
    )
}

pub(crate) fn cross_worker_test_config() -> Config {
    let mut config = test_config(1_000, 10);
    config.transport.rtc_media_worker_count = 2;
    config.transport.room_worker_policy = spillover_policy(2);
    config
}

pub(crate) async fn connect_two_isolated_audio_flows(
    server: &TestServer,
) -> Option<(
    ProtocolFakePeer,
    ProtocolFakePeer,
    ProtocolFakePeer,
    ProtocolFakePeer,
)> {
    let room_a = create_room(server, "issuer-topology-a", TEST_ROOM_KEY).await?;
    let room_b = create_room(server, "issuer-topology-b", TEST_ROOM_KEY).await?;

    let mut publisher_a =
        connect_fake_peer(server, &room_a, UserId::Integer(90), TEST_ROOM_KEY).await?;
    let mut subscriber_a =
        connect_fake_peer(server, &room_a, UserId::Integer(91), TEST_ROOM_KEY).await?;
    let mut publisher_b =
        connect_fake_peer(server, &room_b, UserId::Integer(90), TEST_ROOM_KEY).await?;
    let mut subscriber_b =
        connect_fake_peer(server, &room_b, UserId::Integer(91), TEST_ROOM_KEY).await?;

    for peer in [
        &mut publisher_a,
        &mut subscriber_a,
        &mut publisher_b,
        &mut subscriber_b,
    ] {
        peer.rtc()
            .wait_until_connected(Duration::from_secs(5))
            .await?;
    }

    Some((publisher_a, subscriber_a, publisher_b, subscriber_b))
}

pub(crate) async fn assert_video_subscription_enabled(
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: UserId,
) {
    assert!(
        subscriber
            .update_subscription(
                publisher_user_id,
                DownloadStates {
                    camera: Some(true),
                    camera_layout: Some(VideoLayoutIntent::Featured),
                    ..DownloadStates::default()
                },
            )
            .await
            .is_some()
    );
}
