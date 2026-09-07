use super::{
    media::{
        RouteState, assert_consumer_route, assert_packet_dropped,
        assert_synthetic_video_packet_forwarded, consume_video_source_and_ready_route,
        publish_source_and_ready_route,
    },
    protocol::{assert_peer_joined_message_protocol, assert_track_snapshot},
    setup::room_parts_with_config,
    *,
};

const LARGE_ROOM_SIZE: usize = 64;
const LARGE_ROOM_LOCAL_ROUTER_CAP: usize = 4;
const LARGE_ROOM_USERS_PER_WORKER: usize = 2;
const LARGE_ROOM_MEDIA_LIMIT: usize = 2;

pub(crate) struct SpilloverRoomFakePeers {
    pub(crate) server: TestServer,
    pub(crate) room: String,
    pub(crate) publisher: ProtocolFakePeer,
    pub(crate) local_subscriber: ProtocolFakePeer,
    pub(crate) spillover_subscriber: ProtocolFakePeer,
}

pub(crate) struct LargeRoomSpilloverFakePeers {
    pub(crate) server: TestServer,
    pub(crate) room: String,
    pub(crate) publishers: Vec<ProtocolFakePeer>,
    pub(crate) receivers: Vec<ProtocolFakePeer>,
}

pub(crate) async fn spillover_room_fake_peers(
    issuer: &str,
    publisher_user_id: UserId,
    local_subscriber_user_id: UserId,
    spillover_subscriber_user_id: UserId,
) -> TestResult<SpilloverRoomFakePeers> {
    let (server, room) = room_parts_with_config(overload_spillover_test_config(), issuer).await?;
    server.set_packet_loop_delays_ms(vec![Some(0), None]);
    let (publisher, local_subscriber, spillover_subscriber) = require_some(
        Box::pin(connect_overload_spillover_rtc_peers(
            &server,
            &room,
            publisher_user_id,
            local_subscriber_user_id,
            spillover_subscriber_user_id,
        ))
        .await,
        "overload spillover peers should connect",
    )?;
    Ok(SpilloverRoomFakePeers {
        server,
        room,
        publisher,
        local_subscriber,
        spillover_subscriber,
    })
}

pub(crate) async fn large_room_spillover_fake_peers(
    issuer: &str,
    publisher_user_ids: &[UserId],
    receiver_user_ids: &[UserId],
) -> TestResult<LargeRoomSpilloverFakePeers> {
    let (server, room) = room_parts_with_config(large_room_spillover_test_config(), issuer).await?;
    let mut peers = Vec::with_capacity(publisher_user_ids.len() + receiver_user_ids.len());
    for (peer_index, user_id) in publisher_user_ids
        .iter()
        .chain(receiver_user_ids)
        .enumerate()
    {
        let target_worker =
            (peer_index / LARGE_ROOM_USERS_PER_WORKER).min(LARGE_ROOM_LOCAL_ROUTER_CAP - 1);
        server.set_packet_loop_delays_ms(placement_delays_for_worker(
            LARGE_ROOM_LOCAL_ROUTER_CAP,
            target_worker,
        ));
        let peer = require_some(
            Box::pin(connect_large_room_spillover_peer(
                &server, &room, user_id, peer_index,
            ))
            .await,
            "large-room spillover peer should connect",
        )?;
        peers.push(peer);
    }
    let receivers = peers.split_off(publisher_user_ids.len());
    Ok(LargeRoomSpilloverFakePeers {
        server,
        room,
        publishers: peers,
        receivers,
    })
}

fn overload_spillover_test_config() -> Config {
    let mut config = super::test_config(1_000, 10);
    config.transport.rtc_media_worker_count = 2;
    config.transport.room_worker_policy = spillover_policy(2);
    config
}

fn large_room_spillover_test_config() -> Config {
    let mut config = super::test_config(1_000, LARGE_ROOM_SIZE);
    config.transport.rtc_media_worker_count = LARGE_ROOM_LOCAL_ROUTER_CAP;
    config.transport.room_worker_policy = spillover_policy(LARGE_ROOM_LOCAL_ROUTER_CAP);
    config.transport.room_media_limits =
        match RoomMediaLimits::try_new(LARGE_ROOM_MEDIA_LIMIT, LARGE_ROOM_MEDIA_LIMIT) {
            Ok(limits) => limits,
            Err(error) => panic!("large-room media limits should be valid: {error}"),
        };
    config
}

pub(crate) async fn assert_cross_worker_placement(
    server: &TestServer,
    room: &str,
    publisher_user_id: &UserId,
    subscriber_user_id: &UserId,
) {
    assert_user_media_worker(server, room, publisher_user_id, 0).await;
    assert_user_media_worker(server, room, subscriber_user_id, 1).await;
}

async fn connect_overload_spillover_rtc_peers(
    server: &TestServer,
    room: &str,
    publisher_user_id: UserId,
    local_subscriber_user_id: UserId,
    spillover_subscriber_user_id: UserId,
) -> Option<(ProtocolFakePeer, ProtocolFakePeer, ProtocolFakePeer)> {
    let mut publisher = Box::pin(connect_peer_on_worker(
        server,
        room,
        &publisher_user_id,
        0,
        [],
    ))
    .await?;
    let mut local_subscriber = Box::pin(connect_peer_on_worker(
        server,
        room,
        &local_subscriber_user_id,
        0,
        [&mut publisher],
    ))
    .await?;
    server.set_packet_loop_delays_ms(vec![
        Some(RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS),
        Some(0),
    ]);
    let spillover_subscriber = Box::pin(connect_peer_on_worker(
        server,
        room,
        &spillover_subscriber_user_id,
        1,
        [&mut publisher, &mut local_subscriber],
    ))
    .await?;
    for (user_id, worker_id) in [
        (&publisher_user_id, 0),
        (&local_subscriber_user_id, 0),
        (&spillover_subscriber_user_id, 1),
    ] {
        assert_user_media_worker(server, room, user_id, worker_id).await;
    }

    Some((publisher, local_subscriber, spillover_subscriber))
}

async fn connect_large_room_spillover_peer(
    server: &TestServer,
    room: &str,
    user_id: &UserId,
    peer_index: usize,
) -> Option<ProtocolFakePeer> {
    let mut peer = connect_fake_peer(server, room, user_id.clone(), TEST_ROOM_KEY).await?;
    peer.rtc()
        .wait_until_connected(super::Duration::from_secs(5))
        .await?;
    let asserted_peer_count = LARGE_ROOM_LOCAL_ROUTER_CAP * LARGE_ROOM_USERS_PER_WORKER;
    if peer_index < asserted_peer_count {
        assert_user_media_worker(
            server,
            room,
            user_id,
            peer_index / LARGE_ROOM_USERS_PER_WORKER,
        )
        .await;
    }
    Some(peer)
}

async fn connect_peer_on_worker<const N: usize>(
    server: &TestServer,
    room: &str,
    user_id: &UserId,
    worker_id: usize,
    mut joined_observers: [&mut ProtocolFakePeer; N],
) -> Option<ProtocolFakePeer> {
    let mut peer = connect_fake_peer(server, room, user_id.clone(), TEST_ROOM_KEY).await?;
    peer.rtc()
        .wait_until_connected(super::Duration::from_secs(5))
        .await?;
    assert_user_media_worker(server, room, user_id, worker_id).await;
    for observer in &mut joined_observers {
        assert_peer_joined_message_protocol(observer, user_id.clone()).await;
    }
    Some(peer)
}

async fn assert_user_media_worker(
    server: &TestServer,
    room: &str,
    user_id: &UserId,
    worker_id: usize,
) {
    assert!(
        server
            .wait_for_user_media_worker(room, user_id, worker_id)
            .await
    );
}

async fn connect_spillover_replacement(
    server: &TestServer,
    room: &str,
    previous_peer: &mut ProtocolFakePeer,
    user_id: &UserId,
    worker_id: usize,
) -> Option<ProtocolFakePeer> {
    let mut replacement = connect_fake_peer(server, room, user_id.clone(), TEST_ROOM_KEY).await?;
    assert_eq!(
        previous_peer.read_close_code().await,
        Some(CloseCode::Library(4108))
    );
    replacement
        .rtc()
        .wait_until_connected(super::Duration::from_secs(5))
        .await?;
    assert_user_media_worker(server, room, user_id, worker_id).await;
    Some(replacement)
}

pub(crate) async fn assert_spillover_release_route_flow(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    local_subscriber: &mut ProtocolFakePeer,
    mut spillover_subscriber: ProtocolFakePeer,
    publisher_user_id: &UserId,
    spillover_subscriber_user_id: &UserId,
) -> TestResult {
    let mut source = FakeMediaSource::vp8_camera_high();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let local_track =
        consume_video_source_and_ready_route(server, room, local_subscriber, publisher_user_id)
            .await;
    let spillover_track = consume_video_source_and_ready_route(
        server,
        room,
        &mut spillover_subscriber,
        publisher_user_id,
    )
    .await;
    let mut clock = FakeClock::default();
    assert_synthetic_video_packet_forwarded(
        publisher,
        &mut spillover_subscriber,
        &mut source,
        &mut clock,
    )
    .await;
    assert!(
        local_subscriber
            .rtc()
            .read_rtp_packet(super::Duration::from_secs(2))
            .await?
            .is_some()
    );

    require_some(
        spillover_subscriber.close().await,
        "spillover subscriber should close",
    )?;
    assert!(
        server
            .wait_for_consumer_route_absence(
                room,
                spillover_subscriber_user_id,
                publisher_user_id,
                spillover_track.stream_type,
            )
            .await
    );
    assert_consumer_route(
        server,
        room,
        local_subscriber,
        publisher_user_id,
        local_track.stream_type,
        RouteState::Active,
    )
    .await;
    assert_synthetic_video_packet_forwarded(publisher, local_subscriber, &mut source, &mut clock)
        .await;
    Ok(())
}

pub(crate) async fn assert_spillover_replacement_mute_flow(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    spillover_subscriber: &mut ProtocolFakePeer,
    publisher_user_id: UserId,
    spillover_subscriber_user_id: UserId,
) -> TestResult {
    let mut source = FakeMediaSource::audio();
    let track_binding = publish_source_and_ready_route(
        server,
        room,
        publisher,
        spillover_subscriber,
        &publisher_user_id,
        &source,
    )
    .await;

    assert!(
        spillover_subscriber
            .update_subscription(
                publisher_user_id.clone(),
                DownloadStates {
                    audio: Some(false),
                    ..DownloadStates::default()
                },
            )
            .await
            .is_some()
    );
    assert_consumer_route(
        server,
        room,
        spillover_subscriber,
        &publisher_user_id,
        track_binding.stream_type,
        RouteState::Inactive,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_packet_dropped(publisher, spillover_subscriber, &mut source, &mut clock).await;

    let mut replacement = require_some(
        Box::pin(connect_spillover_replacement(
            server,
            room,
            spillover_subscriber,
            &spillover_subscriber_user_id,
            1,
        ))
        .await,
        "spillover replacement should connect",
    )?;

    let replacement_track = assert_track_snapshot(
        &mut replacement,
        publisher_user_id.clone(),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(replacement.complete_next_negotiation().await.is_some());
    assert_consumer_route(
        server,
        room,
        &replacement,
        &publisher_user_id,
        replacement_track.stream_type,
        RouteState::Inactive,
    )
    .await;
    assert_packet_dropped(publisher, &mut replacement, &mut source, &mut clock).await;
    Ok(())
}
