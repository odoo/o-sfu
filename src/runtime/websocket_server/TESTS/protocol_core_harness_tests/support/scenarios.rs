use super::*;

type ProtocolPeerSetup = (
    TestServer,
    Arc<Room>,
    ProtocolHarnessPeer,
    ProtocolHarnessPeer,
);

type ProtocolSinglePeerSetup = (TestServer, Arc<Room>, ProtocolHarnessPeer);

pub(crate) async fn connect_until_welcome(
    server: &TestServer,
    room: &Arc<Room>,
    user_id: UserId,
) -> TestResult<ProtocolHarnessPeer> {
    let token = require_some(
        signed_connect_claims(TEST_ROOM_KEY, room.uuid(), user_id),
        "protocol peer token should sign",
    )?;
    let mut peer = ProtocolHarnessPeer::default();
    require_some(
        peer.connect(&server.url(), &token, None).await,
        "protocol core should connect to test server",
    )?;
    require_some(
        peer.read_server_frame().await,
        "protocol peer should consume the welcome frame",
    )?;
    Ok(peer)
}

pub(crate) async fn setup_protocol_peer(
    room_name: &str,
    user_id: UserId,
) -> TestResult<ProtocolSinglePeerSetup> {
    let server = TestServerBuilder::new().spawn_required().await?;
    let room = require_some(
        create_room(&server, room_name, CreateRoomQuery::default()).await,
        "test room should be served",
    )?;
    let peer = connect_protocol_peer(&server, &room, user_id).await?;
    Ok((server, room, peer))
}

pub(crate) async fn setup_protocol_peers(
    room_name: &str,
    alice_user_id: UserId,
    bob_user_id: UserId,
) -> TestResult<ProtocolPeerSetup> {
    let server = TestServerBuilder::new().spawn_required().await?;
    Box::pin(setup_protocol_peers_with(
        server,
        room_name,
        alice_user_id,
        bob_user_id,
        ProtocolHarnessPeer::default(),
        ProtocolHarnessPeer::default(),
    ))
    .await
}

pub(crate) async fn connect_protocol_peer(
    server: &TestServer,
    room: &Arc<Room>,
    user_id: UserId,
) -> TestResult<ProtocolHarnessPeer> {
    let mut peer = connect_until_welcome(server, room, user_id).await?;
    require_some(
        peer.read_server_frame().await,
        "protocol peer should consume the initial offer",
    )?;
    Ok(peer)
}

pub(crate) async fn publish_camera_and_setup_subscriber(
    publisher: &mut ProtocolHarnessPeer,
    subscriber: &mut ProtocolHarnessPeer,
    publisher_user_id: &UserId,
    publish_context: &str,
    renegotiation_context: &str,
    snapshot_context: &str,
) -> Option<TrackBinding> {
    assert!(
        publisher
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some(),
        "{publish_context}"
    );
    assert!(
        publisher.read_server_frame().await.is_some(),
        "{renegotiation_context}"
    );
    consume_camera_publish_setup(subscriber, publisher_user_id, snapshot_context).await
}

pub(crate) async fn consume_camera_publish_setup(
    subscriber: &mut ProtocolHarnessPeer,
    publisher_user_id: &UserId,
    snapshot_context: &str,
) -> Option<TrackBinding> {
    let track_snapshot = read_track_snapshot(subscriber).await;
    assert!(track_snapshot.is_some(), "{snapshot_context}");
    let track_snapshot = track_snapshot?;
    let track_binding = track_snapshot.first()?;
    assert_eq!(track_binding.user_id, publisher_user_id.clone());
    assert_eq!(track_binding.stream_type, ProtocolStreamType::Camera);
    assert!(track_binding.active);
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "subscriber should consume the remote-track renegotiation request"
    );
    consume_peer_info_update(
        subscriber,
        publisher_user_id.clone(),
        ProtocolSessionInfo {
            is_camera_on: Some(true),
            ..ProtocolSessionInfo::default().snapshot_complete()
        },
    )
    .await?;
    Some(track_binding.clone())
}

pub(crate) async fn recover_subscriber_and_replay_track(
    publisher: &mut ProtocolHarnessPeer,
    subscriber: &mut ProtocolHarnessPeer,
    publisher_user_id: &UserId,
    reconnect_context: &str,
    welcome_context: &str,
    offer_context: &str,
    snapshot_context: &str,
) -> Option<TrackBinding> {
    assert!(
        close_peer_and_observe_recovery(subscriber, publisher)
            .await
            .is_some()
    );
    assert!(
        subscriber
            .flush_timers_with_delay(RECOVERY_DELAY_MS)
            .await
            .is_some(),
        "{reconnect_context}"
    );
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "{welcome_context}"
    );
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "{offer_context}"
    );
    let replayed_track_snapshot = read_track_snapshot(subscriber).await;
    assert!(replayed_track_snapshot.is_some(), "{snapshot_context}");
    let replayed_track_snapshot = replayed_track_snapshot?;
    assert_track_snapshot_contains(
        &replayed_track_snapshot,
        &publisher_user_id.clone(),
        ProtocolStreamType::Camera,
    );
    let replayed_track = replayed_track_snapshot.first()?;
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "subscriber should consume the replayed remote-track renegotiation request"
    );
    Some(replayed_track.clone())
}

pub(crate) async fn consume_replayed_camera_publish_after_recovery(
    publisher: &ProtocolHarnessPeer,
    subscriber: &mut ProtocolHarnessPeer,
    publisher_user_id: ProtocolSessionId,
) -> Option<()> {
    consume_peer_joined_update(subscriber, publisher_user_id.clone()).await?;
    let replayed_track_snapshot = read_track_snapshot(subscriber).await;
    assert!(
        replayed_track_snapshot.is_some(),
        "subscriber should receive a replayed track snapshot after publisher recovery"
    );
    let replayed_track_snapshot = replayed_track_snapshot?;
    assert_track_snapshot_contains(
        &replayed_track_snapshot,
        &publisher_user_id,
        ProtocolStreamType::Camera,
    );
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "subscriber should receive the replayed remote-track renegotiation request"
    );
    assert!(peer_reached_state(
        publisher,
        BundleConnectionState::Recovering
    ));
    assert!(peer_reached_state(
        publisher,
        BundleConnectionState::Connected
    ));
    Some(())
}

pub(crate) async fn recover_publisher_and_replay_camera_publish(
    publisher: &mut ProtocolHarnessPeer,
    subscriber: &mut ProtocolHarnessPeer,
    publisher_user_id: ProtocolSessionId,
) -> Option<()> {
    subscriber.updates.clear();
    close_peer_and_observe_recovery(publisher, subscriber).await?;
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "subscriber should consume the departure-side renegotiation before recovery rejoin"
    );
    subscriber.updates.clear();

    assert!(
        publisher
            .flush_timers_with_delay(RECOVERY_DELAY_MS)
            .await
            .is_some(),
        "recovery timer should reconnect the publisher"
    );
    assert!(
        publisher.read_server_frame().await.is_some(),
        "publisher should consume the recovery welcome frame"
    );
    assert!(
        publisher.read_server_frame().await.is_some(),
        "publisher should consume the recovery initial offer"
    );
    assert!(
        publisher.read_server_frame().await.is_some(),
        "publisher should consume the replayed publish renegotiation after recovery"
    );

    consume_replayed_camera_publish_after_recovery(publisher, subscriber, publisher_user_id).await
}

pub(crate) async fn setup_real_rtc_protocol_peers(
    room_name: &str,
    alice_user_id: UserId,
    bob_user_id: UserId,
    alice_port: u16,
    bob_port: u16,
) -> Option<ProtocolPeerSetup> {
    let server = TestServerBuilder::new().spawn().await?;
    let alice = ProtocolHarnessPeer::with_real_rtc_negotiation(alice_port)?;
    let bob = ProtocolHarnessPeer::with_real_rtc_negotiation(bob_port)?;
    Box::pin(setup_protocol_peers_with(
        server,
        room_name,
        alice_user_id,
        bob_user_id,
        alice,
        bob,
    ))
    .await
    .ok()
}

pub(crate) async fn setup_protocol_recovery_peers(
    alice_user_id: UserId,
    bob_user_id: UserId,
) -> Option<ProtocolPeerSetup> {
    let server = TestServerBuilder::new().spawn().await?;
    Box::pin(setup_protocol_peers_with(
        server,
        "issuer-protocol-recovery",
        alice_user_id,
        bob_user_id,
        ProtocolHarnessPeer::default(),
        ProtocolHarnessPeer::default(),
    ))
    .await
    .ok()
}

async fn setup_protocol_peers_with(
    server: TestServer,
    room_name: &str,
    alice_user_id: UserId,
    bob_user_id: UserId,
    mut alice: ProtocolHarnessPeer,
    mut bob: ProtocolHarnessPeer,
) -> TestResult<ProtocolPeerSetup> {
    let room = require_some(
        create_room(&server, room_name, CreateRoomQuery::default()).await,
        "test room should be served",
    )?;
    let alice_token = require_some(
        signed_connect_claims(TEST_ROOM_KEY, room.uuid(), alice_user_id),
        "alice protocol peer token should sign",
    )?;
    let bob_token = require_some(
        signed_connect_claims(TEST_ROOM_KEY, room.uuid(), bob_user_id.clone()),
        "bob protocol peer token should sign",
    )?;

    require_some(
        alice
            .connect_and_finish_handshake(&server.url(), &alice_token, None)
            .await,
        "alice protocol peer should finish handshake",
    )?;
    require_some(
        bob.connect_and_finish_handshake(&server.url(), &bob_token, None)
            .await,
        "bob protocol peer should finish handshake",
    )?;
    require_some(
        consume_peer_joined_update(&mut alice, bob_user_id).await,
        "alice should consume bob peer-joined update",
    )?;
    Ok((server, room, alice, bob))
}

pub(crate) async fn update_info_and_deliver_to_peer(
    bob: &mut ProtocolHarnessPeer,
    alice: &mut ProtocolHarnessPeer,
    info: ProtocolSessionInfo,
) -> Option<()> {
    bob.update_info(info).await?;
    alice.read_server_frame().await?;
    Some(())
}

pub(crate) async fn close_peer_and_observe_recovery(
    bob: &mut ProtocolHarnessPeer,
    alice: &mut ProtocolHarnessPeer,
) -> Option<()> {
    bob.websocket.as_mut()?.close(None).await.ok()?;
    bob.websocket = None;
    bob.observe_close(1011).await?;
    alice.read_server_frame().await?;
    Some(())
}

pub(crate) async fn close_peer_and_wait_for_room_cleanup(
    peer: &mut ProtocolHarnessPeer,
    room: &Arc<Room>,
    user_id: &UserId,
) -> Option<()> {
    let connection_id = room.test_api().user_connection_id(user_id).await?;
    peer.websocket.as_mut()?.close(None).await.ok()?;
    peer.websocket = None;
    timeout(Duration::from_secs(1), async {
        loop {
            if room.test_api().user_connection_id(user_id).await != Some(connection_id) {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .ok()
}

pub(crate) async fn recover_peer_with_latest_info(
    bob: &mut ProtocolHarnessPeer,
    info: ProtocolSessionInfo,
) -> Option<()> {
    bob.update_info(info).await?;
    bob.flush_timers_with_delay(RECOVERY_DELAY_MS).await?;
    bob.read_server_frame().await?;
    bob.read_server_frame().await?;
    assert!(bob.websocket.is_some());
    Some(())
}
