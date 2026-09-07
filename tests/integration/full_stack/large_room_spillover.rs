use o_sfu::http::route;
use o_sfu_telemetry::diagnostics::{DiagnosticsRoomDetail, DiagnosticsRouteState};

use super::support::{self as s, media as m, setup as st, spillover as sp};

#[tokio::test]
async fn large_room_overload_spillover_preserves_caps_and_cleanup() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let publisher_user_ids = [
        s::UserId::Integer(200),
        s::UserId::Integer(201),
        s::UserId::Integer(202),
        s::UserId::Integer(203),
    ];
    let [first_publisher, second_publisher, third_publisher, _] = &publisher_user_ids;
    let receiver_user_ids = (204_i64..212).map(s::UserId::Integer).collect::<Vec<_>>();
    let mut peers = sp::large_room_spillover_fake_peers(
        "issuer-large-room-spillover-validation",
        &publisher_user_ids,
        &receiver_user_ids,
    )
    .await?;
    let room = peers.room.clone();
    assert_large_room_worker_shape(&peers.server, &room).await;
    let capped_receiver = s::UserId::Integer(205);
    let cleanup_receivers = [s::UserId::Integer(206), s::UserId::Integer(207)];
    let mut first_video = publish_video_routes(
        &mut peers,
        &room,
        first_publisher,
        second_publisher,
        third_publisher,
        &capped_receiver,
    )
    .await;
    let mut clock = s::FakeClock::default();
    assert_first_video_forwarded(&mut peers, &mut first_video, &mut clock).await;
    publish_audio_routes(
        &mut peers,
        &room,
        first_publisher,
        second_publisher,
        third_publisher,
        &capped_receiver,
        &mut clock,
    )
    .await;
    Box::pin(close_spillover_wave(
        &mut peers,
        &room,
        first_publisher,
        cleanup_receivers,
    ))
    .await?;
    assert_first_video_forwarded(&mut peers, &mut first_video, &mut clock).await;
    Box::pin(close_remaining_peers(peers)).await?;
    Ok(())
}

async fn publish_video_routes(
    peers: &mut sp::LargeRoomSpilloverFakePeers,
    room: &str,
    first_publisher: &s::UserId,
    second_publisher: &s::UserId,
    third_publisher: &s::UserId,
    capped_receiver: &s::UserId,
) -> s::FakeMediaSource {
    let first_video = s::FakeMediaSource::vp8_camera_high();
    publish_source(
        &peers.server,
        room,
        peer_mut(&mut peers.publishers, 0),
        first_publisher,
        &first_video,
    )
    .await;
    ready_video_subscription(
        &peers.server,
        room,
        peer_mut(&mut peers.receivers, 0),
        first_publisher,
    )
    .await;
    m::assert_consumer_route(
        &peers.server,
        room,
        peer_ref(&peers.receivers, 0),
        first_publisher,
        s::StreamType::Camera,
        m::RouteState::Active,
    )
    .await;
    pump_surviving_receiver(peers).await;
    for receiver_index in 1..=3 {
        ready_video_subscription(
            &peers.server,
            room,
            peer_mut(&mut peers.receivers, receiver_index),
            first_publisher,
        )
        .await;
        pump_surviving_receiver(peers).await;
    }

    let second_video = s::FakeMediaSource::vp8_camera_high();
    publish_source(
        &peers.server,
        room,
        peer_mut(&mut peers.publishers, 1),
        second_publisher,
        &second_video,
    )
    .await;
    pump_surviving_receiver(peers).await;
    ready_video_subscription(
        &peers.server,
        room,
        peer_mut(&mut peers.receivers, 1),
        second_publisher,
    )
    .await;
    pump_surviving_receiver(peers).await;

    let third_video = s::FakeMediaSource::vp8_camera_high();
    publish_source(
        &peers.server,
        room,
        peer_mut(&mut peers.publishers, 2),
        third_publisher,
        &third_video,
    )
    .await;
    pump_surviving_receiver(peers).await;
    ready_video_subscription(
        &peers.server,
        room,
        peer_mut(&mut peers.receivers, 1),
        third_publisher,
    )
    .await;
    pump_surviving_receiver(peers).await;
    wait_for_route_state_counts(
        &peers.server,
        room,
        capped_receiver,
        &[first_publisher, second_publisher, third_publisher],
        s::StreamType::Camera,
        2,
        1,
    )
    .await;
    pump_surviving_receiver(peers).await;
    first_video
}

async fn assert_first_video_forwarded(
    peers: &mut sp::LargeRoomSpilloverFakePeers,
    first_video: &mut s::FakeMediaSource,
    clock: &mut s::FakeClock,
) {
    m::assert_synthetic_video_packet_forwarded(
        peer_mut(&mut peers.publishers, 0),
        peer_mut(&mut peers.receivers, 0),
        first_video,
        clock,
    )
    .await;
}

async fn publish_audio_routes(
    peers: &mut sp::LargeRoomSpilloverFakePeers,
    room: &str,
    first_publisher: &s::UserId,
    second_publisher: &s::UserId,
    third_publisher: &s::UserId,
    capped_receiver: &s::UserId,
    clock: &mut s::FakeClock,
) {
    let mut first_audio =
        s::FakeMediaSource::new(s::SyntheticOpusStream::with_audio_activity(-12, true));
    let mut second_audio =
        s::FakeMediaSource::new(s::SyntheticOpusStream::with_audio_activity(-24, true));
    let mut third_audio =
        s::FakeMediaSource::new(s::SyntheticOpusStream::with_audio_activity(-36, true));
    publish_source(
        &peers.server,
        room,
        peer_mut(&mut peers.publishers, 0),
        first_publisher,
        &first_audio,
    )
    .await;
    pump_surviving_receiver(peers).await;
    ready_audio_route(
        &peers.server,
        room,
        peer_mut(&mut peers.receivers, 1),
        first_publisher,
    )
    .await;
    pump_surviving_receiver(peers).await;
    publish_source(
        &peers.server,
        room,
        peer_mut(&mut peers.publishers, 1),
        second_publisher,
        &second_audio,
    )
    .await;
    pump_surviving_receiver(peers).await;
    ready_audio_route(
        &peers.server,
        room,
        peer_mut(&mut peers.receivers, 1),
        second_publisher,
    )
    .await;
    pump_surviving_receiver(peers).await;
    publish_source(
        &peers.server,
        room,
        peer_mut(&mut peers.publishers, 2),
        third_publisher,
        &third_audio,
    )
    .await;
    pump_surviving_receiver(peers).await;
    ready_audio_route(
        &peers.server,
        room,
        peer_mut(&mut peers.receivers, 1),
        third_publisher,
    )
    .await;
    pump_surviving_receiver(peers).await;
    send_audio_activity(peer_mut(&mut peers.publishers, 0), &mut first_audio, clock).await;
    send_audio_activity(peer_mut(&mut peers.publishers, 1), &mut second_audio, clock).await;
    send_audio_activity(peer_mut(&mut peers.publishers, 2), &mut third_audio, clock).await;
    wait_for_route_state_counts(
        &peers.server,
        room,
        capped_receiver,
        &[first_publisher, second_publisher, third_publisher],
        s::StreamType::Audio,
        2,
        1,
    )
    .await;
    pump_surviving_receiver(peers).await;
}

async fn close_spillover_wave(
    peers: &mut sp::LargeRoomSpilloverFakePeers,
    room: &str,
    first_publisher: &s::UserId,
    cleanup_receivers: [s::UserId; 2],
) -> s::TestResult {
    for receiver_index in [3, 2] {
        let cleanup_peer = remove_peer(&mut peers.receivers, receiver_index);
        s::require_some(cleanup_peer.close().await, "cleanup peer should close")?;
        pump_surviving_receiver(peers).await;
    }
    for cleanup_receiver in cleanup_receivers {
        assert!(
            peers
                .server
                .wait_for_consumer_route_absence(
                    room,
                    &cleanup_receiver,
                    first_publisher,
                    s::StreamType::Camera,
                )
                .await
        );
        pump_surviving_receiver(peers).await;
    }
    Ok(())
}

async fn pump_surviving_receiver(peers: &mut sp::LargeRoomSpilloverFakePeers) {
    // ProtocolFakePeer drives str0m only while polled. Refresh consent after
    // each bounded operation so the ICE-lite SFU keeps receiving STUN requests.
    // https://www.rfc-editor.org/rfc/rfc7675.html#section-5.1
    let pumped = peer_mut(&mut peers.receivers, 0)
        .rtc()
        .pump(s::Duration::from_millis(50))
        .await;
    assert!(
        pumped.is_some(),
        "surviving fake RTC should remain connected"
    );
}

fn peer_ref(peers: &[s::ProtocolFakePeer], index: usize) -> &s::ProtocolFakePeer {
    let Some(peer) = peers.get(index) else {
        panic!("expected fake peer at index {index}");
    };
    peer
}

fn peer_mut(peers: &mut [s::ProtocolFakePeer], index: usize) -> &mut s::ProtocolFakePeer {
    let Some(peer) = peers.get_mut(index) else {
        panic!("expected mutable fake peer at index {index}");
    };
    peer
}

fn remove_peer(peers: &mut Vec<s::ProtocolFakePeer>, index: usize) -> s::ProtocolFakePeer {
    assert!(
        index < peers.len(),
        "expected removable fake peer at index {index}"
    );
    peers.remove(index)
}

async fn close_remaining_peers(peers: sp::LargeRoomSpilloverFakePeers) -> s::TestResult {
    let server = peers.server;
    let room = peers.room;
    for peer in peers.publishers.into_iter().chain(peers.receivers) {
        let user_id = peer.user_id().clone();
        s::require_some(peer.close().await, "remaining large-room peer should close")?;
        wait_for_user_absence(&server, &room, &user_id).await;
    }
    Ok(())
}

async fn publish_source(
    server: &s::TestServer,
    room: &str,
    publisher: &mut s::ProtocolFakePeer,
    publisher_user_id: &s::UserId,
    source: &s::FakeMediaSource,
) {
    drain_pending_signaling(publisher).await;
    assert!(publisher.publish_track(source).await.is_some());
    drain_pending_signaling(publisher).await;
    wait_for_source_presence(server, room, publisher_user_id, source.stream_type()).await;
}

async fn send_audio_activity(
    publisher: &mut s::ProtocolFakePeer,
    source: &mut s::FakeMediaSource,
    clock: &mut s::FakeClock,
) {
    assert!(
        publisher
            .rtc()
            .send_rtp_packets(source, clock, 3)
            .await
            .is_some()
    );
}

async fn ready_video_subscription(
    server: &s::TestServer,
    room: &str,
    receiver: &mut s::ProtocolFakePeer,
    publisher_user_id: &s::UserId,
) {
    drain_pending_signaling(receiver).await;
    st::assert_video_subscription_enabled(receiver, publisher_user_id.clone()).await;
    drain_pending_signaling(receiver).await;
    wait_for_route_presence(
        server,
        room,
        receiver.user_id(),
        publisher_user_id,
        s::StreamType::Camera,
    )
    .await;
}

async fn ready_audio_route(
    server: &s::TestServer,
    room: &str,
    receiver: &mut s::ProtocolFakePeer,
    publisher_user_id: &s::UserId,
) {
    drain_pending_signaling(receiver).await;
    m::assert_consumer_route(
        server,
        room,
        receiver,
        publisher_user_id,
        s::StreamType::Audio,
        m::RouteState::Active,
    )
    .await;
}

async fn drain_pending_signaling(peer: &mut s::ProtocolFakePeer) {
    let deadline = s::Instant::now() + s::Duration::from_secs(1);
    loop {
        let now = s::Instant::now();
        if now >= deadline {
            return;
        }
        if peer
            .read_server_message_with_timeout(deadline - now)
            .await
            .is_none()
        {
            return;
        }
    }
}

async fn assert_large_room_worker_shape(server: &s::TestServer, room: &str) {
    let client = reqwest::Client::new();
    let Some(detail) = diagnostics_room(&client, server, room).await else {
        panic!("expected large-room diagnostics");
    };
    let mut media_workers = detail
        .users
        .iter()
        .map(|user| user.transport.media_worker_id)
        .collect::<Vec<_>>();
    media_workers.sort_unstable();
    media_workers.dedup();
    assert_eq!(media_workers, [0, 1, 2, 3]);
}

async fn wait_for_route_state_counts(
    server: &s::TestServer,
    room: &str,
    receiver_user_id: &s::UserId,
    publisher_user_ids: &[&s::UserId],
    stream_type: s::StreamType,
    expected_active_count: usize,
    expected_inactive_count: usize,
) {
    let client = reqwest::Client::new();
    let deadline = s::Instant::now() + s::Duration::from_secs(5);
    loop {
        if let Some(detail) = diagnostics_room(&client, server, room).await {
            let counts =
                route_state_counts(&detail, receiver_user_id, publisher_user_ids, stream_type);
            if counts.total == publisher_user_ids.len()
                && counts.active == expected_active_count
                && counts.inactive == expected_inactive_count
            {
                return;
            }
        }
        assert!(
            s::Instant::now() < deadline,
            "expected route state counts should settle"
        );
        s::sleep(s::Duration::from_millis(50)).await;
    }
}

struct RouteStateCounts {
    total: usize,
    active: usize,
    inactive: usize,
}

fn route_state_counts(
    detail: &DiagnosticsRoomDetail,
    receiver_user_id: &s::UserId,
    publisher_user_ids: &[&s::UserId],
    stream_type: s::StreamType,
) -> RouteStateCounts {
    let mut total_count = 0;
    let mut active_count = 0;
    let mut inactive_count = 0;
    for publisher_user_id in publisher_user_ids {
        let Some(state) = route_state(detail, receiver_user_id, publisher_user_id, stream_type)
        else {
            continue;
        };
        total_count += 1;
        match state {
            DiagnosticsRouteState::Active => active_count += 1,
            DiagnosticsRouteState::Inactive => inactive_count += 1,
            DiagnosticsRouteState::Pending => {}
        }
    }
    RouteStateCounts {
        total: total_count,
        active: active_count,
        inactive: inactive_count,
    }
}

async fn wait_for_source_presence(
    server: &s::TestServer,
    room: &str,
    publisher_user_id: &s::UserId,
    stream_type: s::StreamType,
) {
    let client = reqwest::Client::new();
    let deadline = s::Instant::now() + s::Duration::from_secs(5);
    loop {
        if diagnostics_room(&client, server, room)
            .await
            .is_some_and(|detail| {
                detail.sources.iter().any(|source| {
                    source.owner_user_id == *publisher_user_id
                        && source.stream_id == stream_id(stream_type)
                        && source.active
                })
            })
        {
            return;
        }
        assert!(
            s::Instant::now() < deadline,
            "expected source presence should settle for publisher {publisher_user_id:?}, stream {stream_type:?}"
        );
        s::sleep(s::Duration::from_millis(50)).await;
    }
}

async fn wait_for_user_absence(server: &s::TestServer, room: &str, user_id: &s::UserId) {
    let client = reqwest::Client::new();
    let deadline = s::Instant::now() + s::Duration::from_secs(5);
    loop {
        if diagnostics_room(&client, server, room)
            .await
            .is_none_or(|detail| detail.users.iter().all(|user| user.user_id != *user_id))
        {
            return;
        }
        assert!(
            s::Instant::now() < deadline,
            "expected user absence should settle for user {user_id:?}"
        );
        s::sleep(s::Duration::from_millis(50)).await;
    }
}

async fn wait_for_route_presence(
    server: &s::TestServer,
    room: &str,
    receiver_user_id: &s::UserId,
    publisher_user_id: &s::UserId,
    stream_type: s::StreamType,
) {
    let client = reqwest::Client::new();
    let deadline = s::Instant::now() + s::Duration::from_secs(5);
    loop {
        if diagnostics_room(&client, server, room)
            .await
            .is_some_and(|detail| {
                route_state(&detail, receiver_user_id, publisher_user_id, stream_type).is_some()
            })
        {
            return;
        }
        assert!(
            s::Instant::now() < deadline,
            "expected route presence should settle for receiver {receiver_user_id:?}, publisher {publisher_user_id:?}, stream {stream_type:?}"
        );
        s::sleep(s::Duration::from_millis(50)).await;
    }
}

async fn diagnostics_room(
    client: &reqwest::Client,
    server: &s::TestServer,
    room: &str,
) -> Option<DiagnosticsRoomDetail> {
    let response = client
        .get(format!(
            "{}{}/{}",
            server.http_base_url(),
            route::diagnostics::ROOMS,
            room
        ))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    response.json::<DiagnosticsRoomDetail>().await.ok()
}

fn route_state<'a>(
    detail: &'a DiagnosticsRoomDetail,
    receiver_user_id: &s::UserId,
    publisher_user_id: &s::UserId,
    stream_type: s::StreamType,
) -> Option<&'a DiagnosticsRouteState> {
    detail
        .users
        .iter()
        .find(|user| user.user_id == *receiver_user_id)?
        .subscriptions
        .iter()
        .find(|subscription| {
            subscription.producer_user_id == *publisher_user_id
                && subscription.stream_id == stream_id(stream_type)
        })
        .map(|subscription| &subscription.state)
}

fn stream_id(stream_type: s::StreamType) -> &'static str {
    match stream_type {
        s::StreamType::Audio => "audio",
        s::StreamType::Camera => "camera",
        s::StreamType::Screen => "screen",
    }
}
