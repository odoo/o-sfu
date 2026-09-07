use super::{protocol::assert_track_snapshot, setup::assert_video_subscription_enabled, *};

pub(crate) async fn publish_source_and_ready_route(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: &UserId,
    source: &FakeMediaSource,
) -> TrackBinding {
    assert!(publisher.publish_track(source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track = assert_track_snapshot(
        subscriber,
        publisher_user_id.clone(),
        source.stream_type(),
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_consumer_route(
        server,
        room,
        subscriber,
        publisher_user_id,
        track.stream_type,
        RouteState::Active,
    )
    .await;
    track
}

pub(crate) async fn consume_video_source_and_ready_route(
    server: &TestServer,
    room: &str,
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: &UserId,
) -> TrackBinding {
    let track_binding = consume_video_source(subscriber, publisher_user_id).await;
    assert_consumer_route(
        server,
        room,
        subscriber,
        publisher_user_id,
        track_binding.stream_type,
        RouteState::Active,
    )
    .await;
    track_binding
}

pub(crate) async fn publish_video_source_and_ready_route(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: &UserId,
    source: &FakeMediaSource,
) -> TrackBinding {
    let track = publish_video_source(publisher, subscriber, publisher_user_id, source).await;
    assert_consumer_route(
        server,
        room,
        subscriber,
        publisher_user_id,
        track.stream_type,
        RouteState::Active,
    )
    .await;
    track
}

pub(crate) async fn publish_video_source(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: &UserId,
    source: &FakeMediaSource,
) -> TrackBinding {
    assert!(publisher.publish_track(source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    consume_video_source(subscriber, publisher_user_id).await
}

async fn consume_video_source(
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: &UserId,
) -> TrackBinding {
    let track = assert_track_snapshot(
        subscriber,
        publisher_user_id.clone(),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(subscriber, publisher_user_id.clone()).await;
    track
}

pub(crate) async fn assert_featured_video_subscription(
    server: &TestServer,
    room: &str,
    subscriber: &ProtocolFakePeer,
    publisher_user_id: &UserId,
) {
    assert!(
        server
            .wait_for_video_subscription_layout(
                room,
                subscriber.user_id(),
                publisher_user_id,
                DiagnosticsVideoLayoutRole::Featured,
            )
            .await
    );
}

pub(crate) async fn assert_packet_forwarded(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> u64 {
    let Some(expected_payload) = publisher.rtc().send_rtp_packet(source, clock).await else {
        panic!("synthetic packet should be accepted by fake publisher");
    };
    assert!(
        read_expected_rtp_payload(
            publisher,
            subscriber,
            &expected_payload,
            Duration::from_secs(5),
        )
        .await
    );
    u64::try_from(expected_payload.len()).unwrap_or(u64::MAX)
}

pub(crate) async fn assert_synthetic_video_packet_forwarded(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> u64 {
    for _ in 0..3 {
        let Some(expected_payload) = publisher.rtc().send_rtp_packet(source, clock).await else {
            panic!("synthetic video packet should be accepted by fake publisher");
        };
        if read_expected_rtp_payload(
            publisher,
            subscriber,
            &expected_payload,
            Duration::from_secs(5),
        )
        .await
        {
            return u64::try_from(expected_payload.len()).unwrap_or(u64::MAX);
        }
    }
    panic!("synthetic video packet should be forwarded after route warmup");
}

pub(crate) async fn assert_packet_dropped(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    assert!(
        publisher
            .rtc()
            .send_rtp_packet(source, clock)
            .await
            .is_some()
    );
    let observation_window = Duration::from_millis(300);
    let (publisher_pumped, received_packet) = join!(
        publisher.rtc().pump(observation_window),
        subscriber.rtc().read_rtp_packet(observation_window),
    );
    assert!(publisher_pumped.is_some());
    assert!(
        matches!(received_packet, Ok(None)),
        "subscriber RTC should remain healthy without RTP: {received_packet:?}"
    );
}

pub(crate) async fn read_expected_rtp_payload(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    expected_payload: &[u8],
    timeout_window: Duration,
) -> bool {
    let deadline = Instant::now() + timeout_window;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        let slice = Duration::from_millis(50).min(deadline - now);
        let (publisher_pumped, received_packet) = join!(
            publisher.rtc().pump(slice),
            subscriber.rtc().read_rtp_packet(slice),
        );
        if publisher_pumped.is_none() {
            return false;
        }
        let received_packet = received_packet
            .unwrap_or_else(|error| panic!("subscriber RTC receive failed: {error:#}"));
        if received_packet.is_some_and(|packet| packet.payload.as_ref() == expected_payload) {
            return true;
        }
    }
}

pub(crate) enum RouteState {
    Active,
    Inactive,
}

pub(crate) async fn assert_consumer_route(
    server: &TestServer,
    room: &str,
    subscriber: &ProtocolFakePeer,
    publisher_user_id: &UserId,
    stream_type: StreamType,
    state: RouteState,
) {
    let routed = match state {
        RouteState::Active => {
            server
                .wait_for_consumer_route_active(
                    room,
                    subscriber.user_id(),
                    publisher_user_id,
                    stream_type,
                )
                .await
        }
        RouteState::Inactive => {
            server
                .wait_for_consumer_route_inactive(
                    room,
                    subscriber.user_id(),
                    publisher_user_id,
                    stream_type,
                )
                .await
        }
    };
    assert!(routed);
}
