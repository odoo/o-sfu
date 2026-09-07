use super::support::{self as s, media as m, setup as st, spillover as sp};

enum LowRidProbe {
    None,
    DropThenForwardSelected,
}

#[tokio::test]
async fn fake_rtc_cross_worker_vp8_selected_rid_survives_relay() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let publisher_user_id = s::UserId::Integer(182);
    let subscriber_user_id = s::UserId::Integer(183);
    let st::ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = Box::pin(st::ready_room_fake_peers_with_config(
        st::cross_worker_test_config(),
        "issuer-cross-worker-vp8-selected-rid",
        publisher_user_id.clone(),
        subscriber_user_id.clone(),
    ))
    .await?;
    sp::assert_cross_worker_placement(&server, &room, &publisher_user_id, &subscriber_user_id)
        .await;

    let mut source = s::FakeMediaSource::new(s::SyntheticVp8Stream::with_next_keyframe(false));
    assert_selected_video_relay(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &publisher_user_id,
        &mut source,
        LowRidProbe::DropThenForwardSelected,
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_cross_worker_h264_selected_rid_forwards_opaque_payload() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let mut config = st::cross_worker_test_config();
    config.codecs.flags = s::MediaCodecFlags::default()
        .with_vp8(false)
        .with_h264(true);
    let publisher_user_id = s::UserId::Integer(184);
    let subscriber_user_id = s::UserId::Integer(185);
    let st::ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = Box::pin(st::ready_room_fake_peers_with_config(
        config,
        "issuer-cross-worker-h264-selected-rid",
        publisher_user_id.clone(),
        subscriber_user_id.clone(),
    ))
    .await?;
    sp::assert_cross_worker_placement(&server, &room, &publisher_user_id, &subscriber_user_id)
        .await;

    let mut source = s::FakeMediaSource::new(s::SyntheticH264Stream::with_idr(false));
    assert_selected_video_relay(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &publisher_user_id,
        &mut source,
        LowRidProbe::None,
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_overload_spillover_releases_remote_route_after_subscriber_leaves() -> s::TestResult
{
    let _guard = st::full_stack_test_guard().await;
    let publisher_user_id = s::UserId::Integer(193);
    let local_subscriber_user_id = s::UserId::Integer(194);
    let spillover_subscriber_user_id = s::UserId::Integer(195);
    let sp::SpilloverRoomFakePeers {
        server,
        room,
        mut publisher,
        mut local_subscriber,
        spillover_subscriber,
    } = Box::pin(sp::spillover_room_fake_peers(
        "issuer-overload-spillover-release-route",
        publisher_user_id.clone(),
        local_subscriber_user_id,
        spillover_subscriber_user_id.clone(),
    ))
    .await?;

    Box::pin(sp::assert_spillover_release_route_flow(
        &server,
        &room,
        &mut publisher,
        &mut local_subscriber,
        spillover_subscriber,
        &publisher_user_id,
        &spillover_subscriber_user_id,
    ))
    .await?;
    Ok(())
}

async fn assert_selected_video_relay(
    server: &s::TestServer,
    room: &str,
    publisher: &mut s::ProtocolFakePeer,
    subscriber: &mut s::ProtocolFakePeer,
    publisher_user_id: &s::UserId,
    source: &mut s::FakeMediaSource,
    low_rid_probe: LowRidProbe,
) {
    m::publish_video_source_and_ready_route(
        server,
        room,
        publisher,
        subscriber,
        publisher_user_id,
        source,
    )
    .await;
    m::assert_featured_video_subscription(server, room, subscriber, publisher_user_id).await;
    let mut clock = s::FakeClock::default();
    if source.codec() == s::CodecName::Vp8 {
        m::assert_packet_dropped(publisher, subscriber, source, &mut clock).await;
    }
    m::assert_synthetic_video_packet_forwarded(publisher, subscriber, source, &mut clock).await;
    match low_rid_probe {
        LowRidProbe::None => {}
        LowRidProbe::DropThenForwardSelected => {
            assert_low_rid_dropped(publisher, subscriber, &mut clock).await;
            m::assert_synthetic_video_packet_forwarded(publisher, subscriber, source, &mut clock)
                .await;
        }
    }
}

async fn assert_low_rid_dropped(
    publisher: &mut s::ProtocolFakePeer,
    subscriber: &mut s::ProtocolFakePeer,
    clock: &mut s::FakeClock,
) {
    let mut source = s::FakeMediaSource::vp8_camera_with_rid("lo");
    m::assert_packet_dropped(publisher, subscriber, &mut source, clock).await;
}

#[tokio::test]
async fn fake_rtc_overload_spillover_preserves_download_mute_after_subscriber_replacement()
-> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let publisher_user_id = s::UserId::Integer(196);
    let local_subscriber_user_id = s::UserId::Integer(197);
    let spillover_subscriber_user_id = s::UserId::Integer(198);
    let sp::SpilloverRoomFakePeers {
        server,
        room,
        mut publisher,
        local_subscriber: _local_subscriber,
        mut spillover_subscriber,
    } = Box::pin(sp::spillover_room_fake_peers(
        "issuer-overload-spillover-replacement-mute",
        publisher_user_id.clone(),
        local_subscriber_user_id,
        spillover_subscriber_user_id.clone(),
    ))
    .await?;

    Box::pin(sp::assert_spillover_replacement_mute_flow(
        &server,
        &room,
        &mut publisher,
        &mut spillover_subscriber,
        publisher_user_id,
        spillover_subscriber_user_id,
    ))
    .await?;
    Ok(())
}
