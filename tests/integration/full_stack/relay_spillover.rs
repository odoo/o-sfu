use std::time::{Duration, Instant};

use o_sfu_core::{
    ConnectionId, MediaWorkerId, RoomInstanceId,
    server::transport::{
        TransportMediaId, TransportSessionKey, TransportSourceKey, test_support as transport_test,
    },
};
use o_sfu_router::{
    MediaKind,
    rtp::{MediaStream, StreamBinding},
};
use tokio::{task, time};

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
    server.join().await
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
    server.join().await
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
    server.join().await
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
    server.join().await
}

#[tokio::test]
#[expect(
    clippy::print_stderr,
    reason = "the integration run reports measured relay convergence delay"
)]
async fn saturated_source_queue_converges_without_consumer_input() {
    for install in [true, false] {
        let delay = relay_converges_under_queue_pressure(install).await;
        eprintln!(
            "relay install={install} converged {delay:?} after waiting room commands drained"
        );
        // OS scheduling and Tokio timer granularity can overshoot the 10 ms deadline.
        assert!(
            delay < Duration::from_millis(100),
            "relay retry failed to converge promptly: {delay:?}"
        );
    }
}

fn session_key(room: u64, worker: usize, connection: u64, user: s::UserId) -> TransportSessionKey {
    TransportSessionKey::new(
        RoomInstanceId::from_raw(room),
        MediaWorkerId::from_raw(worker),
        ConnectionId::from_raw(connection),
        user,
    )
}

/// Measures convergence after waiting room commands drain from the source queue.
/// No command reaches the consumer worker after its initial add or removal.
///
/// # Panics
///
/// Panics when worker setup, queue saturation or relay convergence fails.
#[expect(
    clippy::expect_used,
    reason = "integration fixture failures must stop the scenario"
)]
#[expect(
    clippy::too_many_lines,
    reason = "one worker lifecycle covers setup, queue saturation and convergence"
)]
async fn relay_converges_under_queue_pressure(install: bool) -> Duration {
    let adapter = transport_test::test_media_transport(2, transport_test::test_rtc_port_range())
        .expect("transport should build");
    let source_session = session_key(42, 0, 1, s::UserId::Integer(1));
    let consumer_session = session_key(42, 1, 2, s::UserId::Integer(2));
    for session in [&source_session, &consumer_session] {
        adapter
            .create_initial_session_offer("relay-pressure", session, true)
            .await
            .expect("session should initialize");
    }
    let parameters = MediaStream::new(vec![], vec![], vec![StreamBinding::new().with_ssrc(66_000)])
        .with_mid("relay-source");
    let source_media_id = adapter
        .publish_media(&source_session, MediaKind::Audio, &parameters)
        .await
        .expect("source should publish");
    let source = TransportSourceKey::new(source_session.clone(), source_media_id);
    let consumer_parameters =
        MediaStream::new(vec![], vec![], vec![StreamBinding::new().with_ssrc(67_000)])
            .with_mid("relay-target");
    let consume = || {
        adapter.test_api().consume_media(
            &consumer_session,
            MediaKind::Audio,
            &source,
            &consumer_parameters,
        )
    };
    let consumer_media_id = if install {
        None
    } else {
        Some(consume().await.expect("consumer should register"))
    };
    assert_eq!(
        adapter
            .test_api()
            .source_relay_target_count(&source)
            .await
            .expect("probe should complete"),
        usize::from(!install)
    );

    // Room commands waiting for the paused source worker take every permit
    // its queue releases, so the consumer worker cannot enqueue relay control.
    let release = adapter
        .test_api()
        .pause_first_worker()
        .await
        .expect("source worker should pause");
    let waiting_commands = (0..80)
        .map(|_| {
            let adapter = adapter.clone();
            let source_session = source_session.clone();
            tokio::spawn(async move {
                let _ = adapter
                    .test_api()
                    .remove_media(&source_session, TransportMediaId::new(u64::MAX))
                    .await;
            })
        })
        .collect::<Vec<_>>();
    let saturated = async {
        while adapter
            .worker_pressure_snapshots()
            .first()
            .expect("source worker should exist")
            .command_backlog_depth
            < 64
        {
            task::yield_now().await;
        }
    };
    time::timeout(Duration::from_secs(5), saturated)
        .await
        .expect("waiting room commands should fill the source queue");
    if let Some(media) = consumer_media_id {
        adapter
            .test_api()
            .remove_media(&consumer_session, media)
            .await
            .expect("consumer removal should succeed");
    } else {
        let _ = consume()
            .await
            .expect("consumer setup must not wait for the source queue");
    }

    release
        .send(())
        .expect("paused source worker should accept release");
    for command in waiting_commands {
        command.await.expect("waiting room command should finish");
    }
    let drained_at = Instant::now();
    // Only probes reach the source worker from here and nothing reaches the
    // consumer worker, whose retry deadline must deliver relay control.
    let delivered = async {
        while adapter
            .test_api()
            .source_relay_target_count(&source)
            .await
            .expect("test probe must complete")
            != usize::from(install)
        {
            task::yield_now().await;
        }
    };
    time::timeout(Duration::from_secs(5), delivered)
        .await
        .expect("pending relay control should reach the source worker");
    drained_at.elapsed()
}
