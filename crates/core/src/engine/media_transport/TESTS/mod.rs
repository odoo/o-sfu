#![allow(
    clippy::panic,
    reason = "media transport tests fail loudly when fixed test setup is invalid"
)]

use std::{
    net::{Ipv4Addr, SocketAddrV4, UdpSocket},
    time::{Duration, Instant},
};

use o_sfu_router::{
    MediaKind,
    rtp::{MediaStream, StreamBinding},
};
use str0m::media::Mid;

use super::{
    MediaTransport, MediaTransportBuildError, TransportTeardown, route_control::reconcile_applied,
};
use crate::{
    Bitrate, MediaWorkerId, RtcPortRange, RtcUdpIoBackend,
    engine::{
        ConnectionId, RoomInstanceId, UserId,
        media_transport::{
            ActiveSpeakerSource, ConsumerActivity, ConsumerRouteControl, MediaControlPlan,
            ProducerActivity, ReceiverBweTargetUpdate, RelayRouteActivity, SessionOffer,
            SourceActivityRevision, SourceActivityUpdate, SourcePacketGate, TransportAdapterError,
            TransportConsumerRoute, TransportMediaId, TransportRelayRouteAction,
            TransportRelayRouteEffect, TransportResult, TransportSessionHealth,
            TransportSessionKey, TransportSourceKey,
            rtc::{RtcWorker, WorkerMediaControlBatchOutcome},
            test_support::{
                DebugPacketGate, test_media_transport as build_test_media_transport,
                test_media_transport_config, test_media_transport_deps, test_rtc_port_range,
            },
        },
        metrics::{MetricName, test_support::RuntimeMetricsSnapshotLookup},
        sync::lock_unpoisoned,
    },
};

fn test_session_key(
    room_instance_id: u64,
    media_worker_id: usize,
    connection_id: u64,
    user_id: UserId,
) -> TransportSessionKey {
    TransportSessionKey::new(
        RoomInstanceId::from_raw(room_instance_id),
        MediaWorkerId::from_raw(media_worker_id),
        ConnectionId::from_raw(connection_id),
        user_id,
    )
}

fn sample_rtp_parameters(mid: &str, ssrc: u32) -> MediaStream {
    MediaStream::new(vec![], vec![], vec![StreamBinding::new().with_ssrc(ssrc)])
        .with_mid(String::from(mid))
}

async fn prepare_rtc_sessions(adapter: &MediaTransport, session_keys: &[&TransportSessionKey]) {
    for session_key in session_keys {
        let _offer = expect_initial_offer(adapter, session_key).await;
    }
}

async fn publish_audio(
    adapter: &MediaTransport,
    session_key: &TransportSessionKey,
    rtp_parameters: &MediaStream,
) -> TransportMediaId {
    adapter
        .publish_media(session_key, MediaKind::Audio, rtp_parameters)
        .await
        .unwrap_or_else(|error| panic!("test audio publication should succeed: {error:?}"))
}

async fn consume_audio(
    adapter: &MediaTransport,
    consumer_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
    source_media_id: TransportMediaId,
    rtp_parameters: &MediaStream,
) -> TransportMediaId {
    adapter
        .consume_media(
            consumer_session_key,
            MediaKind::Audio,
            source_session_key,
            source_media_id,
            rtp_parameters,
            ConsumerActivity::Active,
        )
        .await
        .unwrap_or_else(|error| panic!("test audio consumption should succeed: {error:?}"))
}

async fn apply_relay_route_effect(
    adapter: &MediaTransport,
    source_session_key: &TransportSessionKey,
    source_media_id: TransportMediaId,
    target_session_key: &TransportSessionKey,
    action: TransportRelayRouteAction,
) {
    let effect = TransportRelayRouteEffect {
        source: TransportSourceKey::new(source_session_key.clone(), source_media_id),
        target_media_worker_id: target_session_key.media_worker_id(),
        action,
    };
    assert!(adapter.apply_relay_route_effect(&effect).await.is_ok());
}

async fn install_active_relay_route(
    adapter: &MediaTransport,
    source_session_key: &TransportSessionKey,
    source_media_id: TransportMediaId,
    target_session_key: &TransportSessionKey,
) {
    apply_relay_route_effect(
        adapter,
        source_session_key,
        source_media_id,
        target_session_key,
        TransportRelayRouteAction::Install,
    )
    .await;
    apply_relay_route_effect(
        adapter,
        source_session_key,
        source_media_id,
        target_session_key,
        TransportRelayRouteAction::SetActivity(RelayRouteActivity::Active),
    )
    .await;
}

async fn set_remote_relay_and_consumer_active(
    adapter: &MediaTransport,
    remote_consumer_session: &TransportSessionKey,
    remote_consumer_media_id: TransportMediaId,
    source_session: &TransportSessionKey,
    source_media_id: TransportMediaId,
    active: bool,
) {
    apply_relay_route_effect(
        adapter,
        source_session,
        source_media_id,
        remote_consumer_session,
        TransportRelayRouteAction::SetActivity(RelayRouteActivity::from_active(active)),
    )
    .await;
    let route = TransportConsumerRoute::new(
        remote_consumer_session.clone(),
        remote_consumer_media_id,
        TransportSourceKey::new(source_session.clone(), source_media_id),
    );
    let mut plan: MediaControlPlan<(), ()> = MediaControlPlan::default();
    plan.push_consumer(
        ConsumerRouteControl::new(route).activity(ConsumerActivity::from_active(active)),
        (),
    );
    let outcome = adapter.apply_media_control(plan).await;
    let [((), consumer_outcome)] = outcome.consumers.as_slice() else {
        panic!("one consumer route-control outcome should be returned");
    };
    assert!(!consumer_outcome.activity_failed());
}

async fn assert_relay_target_counts(
    source_worker: &RtcWorker,
    source_media_id: TransportMediaId,
    total: usize,
    active: usize,
) {
    assert_eq!(
        source_worker
            .debug_relay_target_count(source_media_id)
            .await,
        total
    );
    assert_eq!(
        source_worker
            .debug_active_relay_target_count(source_media_id)
            .await,
        active
    );
}

fn transport_cleanup_failures(adapter: &MediaTransport) -> u64 {
    adapter.metrics.snapshot().counter_value(
        MetricName::TransportCleanupFailuresTotal,
        &[("kind", "terminal")],
    )
}

async fn assert_local_route_active(
    source_worker: &RtcWorker,
    source_session: &TransportSessionKey,
    local_consumer_session: &TransportSessionKey,
    local_consumer_media_id: TransportMediaId,
) {
    let Some(local_route_entry) = source_worker
        .debug_route_entry(source_session, Mid::from("aud-up"))
        .await
    else {
        panic!("local route entry should exist");
    };
    assert!(local_route_entry.destinations.iter().any(|destination| {
        destination.dest_session == *local_consumer_session
            && destination.dest_transport_media_id == local_consumer_media_id
            && destination.active
    }));
}

async fn assert_remote_route_activity(
    consumer_worker: &RtcWorker,
    source_media_id: TransportMediaId,
    remote_consumer_session: &TransportSessionKey,
    remote_consumer_media_id: TransportMediaId,
    active: bool,
) {
    let Some(remote_route_entry) = consumer_worker
        .debug_route_entry_by_media_id(source_media_id)
        .await
    else {
        panic!("remote route entry should exist");
    };
    assert!(remote_route_entry.destinations.iter().any(|destination| {
        destination.dest_session == *remote_consumer_session
            && destination.dest_transport_media_id == remote_consumer_media_id
            && destination.active == active
    }));
}

fn test_media_transport(worker_count: usize, rtc_port_range: RtcPortRange) -> MediaTransport {
    build_test_media_transport(worker_count, rtc_port_range)
        .unwrap_or_else(|error| panic!("RTC transport test config should be valid: {error}"))
}

fn expect_first_candidate_port(offer_sdp: &str) -> u16 {
    offer_sdp
        .lines()
        .find_map(|line| line.trim().strip_prefix("a=candidate:"))
        .and_then(|candidate| candidate.split_whitespace().nth(5))
        .and_then(|port| port.parse::<u16>().ok())
        .unwrap_or_else(|| panic!("RTC offer should include a parseable ICE candidate port"))
}

fn expect_worker_for_user<'a>(
    adapter: &'a MediaTransport,
    session_key: &TransportSessionKey,
) -> &'a RtcWorker {
    let Some(worker) = adapter.worker_for_user(session_key) else {
        panic!("test session should be assigned to a media worker");
    };
    worker
}

async fn expect_initial_offer(
    adapter: &MediaTransport,
    session_key: &TransportSessionKey,
) -> SessionOffer {
    adapter
        .create_initial_session_offer("test-room", session_key)
        .await
        .unwrap_or_else(|error| panic!("test session should create an RTC offer: {error:?}"))
}

async fn assert_media_control_batch_bound(
    adapter: &MediaTransport,
    consumer_session: TransportSessionKey,
    source: TransportSourceKey,
    route: TransportConsumerRoute,
) {
    lock_unpoisoned(&adapter.media_control_batches).clear();
    let mut bounded = MediaControlPlan::default();
    let bwe = ReceiverBweTargetUpdate::new(consumer_session, Bitrate::from_kbps(600));
    bounded.set_receiver_bwe_targets(vec![bwe; 65]);
    for index in 0..65 {
        let producer = if index == 0 {
            TransportSourceKey::new(
                source.session_key().clone(),
                TransportMediaId::new(u64::MAX),
            )
        } else {
            source.clone()
        };
        bounded.push_producer(
            producer,
            SourceActivityUpdate::new(
                ProducerActivity::Active,
                SourceActivityRevision::default().next(),
            ),
            index,
        );
        bounded.push_consumer(
            ConsumerRouteControl::new(route.clone()).activity(ConsumerActivity::Active),
            index,
        );
    }
    let outcome = adapter.apply_media_control(bounded).await;
    assert!(outcome.producers.iter().map(|entry| entry.0).eq(0..65));
    assert!(outcome.consumers.iter().map(|entry| entry.0).eq(0..65));
    assert_eq!(
        outcome.producers.first().map(|entry| entry.1),
        Some(Err(TransportAdapterError::TransportUnavailable))
    );
    assert_eq!(outcome.producers.last().map(|entry| entry.1), Some(Ok(())));
    assert!(
        outcome
            .consumers
            .last()
            .is_some_and(|(_, result)| result.error().is_none())
    );
    let first_chunk = (0..64).collect::<Vec<_>>();
    assert_eq!(
        *lock_unpoisoned(&adapter.media_control_batches),
        vec![
            (1, "bwe", first_chunk.clone()),
            (1, "bwe", vec![64]),
            (1, "producer", first_chunk.clone()),
            (1, "producer", vec![64]),
            (1, "consumer", first_chunk),
            (1, "consumer", vec![64]),
        ]
    );
}

#[tokio::test]
#[expect(clippy::too_many_lines, reason = "one phase-order scenario")]
async fn media_transport_plan_updates_source_route() {
    let adapter = test_media_transport(3, test_rtc_port_range());
    let source_session = test_session_key(60, 1, 1, UserId::Integer(1));
    let consumer_session = test_session_key(60, 1, 2, UserId::Integer(2));
    let peer_source_session = test_session_key(60, 2, 3, UserId::Integer(3));
    let missing_session = test_session_key(60, 0, 4, UserId::Integer(4));
    let source_rtp_parameters = sample_rtp_parameters("cam-up", 81_000);
    let consumer_rtp_parameters = sample_rtp_parameters("cam-down", 82_000);

    prepare_rtc_sessions(
        &adapter,
        &[&source_session, &peer_source_session, &consumer_session],
    )
    .await;

    let source_media_id = adapter
        .publish_media(&source_session, MediaKind::Video, &source_rtp_parameters)
        .await
        .unwrap_or_else(|error| panic!("test video publication should succeed: {error:?}"));
    let peer_source_media_id = adapter
        .publish_media(
            &peer_source_session,
            MediaKind::Video,
            &source_rtp_parameters,
        )
        .await
        .unwrap_or_else(|error| panic!("peer video publication should succeed: {error:?}"));
    let peer_consumer_media_id = adapter
        .consume_media(
            &peer_source_session,
            MediaKind::Video,
            &peer_source_session,
            peer_source_media_id,
            &consumer_rtp_parameters,
            ConsumerActivity::Active,
        )
        .await
        .unwrap_or_else(|error| panic!("peer video consumption should succeed: {error:?}"));
    let consumer_media_id = adapter
        .consume_media(
            &consumer_session,
            MediaKind::Video,
            &source_session,
            source_media_id,
            &consumer_rtp_parameters,
            ConsumerActivity::Active,
        )
        .await
        .unwrap_or_else(|error| panic!("test video consumption should succeed: {error:?}"));
    let source = TransportSourceKey::new(source_session.clone(), source_media_id);
    let peer_source = TransportSourceKey::new(peer_source_session.clone(), peer_source_media_id);
    let route =
        TransportConsumerRoute::new(consumer_session.clone(), consumer_media_id, source.clone());
    let peer_route = TransportConsumerRoute::new(
        peer_source_session.clone(),
        peer_consumer_media_id,
        peer_source.clone(),
    );
    let missing_media_id = TransportMediaId::new(999_999);
    let missing_source = TransportSourceKey::new(missing_session.clone(), missing_media_id);
    let missing_route =
        TransportConsumerRoute::new(missing_session.clone(), missing_media_id, source.clone());
    let misaddressed_route = TransportConsumerRoute::new(
        test_session_key(60, 1, 6, UserId::Integer(6)),
        consumer_media_id,
        source.clone(),
    );
    let cross_room_route = TransportConsumerRoute::new(
        test_session_key(61, 0, 5, UserId::Integer(5)),
        missing_media_id,
        source.clone(),
    );
    let mut plan = MediaControlPlan::default();
    plan.set_receiver_bwe_targets(vec![
        ReceiverBweTargetUpdate::new(consumer_session.clone(), Bitrate::from_kbps(600)),
        ReceiverBweTargetUpdate::new(peer_source_session, Bitrate::from_kbps(500)),
        ReceiverBweTargetUpdate::new(consumer_session.clone(), Bitrate::from_kbps(600)),
        ReceiverBweTargetUpdate::new(missing_session, Bitrate::from_kbps(700)),
    ]);
    let first_revision = SourceActivityRevision::default().next();
    let second_revision = first_revision.next();
    plan.push_producer(
        source.clone(),
        SourceActivityUpdate::new(ProducerActivity::Inactive, first_revision),
        0,
    );
    plan.push_producer(
        peer_source.clone(),
        SourceActivityUpdate::new(ProducerActivity::Inactive, first_revision),
        1,
    );
    plan.push_producer(
        source.clone(),
        SourceActivityUpdate::new(ProducerActivity::Active, second_revision),
        2,
    );
    plan.push_producer(
        peer_source,
        SourceActivityUpdate::new(ProducerActivity::Active, second_revision),
        3,
    );
    plan.push_producer(
        missing_source,
        SourceActivityUpdate::new(ProducerActivity::Inactive, first_revision),
        4,
    );
    let follow_up = |route, rid: Option<&str>, active| {
        let packet_gate = rid.map_or(SourcePacketGate::Open, |rid| {
            SourcePacketGate::Rid(rid.into())
        });
        ConsumerRouteControl::new(route)
            .packet_gate(packet_gate)
            .activity(ConsumerActivity::from_active(active))
            .request_keyframe(true)
    };
    plan.push_consumer(
        ConsumerRouteControl::new(cross_room_route).packet_gate(SourcePacketGate::Rid("hi".into())),
        0,
    );
    plan.push_consumer(follow_up(missing_route.clone(), Some("hi"), false), 1);
    plan.push_consumer(
        ConsumerRouteControl::new(missing_route)
            .activity(ConsumerActivity::Inactive)
            .request_keyframe(true),
        2,
    );
    plan.push_consumer(follow_up(misaddressed_route, Some("hi"), false), 3);
    plan.push_consumer(follow_up(peer_route.clone(), Some("lo"), false), 4);
    plan.push_consumer(follow_up(route.clone(), Some("lo"), false), 5);
    plan.push_consumer(follow_up(peer_route, None, true), 6);
    plan.push_consumer(follow_up(route.clone(), None, true), 7);

    let outcome = adapter.apply_media_control(plan).await;

    assert_eq!(
        outcome.producers.as_slice(),
        &[
            (0, Ok(())),
            (1, Ok(())),
            (2, Ok(())),
            (3, Ok(())),
            (4, Err(TransportAdapterError::TransportUnavailable))
        ]
    );
    let [
        (0, cross_room),
        (1, missing_gate),
        (2, missing_activity),
        (3, misaddressed_gate),
        (4, _),
        (5, valid_after_failed_gate),
        (6, _),
        (7, valid_after_failed_gate_duplicate),
    ] = outcome.consumers.as_slice()
    else {
        panic!("media-control plan should preserve consumer input order");
    };
    assert!(misaddressed_gate.packet_gate_failed());
    assert!(missing_activity.activity_failed());
    assert_eq!(
        [
            cross_room.error(),
            missing_gate.error(),
            missing_activity.error(),
            misaddressed_gate.error(),
        ],
        [
            Some(TransportAdapterError::InvalidInput),
            Some(TransportAdapterError::TransportUnavailable),
            Some(TransportAdapterError::TransportUnavailable),
            Some(TransportAdapterError::InvalidInput),
        ]
    );
    assert_eq!(
        [
            valid_after_failed_gate.error(),
            valid_after_failed_gate_duplicate.error(),
        ],
        [None, None]
    );
    assert_eq!(
        *lock_unpoisoned(&adapter.media_control_batches),
        vec![
            (0, "bwe", vec![3]),
            (1, "bwe", vec![0, 2]),
            (2, "bwe", vec![1]),
            (0, "producer", vec![4]),
            (1, "producer", vec![0, 2]),
            (2, "producer", vec![1, 3]),
            (0, "gates", vec![1]),
            (1, "gates", vec![3, 5, 7]),
            (2, "gates", vec![4, 6]),
            (0, "consumer", vec![2]),
            (1, "consumer", vec![5, 7]),
            (2, "consumer", vec![4, 6]),
        ]
    );
    let route_entry = adapter
        .test_api()
        .route_entry_by_media_id(route.source_transport_media_id())
        .await
        .unwrap_or_else(|| panic!("video route should survive planned route control"));
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Open);
    assert!(route_entry.source_active);
    assert_eq!(route_entry.active_destination_count, 1);
    assert!(route_entry.destinations.iter().any(|destination| {
        destination.dest_session == consumer_session
            && destination.dest_transport_media_id == consumer_media_id
            && destination.active
    }));
    assert_media_control_batch_bound(&adapter, consumer_session, source, route).await;
}

#[test]
fn media_control_reconciliation_rejects_short_worker_results() {
    let results = reconcile_applied(Ok(WorkerMediaControlBatchOutcome::Applied(vec![Ok(())])), 2);
    assert_eq!(
        results,
        vec![Err(TransportAdapterError::TransportUnavailable); 2]
    );
}

#[test]
fn media_transport_build_rejects_invalid_worker_count() {
    let config = test_media_transport_config(0, RtcPortRange::new(46_210, 46_211));
    let result = MediaTransport::build(config, test_media_transport_deps());

    assert_eq!(
        result.err(),
        Some(MediaTransportBuildError::InvalidWorkerCount)
    );
}

#[test]
fn media_transport_build_rejects_invalid_port_split() {
    let config = test_media_transport_config(3, RtcPortRange::new(46_220, 46_221));
    let result = MediaTransport::build(config, test_media_transport_deps());

    assert_eq!(
        result.err(),
        Some(MediaTransportBuildError::InvalidPortSplit {
            worker_count: 3,
            port_count: 2,
        })
    );
}

#[test]
fn media_transport_build_rejects_occupied_port_range() {
    let blocker = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))
        .unwrap_or_else(|error| panic!("test RTC port should bind: {error}"));
    let port = blocker
        .local_addr()
        .unwrap_or_else(|error| panic!("test RTC port should expose its address: {error}"))
        .port();
    assert_eq!(
        MediaTransport::build(
            test_media_transport_config(1, RtcPortRange::new(port, port)),
            test_media_transport_deps(),
        )
        .err(),
        Some(MediaTransportBuildError::WorkerStartup { worker_index: 0 })
    );
}

#[test]
fn media_transport_overlapping_ranges_skip_bound_ports() {
    let range = test_rtc_port_range();
    let _first = test_media_transport(2, range);
    let _second = test_media_transport(2, range);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn media_transport_io_uring_worker_binds_before_first_offer() {
    let blocker = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))
        .unwrap_or_else(|error| panic!("test RTC port should bind: {error}"));
    let blocked_port = blocker
        .local_addr()
        .unwrap_or_else(|error| panic!("test RTC port should expose its address: {error}"))
        .port();
    let mut blocked_config =
        test_media_transport_config(1, RtcPortRange::new(blocked_port, blocked_port));
    blocked_config.rtc_udp_io_backend = RtcUdpIoBackend::IoUring;
    assert_eq!(
        MediaTransport::build(blocked_config, test_media_transport_deps()).err(),
        Some(MediaTransportBuildError::WorkerStartup { worker_index: 0 })
    );
    drop(blocker);

    let range = test_rtc_port_range();
    let mut config = test_media_transport_config(1, range);
    config.rtc_udp_io_backend = RtcUdpIoBackend::IoUring;
    let adapter = MediaTransport::build(config, test_media_transport_deps())
        .unwrap_or_else(|error| panic!("io_uring media transport should start: {error}"));

    let session = test_session_key(1, 0, 1, UserId::Integer(1));
    let offer = expect_initial_offer(&adapter, &session).await;
    let port = expect_first_candidate_port(&offer.sdp);
    assert!(range.ports().any(|candidate| candidate == port));
    assert!(UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)).is_err());
}

#[cfg(not(target_os = "linux"))]
#[test]
fn media_transport_build_rejects_non_linux_io_uring_backend() {
    let mut config = test_media_transport_config(1, RtcPortRange::new(46_230, 46_230));
    config.rtc_udp_io_backend = RtcUdpIoBackend::IoUring;

    let result = MediaTransport::build(config, test_media_transport_deps());

    assert_eq!(
        result.err(),
        Some(MediaTransportBuildError::UnsupportedUdpIoBackend {
            backend: RtcUdpIoBackend::IoUring,
        })
    );
}

#[test]
fn rtc_rejects_answers_without_projectable_client_capabilities() {
    let projected =
        super::fuzz_support::client_rtp_capabilities_from_answer("v=0\r\ns=invalid-answer\r\n");

    assert_eq!(projected, None);
}

#[tokio::test]
async fn rtc_workers_room_bootstrap_by_explicit_media_worker() {
    let rtc_port_range = test_rtc_port_range();
    let adapter = test_media_transport(2, rtc_port_range);
    let first_room_session = test_session_key(10, 0, 1, UserId::Integer(1));
    let second_room_session = test_session_key(11, 1, 1, UserId::Integer(2));
    let same_worker_session = test_session_key(12, 0, 1, UserId::Integer(3));

    let first_offer = expect_initial_offer(&adapter, &first_room_session).await;
    let second_offer = expect_initial_offer(&adapter, &second_room_session).await;
    let same_worker_offer = expect_initial_offer(&adapter, &same_worker_session).await;

    let first_port = expect_first_candidate_port(&first_offer.into_parts().0);
    let second_port = expect_first_candidate_port(&second_offer.into_parts().0);
    let same_worker_port = expect_first_candidate_port(&same_worker_offer.into_parts().0);

    let mut worker_ranges = rtc_port_range
        .split_for_workers(2)
        .unwrap_or_else(|| panic!("test RTC range should split across workers"))
        .into_iter();
    let first_worker_range = worker_ranges
        .next()
        .unwrap_or_else(|| panic!("first worker range should exist"));
    let second_worker_range = worker_ranges
        .next()
        .unwrap_or_else(|| panic!("second worker range should exist"));
    assert!(first_worker_range.ports().any(|port| port == first_port));
    assert!(second_worker_range.ports().any(|port| port == second_port));
    assert!(UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, first_port)).is_err());
    assert!(UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, second_port)).is_err());
    assert_eq!(same_worker_port, first_port);
}

#[tokio::test]
async fn rtc_active_speaker_source_snapshot_canonicalizes_worker_observations() {
    let adapter = test_media_transport(2, test_rtc_port_range());
    let first_session = test_session_key(14, 0, 1, UserId::Integer(1));
    let second_session = test_session_key(14, 1, 2, UserId::Integer(2));
    let first_worker = expect_worker_for_user(&adapter, &first_session);
    let second_worker = expect_worker_for_user(&adapter, &second_session);
    let unique_media_id = TransportMediaId::new(22);
    let tied_media_id = TransportMediaId::new(23);
    let repeated_media_id = TransportMediaId::new(24);
    let oldest = Instant::now() + Duration::from_mins(1);
    let middle = oldest + Duration::from_millis(1);
    let newest = oldest + Duration::from_millis(2);

    first_worker
        .debug_observe_audio_activity(repeated_media_id, Some(true), Some(-30), newest)
        .await;
    first_worker
        .debug_observe_audio_activity(tied_media_id, Some(true), Some(-40), middle)
        .await;
    second_worker
        .debug_observe_audio_activity(unique_media_id, Some(true), Some(-20), middle)
        .await;
    second_worker
        .debug_observe_audio_activity(tied_media_id, Some(true), Some(-5), middle)
        .await;
    second_worker
        .debug_observe_audio_activity(repeated_media_id, Some(true), Some(-10), oldest)
        .await;

    assert_eq!(
        adapter.active_speaker_source_snapshot().await,
        vec![
            ActiveSpeakerSource::with_audio_level(repeated_media_id, newest, Some(-30)),
            ActiveSpeakerSource::with_audio_level(unique_media_id, middle, Some(-20)),
            ActiveSpeakerSource::with_audio_level(tied_media_id, middle, Some(-5)),
        ]
    );
}

#[tokio::test]
async fn rtc_rejects_noncanonical_media_worker_id() {
    let adapter = test_media_transport(2, test_rtc_port_range());
    let session = test_session_key(13, 2, 1, UserId::Integer(4));

    let offer = adapter
        .create_initial_session_offer("test-room", &session)
        .await;

    assert_eq!(
        offer.err(),
        Some(TransportAdapterError::TransportUnavailable)
    );
}

#[tokio::test]
async fn rtc_diagnostics_group_workers_and_preserve_media_ids() {
    let adapter = test_media_transport(2, test_rtc_port_range());
    let first_session = test_session_key(50, 0, 1, UserId::Integer(1));
    let second_session = test_session_key(50, 1, 2, UserId::Integer(2));
    let first_rtp = sample_rtp_parameters("first", 71_000);
    let sibling_rtp = sample_rtp_parameters("sibling", 71_001);
    let unrelated_rtp = sample_rtp_parameters("unrelated", 71_002);
    let second_rtp = sample_rtp_parameters("second", 72_000);

    prepare_rtc_sessions(&adapter, &[&first_session, &second_session]).await;
    let test_api = adapter.test_api();
    test_api.set_session_transport_health(&first_session, TransportSessionHealth::Connected);
    test_api.set_session_transport_health(&second_session, TransportSessionHealth::Disconnected);
    let health =
        adapter.transport_health_snapshot(&[first_session.clone(), second_session.clone()]);
    assert_eq!(health.len(), 2);
    assert_eq!(
        health.get(&first_session),
        Some(&TransportSessionHealth::Connected)
    );
    assert_eq!(
        health.get(&second_session),
        Some(&TransportSessionHealth::Disconnected)
    );

    let first_media_id = publish_audio(&adapter, &first_session, &first_rtp).await;
    let sibling_media_id = publish_audio(&adapter, &first_session, &sibling_rtp).await;
    let unrelated_media_id = publish_audio(&adapter, &first_session, &unrelated_rtp).await;
    let second_media_id = publish_audio(&adapter, &second_session, &second_rtp).await;

    assert_ne!(first_media_id, sibling_media_id);
    assert!(first_media_id.as_u64() < 1_000_000_000);
    assert!(second_media_id.as_u64() >= 1_000_000_000);
    let first_worker = expect_worker_for_user(&adapter, &first_session);
    let second_worker = expect_worker_for_user(&adapter, &second_session);
    let now = Instant::now();
    for (worker, media_id) in [
        (first_worker, first_media_id),
        (first_worker, sibling_media_id),
        (second_worker, second_media_id),
    ] {
        worker.debug_record_incoming_media(media_id, 64, now).await;
    }
    first_worker
        .debug_observe_audio_activity(first_media_id, Some(true), None, now)
        .await;
    second_worker
        .debug_observe_audio_activity(second_media_id, Some(true), None, now)
        .await;
    first_worker
        .debug_observe_audio_activity(unrelated_media_id, Some(true), None, now)
        .await;
    let snapshot = adapter
        .source_diagnostics_snapshot(&[
            TransportSourceKey::new(first_session.clone(), first_media_id),
            TransportSourceKey::new(first_session, sibling_media_id),
            TransportSourceKey::new(second_session, second_media_id),
        ])
        .await;

    assert_eq!(test_api.source_diagnostics_request_count(), 2);
    let [first_activity, sibling_activity, second_activity] = snapshot.activity.as_slice() else {
        panic!("expected diagnostics for all requested sources");
    };
    assert_eq!(first_activity.transport_media_id(), first_media_id);
    assert_eq!(sibling_activity.transport_media_id(), sibling_media_id);
    assert_eq!(second_activity.transport_media_id(), second_media_id);
    let [first_speaker, second_speaker] = snapshot.active_speaker_diagnostics.as_slice() else {
        panic!("expected only requested active speakers");
    };
    assert_eq!(first_speaker.transport_media_id(), first_media_id);
    assert_eq!(second_speaker.transport_media_id(), second_media_id);
}

#[tokio::test]
async fn rtc_rejects_stale_session_removal_without_dropping_consumer_handle() {
    let adapter = test_media_transport(1, test_rtc_port_range());
    let source_session = test_session_key(35, 0, 1, UserId::Integer(1));
    let consumer_session = test_session_key(35, 0, 2, UserId::Integer(2));
    let producer_rtp_parameters = sample_rtp_parameters("aud-up", 54_000);
    let consumer_rtp_parameters = sample_rtp_parameters("aud-down", 55_000);

    prepare_rtc_sessions(&adapter, &[&source_session, &consumer_session]).await;

    let source_media_id = publish_audio(&adapter, &source_session, &producer_rtp_parameters).await;
    let consumer_media_id = consume_audio(
        &adapter,
        &consumer_session,
        &source_session,
        source_media_id,
        &consumer_rtp_parameters,
    )
    .await;

    assert_eq!(
        adapter
            .remove_media(&source_session, consumer_media_id)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );
    let Some(route_entry) = adapter
        .test_api()
        .route_entry_by_media_id(source_media_id)
        .await
    else {
        panic!("source route entry should survive stale removal");
    };
    assert!(route_entry.destinations.iter().any(|destination| {
        destination.dest_session == consumer_session
            && destination.dest_transport_media_id == consumer_media_id
    }));

    assert!(
        adapter
            .remove_media(&consumer_session, consumer_media_id)
            .await
            .is_ok()
    );
    assert!(
        adapter
            .remove_media(&consumer_session, consumer_media_id)
            .await
            .is_ok()
    );
    assert!(
        adapter
            .test_api()
            .route_entry_by_media_id(source_media_id)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn media_transport_terminal_teardown_falls_back_and_continues_batch() {
    let adapter = test_media_transport(1, test_rtc_port_range());
    let source_session = test_session_key(36, 0, 1, UserId::Integer(1));
    let wrong_owner = test_session_key(36, 0, 2, UserId::Integer(2));
    let later_session = test_session_key(36, 0, 3, UserId::Integer(3));
    let source_rtp_parameters = sample_rtp_parameters("first-aud-up", 56_000);
    let later_rtp_parameters = sample_rtp_parameters("later-aud-up", 57_000);

    prepare_rtc_sessions(&adapter, &[&source_session, &wrong_owner, &later_session]).await;
    let source_media_id = publish_audio(&adapter, &source_session, &source_rtp_parameters).await;
    let later_media_id = publish_audio(&adapter, &later_session, &later_rtp_parameters).await;
    let worker = expect_worker_for_user(&adapter, &source_session);

    adapter
        .teardown([
            TransportTeardown::RemoveMedia {
                session_key: wrong_owner.clone(),
                transport_media_id: source_media_id,
            },
            TransportTeardown::RemoveMedia {
                session_key: later_session,
                transport_media_id: later_media_id,
            },
        ])
        .await;

    assert!(worker.debug_resolve_mid(source_media_id).await.is_some());
    assert!(worker.debug_resolve_mid(later_media_id).await.is_none());
    assert!(
        adapter
            .create_initial_session_offer("test-room", &wrong_owner)
            .await
            .is_ok()
    );
    assert_eq!(transport_cleanup_failures(&adapter), 1);
    worker.cancel();
    worker.wait_for_shutdown().await;
    adapter
        .teardown([TransportTeardown::CloseSession {
            session_key: source_session,
        }])
        .await;
    assert_eq!(transport_cleanup_failures(&adapter), 2);
}

#[tokio::test]
async fn rtc_gates_remote_relay_mailboxes_without_touching_local_routes() -> TransportResult<()> {
    let adapter = test_media_transport(2, test_rtc_port_range());
    let source_session = test_session_key(40, 0, 1, UserId::Integer(1));
    let local_consumer_session = test_session_key(40, 0, 2, UserId::Integer(2));
    let remote_consumer_session = test_session_key(40, 1, 3, UserId::Integer(3));
    let producer_rtp_parameters = sample_rtp_parameters("aud-up", 61_000);
    let local_consumer_rtp_parameters = sample_rtp_parameters("aud-down-local", 62_000);
    let remote_consumer_rtp_parameters = sample_rtp_parameters("aud-down-remote", 63_000);

    let sessions = [
        &source_session,
        &local_consumer_session,
        &remote_consumer_session,
    ];
    prepare_rtc_sessions(&adapter, &sessions).await;

    let source_media_id = publish_audio(&adapter, &source_session, &producer_rtp_parameters).await;
    install_active_relay_route(
        &adapter,
        &source_session,
        source_media_id,
        &remote_consumer_session,
    )
    .await;
    let local_consumer_media_id = consume_audio(
        &adapter,
        &local_consumer_session,
        &source_session,
        source_media_id,
        &local_consumer_rtp_parameters,
    )
    .await;
    let remote_consumer_media_id = consume_audio(
        &adapter,
        &remote_consumer_session,
        &source_session,
        source_media_id,
        &remote_consumer_rtp_parameters,
    )
    .await;

    let source_worker = expect_worker_for_user(&adapter, &source_session);
    let remote_consumer_worker = expect_worker_for_user(&adapter, &remote_consumer_session);

    assert_relay_target_counts(source_worker, source_media_id, 1, 1).await;

    set_remote_relay_and_consumer_active(
        &adapter,
        &remote_consumer_session,
        remote_consumer_media_id,
        &source_session,
        source_media_id,
        false,
    )
    .await;

    assert_relay_target_counts(source_worker, source_media_id, 1, 0).await;
    assert_local_route_active(
        source_worker,
        &source_session,
        &local_consumer_session,
        local_consumer_media_id,
    )
    .await;
    assert_remote_route_activity(
        remote_consumer_worker,
        source_media_id,
        &remote_consumer_session,
        remote_consumer_media_id,
        false,
    )
    .await;

    set_remote_relay_and_consumer_active(
        &adapter,
        &remote_consumer_session,
        remote_consumer_media_id,
        &source_session,
        source_media_id,
        true,
    )
    .await;
    assert_relay_target_counts(source_worker, source_media_id, 1, 1).await;

    adapter
        .remove_media(&remote_consumer_session, remote_consumer_media_id)
        .await?;
    assert_relay_target_counts(source_worker, source_media_id, 1, 1).await;
    let release = |session_key| TransportTeardown::ReleaseRelayRoute {
        source: TransportSourceKey::new(session_key, source_media_id),
        target_media_worker_id: remote_consumer_session.media_worker_id(),
    };
    adapter
        .teardown([release(local_consumer_session.clone())])
        .await;
    assert_relay_target_counts(source_worker, source_media_id, 1, 1).await;
    let _offer = adapter
        .create_initial_session_offer("test-room", &local_consumer_session)
        .await?;
    adapter.teardown([release(source_session.clone())]).await;
    assert_relay_target_counts(source_worker, source_media_id, 0, 0).await;
    adapter
        .remove_media(&source_session, source_media_id)
        .await?;
    adapter.teardown([release(source_session)]).await;
    assert_relay_target_counts(source_worker, source_media_id, 0, 0).await;
    assert_eq!(transport_cleanup_failures(&adapter), 1);
    Ok(())
}
