use std::collections::BTreeSet;

use tokio::time::timeout;

use super::{
    super::{route_control::PacketLayerGate, test_support::RecordIncomingMediaProbe},
    fixtures::*,
};

fn assert_applied(outcome: WorkerMediaControlBatchOutcome) {
    let WorkerMediaControlBatchOutcome::Applied(results) = outcome else {
        panic!("worker media control should return applied results");
    };
    assert_eq!(results, [Ok(())]);
}

#[tokio::test]
async fn rtc_metrics_track_live_transport_users_without_double_counting() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 16, UserId::Integer(16));

    assert_eq!(adapter.metrics.snapshot().active_transport_users(), 0);
    let _offer = expect_initial_offer(&adapter, &session_key).await;
    assert_eq!(adapter.metrics.snapshot().active_transport_users(), 1);

    assert!(matches!(
        adapter
            .create_initial_session_offer("test-room", &session_key)
            .await,
        Err(TransportAdapterError::InvalidInput)
    ));
    assert_eq!(adapter.metrics.snapshot().active_transport_users(), 1);

    assert!(adapter.close_session(&session_key).await.is_ok());
    let snapshot = adapter.metrics.snapshot();
    assert_eq!(snapshot.active_transport_users(), 0);
    assert_eq!(snapshot.transport_user_lifetime_le_1_second(), 1);
    assert_eq!(snapshot.transport_user_lifetime_count(), 1);
}

#[tokio::test]
async fn rtc_publish_media_uses_signaled_mid_and_ssrc() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 18, UserId::Integer(18));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 42_424);
    let bootstrap_result = adapter
        .create_initial_session_offer("test-room", &session_key)
        .await;
    assert!(bootstrap_result.is_ok());

    let transport_media_id = adapter
        .add_recv_media(&session_key, Str0mMediaKind::Audio, &rtp_parameters)
        .await;
    assert!(transport_media_id.is_ok());
    let Some(transport_media_id) = transport_media_id.ok() else {
        return;
    };

    let expected_mid: Mid = "aud-up".into();
    assert_eq!(
        adapter.debug_resolve_mid(transport_media_id).await,
        Some(expected_mid)
    );
    assert_eq!(
        adapter
            .debug_session_stream_rx_ssrc(&session_key, expected_mid)
            .await,
        Some(42_424)
    );
}

#[tokio::test]
async fn rtc_session_bootstrap_applies_configured_outgoing_bitrate_cap() {
    let adapter = rtc_with_bitrate_limits(Bitrate::from_kbps(1_500), Bitrate::from_kbps(2_500));
    let session_key = transport_key(1, 181, UserId::Integer(181));

    let _offer = expect_initial_offer(&adapter, &session_key).await;
    assert_eq!(
        adapter.debug_session_max_bitrate_out(&session_key).await,
        Some(Bitrate::from_kbps(2_500))
    );
}

#[tokio::test]
async fn rtc_receiver_bwe_batch_preserves_target_semantics() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 183, UserId::Integer(183));
    expect_initial_offer(&adapter, &session_key).await;
    let update = ReceiverBweTargetUpdate::new(session_key.clone(), Bitrate::from_kbps(850));

    let _ = apply_receiver_bwe_batch(&adapter, [update.clone()]).await;
    assert_eq!(
        adapter
            .debug_session_receiver_bwe_target(&session_key)
            .await,
        Some(Bitrate::from_kbps(850))
    );
    assert_eq!(
        apply_receiver_bwe_batch(&adapter, [update]).await,
        vec![Ok(())]
    );
    assert_eq!(
        adapter
            .debug_session_receiver_bwe_str0m_update_count(&session_key)
            .await,
        Some(1)
    );
    let zero = ReceiverBweTargetUpdate::new(session_key.clone(), Bitrate::zero());
    let _ = apply_receiver_bwe_batch(&adapter, [zero]).await;
    assert_eq!(
        adapter
            .debug_session_receiver_bwe_target(&session_key)
            .await,
        Some(Bitrate::zero())
    );
    let missing = ReceiverBweTargetUpdate::new(
        transport_key(1, 186, UserId::Integer(186)),
        Bitrate::from_kbps(400),
    );
    assert!(matches!(
        apply_receiver_bwe_batch(&adapter, [missing])
            .await
            .as_slice(),
        [Err(TransportAdapterError::InvalidInput)]
    ));

    let capped = rtc_with_bitrate_limits(Bitrate::from_mbps(8), Bitrate::from_kbps(500));
    let capped_session = transport_key(1, 185, UserId::Integer(185));
    expect_initial_offer(&capped, &capped_session).await;
    let above_cap = ReceiverBweTargetUpdate::new(capped_session.clone(), Bitrate::from_kbps(900));
    let _ = apply_receiver_bwe_batch(&capped, [above_cap]).await;
    assert_eq!(
        capped
            .debug_session_receiver_bwe_target(&capped_session)
            .await,
        Some(Bitrate::from_kbps(500))
    );
}

#[tokio::test]
async fn rtc_recv_media_applies_configured_incoming_bitrate_cap() {
    let adapter =
        rtc_with_bitrate_limits(Bitrate::from_bps(1_234_567), Bitrate::from_bps(7_654_321));
    let session_key = transport_key(1, 182, UserId::Integer(182));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 52_525);

    let _offer = expect_initial_offer(&adapter, &session_key).await;
    assert!(
        adapter
            .add_recv_media(&session_key, Str0mMediaKind::Audio, &rtp_parameters)
            .await
            .is_ok()
    );
    assert_eq!(
        adapter.debug_session_max_bitrate_in(&session_key).await,
        Some(Bitrate::from_bps(1_234_567))
    );
}

#[tokio::test]
async fn rtc_consume_media_uses_negotiated_mid_and_destination_ssrc() {
    let adapter = RtcWorker::default();
    let producer_session_key = transport_key(1, 19, UserId::Integer(19));
    let consumer_key = transport_key(1, 20, UserId::Integer(20));
    let producer_rtp_parameters = sample_router_rtp_parameters("aud-up", 51_000);
    let consumer_rtp_parameters = sample_router_rtp_parameters("aud-down", 61_000);

    let _offer = expect_initial_offer(&adapter, &producer_session_key).await;
    let _offer = expect_initial_offer(&adapter, &consumer_key).await;

    let source_media_id = adapter
        .add_recv_media(
            &producer_session_key,
            Str0mMediaKind::Audio,
            &producer_rtp_parameters,
        )
        .await;
    assert!(source_media_id.is_ok());
    let Some(source_media_id) = source_media_id.ok() else {
        return;
    };

    let consumer_media_id = adapter
        .add_send_media(
            &consumer_key,
            Str0mMediaKind::Audio,
            TransportSourceKey::new(producer_session_key.clone(), source_media_id),
            &consumer_rtp_parameters,
            true,
        )
        .await;
    assert!(consumer_media_id.is_ok());
    let Some(consumer_media_id) = consumer_media_id.ok() else {
        return;
    };

    let expected_dest_mid: Mid = "aud-down".into();
    let route_entry = adapter.debug_route_entry_by_media_id(source_media_id).await;
    assert!(route_entry.is_some());
    let Some(route_entry) = route_entry else {
        return;
    };
    assert_eq!(route_entry.source_transport_media_id, source_media_id);
    assert!(route_entry.source_active);
    assert_eq!(route_entry.active_destination_count, 1);
    assert!(route_entry.destinations.iter().any(|dest| {
        dest.dest_session == consumer_key
            && dest.dest_transport_media_id == consumer_media_id
            && dest.dest_mid == expected_dest_mid
    }));
    assert!(
        adapter
            .debug_session_stream_tx_pair(&consumer_key, expected_dest_mid)
            .await
            .is_some_and(|(ssrc, _)| ssrc != 61_000)
    );
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Open);
}

#[tokio::test]
async fn rtc_consume_media_can_start_route_inactive() {
    let adapter = RtcWorker::default();
    let producer_session_key = transport_key(1, 221, UserId::Integer(221));
    let consumer_key = transport_key(1, 222, UserId::Integer(222));
    let producer_rtp_parameters = sample_router_rtp_parameters("aud-up", 51_100);
    let consumer_rtp_parameters = sample_router_rtp_parameters("aud-down", 61_100);

    let _offer = expect_initial_offer(&adapter, &producer_session_key).await;
    let _offer = expect_initial_offer(&adapter, &consumer_key).await;

    let source_media_id = adapter
        .add_recv_media(
            &producer_session_key,
            Str0mMediaKind::Audio,
            &producer_rtp_parameters,
        )
        .await
        .expect("producer media should be accepted");
    let consumer_media_id = adapter
        .add_send_media(
            &consumer_key,
            Str0mMediaKind::Audio,
            TransportSourceKey::new(producer_session_key.clone(), source_media_id),
            &consumer_rtp_parameters,
            false,
        )
        .await
        .expect("consumer media should be accepted");

    let route_entry = adapter
        .debug_route_entry_by_media_id(source_media_id)
        .await
        .expect("source route should exist");
    assert_eq!(route_entry.active_destination_count, 0);
    assert!(route_entry.destinations.iter().any(|dest| {
        dest.dest_session == consumer_key
            && dest.dest_transport_media_id == consumer_media_id
            && !dest.active
    }));
}

#[tokio::test]
async fn rtc_consumer_routes_keep_the_aggregate_blocked_before_decoder_refresh() {
    let adapter = RtcWorker::default();
    let producer_session_key = transport_key(1, 21, UserId::Integer(21));
    let first_consumer_key = transport_key(1, 22, UserId::Integer(22));
    let second_consumer_key = transport_key(1, 23, UserId::Integer(23));
    let producer_rtp_parameters = sample_vp8_rtp_parameters("vid-up", 71_000, None);
    let selected_consumer_rtp_parameters =
        sample_vp8_rtp_parameters("vid-down-1", 72_000, Some("hi"));
    let open_consumer_rtp_parameters = sample_vp8_rtp_parameters("vid-down-2", 73_000, None);

    for session_key in [
        &producer_session_key,
        &first_consumer_key,
        &second_consumer_key,
    ] {
        assert!(
            adapter
                .create_initial_session_offer("test-room", session_key)
                .await
                .is_ok()
        );
    }

    let source_media_id = adapter
        .add_recv_media(
            &producer_session_key,
            Str0mMediaKind::Video,
            &producer_rtp_parameters,
        )
        .await
        .expect("producer media should register");

    let _first_consumer_media_id = adapter
        .add_send_media(
            &first_consumer_key,
            Str0mMediaKind::Video,
            TransportSourceKey::new(producer_session_key.clone(), source_media_id),
            &selected_consumer_rtp_parameters,
            true,
        )
        .await
        .expect("selected-rid consumer should register");

    let route_entry = adapter
        .debug_route_entry_by_media_id(source_media_id)
        .await
        .expect("route entry should exist after first consumer registration");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Block);

    let _second_consumer_media_id = adapter
        .add_send_media(
            &second_consumer_key,
            Str0mMediaKind::Video,
            TransportSourceKey::new(producer_session_key.clone(), source_media_id),
            &open_consumer_rtp_parameters,
            true,
        )
        .await
        .expect("open consumer should register");

    let route_entry = adapter
        .debug_route_entry_by_media_id(source_media_id)
        .await
        .expect("route entry should still exist after mixed policy registration");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Block);
}

#[tokio::test]
async fn rtc_consumer_packet_gate_update_waits_for_live_rid_before_strict_aggregate_gate() {
    let adapter = RtcWorker::default();
    let producer_session_key = transport_key(1, 123, UserId::Integer(123));
    let consumer_key = transport_key(1, 124, UserId::Integer(124));
    let producer_rtp_parameters = sample_vp8_rtp_parameters("vid-up", 81_000, None);
    let consumer_rtp_parameters = sample_vp8_rtp_parameters("vid-down", 82_000, Some("hi"));

    for session_key in [&producer_session_key, &consumer_key] {
        assert!(
            adapter
                .create_initial_session_offer("test-room", session_key)
                .await
                .is_ok()
        );
    }

    let source_media_id = adapter
        .add_recv_media(
            &producer_session_key,
            Str0mMediaKind::Video,
            &producer_rtp_parameters,
        )
        .await
        .expect("producer media should register");

    let consumer_media_id = adapter
        .add_send_media(
            &consumer_key,
            Str0mMediaKind::Video,
            TransportSourceKey::new(producer_session_key.clone(), source_media_id),
            &consumer_rtp_parameters,
            true,
        )
        .await
        .expect("consumer media should register");

    let route_entry = adapter
        .debug_route_entry_by_media_id(source_media_id)
        .await
        .expect("route entry should exist after consumer registration");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Block);

    let route = transport_consumer_route(
        &consumer_key,
        consumer_media_id,
        &producer_session_key,
        source_media_id,
    );
    assert_applied(
        apply_worker_media_control(
            &adapter,
            WorkerMediaControlBatch::ConsumerGates {
                source: route.source().clone(),
                updates: vec![(0, route.clone(), PacketLayerGate::Rid("lo".into()))],
            },
        )
        .await,
    );

    let route_entry = adapter
        .debug_route_entry_by_media_id(source_media_id)
        .await
        .expect("route entry should still exist after consumer gate update");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Block);

    assert_applied(
        apply_worker_media_control(
            &adapter,
            WorkerMediaControlBatch::ConsumerGates {
                source: route.source().clone(),
                updates: vec![(0, route, PacketLayerGate::Open)],
            },
        )
        .await,
    );

    let route_entry = adapter
        .debug_route_entry_by_media_id(source_media_id)
        .await
        .expect("route entry should still exist after opening the consumer gate");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Block);
}

#[tokio::test]
async fn incoming_media_probe_does_not_register_missing_publications() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 21, UserId::Integer(21));
    let _offer = expect_initial_offer(&adapter, &session_key).await;
    let now = Instant::now();

    let recorded = adapter
        .test_handle()
        .debug_handle
        .probe(RecordIncomingMediaProbe {
            transport_media_id: TransportMediaId::new(99),
            payload_bytes: 120,
            now,
        })
        .await;

    assert_eq!(recorded, Some(false));
    assert!(
        adapter
            .test_handle()
            .bitrate_registry
            .lock()
            .expect("bitrate registry mutex should not be poisoned")
            .incoming_bitrates_by_session
            .is_empty()
    );
}

#[tokio::test]
async fn rtc_incoming_bitrate_snapshot_counts_recent_media_bytes() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 21, UserId::Integer(21));
    let rtp_parameters = sample_router_rtp_parameters("cam-up", 77_777);

    let _offer = expect_initial_offer(&adapter, &session_key).await;
    let transport_media_id = adapter
        .add_recv_media(&session_key, Str0mMediaKind::Video, &rtp_parameters)
        .await
        .expect("should declare recv media");

    let now = Instant::now();
    for (elapsed, bytes) in [
        (Duration::ZERO, 120),
        (Duration::from_millis(500), 120),
        (Duration::from_secs(1), 1),
    ] {
        adapter
            .debug_record_incoming_media(transport_media_id, bytes, now + elapsed)
            .await;
    }
    let worker_handle = adapter.test_handle();
    let snapshot = worker_handle
        .bitrate_registry
        .lock()
        .expect("bitrate registry mutex should not be poisoned")
        .transport_bitrate_snapshot_at(slice::from_ref(&session_key), now + Duration::from_secs(1));
    assert_eq!(snapshot.total, Bitrate::from_bps(1_920));
    assert_eq!(snapshot.per_media.len(), 1);
    assert_eq!(
        snapshot
            .per_media
            .first()
            .expect("should have media bitrate"),
        &(transport_media_id, Bitrate::from_bps(1_920))
    );
}

#[tokio::test]
async fn rtc_incoming_bitrate_snapshot_ignores_closed_sessions() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 23, UserId::Integer(23));
    let rtp_parameters = sample_router_rtp_parameters("cam-up", 99_999);

    let _offer = expect_initial_offer(&adapter, &session_key).await;
    let transport_media_id = adapter
        .add_recv_media(&session_key, Str0mMediaKind::Video, &rtp_parameters)
        .await
        .expect("should declare recv media");

    let now = Instant::now();
    for (elapsed, bytes) in [
        (Duration::ZERO, 120),
        (Duration::from_millis(500), 120),
        (Duration::from_secs(1), 1),
    ] {
        adapter
            .debug_record_incoming_media(transport_media_id, bytes, now + elapsed)
            .await;
    }
    let worker_handle = adapter.test_handle();
    let before_close = worker_handle
        .bitrate_registry
        .lock()
        .expect("bitrate registry mutex should not be poisoned")
        .transport_bitrate_snapshot_at(slice::from_ref(&session_key), now + Duration::from_secs(1));
    assert_eq!(before_close.total, Bitrate::from_bps(1_920));

    assert!(adapter.close_session(&session_key).await.is_ok());

    let snapshot = adapter.transport_bitrate_snapshot(slice::from_ref(&session_key));
    assert_eq!(snapshot.total, Bitrate::zero());
    assert!(snapshot.per_media.is_empty());
}

#[tokio::test]
async fn rtc_active_speaker_source_snapshot_orders_recent_audio_sources() {
    let adapter = RtcWorker::default();
    let first_session_key = transport_key(9, 31, UserId::Integer(31));
    let second_session_key = transport_key(9, 32, UserId::Integer(32));
    let first_rtp_parameters = sample_router_rtp_parameters("aud-up-1", 93_001);
    let second_rtp_parameters = sample_router_rtp_parameters("aud-up-2", 93_002);

    for session_key in [&first_session_key, &second_session_key] {
        assert!(
            adapter
                .create_initial_session_offer("test-room", session_key)
                .await
                .is_ok()
        );
    }

    let first_media_id = adapter
        .add_recv_media(
            &first_session_key,
            Str0mMediaKind::Audio,
            &first_rtp_parameters,
        )
        .await
        .expect("first audio media should register");
    let second_media_id = adapter
        .add_recv_media(
            &second_session_key,
            Str0mMediaKind::Audio,
            &second_rtp_parameters,
        )
        .await
        .expect("second audio media should register");

    let now = Instant::now();
    adapter
        .debug_observe_audio_activity(first_media_id, Some(true), None, now)
        .await;
    adapter
        .debug_observe_audio_activity(
            second_media_id,
            Some(true),
            None,
            now + Duration::from_millis(10),
        )
        .await;

    let snapshot = adapter.active_speaker_source_snapshot().await;
    assert_eq!(
        snapshot
            .into_iter()
            .map(ActiveSpeakerSource::transport_media_id)
            .collect::<Vec<_>>(),
        vec![second_media_id, first_media_id]
    );
}

#[tokio::test]
async fn active_speaker_expiry_wakes_policy_without_input() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(9, 41, UserId::Integer(41));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 94_001);

    let _offer = expect_initial_offer(&adapter, &session_key).await;

    let media_id = adapter
        .add_recv_media(&session_key, Str0mMediaKind::Audio, &rtp_parameters)
        .await
        .expect("audio media should register");
    let updates = adapter.source_policy_signal.subscribe();
    let room_instance_id = session_key.room_instance_id();
    adapter
        .debug_observe_audio_activity(media_id, Some(true), None, Instant::now())
        .await;

    let observed_rooms = timeout(Duration::from_secs(1), updates.wait_for_update())
        .await
        .expect("active-speaker observation should wake room policy");
    assert_eq!(observed_rooms, BTreeSet::from([room_instance_id]));
    let dirty_rooms = timeout(Duration::from_secs(1), updates.wait_for_update())
        .await
        .expect("active-speaker expiry should wake room policy");
    assert_eq!(dirty_rooms, BTreeSet::from([room_instance_id]));
    assert!(adapter.active_speaker_source_snapshot().await.is_empty());
    assert!(
        timeout(Duration::from_millis(50), updates.wait_for_update())
            .await
            .is_err()
    );
}
