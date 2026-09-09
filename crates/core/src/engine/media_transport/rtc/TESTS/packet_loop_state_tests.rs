use str0m::media::Rid;

use super::{
    super::{
        bootstrap::ensure_session_rtc_state,
        state::{
            PacketLoopState,
            bitrate::{BitrateRegistry, IncomingBitrateObservation},
        },
        test_support::collect_ready_session_keys,
    },
    fixtures::*,
};

fn insert_live_session(state: &mut PacketLoopState, session_key: &TransportSessionKey) {
    assert!(matches!(
        ensure_session_rtc_state(
            &mut state.users,
            session_key,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40_000),
            Bitrate::from_mbps(10),
        ),
        Ok(true)
    ));
}

fn replace_live_session(state: &mut PacketLoopState, session_key: &TransportSessionKey) {
    let mut replacement_state = PacketLoopState::default();
    insert_live_session(&mut replacement_state, session_key);
    let replacement_session = replacement_state
        .users
        .remove(session_key)
        .expect("replacement session should exist");

    assert!(
        state
            .users
            .insert(session_key.clone(), replacement_session)
            .is_some()
    );
}

#[test]
fn packet_loop_state_reassigns_remote_addr_between_sessions() {
    let mut packet_loop_state = PacketLoopState::default();
    let source_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 45_001);
    let first_session_key = transport_key_on_worker(1, 0, 30, UserId::Integer(30));
    let second_session_key = transport_key_on_worker(2, 1, 30, UserId::Integer(30));

    let _ = packet_loop_state
        .remote_addr_demux
        .remember_remote_addr(source_addr, &first_session_key);
    assert_eq!(
        packet_loop_state
            .remote_addr_demux
            .session_key_for_remote_addr(source_addr),
        Some(&first_session_key)
    );

    let _ = packet_loop_state
        .remote_addr_demux
        .remember_remote_addr(source_addr, &second_session_key);

    assert_eq!(
        packet_loop_state
            .remote_addr_demux
            .session_key_for_remote_addr(source_addr),
        Some(&second_session_key)
    );
    assert!(
        packet_loop_state
            .remote_addr_demux
            .session_addrs_for(&first_session_key)
            .is_none()
    );
    assert_eq!(
        packet_loop_state
            .remote_addr_demux
            .session_addrs_for(&second_session_key),
        Some([source_addr].as_slice())
    );
}

#[test]
fn packet_loop_state_tracks_dirty_and_timed_out_sessions_separately() {
    let mut state = PacketLoopState::default();
    let first_session_key = transport_key_on_worker(1, 0, 31, UserId::Integer(31));
    let second_session_key = transport_key_on_worker(1, 0, 32, UserId::Integer(32));
    let now = Instant::now();
    let first_timeout = now + Duration::from_millis(20);
    let second_timeout = now + Duration::from_millis(40);

    insert_live_session(&mut state, &first_session_key);
    insert_live_session(&mut state, &second_session_key);
    state.update_session_timeout(&first_session_key, Some(first_timeout));
    state.update_session_timeout(&second_session_key, Some(second_timeout));
    state.mark_session_dirty(&second_session_key);

    assert_eq!(state.next_timeout_deadline(), Some(first_timeout));

    let ready_sessions = collect_ready_session_keys(&mut state, now + Duration::from_millis(25));
    assert!(ready_sessions.contains(&first_session_key));
    assert!(ready_sessions.contains(&second_session_key));
    assert_eq!(ready_sessions.len(), 2);
    assert_eq!(state.next_timeout_deadline(), Some(second_timeout));
}

#[test]
fn packet_loop_state_prefers_latest_session_timeout_deadline() {
    let mut state = PacketLoopState::default();
    let session_key = transport_key_on_worker(1, 0, 33, UserId::Integer(33));
    let now = Instant::now();
    let first_timeout = now + Duration::from_millis(50);
    let updated_timeout = now + Duration::from_millis(10);

    insert_live_session(&mut state, &session_key);
    state.update_session_timeout(&session_key, Some(first_timeout));
    state.update_session_timeout(&session_key, Some(updated_timeout));
    for _ in 0..512 {
        state.mark_session_dirty(&session_key);
        assert_eq!(
            collect_ready_session_keys(&mut state, now),
            vec![session_key.clone()]
        );
        state.update_session_timeout(&session_key, Some(updated_timeout));
    }

    assert_eq!(state.timeout_queue.len(), 2);
    assert_eq!(state.next_timeout_deadline(), Some(updated_timeout));

    let ready_sessions = collect_ready_session_keys(&mut state, now + Duration::from_millis(15));
    assert_eq!(ready_sessions.len(), 1);
    assert!(ready_sessions.contains(&session_key));
    assert_eq!(state.next_timeout_deadline(), None);

    state.update_session_timeout(&session_key, Some(updated_timeout));
    assert_eq!(
        collect_ready_session_keys(&mut state, now + Duration::from_millis(15)),
        vec![session_key]
    );
    assert_eq!(state.next_timeout_deadline(), None);
}

#[test]
fn packet_loop_state_deduplicates_repeated_dirty_session_marks_on_drain() {
    let mut state = PacketLoopState::default();
    let session_key = transport_key_on_worker(1, 0, 34, UserId::Integer(34));
    let now = Instant::now();

    insert_live_session(&mut state, &session_key);
    state.mark_session_dirty(&session_key);
    state.mark_session_dirty(&session_key);
    state.update_session_timeout(&session_key, Some(now));

    let ready_sessions = collect_ready_session_keys(&mut state, now);

    assert_eq!(ready_sessions, vec![session_key]);
    assert!(!state.has_dirty_sessions());
}

#[test]
fn packet_loop_state_clears_dirty_and_timeout_schedule_for_removed_session() {
    let mut state = PacketLoopState::default();
    let removed_session_key = transport_key_on_worker(1, 0, 35, UserId::Integer(35));
    let retained_session_key = transport_key_on_worker(1, 0, 36, UserId::Integer(36));
    let now = Instant::now();

    insert_live_session(&mut state, &removed_session_key);
    insert_live_session(&mut state, &retained_session_key);
    state.mark_session_dirty(&removed_session_key);
    state.mark_session_dirty(&retained_session_key);
    state.mark_session_dirty(&removed_session_key);
    state.update_session_timeout(&removed_session_key, Some(now));
    state.clear_session_schedule(&removed_session_key);

    let ready_sessions = collect_ready_session_keys(&mut state, now);

    assert_eq!(ready_sessions, vec![retained_session_key]);
    assert_eq!(state.next_timeout_deadline(), None);
}

#[test]
fn closing_session_repairs_surviving_consumer_feedback_indexes() {
    use std::sync::Mutex;

    use super::super::{
        control::{SessionCloseDisposition, worker_close_session},
        state::{
            RtcSnapshotState, media_registry::ConsumerKeyframeTarget,
            route_control::PacketLayerGate,
        },
        test_support::MediaWorkerScenario,
    };
    use crate::engine::metrics::RuntimeMetrics;

    let mut state = PacketLoopState::default();
    let source = TransportMediaId::new(80);
    let removed = transport_key(1, 39, UserId::Integer(39));
    let first = transport_key(1, 40, UserId::Integer(40));
    let second = transport_key(1, 41, UserId::Integer(41));
    let mid = Mid::from("cam-down");
    insert_live_session(&mut state, &removed);
    let mut scenario = MediaWorkerScenario::new(&mut state);
    scenario.destination(source, removed.clone(), mid);
    for (session, rid) in [(&first, "lo"), (&second, "hi")] {
        scenario.destination_with_gate(
            source,
            session.clone(),
            mid,
            PacketLayerGate::Rid(rid.into()),
        );
    }

    worker_close_session(
        &mut state,
        &Arc::new(Mutex::new(BitrateRegistry::default())),
        &Arc::new(Mutex::new(RtcSnapshotState::default())),
        &removed,
        SessionCloseDisposition::OwnerClose,
        &RuntimeMetrics::default(),
    );

    assert_eq!(state.active_consumer_kf_target(&removed, mid, None), None);
    for (session, rid) in [(&first, "lo"), (&second, "hi")] {
        assert_eq!(
            state.active_consumer_kf_target(session, mid, None),
            Some(ConsumerKeyframeTarget {
                src_media: source,
                rid: Some(rid.into()),
            }),
        );
    }
    assert_eq!(
        state
            .routes
            .local_route(source)
            .expect("surviving consumers keep the source route")
            .destinations
            .iter()
            .map(|destination| &destination.dest_session)
            .collect::<Vec<_>>(),
        vec![&first, &second],
    );
}

#[test]
fn packet_loop_state_ignores_stale_dirty_handle_after_session_replacement() {
    let mut state = PacketLoopState::default();
    let session_key = transport_key_on_worker(1, 0, 37, UserId::Integer(37));
    let now = Instant::now();

    insert_live_session(&mut state, &session_key);
    state.mark_session_dirty(&session_key);
    replace_live_session(&mut state, &session_key);

    let ready_sessions = collect_ready_session_keys(&mut state, now);

    assert!(ready_sessions.is_empty());
}

#[test]
fn packet_loop_state_ignores_stale_timeout_handle_after_session_replacement() {
    let mut state = PacketLoopState::default();
    let session_key = transport_key_on_worker(1, 0, 38, UserId::Integer(38));
    let now = Instant::now();

    insert_live_session(&mut state, &session_key);
    state.update_session_timeout(&session_key, Some(now + Duration::from_millis(10)));
    replace_live_session(&mut state, &session_key);

    let ready_sessions = collect_ready_session_keys(&mut state, now + Duration::from_millis(11));

    assert!(ready_sessions.is_empty());
    assert_eq!(state.next_timeout_deadline(), None);
}

#[test]
fn packet_loop_state_snapshots_source_and_rid_packet_activity() {
    let mut state = PacketLoopState::default();
    let transport_media_id = TransportMediaId::new(77);
    let rid = Rid::from("hi");
    let now = Instant::now();

    state
        .routes
        .observe_producer_packet(transport_media_id, Some(rid), false, now);
    state.routes.observe_producer_packet(
        transport_media_id,
        Some(rid),
        true,
        now + Duration::from_millis(40),
    );
    let activity = state.routes.source_activity_snapshot(
        &[transport_media_id],
        now + Duration::from_millis(100),
        &state.incoming_bitrate_counters,
    );

    let source = activity.first().expect("source activity should be present");
    assert_eq!(source.last_packet_age(), Duration::from_millis(60));
    assert_eq!(source.last_keyframe_age(), Some(Duration::from_millis(60)));
    let rid_activity = source
        .rids()
        .first()
        .expect("rid activity should be present");
    assert_eq!(rid_activity.rid(), "hi");
    assert_eq!(rid_activity.last_packet_age(), Duration::from_millis(60));
    assert_eq!(
        rid_activity.last_keyframe_age(),
        Some(Duration::from_millis(60))
    );
}

#[test]
fn packet_loop_state_snapshots_ridless_packet_activity_from_ingress_counter() {
    let mut bitrate_registry = BitrateRegistry::default();
    let mut state = PacketLoopState::default();
    let session_key = transport_key(1, 2, UserId::Integer(3));
    let transport_media_id = TransportMediaId::new(77);
    let now = Instant::now();
    let counter = bitrate_registry.register_incoming_media(&session_key, transport_media_id, now);
    state.register_incoming_bitrate_counter(transport_media_id, counter);

    state
        .routes
        .observe_producer_packet(transport_media_id, None, true, now);
    assert_eq!(
        state.record_incoming_bitrate(transport_media_id, now, 32),
        Some(IncomingBitrateObservation::IngressStarted)
    );
    assert_eq!(
        state.record_incoming_bitrate(transport_media_id, now + Duration::from_millis(40), 32,),
        Some(IncomingBitrateObservation::default())
    );
    let activity = state.routes.source_activity_snapshot(
        &[transport_media_id],
        now + Duration::from_millis(100),
        &state.incoming_bitrate_counters,
    );

    let source = activity.first().expect("source activity should be present");
    assert_eq!(source.last_packet_age(), Duration::from_millis(60));
    assert_eq!(source.last_keyframe_age(), Some(Duration::from_millis(100)));
    assert!(source.rids().is_empty());
}
