use std::time::{Duration, Instant};

use str0m::media::{KeyframeRequestKind, Mid, Rid};

use super::super::{
    state::{
        keyframe_tracker::{
            KEYFRAME_REQUEST_RETRY_ATTEMPTS, KeyframeRequestDecision, KeyframeRequestOrigin,
            KeyframeRequestTracker,
        },
        relay_registry::{RelayPacketMailbox, RelayTargetId},
        route_control::PacketLayerGate,
        route_table::{RidReadinessScratch, RouteTable},
        slots::ConsumerStreamHandle,
        source_route::{DecoderDelivery, MediaRouteDestination},
    },
    test_support::test_transport_session_key,
};
use crate::engine::{
    UserId,
    media_transport::{
        ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
        TransportMediaId,
    },
};

fn assert_active_speaker_ids(state: &RouteTable, now: Instant, expected: &[TransportMediaId]) {
    let ids = state
        .active_speaker_sources(now)
        .into_iter()
        .map(ActiveSpeakerSource::transport_media_id)
        .collect::<Vec<_>>();
    assert_eq!(ids.as_slice(), expected);
}

fn assert_single_active_speaker(
    state: &RouteTable,
    now: Instant,
    src_media: TransportMediaId,
    last_audio_level_dbov: Option<i8>,
) {
    let snapshot = state.active_speaker_sources(now);
    assert_eq!(snapshot.len(), 1);
    let source = &snapshot[0];
    assert_eq!(source.transport_media_id(), src_media);
    assert_eq!(source.last_audio_level_dbov(), last_audio_level_dbov);
}

fn assert_single_active_speaker_diagnostic(
    state: &RouteTable,
    now: Instant,
    src_media: TransportMediaId,
    activity_state: ActiveSpeakerActivityState,
    reason: ActiveSpeakerActivityReason,
) {
    let diagnostics = state.active_speaker_diagnostics(&[src_media], now);
    assert_eq!(diagnostics.len(), 1);
    let diagnostic = &diagnostics[0];
    assert_eq!(diagnostic.state(), activity_state);
    assert_eq!(diagnostic.reason(), reason);
}

#[test]
fn video_route_resumes_only_from_its_selected_rid_keyframe() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(10);
    let consumer_media = TransportMediaId::new(11);
    let consumer_session = test_transport_session_key(10, 0, 11, UserId::Integer(12));
    let selected_rid = Rid::from("hi");
    let dst_idx = state.add_consumer_route(
        src_media,
        MediaRouteDestination {
            dest_session: consumer_session.clone(),
            dest_transport_media_id: consumer_media,
            dest_stream: ConsumerStreamHandle::default(),
            dest_mid: Mid::from("cam-down"),
            dest_payload_type: None,
            repair_enabled: false,
            active: true,
            delivery: DecoderDelivery::fixture(true, PacketLayerGate::Rid(selected_rid), None),
        },
    );

    let update = state
        .set_consumer_active(src_media, dst_idx, &consumer_session, consumer_media, false)
        .unwrap();
    assert!(update.route_changed);
    assert!(!update.repair_delivery_changed);
    let update = state
        .set_consumer_active(src_media, dst_idx, &consumer_session, consumer_media, true)
        .unwrap();
    assert!(update.route_changed);
    assert!(!update.repair_delivery_changed);
    let destination = &state.local_route(src_media).unwrap().destinations[dst_idx];
    assert_eq!(
        destination.delivery.effective_gate(),
        PacketLayerGate::Block
    );
    assert_eq!(
        destination.delivery.pending_gate(),
        Some(PacketLayerGate::Rid(selected_rid))
    );

    let mut scratch = RidReadinessScratch::default();
    scratch.ready.push(selected_rid);
    state.update_decoder_readiness(src_media, Some(selected_rid), false, &mut scratch, |_| {
        panic!("repair-disabled destination invalidated repair state")
    });
    assert_eq!(
        state.local_route(src_media).unwrap().destinations[dst_idx]
            .delivery
            .effective_gate(),
        PacketLayerGate::Block
    );

    state.update_decoder_readiness(src_media, Some(selected_rid), true, &mut scratch, |_| {
        panic!("repair-disabled destination invalidated repair state")
    });
    let destination = &state.local_route(src_media).unwrap().destinations[dst_idx];
    assert_eq!(
        destination.delivery.effective_gate(),
        PacketLayerGate::Rid(selected_rid)
    );
    assert_eq!(destination.delivery.pending_gate(), None);
    assert!(destination.delivery.generation() >= 3);

    let update = state
        .set_consumer_pkt_gate(
            src_media,
            dst_idx,
            &consumer_session,
            consumer_media,
            PacketLayerGate::Open,
        )
        .unwrap();
    assert!(update.route_changed);
    assert!(!update.repair_delivery_changed);
    state.update_decoder_readiness(src_media, None, true, &mut scratch, |_| {
        panic!("repair-disabled destination invalidated repair state");
    });
    let destination = &state.local_route(src_media).unwrap().destinations[dst_idx];
    assert_eq!(destination.delivery.effective_gate(), PacketLayerGate::Open);
    assert_eq!(destination.delivery.pending_gate(), None);
}

fn track_source_wide(
    state: &mut KeyframeRequestTracker,
    src_media: TransportMediaId,
    now: Instant,
) -> KeyframeRequestDecision {
    state.track(
        src_media,
        None,
        KeyframeRequestKind::Pli,
        KeyframeRequestOrigin::DecoderTransition,
        now,
    )
}

#[test]
fn keyframe_tracker_absorbs_repeated_requests_and_forgets_source_wakeup() {
    let mut state = KeyframeRequestTracker::default();
    let src_media = TransportMediaId::new(17);
    let now = Instant::now();

    assert_eq!(
        track_source_wide(&mut state, src_media, now),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(state.next_deadline(), Some(now + Duration::from_secs(1)));
    assert_eq!(
        track_source_wide(&mut state, src_media, now),
        KeyframeRequestDecision::Absorb
    );
    state.forget_source(src_media);
    assert_eq!(state.next_deadline(), None);
}

#[test]
fn keyframe_tracker_retries_until_a_refresh_without_duplicate_feedback() {
    let mut state = KeyframeRequestTracker::default();
    let src_media = TransportMediaId::new(18);
    let now = Instant::now();
    let mut retries = Vec::new();

    assert_eq!(
        track_source_wide(&mut state, src_media, now),
        KeyframeRequestDecision::Forward
    );
    state.drain_due(now + Duration::from_secs(1), &mut retries);

    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].src_media, src_media);
    assert_eq!(state.next_deadline(), Some(now + Duration::from_secs(2)));

    retries.clear();
    state.drain_due(now + Duration::from_secs(2), &mut retries);
    assert_eq!(retries.len(), 1);
    assert_eq!(state.observe_refresh(src_media, None), 1);
    assert_eq!(state.next_deadline(), None);
}

#[test]
fn opaque_recovery_stops_after_the_retry_budget() {
    let mut state = KeyframeRequestTracker::default();
    let src_media = TransportMediaId::new(180);
    let now = Instant::now();
    let mut retries = Vec::new();

    assert_eq!(
        state.track(
            src_media,
            None,
            KeyframeRequestKind::Pli,
            KeyframeRequestOrigin::RecoveryHint,
            now,
        ),
        KeyframeRequestDecision::Forward
    );
    for attempt in 1..=KEYFRAME_REQUEST_RETRY_ATTEMPTS {
        retries.clear();
        state.drain_due(now + Duration::from_secs(u64::from(attempt)), &mut retries);
        assert_eq!(retries.len(), 1);
    }

    assert_eq!(state.next_deadline(), None);
    retries.clear();
    state.drain_due(now + Duration::from_mins(1), &mut retries);
    assert!(retries.is_empty());
}

#[test]
fn keyframe_tracker_retries_after_duplicate_feedback() {
    let mut state = KeyframeRequestTracker::default();
    let src_media = TransportMediaId::new(181);
    let now = Instant::now();
    let mut retries = Vec::new();

    assert_eq!(
        track_source_wide(&mut state, src_media, now),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        state.track(
            src_media,
            None,
            KeyframeRequestKind::Fir,
            KeyframeRequestOrigin::ConsumerFeedback,
            now + Duration::from_millis(100)
        ),
        KeyframeRequestDecision::Absorb
    );

    state.drain_due(now + Duration::from_secs(1), &mut retries);

    assert_eq!(retries.len(), 1);
    let retry = retries[0];
    assert_eq!(retry.src_media, src_media);
    assert_eq!(retry.rid, None);
    assert_eq!(retry.kind, KeyframeRequestKind::Fir);
}

#[test]
fn keyframe_tracker_tracks_explicit_rids_independently() {
    let mut state = KeyframeRequestTracker::default();
    let src_media = TransportMediaId::new(118);
    let now = Instant::now();

    assert_eq!(
        track_source_wide(&mut state, src_media, now),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        state.track(
            src_media,
            Some(Rid::from("hi")),
            KeyframeRequestKind::Pli,
            KeyframeRequestOrigin::DecoderTransition,
            now
        ),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        state.track(
            src_media,
            Some(Rid::from("hi")),
            KeyframeRequestKind::Pli,
            KeyframeRequestOrigin::DecoderTransition,
            now
        ),
        KeyframeRequestDecision::Absorb
    );
    assert_eq!(
        state.track(
            src_media,
            Some(Rid::from("lo")),
            KeyframeRequestKind::Pli,
            KeyframeRequestOrigin::DecoderTransition,
            now
        ),
        KeyframeRequestDecision::Forward
    );
}

#[test]
fn keyframe_tracker_decoder_refresh_clears_matching_pending_requests() {
    let mut state = KeyframeRequestTracker::default();
    let src_media = TransportMediaId::new(182);
    let now = Instant::now();
    let source_wide_at = now + Duration::from_millis(100);
    let lo_at = now + Duration::from_millis(200);
    let lo_deadline = lo_at + Duration::from_secs(1);
    let hi = Rid::from("hi");
    let lo = Rid::from("lo");
    let mut retries = Vec::new();

    assert_eq!(
        state.track(
            src_media,
            Some(hi),
            KeyframeRequestKind::Pli,
            KeyframeRequestOrigin::DecoderTransition,
            now,
        ),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        state.track(
            src_media,
            Some(hi),
            KeyframeRequestKind::Pli,
            KeyframeRequestOrigin::DecoderTransition,
            now + Duration::from_millis(50)
        ),
        KeyframeRequestDecision::Absorb
    );
    assert_eq!(
        track_source_wide(&mut state, src_media, source_wide_at),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        state.track(
            src_media,
            Some(lo),
            KeyframeRequestKind::Pli,
            KeyframeRequestOrigin::DecoderTransition,
            lo_at,
        ),
        KeyframeRequestDecision::Forward
    );

    assert_eq!(state.observe_refresh(src_media, Some(hi)), 2);
    assert_eq!(state.next_deadline(), Some(lo_deadline));
    state.drain_due(lo_deadline, &mut retries);

    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].rid, Some(lo));
}

#[test]
fn keyframe_tracker_decoder_refresh_clears_source_wide_pending_request() {
    let mut state = KeyframeRequestTracker::default();
    let src_media = TransportMediaId::new(183);
    let now = Instant::now();
    let mut retries = Vec::new();

    assert_eq!(
        track_source_wide(&mut state, src_media, now),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        track_source_wide(&mut state, src_media, now + Duration::from_millis(100)),
        KeyframeRequestDecision::Absorb
    );
    assert_eq!(state.observe_refresh(src_media, Some(Rid::from("hi"))), 1);

    state.drain_due(now + Duration::from_secs(1), &mut retries);

    assert!(retries.is_empty());
}

#[test]
fn route_control_combines_local_and_remote_target_gates() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(21);

    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Rid("hi".into())));
    state.set_relay_pkt_gate(
        src_media,
        RelayTargetId::new(1),
        PacketLayerGate::Rid("hi".into()),
    );

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Rid("hi".into()))
    );

    state.set_relay_pkt_gate(
        src_media,
        RelayTargetId::new(2),
        PacketLayerGate::Rid("lo".into()),
    );

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Open)
    );
}

#[test]
fn route_control_refreshes_source_gate_after_relay_gate_removal() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(121);
    let consumer_media = TransportMediaId::new(122);
    let consumer_session = test_transport_session_key(121, 0, 122, UserId::Integer(123));
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let relay_target = RelayTargetId::new(1);
    let dst_idx = state.add_consumer_route(
        src_media,
        MediaRouteDestination {
            dest_session: consumer_session,
            dest_transport_media_id: consumer_media,
            dest_stream: ConsumerStreamHandle::default(),
            dest_mid: Mid::from("cam-down"),
            dest_payload_type: None,
            repair_enabled: false,
            active: true,
            delivery: DecoderDelivery::fixture(false, PacketLayerGate::Open, None),
        },
    );
    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Rid("hi".into())));
    state.add_relay_target(src_media, relay_target, relay_mailbox);
    state.set_relay_pkt_gate(src_media, relay_target, PacketLayerGate::Rid("lo".into()));

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Open)
    );

    state.remove_relay_target(src_media, relay_target);

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Rid("hi".into()))
    );
    assert_eq!(
        state.local_route(src_media).unwrap().destinations[dst_idx]
            .delivery
            .generation(),
        0
    );
}

#[test]
fn route_control_refreshes_source_gate_after_local_gate_clear() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(122);
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let relay_target = RelayTargetId::new(1);
    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Rid("hi".into())));
    state.add_relay_target(src_media, relay_target, relay_mailbox);
    state.set_relay_pkt_gate(src_media, relay_target, PacketLayerGate::Rid("hi".into()));

    state.set_local_pkt_gate(src_media, None);

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Rid("hi".into()))
    );

    state.remove_relay_target(src_media, relay_target);

    assert_eq!(state.effective_packet_gate(src_media), None);
}

#[test]
fn route_control_removing_last_consumer_route_preserves_producer_packet_state() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(184);
    let consumer_media = TransportMediaId::new(185);
    let consumer_session = test_transport_session_key(184, 0, 185, UserId::Integer(186));
    let rid = Rid::from("hi");
    let now = Instant::now();
    state.register_local_source(src_media);

    assert!(!state.has_forwarding_sources());
    assert!(state.observe_producer_packet(src_media, Some(rid), true, now));
    state.add_consumer_route(
        src_media,
        MediaRouteDestination {
            dest_session: consumer_session.clone(),
            dest_transport_media_id: consumer_media,
            dest_stream: ConsumerStreamHandle::default(),
            dest_mid: Mid::from("cam-down"),
            dest_payload_type: None,
            repair_enabled: false,
            active: true,
            delivery: DecoderDelivery::fixture(false, PacketLayerGate::Open, None),
        },
    );
    assert!(state.has_forwarding_sources());

    assert!(
        state
            .remove_consumer_route(src_media, &consumer_session, consumer_media)
            .is_some()
    );
    assert!(state.local_route(src_media).is_none());
    assert!(!state.has_forwarding_sources());
    assert!(state.producer_rid_is_ready(
        src_media,
        rid,
        now + Duration::from_millis(1),
        Duration::from_secs(1),
    ));
}

#[test]
fn route_control_transport_audio_policy_blocks_silent_sources() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(22);
    let now = Instant::now();

    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Open));
    state.observe_audio_activity(src_media, Some(false), None, now);

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Block)
    );
}

#[test]
fn route_control_vad_true_promotes_active_speaker_immediately() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(28);
    let now = Instant::now();

    assert!(state.observe_audio_activity(src_media, Some(true), Some(-90), now));

    assert_single_active_speaker(&state, now, src_media, Some(-90));
    assert_single_active_speaker_diagnostic(
        &state,
        now,
        src_media,
        ActiveSpeakerActivityState::Active,
        ActiveSpeakerActivityReason::Vad,
    );
}

#[test]
fn route_control_vad_true_refresh_extends_deadline_without_dirtying_room_policy() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(33);
    let now = Instant::now();

    assert!(state.observe_audio_activity(src_media, Some(true), None, now));
    let first_deadline = state.next_active_speaker_deadline(now).unwrap();
    let refresh_at = now + Duration::from_millis(20);

    assert!(!state.observe_audio_activity(src_media, Some(true), None, refresh_at));
    assert_eq!(
        state.next_active_speaker_deadline(refresh_at),
        Some(first_deadline + Duration::from_millis(20))
    );
    assert_active_speaker_ids(&state, refresh_at, &[src_media]);
}

#[test]
fn route_control_vad_false_inside_hold_window_does_not_dirty_room_policy() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(34);
    let now = Instant::now();

    assert!(state.observe_audio_activity(src_media, Some(true), None, now));

    assert!(!state.observe_audio_activity(
        src_media,
        Some(false),
        None,
        now + Duration::from_millis(100)
    ));
    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Open)
    );
    assert_active_speaker_ids(&state, now + Duration::from_millis(100), &[src_media]);
}

#[test]
fn route_control_newer_speaker_order_change_dirties_room_policy() {
    let mut state = RouteTable::default();
    let first_src_media = TransportMediaId::new(35);
    let second_src_media = TransportMediaId::new(36);
    let now = Instant::now();

    assert!(state.observe_audio_activity(first_src_media, Some(true), None, now));
    assert!(state.observe_audio_activity(
        second_src_media,
        Some(true),
        None,
        now + Duration::from_millis(10)
    ));
    assert_active_speaker_ids(
        &state,
        now + Duration::from_millis(10),
        &[second_src_media, first_src_media],
    );

    assert!(state.observe_audio_activity(
        first_src_media,
        Some(true),
        None,
        now + Duration::from_millis(20)
    ));
    assert_active_speaker_ids(
        &state,
        now + Duration::from_millis(20),
        &[first_src_media, second_src_media],
    );
}

#[test]
fn route_control_top_speaker_refresh_keeps_room_policy_clean() {
    let mut state = RouteTable::default();
    let first_src_media = TransportMediaId::new(135);
    let second_src_media = TransportMediaId::new(136);
    let now = Instant::now();

    assert!(state.observe_audio_activity(first_src_media, Some(true), None, now));
    assert!(state.observe_audio_activity(
        second_src_media,
        Some(true),
        None,
        now + Duration::from_millis(10)
    ));

    assert!(!state.observe_audio_activity(
        second_src_media,
        Some(true),
        None,
        now + Duration::from_millis(20)
    ));
    assert_active_speaker_ids(
        &state,
        now + Duration::from_millis(20),
        &[second_src_media, first_src_media],
    );
}

#[test]
fn route_control_expired_rank_dirties_refresh_after_all_sources_expire() {
    let mut state = RouteTable::default();
    let first_src_media = TransportMediaId::new(137);
    let second_src_media = TransportMediaId::new(138);
    let now = Instant::now();

    state.observe_audio_activity(first_src_media, Some(true), None, now);
    state.observe_audio_activity(second_src_media, Some(true), None, now);

    let refresh_at = now + Duration::from_millis(300);

    assert!(state.observe_audio_activity(first_src_media, Some(true), None, refresh_at));
    assert_active_speaker_ids(&state, refresh_at, &[first_src_media]);
}

#[test]
fn route_control_expiry_consumes_cached_rank_tail() {
    let mut state = RouteTable::default();
    let first_src_media = TransportMediaId::new(139);
    let second_src_media = TransportMediaId::new(140);
    let now = Instant::now();

    state.observe_audio_activity(first_src_media, Some(true), None, now);
    state.observe_audio_activity(
        second_src_media,
        Some(true),
        None,
        now + Duration::from_millis(100),
    );

    let refresh_at = now + Duration::from_millis(300);

    assert!(!state.observe_audio_activity(second_src_media, Some(true), None, refresh_at));
    assert_eq!(
        state.next_active_speaker_deadline(refresh_at),
        Some(refresh_at + Duration::from_millis(250))
    );
    assert_eq!(
        state.take_expired_speakers(refresh_at),
        vec![first_src_media]
    );
}

#[test]
fn route_control_same_timestamp_audio_level_rank_change_dirties_room_policy() {
    let mut state = RouteTable::default();
    let first_src_media = TransportMediaId::new(37);
    let second_src_media = TransportMediaId::new(38);
    let now = Instant::now();

    state.observe_audio_activity(first_src_media, Some(true), Some(-30), now);
    state.observe_audio_activity(second_src_media, Some(true), Some(-10), now);
    assert!(state.observe_audio_activity(first_src_media, Some(true), Some(-1), now));
    assert!(!state.observe_audio_activity(first_src_media, Some(true), Some(-1), now));
}

#[test]
fn route_control_vad_false_overrides_loud_audio_level() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(29);
    let now = Instant::now();

    state.observe_audio_activity(src_media, Some(false), Some(-12), now);

    assert!(state.active_speaker_sources(now).is_empty());
    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Block)
    );
    assert_single_active_speaker_diagnostic(
        &state,
        now,
        src_media,
        ActiveSpeakerActivityState::Blocked,
        ActiveSpeakerActivityReason::VadFalse,
    );
}

#[test]
fn route_control_transport_audio_policy_holds_recent_speech_open() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(23);
    let now = Instant::now();

    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Rid("hi".into())));
    state.observe_audio_activity(src_media, Some(true), None, now);
    state.observe_audio_activity(
        src_media,
        Some(false),
        None,
        now + Duration::from_millis(100),
    );

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Rid("hi".into()))
    );
}

#[test]
fn route_control_transport_audio_policy_reblocks_after_the_hold_window() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(24);
    let now = Instant::now();

    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Open));
    state.observe_audio_activity(src_media, Some(true), None, now);
    state.observe_audio_activity(
        src_media,
        Some(false),
        None,
        now + Duration::from_millis(300),
    );

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Block)
    );
}

#[test]
fn route_control_transport_audio_policy_uses_repeated_audio_level_fallback() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(25);
    let now = Instant::now();

    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Open));
    state.observe_audio_activity(src_media, None, Some(-24), now);

    assert!(state.active_speaker_sources(now).is_empty());
    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Open)
    );

    state.observe_audio_activity(src_media, None, Some(-24), now + Duration::from_millis(20));

    let observed_at = now + Duration::from_millis(20);
    assert_single_active_speaker(&state, observed_at, src_media, Some(-24));
    assert_single_active_speaker_diagnostic(
        &state,
        observed_at,
        src_media,
        ActiveSpeakerActivityState::Active,
        ActiveSpeakerActivityReason::AudioLevel,
    );
}

#[test]
fn route_control_transport_audio_policy_rejects_persistent_low_noise() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(26);
    let now = Instant::now();

    for offset in [0, 20, 40] {
        state.observe_audio_activity(
            src_media,
            None,
            Some(-80),
            now + Duration::from_millis(offset),
        );
    }

    assert!(
        state
            .active_speaker_sources(now + Duration::from_millis(40))
            .is_empty()
    );
    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Block)
    );
    assert_single_active_speaker_diagnostic(
        &state,
        now + Duration::from_millis(40),
        src_media,
        ActiveSpeakerActivityState::Blocked,
        ActiveSpeakerActivityReason::LowNoise,
    );
}

#[test]
fn route_control_active_speaker_expiry_is_observable() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(30);
    let now = Instant::now();

    state.observe_audio_activity(src_media, Some(true), None, now);

    let expired_at = now + Duration::from_millis(300);
    assert!(state.active_speaker_sources(expired_at).is_empty());
    assert_single_active_speaker_diagnostic(
        &state,
        expired_at,
        src_media,
        ActiveSpeakerActivityState::RecentlyExpired,
        ActiveSpeakerActivityReason::Expired,
    );
}

#[test]
fn route_control_active_speaker_order_is_deterministic_for_equal_observations() {
    let mut state = RouteTable::default();
    let first_src_media = TransportMediaId::new(31);
    let second_src_media = TransportMediaId::new(32);
    let now = Instant::now();

    state.observe_audio_activity(second_src_media, Some(true), None, now);
    state.observe_audio_activity(first_src_media, Some(true), None, now);

    assert_active_speaker_ids(&state, now, &[first_src_media, second_src_media]);
}

#[test]
fn route_control_local_packet_gate_composes_with_transport_audio_policy() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(27);
    let now = Instant::now();

    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Rid("hi".into())));
    state.observe_audio_activity(src_media, Some(true), None, now);

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Rid("hi".into()))
    );

    state.observe_audio_activity(
        src_media,
        Some(false),
        None,
        now + Duration::from_millis(300),
    );

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Block)
    );
}
