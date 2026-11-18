use o_sfu_model::UserId;

use super::super::{
    state::{
        PacketLoopState,
        relay_registry::{RelayEnqueueOutcome, RelayPacketMailbox, RelayTargetId},
        route_control::PacketLayerGate,
    },
    test_support::{sample_forwarded_packet, test_transport_session_key},
};
use crate::engine::media_transport::TransportMediaId;

#[test]
fn worker_local_relay_targets_track_active_sources() {
    let mut state = PacketLoopState::default();
    let (mailbox, _rx) = RelayPacketMailbox::channel_for_test();
    let src_media = TransportMediaId::new(8);
    let relay_target = RelayTargetId::new(1);

    let _activity = state
        .routes
        .add_open_relay_target(src_media, relay_target, mailbox);
    assert!(state.routes.has_forwarding_sources());
    assert!(state.routes.relay_targets_for_source(src_media).is_some());

    state.routes.remove_relay_target(src_media, relay_target);
    assert!(state.routes.relay_targets_for_source(src_media).is_none());
    assert!(!state.routes.has_forwarding_sources());
}

#[test]
fn repeated_relay_target_adds_keep_one_registration() {
    let mut state = PacketLoopState::default();
    let (mailbox, _rx) = RelayPacketMailbox::channel_for_test();
    let src_media = TransportMediaId::new(12);
    let relay_target = RelayTargetId::new(1);

    let _first = state
        .routes
        .add_open_relay_target(src_media, relay_target, mailbox.clone());
    let _second = state
        .routes
        .add_open_relay_target(src_media, relay_target, mailbox);
    assert_eq!(state.routes.relay_target_count(src_media), 1);
    assert_eq!(state.routes.active_relay_target_count(src_media), 1);

    state.routes.remove_relay_target(src_media, relay_target);
    assert!(state.routes.relay_targets_for_source(src_media).is_none());
}

#[test]
fn worker_local_relay_targets_keep_sources_independent() {
    let mut state = PacketLoopState::default();
    let first_src_media = TransportMediaId::new(31);
    let second_src_media = TransportMediaId::new(32);
    let (first_mailbox, _first_rx) = RelayPacketMailbox::channel_for_test();
    let (second_mailbox, _second_rx) = RelayPacketMailbox::channel_for_test();

    let _first =
        state
            .routes
            .add_open_relay_target(first_src_media, RelayTargetId::new(1), first_mailbox);
    let _second =
        state
            .routes
            .add_open_relay_target(second_src_media, RelayTargetId::new(2), second_mailbox);

    assert_eq!(state.routes.relay_target_count(first_src_media), 1);
    assert_eq!(state.routes.relay_target_count(second_src_media), 1);
    state
        .routes
        .remove_relay_target(first_src_media, RelayTargetId::new(1));
    assert!(
        state
            .routes
            .relay_targets_for_source(first_src_media)
            .is_none()
    );
    assert!(
        state
            .routes
            .relay_targets_for_source(second_src_media)
            .is_some()
    );
}

#[test]
fn relay_targets_are_active_while_their_gate_admits_packets() {
    let mut state = PacketLoopState::default();
    let (first_mailbox, _first_rx) = RelayPacketMailbox::channel_for_test();
    let (second_mailbox, _second_rx) = RelayPacketMailbox::channel_for_test();
    let src_media = TransportMediaId::new(41);
    let first_target = RelayTargetId::new(1);
    let second_target = RelayTargetId::new(2);

    let _first = state
        .routes
        .add_open_relay_target(src_media, first_target, first_mailbox);
    let _second = state
        .routes
        .add_open_relay_target(src_media, second_target, second_mailbox);
    state
        .routes
        .set_relay_pkt_gate(src_media, first_target, PacketLayerGate::Block);
    assert_eq!(state.routes.active_relay_target_count(src_media), 1);

    state
        .routes
        .set_relay_pkt_gate(src_media, second_target, PacketLayerGate::Block);
    assert!(state.routes.relay_targets_for_source(src_media).is_none());
    assert_eq!(state.routes.relay_target_count(src_media), 2);

    state
        .routes
        .set_relay_pkt_gate(src_media, second_target, PacketLayerGate::Rid("hi".into()));
    assert_eq!(state.routes.active_relay_target_count(src_media), 1);
    // A gate for an unknown target neither registers nor activates it.
    state
        .routes
        .set_relay_pkt_gate(src_media, RelayTargetId::new(3), PacketLayerGate::Open);
    assert_eq!(state.routes.relay_target_count(src_media), 2);
}

#[test]
fn worker_local_relay_targets_report_overload_when_a_bounded_mailbox_is_full() {
    let (mailbox, _rx) = RelayPacketMailbox::channel_for_test_with_capacity(1);
    let src_media = TransportMediaId::new(42);
    let session_key = test_transport_session_key(36, 0, 37, UserId::Integer(38));
    let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");
    let state = PacketLoopState::default();

    assert_eq!(
        mailbox
            .forward_packet(&state, &packet, src_media)
            .map(|report| report.outcome),
        Some(RelayEnqueueOutcome::Enqueued)
    );
    assert_eq!(
        mailbox
            .forward_packet(&state, &packet, src_media)
            .map(|report| report.outcome),
        Some(RelayEnqueueOutcome::Overloaded)
    );
}
