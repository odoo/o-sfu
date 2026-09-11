use super::super::{
    state::{
        PacketLoopState,
        relay_registry::{RelayEnqueueOutcome, RelayPacketMailbox, RelayTargetId},
    },
    test_support::{sample_forwarded_packet, test_transport_session_key},
};
use crate::engine::{UserId, media_transport::TransportMediaId};

#[test]
fn worker_local_relay_targets_track_active_sources() {
    let mut state = PacketLoopState::default();
    let (mailbox, _rx) = RelayPacketMailbox::channel_for_test();
    let src_media = TransportMediaId::new(8);
    let relay_target = RelayTargetId::new(1);

    state
        .routes
        .add_relay_target(src_media, relay_target, mailbox);
    assert!(state.routes.has_forwarding_sources());
    state
        .routes
        .set_relay_target_active(src_media, relay_target, true);
    assert!(state.routes.relay_targets_for_source(src_media).is_some());

    state.routes.remove_relay_target(src_media, relay_target);
    assert!(state.routes.relay_targets_for_source(src_media).is_none());
    assert!(!state.routes.has_forwarding_sources());
}

#[test]
fn worker_local_relay_targets_do_not_reference_count_room_owners() {
    let mut state = PacketLoopState::default();
    let (mailbox, _rx) = RelayPacketMailbox::channel_for_test();
    let src_media = TransportMediaId::new(12);
    let relay_target = RelayTargetId::new(1);

    state
        .routes
        .add_relay_target(src_media, relay_target, mailbox.clone());
    state
        .routes
        .add_relay_target(src_media, relay_target, mailbox);
    state
        .routes
        .set_relay_target_active(src_media, relay_target, true);
    state
        .routes
        .set_relay_target_active(src_media, relay_target, true);
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

    state
        .routes
        .add_relay_target(first_src_media, RelayTargetId::new(1), first_mailbox);
    state
        .routes
        .set_relay_target_active(first_src_media, RelayTargetId::new(1), true);
    state
        .routes
        .add_relay_target(second_src_media, RelayTargetId::new(2), second_mailbox);
    state
        .routes
        .set_relay_target_active(second_src_media, RelayTargetId::new(2), true);

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
fn worker_local_relay_targets_only_forward_to_targets_with_active_routes() {
    let mut state = PacketLoopState::default();
    let (first_mailbox, _first_rx) = RelayPacketMailbox::channel_for_test();
    let (second_mailbox, _second_rx) = RelayPacketMailbox::channel_for_test();
    let src_media = TransportMediaId::new(41);
    let first_target = RelayTargetId::new(1);
    let second_target = RelayTargetId::new(2);

    state
        .routes
        .add_relay_target(src_media, first_target, first_mailbox);
    state
        .routes
        .add_relay_target(src_media, second_target, second_mailbox);
    assert!(state.routes.relay_targets_for_source(src_media).is_none());

    state
        .routes
        .set_relay_target_active(src_media, second_target, true);
    let relay_targets = state.routes.relay_targets_for_source(src_media);
    assert!(relay_targets.is_some());
    let Some(relay_targets) = relay_targets else {
        return;
    };
    assert_eq!(relay_targets.len(), 1);
    assert_eq!(state.routes.active_relay_target_count(src_media), 1);

    state
        .routes
        .set_relay_target_active(src_media, second_target, false);
    assert!(state.routes.relay_targets_for_source(src_media).is_none());
    assert_eq!(state.routes.active_relay_target_count(src_media), 0);
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
