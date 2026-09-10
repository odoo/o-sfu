#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "state-level test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

use std::{slice::from_ref, sync::Arc};

use o_sfu_router::{
    RouterId,
    rtp::MediaStream,
    test_support::rtp_samples::{sample_client_rtp_capabilities, sample_video_rtp_parameters},
};

use super::*;
use crate::{
    RoomMediaLimits, VideoAdaptationTuning,
    engine::{
        ConnectionId, MediaWorkerId, RoomInstanceId, TestSourceKind,
        media_transport::{TransportConsumerRoute, TransportMediaId, TransportTeardown},
        metrics::RuntimeMetrics,
        room::{
            RoomAdmissionPolicy, RoomRuntimeContext, RouterPlacement, UserOutboundSender,
            media_graph::{ConsumerSetupOrigin, ConsumerSetupOutcome, ValidatedPublish},
        },
        source_model::{PublishedSourceId, test_support::source_publish_intent_for_source},
    },
};

fn test_state() -> RoomState {
    let runtime_context = RoomRuntimeContext::new(
        RoomInstanceId::from_raw(0),
        RouterPlacement {
            router: RouterId(1),
            media_worker: MediaWorkerId::from_raw(0),
        },
        Vec::new(),
    );
    RoomState::new(
        &runtime_context,
        RoomAdmissionPolicy::new(4),
        RoomMediaLimits::default(),
        VideoAdaptationTuning::default(),
        sample_client_rtp_capabilities(),
    )
}

fn test_sender() -> UserOutboundSender {
    UserOutboundSender::channel(128, Arc::new(RuntimeMetrics::default())).0
}

fn join_test_user(state: &mut RoomState, user_id: &UserId) -> ConnectionId {
    state
        .apply_join(user_id, test_sender())
        .expect("test user should join")
        .receipt
        .transport_session_key
        .connection_id()
}

fn join_test_user_on_placement(
    state: &mut RoomState,
    user_id: &UserId,
    placement: RouterPlacement,
) -> ConnectionId {
    state
        .apply_join_on_placement(
            user_id,
            test_sender(),
            UserJoinedFanout::Suppress,
            placement,
        )
        .expect("test user should join on placement")
        .receipt
        .transport_session_key
        .connection_id()
}

fn commit_test_publication(
    state: &mut RoomState,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: TestSourceKind,
    transport_media_id: TransportMediaId,
    consumable_rtp_parameters: MediaStream,
) -> PublishedSourceId {
    let intent = source_publish_intent_for_source(stream_type);
    state
        .topology
        .commit_publication(
            ValidatedPublish {
                session_key: state.transport_user_key(user_id, connection_id),
                intent,
            },
            consumable_rtp_parameters,
            &[],
            transport_media_id,
        )
        .expect("test publication should commit")
}

#[derive(Debug)]
struct RelayedSource {
    route: TransportConsumerRoute,
    target_media_worker_id: MediaWorkerId,
}

fn install_relayed_source(state: &mut RoomState) -> RelayedSource {
    let publisher = UserId::Integer(1);
    let subscriber = UserId::Integer(2);
    let publisher_connection = join_test_user(state, &publisher);
    let target_worker = MediaWorkerId::from_raw(1);
    let subscriber_connection = join_test_user_on_placement(
        state,
        &subscriber,
        RouterPlacement {
            router: RouterId(2),
            media_worker: target_worker,
        },
    );
    assert!(
        state
            .set_user_negotiated(
                &subscriber,
                subscriber_connection,
                sample_client_rtp_capabilities()
            )
            .is_some()
    );
    let source_media = TransportMediaId::new(11);
    let consumer_media = TransportMediaId::new(21);
    commit_test_publication(
        state,
        &publisher,
        publisher_connection,
        TestSourceKind::ScalableVideo,
        source_media,
        sample_video_rtp_parameters(None, 77_777),
    );
    let mut setups = state
        .plan_missing_consumers(&subscriber, subscriber_connection)
        .expect("subscriber should still be current");
    let setup = setups
        .pop()
        .expect("relay consumer setup should be planned");
    assert!(setups.is_empty());
    let setup = setup.declared(consumer_media, None);
    let setup_outcome = state.commit_declared_consumer_setup(setup, ConsumerSetupOrigin::Subscribe);
    let ConsumerSetupOutcome::Committed { route, .. } = setup_outcome else {
        panic!("relay consumer setup should commit");
    };
    RelayedSource {
        route,
        target_media_worker_id: target_worker,
    }
}

fn source_relay_teardown_count(teardown: &[TransportTeardown], relay: &RelayedSource) -> usize {
    teardown
        .iter()
        .filter(|operation| {
            matches!(
                operation,
                TransportTeardown::ReleaseRelayRoute {
                    source,
                    target_media_worker_id,
                } if source == relay.route.source()
                    && *target_media_worker_id == relay.target_media_worker_id
            )
        })
        .count()
}

#[test]
fn disconnect_sessions_removes_current_members_and_fanouts_departures() {
    let mut state = test_state();
    let user_a = UserId::Integer(1);
    let user_b = UserId::Integer(2);
    let connection_a = join_test_user(&mut state, &user_a);
    let connection_b = join_test_user(&mut state, &user_b);

    let outcome = state.apply_disconnect_users(&[user_a.clone(), user_b.clone()]);

    assert_eq!(state.users.len(), 0);
    assert_eq!(outcome.session_teardowns.len(), 2);
    assert!(outcome.session_teardowns.iter().any(|operation| {
        let session = operation.session_key();
        session.user_id() == &user_a && session.connection_id() == connection_a
    }));
    assert!(outcome.session_teardowns.iter().any(|operation| {
        let session = operation.session_key();
        session.user_id() == &user_b && session.connection_id() == connection_b
    }));
    assert_eq!(outcome.effects.close_requests.len(), 2);
    assert_eq!(outcome.effects.fanouts.len(), 2);
}

#[test]
fn stale_close_unregisters_committed_placement_and_returns_teardown() {
    let mut state = test_state();
    let user_id = UserId::Integer(1);
    let connection_id = join_test_user_on_placement(
        &mut state,
        &user_id,
        RouterPlacement {
            router: RouterId(2),
            media_worker: MediaWorkerId::from_raw(1),
        },
    );
    let session_key = state.transport_user_key(&user_id, connection_id);
    state
        .users
        .remove(&user_id)
        .expect("test should leave a stale committed placement");

    let commit = state
        .close_connection(&user_id, connection_id)
        .expect("stale committed placement should close");

    assert!(matches!(
        commit,
        ConnectionCloseCommit::StalePlacement {
            session_teardown: TransportTeardown::CloseSession {
                session_key: teardown_key,
            },
            ..
        } if teardown_key == session_key
    ));
    assert_eq!(
        state.committed_transport_user_key(&user_id, connection_id),
        None
    );
}

#[test]
fn leave_removes_consumer_routes_for_departed_session() {
    let mut state = test_state();
    let producer_connection_id = join_test_user(&mut state, &UserId::Integer(1));
    let consumer_connection_id = join_test_user(&mut state, &UserId::Integer(2));
    assert!(
        state
            .set_user_negotiated(
                &UserId::Integer(2),
                consumer_connection_id,
                sample_client_rtp_capabilities()
            )
            .is_some()
    );
    let source_id = commit_test_publication(
        &mut state,
        &UserId::Integer(1),
        producer_connection_id,
        TestSourceKind::ScalableVideo,
        TransportMediaId::new(11),
        sample_video_rtp_parameters(None, 77_777),
    );
    let mut setups = state
        .plan_missing_consumers(&UserId::Integer(2), consumer_connection_id)
        .expect("consumer should still be current");
    assert_eq!(setups.len(), 1);
    let setup = setups.pop().expect("consumer setup should be planned");
    let setup = setup.declared(TransportMediaId::new(21), Some(String::from("camera-down")));
    let setup_outcome = state.commit_declared_consumer_setup(setup, ConsumerSetupOrigin::Subscribe);
    assert!(matches!(
        setup_outcome,
        ConsumerSetupOutcome::Committed { .. }
    ));

    let outcome = state.close_connection(&UserId::Integer(2), consumer_connection_id);

    assert!(outcome.is_some());
    assert_eq!(state.consumer_count(), 0);
    assert_eq!(state.producer_count(), 1);
    assert!(state.topology.source_descriptor(source_id).is_some());
}

#[test]
fn stale_connection_cannot_broadcast() {
    let mut state = test_state();
    join_test_user(&mut state, &UserId::Integer(1));

    let fanout = state.broadcast_fanout(
        &UserId::Integer(1),
        ConnectionId::from_raw(999),
        serde_json::Value::String(String::from("hello")),
    );

    assert!(matches!(fanout, Ok(None)));
}

#[test]
fn presence_update_returns_none_for_stale_connection() {
    let mut state = test_state();
    join_test_user(&mut state, &UserId::Integer(1));

    let outcome = state.apply_presence_update(
        &UserId::Integer(1),
        ConnectionId::from_raw(999),
        &UserInfo::default(),
    );

    assert!(outcome.is_none());
}

#[test]
fn disconnect_sessions_ignores_missing_members() {
    let mut state = test_state();
    let outcome = state.apply_disconnect_users(&[UserId::Integer(1)]);
    let (relays, teardown) = outcome.transport_plan.relays_and_teardown();

    assert!(relays.is_empty());
    assert!(teardown.is_empty());
    assert!(outcome.effects.close_requests.is_empty());
    assert!(outcome.effects.fanouts.is_empty());
}

#[test]
fn replacement_join_releases_relay_with_displaced_source_session() {
    let mut state = test_state();
    let relay = install_relayed_source(&mut state);

    let outcome = state
        .apply_join_on_placement(
            relay.route.source_session_key().user_id(),
            test_sender(),
            UserJoinedFanout::Suppress,
            RouterPlacement {
                router: RouterId(2),
                media_worker: relay.target_media_worker_id,
            },
        )
        .expect("replacement join should succeed");
    assert_eq!(
        outcome.receipt.transport_session_key.media_worker_id(),
        relay.target_media_worker_id
    );
    assert_ne!(
        outcome.receipt.transport_session_key.media_worker_id(),
        relay.route.source_session_key().media_worker_id()
    );
    let (relays, teardown) = outcome.transport_plan.relays_and_teardown();

    assert!(relays.is_empty());
    assert_eq!(source_relay_teardown_count(teardown, &relay), 1);
    assert!(teardown.iter().any(|operation| {
        matches!(
            operation,
            TransportTeardown::CloseSession {
                session_key,
            } if session_key == relay.route.source_session_key()
        )
    }));
    assert!(!teardown.iter().any(|operation| {
        matches!(
            operation,
            TransportTeardown::RemoveMedia { session_key, .. }
                if session_key == relay.route.source_session_key()
        )
    }));
    assert!(teardown.iter().any(|operation| {
        matches!(
            operation,
            TransportTeardown::RemoveMedia {
                session_key,
                transport_media_id,
            } if session_key == relay.route.consumer_session_key()
                && *transport_media_id == relay.route.consumer_transport_media_id()
        )
    }));
}

#[test]
fn leave_releases_relay_before_forgetting_source_session() {
    let mut state = test_state();
    let relay = install_relayed_source(&mut state);

    let outcome = state
        .close_connection(
            relay.route.source_session_key().user_id(),
            relay.route.source_session_key().connection_id(),
        )
        .expect("publisher leave should succeed");
    let ConnectionCloseCommit::Current { transport_plan, .. } = outcome else {
        panic!("publisher leave should remove the current user");
    };
    let (relays, teardown) = transport_plan.relays_and_teardown();

    assert!(relays.is_empty());
    assert_eq!(source_relay_teardown_count(teardown, &relay), 1);
}

#[test]
fn disconnect_releases_relay_before_forgetting_source_session() {
    let mut state = test_state();
    let relay = install_relayed_source(&mut state);

    let outcome =
        state.apply_disconnect_users(from_ref(relay.route.source_session_key().user_id()));
    let (relays, teardown) = outcome.transport_plan.relays_and_teardown();

    assert!(relays.is_empty());
    assert_eq!(source_relay_teardown_count(teardown, &relay), 1);
}
