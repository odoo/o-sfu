#![allow(
    clippy::expect_used,
    reason = "route graph tests fail loudly when fixed route reservations are invalid"
)]

use o_sfu_router::{
    ConsumerId, MediaKind, ProducerId, RouterId,
    topology::{RoutedConsumerId, RoutedProducerId},
};

use super::{
    ConsumerSourceSelection, SubscriptionKey,
    consumer_setup::ConsumerSetupTarget,
    route_graph::{ConsumerRouteReservation, RouteGraph},
};
use crate::{
    Bitrate,
    engine::{
        ConnectionId, MediaWorkerId, RoomInstanceId, UserId, VideoLayoutIntent,
        media_transport::{
            RelayRouteActivity, TransportConsumerRoute, TransportMediaId,
            TransportRelayRouteAction, TransportRelayRouteEffect, TransportSessionKey,
            TransportSourceKey,
        },
        source_model::{
            PolicyPauseReason, PublishedSourceId, ReceiverVideoBudgetDiagnostics, SourceEncodingId,
            SourceSelector, SourceSubscriptionIntent, UserStreamId,
        },
    },
};

const SOURCE_ONE: PublishedSourceId = PublishedSourceId::from_raw(1);
const SOURCE_TWO: PublishedSourceId = PublishedSourceId::from_raw(2);

fn key(receiver: i64) -> SubscriptionKey {
    SubscriptionKey::new(
        &UserId::Integer(receiver),
        &UserId::Integer(1),
        &UserStreamId::from("camera"),
    )
}

fn target(receiver: i64, connection: u64, source_id: PublishedSourceId) -> ConsumerSetupTarget {
    let source_connection = ConnectionId::from_raw(10);
    ConsumerSetupTarget {
        session: session_key(
            UserId::Integer(receiver),
            ConnectionId::from_raw(connection),
        ),
        source: TransportSourceKey::new(
            session_key(UserId::Integer(1), source_connection),
            TransportMediaId::new(50),
        ),
        source_id,
        stream: UserStreamId::from("camera"),
        kind: MediaKind::Video,
        routed: RoutedProducerId::for_test(RouterId(1), source_connection, ProducerId(10)),
    }
}

fn session_key(user: UserId, connection: ConnectionId) -> TransportSessionKey {
    TransportSessionKey::new(
        RoomInstanceId::from_raw(0),
        MediaWorkerId::from_raw(0),
        connection,
        user,
    )
}

fn route(target: &ConsumerSetupTarget, media: u64) -> TransportConsumerRoute {
    target.transport_consumer_route(TransportMediaId::new(media))
}

fn routed_consumer(target: &ConsumerSetupTarget, consumer: u64) -> RoutedConsumerId {
    RoutedConsumerId::for_test(
        target.routed.router_id(),
        target.session.connection_id(),
        ConsumerId(consumer),
    )
}

fn reserve(
    graph: &mut RouteGraph,
    key: &SubscriptionKey,
    source_id: PublishedSourceId,
    selection: ConsumerSourceSelection,
) -> ConsumerRouteReservation {
    assert!(graph.attach_for_setup(key.clone(), source_id));
    graph
        .reserve_consumer_setup(key.clone(), source_id, selection)
        .expect("attached subscription should reserve an absent realization")
}

fn commit_route(
    graph: &mut RouteGraph,
    reservation: ConsumerRouteReservation,
    target: &ConsumerSetupTarget,
    media: u64,
    selection: ConsumerSourceSelection,
) -> TransportConsumerRoute {
    let route = route(target, media);
    graph
        .commit(
            reservation,
            route.clone(),
            String::from("mid"),
            selection,
            || Some(routed_consumer(target, media)),
        )
        .expect("current reservation should commit");
    route
}

fn actions(effects: Vec<TransportRelayRouteEffect>) -> Vec<TransportRelayRouteAction> {
    effects.into_iter().map(|effect| effect.action).collect()
}

#[test]
fn intent_survives_detach_and_resets_source_selection() {
    let mut graph = RouteGraph::default();
    let key = key(2);
    let target = target(2, 20, SOURCE_ONE);
    graph.merge_intent(
        key.clone(),
        SourceSubscriptionIntent::new(Some(false), Some(VideoLayoutIntent::Pinned)),
    );
    graph.merge_intent(key.clone(), SourceSubscriptionIntent::default());
    let reservation = reserve(
        &mut graph,
        &key,
        SOURCE_ONE,
        ConsumerSourceSelection::open(false),
    );
    let route = commit_route(
        &mut graph,
        reservation,
        &target,
        100,
        ConsumerSourceSelection::open(false),
    );
    assert!(
        graph.update_selection(&key, SOURCE_ONE, &route, |selection| {
            selection.set_selector(SourceSelector::Encoding(SourceEncodingId::from_raw(7)));
            selection.set_policy_pause_reason(Some(PolicyPauseReason::HiddenTile));
            selection.set_budget(ReceiverVideoBudgetDiagnostics::new(
                Some(Bitrate::from_kbps(800)),
                Some(Bitrate::from_kbps(600)),
                2,
                Bitrate::from_kbps(500),
            ));
        })
    );
    graph.detach_source(SOURCE_ONE);
    assert_eq!(graph.intent(&key).layout(), Some(VideoLayoutIntent::Pinned));
    assert!(graph.attach_for_setup(key.clone(), SOURCE_TWO));
    assert_eq!(
        graph.selection(&key, SOURCE_TWO),
        Some(ConsumerSourceSelection::open(false))
    );
}

#[test]
fn receiver_reset_rejects_stale_reservation_for_same_publication() {
    let mut graph = RouteGraph::default();
    let key = key(2);
    let target = target(2, 20, SOURCE_ONE);
    let stale = reserve(
        &mut graph,
        &key,
        SOURCE_ONE,
        ConsumerSourceSelection::open(true),
    );
    graph.reset_receiver_for_replacement(&UserId::Integer(2));
    let fresh = reserve(
        &mut graph,
        &key,
        SOURCE_ONE,
        ConsumerSourceSelection::open(false),
    );
    let mut accepted = false;
    assert!(
        graph
            .commit(
                stale,
                route(&target, 100),
                String::from("stale"),
                ConsumerSourceSelection::open(true),
                || {
                    accepted = true;
                    Some(routed_consumer(&target, 100))
                },
            )
            .is_err()
    );
    assert!(!accepted);
    commit_route(
        &mut graph,
        fresh,
        &target,
        101,
        ConsumerSourceSelection::open(false),
    );
}

#[test]
fn detach_prunes_default_record_and_full_leave_deletes_intent() {
    let mut graph = RouteGraph::default();
    let default_key = key(2);
    assert!(graph.attach_for_setup(default_key, SOURCE_ONE));
    graph.detach_source(SOURCE_ONE);
    assert_eq!(graph.record_count(), 0);

    let explicit_key = key(3);
    graph.merge_intent(
        explicit_key.clone(),
        SourceSubscriptionIntent::new(Some(false), None),
    );
    assert!(graph.attach_for_setup(explicit_key, SOURCE_TWO));
    graph.remove_receiver(&UserId::Integer(3));
    assert_eq!(graph.record_count(), 0);
}

#[test]
fn pending_and_committed_counts_follow_realization_state() {
    let mut graph = RouteGraph::default();
    let pending_key = key(2);
    let committed_key = key(3);
    let pending = reserve(
        &mut graph,
        &pending_key,
        SOURCE_ONE,
        ConsumerSourceSelection::open(true),
    );
    let committed_target = target(3, 30, SOURCE_ONE);
    let committed = reserve(
        &mut graph,
        &committed_key,
        SOURCE_ONE,
        ConsumerSourceSelection::open(true),
    );
    assert_eq!(graph.subscription_count(), 2);
    commit_route(
        &mut graph,
        committed,
        &committed_target,
        101,
        ConsumerSourceSelection::open(true),
    );
    assert_eq!(graph.count(), 1);
    let committed = graph
        .current(&committed_key)
        .and_then(|(_, current)| current.committed())
        .expect("committed realization should be projected");
    assert_eq!(committed.mid, "mid");

    assert!(graph.release_consumer_setup(pending).is_empty());
    assert_eq!(graph.subscription_count(), 1);
    graph.detach_source(SOURCE_ONE);
    assert_eq!(graph.subscription_count(), 0);
}

#[test]
fn receiver_reset_clears_realization_and_preserves_intent() {
    let mut graph = RouteGraph::default();
    let key = key(2);
    let target = target(2, 20, SOURCE_ONE);
    graph.merge_intent(
        key.clone(),
        SourceSubscriptionIntent::new(Some(false), None),
    );
    let reservation = reserve(
        &mut graph,
        &key,
        SOURCE_ONE,
        ConsumerSourceSelection::open(false),
    );
    commit_route(
        &mut graph,
        reservation,
        &target,
        100,
        ConsumerSourceSelection::open(false),
    );

    graph.reset_receiver_for_replacement(&UserId::Integer(2));
    assert_eq!(
        graph.selection(&key, SOURCE_ONE),
        Some(ConsumerSourceSelection::open(false))
    );
    assert!(
        graph
            .reserve_consumer_setup(key, SOURCE_ONE, ConsumerSourceSelection::open(false))
            .is_some()
    );
}

#[test]
fn stale_source_or_route_cannot_commit_after_reattach() {
    let mut graph = RouteGraph::default();
    let key = key(2);
    let old_target = target(2, 20, SOURCE_ONE);
    let stale = reserve(
        &mut graph,
        &key,
        SOURCE_ONE,
        ConsumerSourceSelection::open(true),
    );
    graph.detach_source(SOURCE_ONE);

    let fresh_target = target(2, 21, SOURCE_TWO);
    let fresh = reserve(
        &mut graph,
        &key,
        SOURCE_TWO,
        ConsumerSourceSelection::open(true),
    );
    let mut router_called = false;
    assert!(
        graph
            .commit(
                stale,
                route(&old_target, 100),
                String::from("old"),
                ConsumerSourceSelection::open(true),
                || {
                    router_called = true;
                    Some(routed_consumer(&old_target, 100))
                },
            )
            .is_err()
    );
    assert!(!router_called);

    let current_route = commit_route(
        &mut graph,
        fresh,
        &fresh_target,
        101,
        ConsumerSourceSelection::open(true),
    );
    assert!(
        graph
            .set_activity(&key, SOURCE_TWO, ConnectionId::from_raw(20), false, None)
            .is_none()
    );
    assert_eq!(
        graph.selection(&key, SOURCE_TWO),
        Some(ConsumerSourceSelection::open(true))
    );
    assert!(!graph.update_selection(&key, SOURCE_ONE, &current_route, |_| {}));
    assert!(!graph.update_selection(&key, SOURCE_TWO, &route(&fresh_target, 102), |_| {}));
    assert!(
        graph.update_selection(&key, SOURCE_TWO, &current_route, |selection| {
            selection.set_active(false);
        })
    );
}

#[test]
fn policy_pause_stamps_the_selection_but_leaves_the_relay_on_intent() {
    let mut graph = RouteGraph::default();
    let key = key(2);
    let target = target(2, 20, SOURCE_ONE);
    let reservation = reserve(
        &mut graph,
        &key,
        SOURCE_ONE,
        ConsumerSourceSelection::open(false),
    );
    let worker = MediaWorkerId::from_raw(1);
    assert_eq!(
        actions(graph.reserve_relay(&reservation, &target, worker, false)),
        [TransportRelayRouteAction::Install]
    );
    let _ = commit_route(
        &mut graph,
        reservation,
        &target,
        101,
        ConsumerSourceSelection::open(false),
    );

    // Reactivating while the receiver is deaf stamps the pause so the consumer
    // destination stays closed, and still raises the relay: source policy resumes
    // the destination only, so a relay left inactive here could never come back.
    assert_eq!(
        actions(
            graph
                .set_activity(
                    &key,
                    SOURCE_ONE,
                    ConnectionId::from_raw(20),
                    true,
                    Some(PolicyPauseReason::ReceiverDeafened),
                )
                .expect("activity update should apply to the committed route")
        ),
        [TransportRelayRouteAction::SetActivity(
            RelayRouteActivity::Active
        )]
    );
    let selection = graph
        .selection(&key, SOURCE_ONE)
        .expect("committed route should expose its selection");
    assert!(selection.active());
    assert_eq!(
        selection.policy_pause_reason(),
        Some(PolicyPauseReason::ReceiverDeafened)
    );
    assert!(!selection.delivery_active());
}

#[test]
fn rejected_route_preserves_selection_and_shared_relay() {
    let mut graph = RouteGraph::default();
    let inactive_target = target(2, 20, SOURCE_ONE);
    let active_target = target(3, 30, SOURCE_ONE);
    let inactive_key = key(2);
    let active_key = key(3);
    let inactive = reserve(
        &mut graph,
        &inactive_key,
        SOURCE_ONE,
        ConsumerSourceSelection::open(false),
    );
    let active = reserve(
        &mut graph,
        &active_key,
        SOURCE_ONE,
        ConsumerSourceSelection::open(true),
    );
    let worker = MediaWorkerId::from_raw(1);
    assert_eq!(
        actions(graph.reserve_relay(&inactive, &inactive_target, worker, false)),
        [TransportRelayRouteAction::Install]
    );
    assert_eq!(
        actions(graph.reserve_relay(&active, &active_target, worker, true)),
        [TransportRelayRouteAction::SetActivity(
            RelayRouteActivity::Active
        )]
    );

    assert_eq!(
        actions(
            graph
                .commit(
                    active,
                    route(&active_target, 101),
                    String::from("active"),
                    ConsumerSourceSelection::open(true),
                    || None,
                )
                .expect_err("router rejection should release the active relay owner")
        ),
        [TransportRelayRouteAction::SetActivity(
            RelayRouteActivity::Inactive
        )]
    );
    assert_eq!(
        graph.selection(&active_key, SOURCE_ONE),
        Some(ConsumerSourceSelection::open(true))
    );
    assert_eq!(
        actions(graph.release_consumer_setup(inactive)),
        [TransportRelayRouteAction::Release]
    );
}

#[test]
fn source_activity_targets_pending_and_committed_relay_workers() {
    let mut graph = RouteGraph::default();
    let target = target(2, 20, SOURCE_ONE);
    let key = key(2);
    let reservation = reserve(
        &mut graph,
        &key,
        SOURCE_ONE,
        ConsumerSourceSelection::open(true),
    );
    let worker = MediaWorkerId::from_raw(1);
    let _ = graph.reserve_relay(&reservation, &target, worker, true);
    assert_eq!(
        graph
            .source_activity_target_workers(&target.source)
            .collect::<Vec<_>>(),
        [worker]
    );
    commit_route(
        &mut graph,
        reservation,
        &target,
        101,
        ConsumerSourceSelection::open(true),
    );
    assert_eq!(
        graph
            .source_activity_target_workers(&target.source)
            .collect::<Vec<_>>(),
        [worker]
    );
}
