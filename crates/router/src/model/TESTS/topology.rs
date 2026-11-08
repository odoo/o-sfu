use o_sfu_model::UserId;

use crate::{
    ConnectionId, ConsumerId, MediaWorkerId, ProducerId, Router, RouterError, RouterId,
    model::topology::test_support::InvariantView,
    rtp::MediaCapabilities,
    topology::{RoutedProducerId, RouterPlacement, RouterPlacements},
};

fn placement(router: u64, worker: usize) -> RouterPlacement {
    RouterPlacement {
        router: RouterId(router),
        media_worker: MediaWorkerId::from_raw(worker),
    }
}

fn router() -> Router {
    Router::new(RouterId(9), MediaCapabilities::new(Vec::new(), Vec::new()))
}

fn join(router: &mut Router, user: &UserId, connection: u64, home: u64, worker: usize) {
    assert_eq!(
        router.commit_session_placement(
            user,
            ConnectionId::from_raw(connection),
            placement(home, worker),
        ),
        Ok(MediaWorkerId::from_raw(worker))
    );
}

#[test]
fn router_routes_media_through_one_facade() -> Result<(), RouterError> {
    let mut router = router();
    let publisher = UserId::Integer(10);
    let receiver = UserId::Integer(20);
    join(&mut router, &publisher, 1, 9, 0);
    join(&mut router, &receiver, 2, 9, 0);

    let producer = router.add_producer(&publisher, ProducerId(7))?;
    let consumer = router.add_consumer(&receiver, ConsumerId(8), producer)?;

    assert_eq!(
        router.add_producer(&publisher, ProducerId(7)),
        Err(RouterError::DuplicateProducer(ProducerId(7)))
    );
    assert_eq!(
        router.add_consumer(&receiver, ConsumerId(8), producer),
        Err(RouterError::DuplicateConsumer(ConsumerId(8)))
    );

    assert_eq!(producer.router_id(), RouterId(9));
    assert_eq!(producer.connection_id(), ConnectionId::from_raw(1));
    assert_eq!(producer.producer_id(), ProducerId(7));
    assert_eq!(consumer.router_id(), RouterId(9));
    assert_eq!(consumer.connection_id(), ConnectionId::from_raw(2));
    assert_eq!(consumer.consumer_id(), ConsumerId(8));
    let view = InvariantView::new(&router);
    assert!(view.has_session(&publisher, ConnectionId::from_raw(1), RouterId(9)));
    assert!(view.has_session(&receiver, ConnectionId::from_raw(2), RouterId(9)));
    assert!(view.has_only_route(producer, consumer));
    assert!(view.has_committed_sessions(2));
    assert_eq!(view.session_count(RouterId(9)), Some(2));
    assert!(view.is_valid());
    Ok(())
}

#[test]
fn router_rejects_duplicate_connections_without_mutation() -> Result<(), RouterError> {
    let mut router = router();
    let first = UserId::Integer(10);
    let second = UserId::Integer(20);
    let third = UserId::Integer(30);
    let connection = ConnectionId::from_raw(1);
    join(&mut router, &first, 1, 9, 0);
    join(&mut router, &second, 2, 10, 1);
    let producer = router.add_producer(&first, ProducerId(7))?;
    let consumer = router.add_consumer(&second, ConsumerId(8), producer)?;

    assert_eq!(
        router.commit_session_placement(&first, connection, placement(11, 2)),
        Err(RouterError::DuplicateConnection(connection))
    );

    assert_eq!(
        router.commit_session_placement(&third, connection, placement(11, 2)),
        Err(RouterError::DuplicateConnection(connection))
    );

    assert_eq!(
        router.committed_media_worker_id(&first, connection),
        Some(MediaWorkerId::from_raw(0))
    );
    assert_eq!(
        router.committed_media_worker_id(&second, ConnectionId::from_raw(2)),
        Some(MediaWorkerId::from_raw(1))
    );
    assert_eq!(router.committed_media_worker_id(&third, connection), None);
    let view = InvariantView::new(&router);
    assert_eq!(view.home_router(&first), Some(RouterId(9)));
    assert_eq!(view.home_router(&second), Some(RouterId(10)));
    assert_eq!(view.home_router(&third), None);
    assert!(view.has_placement_pair(placement(9, 0), placement(10, 1)));
    assert!(view.has_producer(producer));
    assert!(view.has_consumer(consumer));
    assert_eq!(view.dependent_count(producer), Some(1));
    assert_eq!(view.session_count(RouterId(11)), None);
    assert_eq!(router.router_count(), 2);
    assert!(view.is_valid());
    Ok(())
}

#[test]
fn router_rejects_conflicting_worker_before_replacement() -> Result<(), RouterError> {
    let mut router = router();
    let user = UserId::Integer(10);
    let connection = ConnectionId::from_raw(1);
    join(&mut router, &user, 1, 9, 0);
    let producer = router.add_producer(&user, ProducerId(7))?;

    assert_eq!(
        router.commit_session_placement(&user, ConnectionId::from_raw(2), placement(9, 1)),
        Err(RouterError::MediaWorkerMismatch {
            router: RouterId(9),
            expected: MediaWorkerId::from_raw(0),
            actual: MediaWorkerId::from_raw(1),
        })
    );

    assert_eq!(
        router.committed_media_worker_id(&user, connection),
        Some(MediaWorkerId::from_raw(0))
    );
    let view = InvariantView::new(&router);
    assert!(view.has_connection(connection));
    assert!(!view.has_connection(ConnectionId::from_raw(2)));
    assert!(view.has_producer(producer));
    assert!(view.is_valid());
    Ok(())
}

#[test]
fn unassigned_router_reserves_its_primary() {
    let mut router = router();
    let user = UserId::Integer(10);

    assert_eq!(
        router.commit_session_placement(&user, ConnectionId::from_raw(1), placement(10, 1)),
        Err(RouterError::PrimaryRouterMismatch {
            expected: RouterId(9),
            actual: RouterId(10),
        })
    );

    assert_eq!(router.router_count(), 1);
    assert!(router.placement_snapshot().assigned_placements().is_empty());
    assert!(InvariantView::new(&router).is_valid());
}

#[test]
fn replacement_rehomes_the_session_and_retires_its_old_graph() -> Result<(), RouterError> {
    let mut router = router();
    let publisher = UserId::Integer(10);
    let receiver = UserId::Integer(20);
    join(&mut router, &publisher, 1, 9, 0);
    join(&mut router, &receiver, 2, 10, 1);
    let producer = router.add_producer(&publisher, ProducerId(7))?;
    let consumer = router.add_consumer(&receiver, ConsumerId(8), producer)?;

    join(&mut router, &receiver, 3, 11, 2);

    assert_eq!(
        router.committed_media_worker_id(&receiver, ConnectionId::from_raw(2)),
        None
    );
    assert_eq!(
        router.committed_media_worker_id(&receiver, ConnectionId::from_raw(3)),
        Some(MediaWorkerId::from_raw(2))
    );
    let view = InvariantView::new(&router);
    assert!(view.has_empty_session(&receiver, ConnectionId::from_raw(3), RouterId(11)));
    assert!(view.has_only_producer(producer));
    assert_eq!(view.dependent_count(producer), Some(0));
    assert!(!view.has_consumer(consumer));
    assert_eq!(view.session_count(RouterId(9)), Some(1));
    assert_eq!(view.session_count(RouterId(10)), Some(0));
    assert_eq!(view.session_count(RouterId(11)), Some(1));
    assert!(view.is_valid());

    join(&mut router, &publisher, 4, 12, 3);
    let replacement_producer = router.add_producer(&publisher, ProducerId(7))?;
    let replacement_consumer =
        router.add_consumer(&receiver, ConsumerId(8), replacement_producer)?;

    assert_ne!(replacement_producer, producer);
    assert_ne!(replacement_consumer, consumer);
    assert_eq!(
        router.remove_consumer(consumer),
        Err(RouterError::MissingConsumer(consumer))
    );
    let view = InvariantView::new(&router);
    assert!(!view.has_producer(producer));
    assert_eq!(replacement_producer.router_id(), RouterId(12));
    assert!(view.has_producer(replacement_producer));
    assert!(view.has_consumer(replacement_consumer));
    assert_eq!(view.dependent_count(replacement_producer), Some(1));
    assert_eq!(view.session_count(RouterId(9)), Some(0));
    assert_eq!(view.session_count(RouterId(12)), Some(2));
    assert!(view.is_valid());
    Ok(())
}

#[test]
fn foreign_session_lives_until_its_last_consumer() -> Result<(), RouterError> {
    let mut router = router();
    let publisher = UserId::Integer(10);
    let receiver = UserId::Integer(20);
    join(&mut router, &publisher, 1, 9, 0);
    join(&mut router, &receiver, 2, 10, 1);
    let producer = router.add_producer(&publisher, ProducerId(7))?;
    let first_consumer = router.add_consumer(&receiver, ConsumerId(9), producer)?;
    let second_consumer = router.add_consumer(&receiver, ConsumerId(10), producer)?;

    assert_eq!(
        InvariantView::new(&router).dependent_count(producer),
        Some(2)
    );

    router.remove_consumer(first_consumer)?;
    let view = InvariantView::new(&router);
    assert_eq!(view.dependent_count(producer), Some(1));
    assert_eq!(view.session_count(RouterId(9)), Some(2));
    router.remove_consumer(second_consumer)?;

    let view = InvariantView::new(&router);
    assert_eq!(view.dependent_count(producer), Some(0));
    assert_eq!(view.session_count(RouterId(9)), Some(1));
    assert_eq!(view.home_router(&receiver), Some(RouterId(10)));
    assert!(view.is_valid());
    Ok(())
}

#[test]
fn session_teardown_cascades_routes_and_keeps_other_home_sessions() -> Result<(), RouterError> {
    let mut router = router();
    let removed_source = UserId::Integer(10);
    let receiver = UserId::Integer(20);
    let sibling_source = UserId::Integer(30);
    join(&mut router, &removed_source, 1, 9, 0);
    join(&mut router, &receiver, 2, 10, 1);
    join(&mut router, &sibling_source, 3, 11, 2);
    let producer = router.add_producer(&removed_source, ProducerId(7))?;
    let second_producer = router.add_producer(&removed_source, ProducerId(9))?;
    let sibling_producer = router.add_producer(&sibling_source, ProducerId(8))?;
    let consumer = router.add_consumer(&receiver, ConsumerId(9), producer)?;
    let second_consumer = router.add_consumer(&receiver, ConsumerId(11), second_producer)?;
    let sibling_consumer = router.add_consumer(&receiver, ConsumerId(10), sibling_producer)?;

    router.remove_session(&removed_source)?;

    let view = InvariantView::new(&router);
    assert!(!view.has_connection(ConnectionId::from_raw(1)));
    assert!(view.has_connection(ConnectionId::from_raw(2)));
    assert!(!view.has_producer(producer));
    assert!(!view.has_producer(second_producer));
    assert!(!view.has_consumer(consumer));
    assert!(!view.has_consumer(second_consumer));
    assert!(view.has_producer(sibling_producer));
    assert!(view.has_consumer(sibling_consumer));
    assert_eq!(view.dependent_count(sibling_producer), Some(1));
    assert_eq!(view.session_count(RouterId(9)), Some(0));
    assert_eq!(view.session_count(RouterId(10)), Some(1));
    assert_eq!(view.session_count(RouterId(11)), Some(2));
    assert!(view.is_valid());
    Ok(())
}

#[test]
fn receiver_teardown_clears_all_foreign_sessions() -> Result<(), RouterError> {
    let mut router = router();
    let first_source = UserId::Integer(10);
    let receiver = UserId::Integer(20);
    let second_source = UserId::Integer(30);
    join(&mut router, &first_source, 1, 9, 0);
    join(&mut router, &receiver, 2, 10, 1);
    join(&mut router, &second_source, 3, 11, 2);
    let first_producer = router.add_producer(&first_source, ProducerId(7))?;
    let second_producer = router.add_producer(&second_source, ProducerId(8))?;
    let first_consumer = router.add_consumer(&receiver, ConsumerId(9), first_producer)?;
    let second_consumer = router.add_consumer(&receiver, ConsumerId(10), second_producer)?;

    router.remove_session(&receiver)?;

    let view = InvariantView::new(&router);
    assert!(view.has_producer(first_producer));
    assert!(view.has_producer(second_producer));
    assert!(!view.has_consumer(first_consumer));
    assert!(!view.has_consumer(second_consumer));
    assert_eq!(view.dependent_count(first_producer), Some(0));
    assert_eq!(view.dependent_count(second_producer), Some(0));
    assert_eq!(view.session_count(RouterId(9)), Some(1));
    assert_eq!(view.session_count(RouterId(10)), Some(0));
    assert_eq!(view.session_count(RouterId(11)), Some(1));
    assert!(view.is_valid());
    Ok(())
}

#[test]
fn router_rejects_unknown_graph_identifiers_before_mutation() {
    let mut router = router();
    let receiver = UserId::Integer(20);
    join(&mut router, &receiver, 2, 9, 0);
    let missing =
        RoutedProducerId::for_test(RouterId(99), ConnectionId::from_raw(1), ProducerId(7));

    assert_eq!(
        router.add_consumer(&receiver, ConsumerId(8), missing),
        Err(RouterError::MissingRouter(RouterId(99)))
    );
    assert_eq!(
        router.add_producer(&UserId::Integer(30), ProducerId(9)),
        Err(RouterError::MissingSession(UserId::Integer(30)))
    );
    assert!(InvariantView::new(&router).is_valid());
}

#[test]
fn session_retirement_keeps_spillover_placement() {
    let mut router = router();
    let primary = UserId::Integer(5);
    let user = UserId::Integer(10);
    join(&mut router, &primary, 5, 9, 0);
    join(&mut router, &user, 1, 10, 3);

    assert_eq!(
        router.retire_committed_placement(&user, ConnectionId::from_raw(2)),
        None
    );
    assert_eq!(
        router.retire_committed_placement(&user, ConnectionId::from_raw(1)),
        Some(MediaWorkerId::from_raw(3))
    );
    assert_eq!(
        router.placement_snapshot().assigned_placements(),
        &[placement(9, 0), placement(10, 3)]
    );

    let view = InvariantView::new(&router);
    assert!(!view.has_connection(ConnectionId::from_raw(1)));
    assert!(view.is_valid());
}

#[test]
fn placement_values_preserve_worker_assignment() {
    let placements = RouterPlacements::new(
        placement(9, 0),
        vec![placement(10, 1), placement(10, 2), placement(9, 3)],
    );
    let router =
        Router::with_placements(placements, MediaCapabilities::new(Vec::new(), Vec::new()));
    let snapshot = router.placement_snapshot();
    assert_eq!(snapshot.primary(), RouterId(9));
    assert_eq!(snapshot.next_router(), RouterId(11));
    assert_eq!(router.primary_worker(), Some(MediaWorkerId::from_raw(0)));
    assert_eq!(
        snapshot.assigned_placements(),
        &[placement(9, 0), placement(10, 2)]
    );
}
