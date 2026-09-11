use std::{
    num::{NonZeroU64, NonZeroUsize},
    slice,
    sync::Arc,
    time::Duration,
};

use o_sfu_telemetry::schema::event as telemetry_event;
use serde_json::Value;
use tokio::{sync::Notify, task::yield_now, time::timeout};

use super::{
    super::{RoomManagerJoinError, RoomManagerServeError, manager::JoinPlacementTestGate},
    api::NegotiatedPublish,
    fixtures::*,
    tracing::{assert_exact, assert_user_exact, capture},
};
use crate::{RoomWorkerPolicy, RuntimeFeatureFlags, engine::metrics::RoomGaugeValues};

type TestRoom = super::super::Room;

/// reservation window that no test can reach, so expiry only happens on demand
const LONG_EXPIRATION: Duration = Duration::from_hours(1);

async fn manager_join_user(
    manager: &RoomManager,
    room: &Arc<TestRoom>,
    raw_user_id: i64,
    media_transport: &MediaTransport,
) -> ConnectionId {
    let (tx, _rx) = test_sender();
    let admission = manager
        .join_user(
            room.uuid(),
            JoinUserRequest {
                user_id: UserId::Integer(raw_user_id),
                label: None,
                permissions: UserPermissions::default(),
                sender: tx,
            },
            media_transport,
        )
        .await
        .expect("user should join through manager");
    admission.connection_id
}

async fn try_manager_join_user(
    manager: &RoomManager,
    room_id: &str,
    raw_user_id: i64,
    media_transport: &MediaTransport,
) -> Result<ConnectionId, RoomManagerJoinError> {
    let (sender, _receiver) = test_sender();
    manager
        .join_user(
            room_id,
            JoinUserRequest {
                user_id: UserId::Integer(raw_user_id),
                label: None,
                permissions: UserPermissions::default(),
                sender,
            },
            media_transport,
        )
        .await
        .map(|admission| admission.connection_id)
}

fn manager_with_room_worker_policy(room_worker_policy: RoomWorkerPolicy) -> RoomManager {
    RoomManager::for_test_with_runtime_policy(
        super::super::RoomRuntimePolicy::new(
            RoomAdmissionPolicy::new(100),
            RuntimeFeatureFlags::default(),
            test_client_rtp_capabilities(),
        )
        .with_room_worker_policy(room_worker_policy),
    )
}

async fn serve_test_room(manager: &RoomManager, issuer: &str) -> Arc<TestRoom> {
    manager
        .serve_room(issuer, TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await
        .expect("test room should be served")
}

fn spillover_policy(max_local_routers: usize) -> RoomWorkerPolicy {
    RoomWorkerPolicy::new(
        NonZeroUsize::new(max_local_routers).expect("test router cap should be positive"),
        NonZeroU64::new(RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS)
            .expect("default delay threshold should be positive"),
    )
}

async fn assert_home_worker(room: &Arc<TestRoom>, raw_user_id: i64, media_worker: usize) {
    assert_eq!(
        room.test_api()
            .home_media_worker_id(&UserId::Integer(raw_user_id))
            .await,
        Some(media_worker)
    );
}

async fn assert_router_count(room: &Arc<TestRoom>, expected: usize) {
    assert_eq!(room.test_api().router_count().await, expected);
}

#[tokio::test(flavor = "current_thread")]
async fn room_and_user_lifecycle_events_preserve_contract_fields() {
    let _guard = capture().await;
    let manager = RoomManager::for_test();
    let media_transport = real_adapter();
    let room = manager
        .serve_room(
            "issuer-lifecycle-events",
            TEST_ROOM_KEY,
            &RoomConfig::default(),
            Some("203.0.113.1"),
        )
        .await
        .expect("test room should be served");
    let closed_user = UserId::Integer(1);
    let disconnected_user = UserId::Integer(2);
    let closed_connection = manager_join_user(&manager, &room, 1, &media_transport).await;
    let disconnected_connection = manager_join_user(&manager, &room, 2, &media_transport).await;

    assert!(
        manager
            .close_session(
                room.uuid(),
                &closed_user,
                closed_connection,
                &media_transport,
            )
            .await
    );
    manager
        .disconnect_users(
            room.uuid(),
            slice::from_ref(&disconnected_user),
            &media_transport,
        )
        .await;

    assert_exact(
        telemetry_event::ROOM_CREATED,
        &[
            ("room_id", Value::from(room.uuid())),
            ("remote_address", Value::from("203.0.113.1")),
            ("web_rtc_enabled", Value::from(true)),
        ],
    );
    for (name, user, connection) in [
        (
            telemetry_event::USER_JOINED,
            &closed_user,
            closed_connection,
        ),
        (
            telemetry_event::USER_JOINED,
            &disconnected_user,
            disconnected_connection,
        ),
        (
            telemetry_event::USER_CLOSED,
            &closed_user,
            closed_connection,
        ),
        (
            telemetry_event::USER_DISCONNECTED,
            &disconnected_user,
            disconnected_connection,
        ),
    ] {
        assert_user_exact(
            name,
            room.uuid(),
            user.path_segment().as_ref(),
            connection.as_u64(),
            0,
            &[],
        );
    }
    assert_exact(
        telemetry_event::ROOM_DESTROYED,
        &[("room_id", Value::from(room.uuid()))],
    );
}

#[tokio::test(flavor = "current_thread")]
async fn users_bulk_disconnected_event_preserves_contract_fields() {
    let _guard = capture().await;
    let manager = RoomManager::for_test();
    let media_transport = real_adapter();
    let room = manager
        .serve_room(
            "issuer-bulk-disconnect",
            TEST_ROOM_KEY,
            &RoomConfig::default(),
            Some("203.0.113.1"),
        )
        .await
        .expect("test room should be served");
    manager_join_user(&manager, &room, 1, &media_transport).await;
    // success case: user is in a live room; this also empties and destroys the room
    manager
        .disconnect_users(room.uuid(), &[UserId::Integer(1)], &media_transport)
        .await;
    assert_exact(
        telemetry_event::USERS_BULK_DISCONNECTED,
        &[
            ("room_id", Value::from(room.uuid())),
            ("requested_user_count", Value::from(1u64)),
            ("disconnected_session_count", Value::from(1u64)),
            ("outcome", Value::from("success")),
        ],
    );
    // room_missing case: the same room was destroyed by the previous call
    manager
        .disconnect_users(room.uuid(), &[UserId::Integer(1)], &media_transport)
        .await;
    assert_exact(
        telemetry_event::USERS_BULK_DISCONNECTED,
        &[
            ("room_id", Value::from(room.uuid())),
            ("requested_user_count", Value::from(1u64)),
            ("disconnected_session_count", Value::from(0u64)),
            ("outcome", Value::from("room_missing")),
        ],
    );
}

#[tokio::test]
async fn room_manager_concurrent_create_and_cleanup_are_idempotent() {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(2));
    let config = RoomConfig::default();

    let (first, second) = tokio::join!(
        manager.serve_room("issuer-a", TEST_ROOM_KEY, &config, None),
        manager.serve_room("issuer-a", TEST_ROOM_KEY, &config, None),
    );

    let first = first.expect("test room should be served");
    let second = second.expect("test room should be served");
    assert_eq!(first.uuid(), second.uuid());
    let media_transport = real_adapter();
    manager_join_user(&manager, &first, 1, &media_transport).await;
    let room_id = first.uuid().to_owned();
    let user_ids = [UserId::Integer(1)];

    tokio::join!(
        manager.disconnect_users(&room_id, &user_ids, &media_transport),
        manager.disconnect_users(&room_id, &user_ids, &media_transport),
    );

    assert!(manager.get_by_uuid(&room_id).await.is_none());
}

#[tokio::test]
async fn manager_lifecycle_future_does_not_block_empty_cleanup() {
    let manager = Arc::new(RoomManager::for_test_with_admission_policy(
        RoomAdmissionPolicy::new(2),
    ));
    let media_transport = real_adapter();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await
        .expect("test room should be served");
    let room_id = room.uuid().to_owned();
    let first_user = UserId::Integer(1);
    let (first_tx, _first_rx) = test_sender();
    let first_session = manager
        .join_user(
            &room_id,
            JoinUserRequest {
                user_id: first_user.clone(),
                label: None,
                permissions: UserPermissions::default(),
                sender: first_tx,
            },
            &media_transport,
        )
        .await
        .expect("initial user should join");
    let first_connection_id = first_session.connection_id;

    let lifecycle_entered = Arc::new(Notify::new());
    let release_lifecycle = Arc::new(Notify::new());
    let manager_for_hold = Arc::clone(&manager);
    let room_id_for_hold = room_id.clone();
    let lifecycle_entered_for_hold = Arc::clone(&lifecycle_entered);
    let release_lifecycle_for_hold = Arc::clone(&release_lifecycle);
    let hold_lifecycle = tokio::spawn(async move {
        manager_for_hold
            .with_current_room(&room_id_for_hold, |room| async move {
                lifecycle_entered_for_hold.notify_one();
                release_lifecycle_for_hold.notified().await;
                room
            })
            .await
    });
    lifecycle_entered.notified().await;

    let manager_for_close = Arc::clone(&manager);
    let room_id_for_close = room_id.clone();
    let first_user_for_close = first_user.clone();
    let transport_for_close = media_transport.clone();
    let close_task = tokio::spawn(async move {
        manager_for_close
            .close_session(
                &room_id_for_close,
                &first_user_for_close,
                first_connection_id,
                &transport_for_close,
            )
            .await
    });
    yield_now().await;

    let did_close = timeout(Duration::from_secs(1), close_task)
        .await
        .expect("close should finish while another lifecycle future is parked")
        .expect("close task should not panic");
    assert!(did_close);

    release_lifecycle.notify_one();
    let held_room = timeout(Duration::from_secs(1), hold_lifecycle)
        .await
        .expect("lifecycle holder should finish")
        .expect("lifecycle holder task should not panic");
    assert!(held_room.is_some());
    assert!(manager.get_by_uuid(&room_id).await.is_none());
}

#[tokio::test]
async fn manager_concurrent_overload_joins_revalidate_local_router_cap_at_commit() {
    const LOCAL_ROUTER_CAP: usize = 2;
    const WORKER_COUNT: usize = 4;
    const CONCURRENT_JOINS: usize = 12;

    let manager = Arc::new(manager_with_room_worker_policy(spillover_policy(
        LOCAL_ROUTER_CAP,
    )));
    let media_transport = real_adapter();
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(0); WORKER_COUNT]);
    let room = serve_test_room(&manager, "issuer-concurrent-large-room-cap").await;
    manager_join_user(&manager, &room, 1, &media_transport).await;
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(20), Some(0), Some(0), Some(0)]);
    let placement_gate = Arc::new(JoinPlacementTestGate::new(CONCURRENT_JOINS));
    manager.set_join_placement_gate_for_test(Arc::clone(&placement_gate));

    let mut join_tasks = Vec::with_capacity(CONCURRENT_JOINS);
    for user_offset in 0..CONCURRENT_JOINS {
        let manager = Arc::clone(&manager);
        let room_id = room.uuid().to_owned();
        let media_transport = media_transport.clone();
        join_tasks.push(tokio::spawn(async move {
            let (sender, _receiver) = test_sender();
            manager
                .join_user(
                    &room_id,
                    JoinUserRequest {
                        user_id: UserId::Integer(i64::try_from(user_offset + 2).unwrap()),
                        label: None,
                        permissions: UserPermissions::default(),
                        sender,
                    },
                    &media_transport,
                )
                .await
                .expect("concurrent user should join through manager");
        }));
    }

    timeout(Duration::from_secs(5), placement_gate.hold_all_ready())
        .await
        .expect("all concurrent joins should reach placement commit");
    assert_router_count(&room, 1).await;
    placement_gate.release_all().await;

    for join_task in join_tasks {
        join_task.await.expect("join task should not panic");
        assert!(
            room.test_api().router_count().await <= LOCAL_ROUTER_CAP,
            "concurrent placement should not exceed the configured local router cap"
        );
    }

    assert_router_count(&room, LOCAL_ROUTER_CAP).await;
}

#[tokio::test(flavor = "current_thread")]
async fn placement_reads_worker_health_after_the_commit_gate() {
    const WORKER_COUNT: usize = 4;

    let manager = Arc::new(manager_with_room_worker_policy(spillover_policy(2)));
    let media_transport = real_adapter();
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(0); WORKER_COUNT]);
    let room = serve_test_room(&manager, "issuer-fresh-placement-health").await;
    manager_join_user(&manager, &room, 1, &media_transport).await;
    let primary_worker = room
        .test_api()
        .home_media_worker_id(&UserId::Integer(1))
        .await
        .expect("first user should have a worker");
    let mut overloaded = vec![Some(0); WORKER_COUNT];
    *overloaded
        .get_mut(primary_worker)
        .expect("primary worker should exist") = Some(20);
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(overloaded);
    let gate = Arc::new(JoinPlacementTestGate::new(1));
    manager.set_join_placement_gate_for_test(Arc::clone(&gate));
    let pending_join = {
        let manager = Arc::clone(&manager);
        let room = Arc::clone(&room);
        let media_transport = media_transport.clone();
        tokio::spawn(async move {
            manager_join_user(&manager, &room, 2, &media_transport).await;
        })
    };

    timeout(Duration::from_secs(5), gate.hold_all_ready())
        .await
        .expect("join should reach the placement gate");
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(0); WORKER_COUNT]);
    gate.release_all().await;
    pending_join.await.expect("join task should not panic");

    assert_home_worker(&room, 2, primary_worker).await;
    assert_router_count(&room, 1).await;
}

#[tokio::test(flavor = "current_thread")]
async fn spillover_media_diagnostics_use_connection_worker() {
    let _guard = capture().await;
    let manager = manager_with_room_worker_policy(spillover_policy(2));
    let media_transport = real_adapter();
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(0), Some(0), None, None]);
    let room = serve_test_room(&manager, "issuer-spillover-diagnostics").await;
    manager_join_user(&manager, &room, 1, &media_transport).await;
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(20), Some(0), None, None]);
    let publisher_connection_id = manager_join_user(&manager, &room, 2, &media_transport).await;
    let publisher_id = UserId::Integer(2);
    let media_worker_id = 1;
    assert_home_worker(&room, 2, media_worker_id).await;
    let mut state = room.state.write().await;
    let session_negotiated = state
        .set_user_negotiated(
            &publisher_id,
            publisher_connection_id,
            test_client_rtp_capabilities(),
        )
        .is_some();
    drop(state);
    assert!(session_negotiated);

    let transport_media_id = TransportMediaId::new(99);
    let stream_id = room
        .test_api()
        .publish_negotiated_track(
            &publisher_id,
            NegotiatedPublish {
                connection_id: publisher_connection_id,
                stream_type: TestSourceKind::AudioDetector,
                transport_media_id,
                consumable_rtp_parameters: test_audio_rtp_parameters(),
            },
            &media_transport,
        )
        .await
        .expect("synthetic negotiated publish should commit");
    assert_user_exact(
        telemetry_event::PUBLISH_COMMITTED,
        room.uuid(),
        publisher_id.path_segment().as_ref(),
        publisher_connection_id.as_u64(),
        media_worker_id,
        &[(
            "transport_media_id",
            Value::from(transport_media_id.as_u64()),
        )],
    );

    assert!(
        room.test_api()
            .deactivate_publication(&publisher_id, &stream_id, &media_transport)
            .await
    );
    assert!(
        media_transport
            .test_api()
            .route_entry_by_media_id(transport_media_id)
            .await
            .is_none()
    );
    assert_user_exact(
        telemetry_event::PUBLICATION_ACTIVITY_CHANGED,
        room.uuid(),
        publisher_id.path_segment().as_ref(),
        publisher_connection_id.as_u64(),
        media_worker_id,
        &[
            (
                "transport_media_id",
                Value::from(transport_media_id.as_u64()),
            ),
            ("active", Value::from(false)),
            ("stream_id", Value::from(stream_id.to_string())),
        ],
    );
}

#[tokio::test]
async fn expired_reservation_is_reaped_and_frees_the_issuer_alias() {
    const ISSUER: &str = "issuer-reservation-expiry";

    let manager = RoomManager::for_test_with_reservation_ttl(Duration::ZERO);
    let room = serve_test_room(&manager, ISSUER).await;
    let room_id = room.uuid().to_owned();

    // a room is available until it is reaped, so `/v1/channel` keeps serving a
    // uuid whose deadline has passed. that uuid is still joinable.
    assert_eq!(serve_test_room(&manager, ISSUER).await.uuid(), room_id);

    manager.check_expired_room_reservations().await;

    assert!(
        manager.get_by_uuid(&room_id).await.is_none(),
        "an expired reservation should leave the directory"
    );
    assert_eq!(
        manager.room_gauges().await,
        RoomGaugeValues::default(),
        "the reaped room should stop counting towards the active-room gauge"
    );
    assert_ne!(
        serve_test_room(&manager, ISSUER).await.uuid(),
        room_id,
        "the issuer alias should be free for a fresh room"
    );
}

#[tokio::test]
async fn serving_an_existing_room_renews_its_reservation() {
    const ISSUER: &str = "issuer-serve-renews-reservation";

    let manager = RoomManager::for_test_with_reservation_ttl(LONG_EXPIRATION);
    let room = serve_test_room(&manager, ISSUER).await;

    assert!(
        manager
            .expire_room_reservation_now_for_test(room.uuid())
            .await
    );

    assert_eq!(
        serve_test_room(&manager, ISSUER).await.uuid(),
        room.uuid(),
        "a second serve for the same issuer should return the same room"
    );

    // if the second serve had not renewed the reservation, this pass would
    // reap the room since it was force expired
    manager.check_expired_room_reservations().await;

    assert!(
        manager.get_by_uuid(room.uuid()).await.is_some(),
        "serving the room again should renew its reservation before the reaper runs"
    );
}

#[tokio::test]
async fn serving_a_joined_room_does_not_rearm_its_reservation() {
    const ISSUER: &str = "issuer-serve-after-join";

    let manager = RoomManager::for_test_with_reservation_ttl(Duration::ZERO);
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, ISSUER).await;
    manager_join_user(&manager, &room, 1, &media_transport).await;

    // `/v1/channel` keeps serving a room its users are already in, and the
    // reaper does not check membership, so a deadline re-armed here would reap
    // an occupied room
    assert_eq!(serve_test_room(&manager, ISSUER).await.uuid(), room.uuid());
    assert!(
        !manager
            .has_room_reservation_deadline_for_test(room.uuid())
            .await,
        "serving a joined room must not rearm the reservation its join retired"
    );

    manager.check_expired_room_reservations().await;

    assert!(
        manager.get_by_uuid(room.uuid()).await.is_some(),
        "an occupied room must survive the reaper"
    );
    assert_eq!(
        manager.room_gauges().await,
        RoomGaugeValues {
            rooms: 1,
            users: 1,
            ..RoomGaugeValues::default()
        }
    );
}

#[tokio::test]
async fn a_joined_room_has_no_reservation_deadline() {
    let manager = RoomManager::for_test_with_reservation_ttl(LONG_EXPIRATION);
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, "issuer-joined-reservation").await;
    assert!(
        manager
            .has_room_reservation_deadline_for_test(room.uuid())
            .await,
        "an unjoined room should carry a reservation deadline"
    );

    let connection_id = manager_join_user(&manager, &room, 1, &media_transport).await;

    assert!(
        !manager
            .has_room_reservation_deadline_for_test(room.uuid())
            .await,
        "a successful first join should retire the reservation"
    );
    manager.check_expired_room_reservations().await;
    assert_eq!(
        manager.room_gauges().await,
        RoomGaugeValues {
            rooms: 1,
            users: 1,
            ..RoomGaugeValues::default()
        }
    );

    assert!(
        manager
            .close_session(
                room.uuid(),
                &UserId::Integer(1),
                connection_id,
                &media_transport,
            )
            .await
    );
    assert!(
        manager.get_by_uuid(room.uuid()).await.is_none(),
        "last-user cleanup should stay unchanged by reservations"
    );
}

#[tokio::test]
async fn expiry_loses_to_an_in_flight_first_join() {
    let manager = Arc::new(RoomManager::for_test_with_reservation_ttl(LONG_EXPIRATION));
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, "issuer-join-outruns-expiry").await;
    let gate = Arc::new(JoinPlacementTestGate::new(1));
    manager.set_join_placement_gate_for_test(Arc::clone(&gate));
    let pending_join = {
        let manager = Arc::clone(&manager);
        let room = Arc::clone(&room);
        let media_transport = media_transport.clone();
        tokio::spawn(async move { manager_join_user(&manager, &room, 1, &media_transport).await })
    };

    timeout(Duration::from_secs(5), gate.hold_all_ready())
        .await
        .expect("the join should reach the placement gate");
    assert!(
        manager
            .expire_room_reservation_now_for_test(room.uuid())
            .await
    );
    manager.check_expired_room_reservations().await;

    assert!(
        manager.get_by_uuid(room.uuid()).await.is_some(),
        "expiry must not reap a room whose first join already holds a lease"
    );

    gate.release_all().await;
    pending_join.await.expect("join task should not panic");

    assert!(
        !manager
            .has_room_reservation_deadline_for_test(room.uuid())
            .await,
        "the join that outran expiry should retire the reservation"
    );
    manager.check_expired_room_reservations().await;
    assert_eq!(
        manager.room_gauges().await,
        RoomGaugeValues {
            rooms: 1,
            users: 1,
            ..RoomGaugeValues::default()
        }
    );
}

#[tokio::test]
async fn a_join_between_the_deadline_and_the_reaper_keeps_the_room() {
    const ISSUER: &str = "issuer-join-outruns-reaper";

    let manager = RoomManager::for_test_with_reservation_ttl(Duration::ZERO);
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, ISSUER).await;
    let room_id = room.uuid().to_owned();

    // the deadline has already passed, but no reaper pass has claimed the row,
    // so the room is still in the directory and still joinable
    assert!(
        try_manager_join_user(&manager, &room_id, 1, &media_transport)
            .await
            .is_ok(),
        "a room past its deadline stays joinable until the reaper claims it"
    );

    assert!(
        manager
            .test_api()
            .has_session(&room_id, &UserId::Integer(1))
            .await,
        "the accepted join should leave a room session behind"
    );
    assert!(
        !manager
            .has_room_reservation_deadline_for_test(&room_id)
            .await,
        "the join that outran the reaper should retire the reservation"
    );

    manager.check_expired_room_reservations().await;

    assert!(
        manager.get_by_uuid(&room_id).await.is_some(),
        "a reaper pass after the join must leave the occupied room alone"
    );
    assert_eq!(
        manager.room_gauges().await,
        RoomGaugeValues {
            rooms: 1,
            users: 1,
            ..RoomGaugeValues::default()
        }
    );
}

#[tokio::test]
async fn a_failed_first_join_does_not_extend_the_reservation() {
    let manager = RoomManager::for_test_with_runtime_policy_and_reservation_ttl(
        super::super::RoomRuntimePolicy::new(
            RoomAdmissionPolicy::new(0),
            RuntimeFeatureFlags::default(),
            test_client_rtp_capabilities(),
        ),
        LONG_EXPIRATION,
    );
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, "issuer-failed-join-reservation").await;

    assert!(matches!(
        try_manager_join_user(&manager, room.uuid(), 1, &media_transport).await,
        Err(RoomManagerJoinError::RoomFull)
    ));

    assert!(
        manager
            .has_room_reservation_deadline_for_test(room.uuid())
            .await,
        "only a successful join may retire the reservation"
    );
    assert!(
        manager
            .expire_room_reservation_now_for_test(room.uuid())
            .await
    );
    manager.check_expired_room_reservations().await;

    assert!(manager.get_by_uuid(room.uuid()).await.is_none());
    assert_eq!(manager.room_gauges().await, RoomGaugeValues::default());
}

#[tokio::test(flavor = "current_thread")]
async fn room_reservation_expired_event_preserves_contract_fields() {
    let _guard = capture().await;
    let manager = RoomManager::for_test_with_reservation_ttl(Duration::ZERO);
    let room = serve_test_room(&manager, "issuer-reservation-expiry-events").await;

    // `assert_exact` requires exactly one matching event, so a reaper that re-logged every tick would fail here
    manager.check_expired_room_reservations().await;
    manager.check_expired_room_reservations().await;

    assert_exact(
        telemetry_event::ROOM_RESERVATION_EXPIRED,
        &[("room_id", Value::from(room.uuid()))],
    );
}

#[tokio::test]
async fn a_conflicting_reservation_leaves_the_current_room_in_place() {
    let manager = RoomManager::for_test();
    let room = serve_test_room(&manager, "issuer-conflicting-reservation").await;

    assert!(matches!(
        manager
            .serve_room(
                "issuer-conflicting-reservation",
                "other-key",
                &RoomConfig::default(),
                None,
            )
            .await,
        Err(RoomManagerServeError::ConflictingReservation)
    ));

    assert!(
        manager.get_by_uuid(room.uuid()).await.is_some(),
        "a rejected request must not retire the current room"
    );
    assert_eq!(
        serve_test_room(&manager, "issuer-conflicting-reservation")
            .await
            .uuid(),
        room.uuid(),
        "the issuer alias should still resolve to the room it reserved"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn room_reservation_conflict_event_preserves_contract_fields() {
    let _guard = capture().await;
    let manager = RoomManager::for_test();
    let room = serve_test_room(&manager, "issuer-reservation-conflict-events").await;
    let conflicting_config = RoomConfig {
        web_rtc_enabled: false,
        ..RoomConfig::default()
    };

    assert!(matches!(
        manager
            .serve_room(
                "issuer-reservation-conflict-events",
                TEST_ROOM_KEY,
                &conflicting_config,
                None,
            )
            .await,
        Err(RoomManagerServeError::ConflictingReservation)
    ));

    assert_exact(
        telemetry_event::ROOM_RESERVATION_CONFLICT,
        &[
            ("room_id", Value::from(room.uuid())),
            ("issuer", Value::from("issuer-reservation-conflict-events")),
            ("config", Value::from(format!("{conflicting_config:?}"))),
        ],
    );
}
