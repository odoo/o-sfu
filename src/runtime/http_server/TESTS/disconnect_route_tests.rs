use std::time::Duration;

use tokio::{
    sync::oneshot,
    task::{JoinHandle, yield_now},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::fixtures::*;
use crate::{
    config::DeadlineDuration,
    core::server::{
        room::{RoomEventMessage, UserCloseReason, UserOutbound},
        session::UserPermissions,
    },
    runtime::Runtime,
};

const MUTATION_TIMEOUT: Duration = Duration::from_secs(1);

async fn wait_for_disconnect(room: &Room, user_id: &UserId) -> TestResult {
    timeout(MUTATION_TIMEOUT, async {
        while room.test_api().has_session(user_id).await {
            yield_now().await;
        }
    })
    .await?;
    Ok(())
}

fn disconnect_request(
    state: &RuntimeState,
    users: BTreeMap<String, Vec<UserId>>,
) -> TestResult<JoinHandle<TestResult>> {
    let token = require_some(
        signed_disconnect_claims(users),
        "disconnect JWT should sign",
    )?;
    let state = state.clone();
    Ok(tokio::spawn(async move {
        route_status(
            &state,
            Request::post(route::v1::DISCONNECT),
            Body::from(token),
            StatusCode::OK,
            "disconnect request should complete",
        )
        .await
    }))
}

#[tokio::test]
async fn shutdown_drains_lost_disconnect_request_through_all_room_effects() -> TestResult {
    let mut config = RuntimeTestBuilder::new().config().clone();
    config.http.shutdown_timeout = DeadlineDuration::from_millis(500)?;
    let runtime = Runtime::new(&config)?;
    let shutdown = CancellationToken::new();
    let trigger = shutdown.clone();
    let (state_tx, state_rx) = oneshot::channel();
    let mut server = tokio::spawn(runtime.serve(
        move |state, listener_shutdown| async move {
            let _result = state_tx.send(state);
            listener_shutdown.cancelled().await;
            Ok(())
        },
        async move {
            trigger.cancelled().await;
            Ok(())
        },
    ));
    let state = timeout(MUTATION_TIMEOUT, state_rx).await??;
    let room_manager = &state.room_manager;
    let mut rooms = Vec::new();
    for issuer in ["issuer-lost-a", "issuer-lost-b"] {
        rooms.push(require_ok(
            room_manager
                .serve_room(issuer, test_room_key(), &RoomConfig::default(), None)
                .await,
            "test room should be served",
        )?);
    }
    rooms.sort_by(|left, right| left.uuid().cmp(right.uuid()));
    let alice_id = UserId::Integer(1);
    let bob_id = UserId::Integer(2);
    let carol_id = UserId::Integer(3);
    let (alice_tx, mut alice_rx) = test_outbound_sender(&state);
    let (bob_tx, mut bob_rx) = test_outbound_sender(&state);
    let (carol_tx, mut carol_rx) = test_outbound_sender(&state);
    for (room, user_id, sender) in [
        (&rooms[0], &alice_id, alice_tx),
        (&rooms[0], &bob_id, bob_tx),
        (&rooms[1], &carol_id, carol_tx),
    ] {
        require_ok(
            room.test_api()
                .join_user(user_id.clone(), None, UserPermissions::default(), sender)
                .await,
            "user should join",
        )?;
    }
    for (room, user_id) in [(&rooms[0], &alice_id), (&rooms[1], &carol_id)] {
        require_some(
            create_transport_session_offer(room, user_id, &state.media_transport).await,
            "transport session should start",
        )?;
    }
    let release = require_some(
        state.media_transport.test_api().pause_first_worker().await,
        "worker should pause",
    )?;
    let request = disconnect_request(
        &state,
        BTreeMap::from([
            (rooms[0].uuid().to_owned(), vec![alice_id.clone()]),
            (rooms[1].uuid().to_owned(), vec![carol_id.clone()]),
        ]),
    )?;
    wait_for_disconnect(&rooms[0], &alice_id).await?;
    assert!(rooms[1].test_api().has_session(&carol_id).await);
    request.abort();
    assert!(request.await.is_err());
    shutdown.cancel();
    timeout(MUTATION_TIMEOUT, state.session_shutdown.cancelled()).await?;
    assert!(
        timeout(Duration::from_millis(20), &mut server)
            .await
            .is_err()
    );
    assert_eq!(state.metrics.snapshot().active_transport_users(), 2);
    assert_eq!(state.session_tasks.len(), 1);
    release.send(())?;
    timeout(MUTATION_TIMEOUT, server).await???;
    for receiver in [&mut alice_rx, &mut carol_rx] {
        assert!(matches!(
            receiver.try_recv(),
            Ok(UserOutbound::Close(UserCloseReason::RemovedByRuntime))
        ));
    }
    assert!(
        matches!(bob_rx.try_recv(), Ok(UserOutbound::Message(RoomEventMessage::UserDeparted { user_id })) if user_id == alice_id)
    );
    assert!(room_manager.get_by_uuid(rooms[1].uuid()).await.is_none());
    assert!(room_manager.get_by_uuid(rooms[0].uuid()).await.is_some());
    let metrics = state.metrics.snapshot();
    assert_eq!(metrics.active_transport_users(), 0);
    assert_eq!(metrics.http_disconnect_success(), 1);
    Ok(())
}

#[tokio::test]
async fn lost_disconnect_requests_retain_the_mutation_bound() -> TestResult {
    let test = RuntimeTestBuilder::new()
        .max_http_connections(1)
        .build_state();
    let room = require_ok(
        test.room_manager
            .serve_room(
                "issuer-bound",
                test_room_key(),
                &RoomConfig::default(),
                None,
            )
            .await,
        "test room should be served",
    )?;
    let first = UserId::Integer(1);
    let waiting = UserId::Integer(2);
    let (first_tx, mut first_rx) = test_outbound_sender(&test.state);
    let (waiting_tx, mut waiting_rx) = test_outbound_sender(&test.state);
    for (user_id, sender) in [(&first, first_tx), (&waiting, waiting_tx)] {
        require_ok(
            room.test_api()
                .join_user(user_id.clone(), None, UserPermissions::default(), sender)
                .await,
            "user should join",
        )?;
        require_some(
            create_transport_session_offer(&room, user_id, &test.media_transport).await,
            "transport session should start",
        )?;
    }
    let release = require_some(
        test.media_transport.test_api().pause_first_worker().await,
        "worker should pause",
    )?;
    let request = disconnect_request(
        &test.state,
        BTreeMap::from([(room.uuid().to_owned(), vec![first.clone()])]),
    )?;
    wait_for_disconnect(&room, &first).await?;
    request.abort();
    assert!(request.await.is_err());
    for _ in 0..2 {
        let mut request = disconnect_request(
            &test.state,
            BTreeMap::from([(room.uuid().to_owned(), vec![waiting.clone()])]),
        )?;
        assert!(
            timeout(Duration::from_millis(20), &mut request)
                .await
                .is_err()
        );
        request.abort();
        assert!(request.await.is_err());
        assert!(room.test_api().has_session(&waiting).await);
        assert_eq!(test.state.session_tasks.len(), 1);
    }
    let mut request = disconnect_request(
        &test.state,
        BTreeMap::from([(room.uuid().to_owned(), vec![waiting])]),
    )?;
    assert!(
        timeout(Duration::from_millis(20), &mut request)
            .await
            .is_err()
    );
    release.send(())?;
    timeout(MUTATION_TIMEOUT, request).await???;
    test.state.session_tasks.close();
    timeout(MUTATION_TIMEOUT, test.state.session_tasks.wait()).await?;
    assert!(matches!(
        waiting_rx.try_recv(),
        Ok(UserOutbound::Message(RoomEventMessage::UserDeparted { user_id })) if user_id == first
    ));
    for receiver in [&mut first_rx, &mut waiting_rx] {
        assert!(matches!(
            receiver.try_recv(),
            Ok(UserOutbound::Close(UserCloseReason::RemovedByRuntime))
        ));
    }
    assert!(test.room_manager.get_by_uuid(room.uuid()).await.is_none());
    assert_eq!(test.state.metrics.snapshot().active_transport_users(), 0);
    assert_eq!(test.state.metrics.snapshot().http_disconnect_success(), 2);
    Ok(())
}

#[tokio::test]
async fn disconnect_rejects_oversized_body_before_auth_decode() -> TestResult {
    let oversized_body = "x".repeat(auth::MAX_JWT_TOKEN_BYTES + 1);
    route_status(
        &test_state(),
        Request::post(route::v1::DISCONNECT),
        Body::from(oversized_body),
        StatusCode::PAYLOAD_TOO_LARGE,
        "oversized disconnect request should complete",
    )
    .await
}

#[tokio::test]
async fn disconnect_route_kicks_live_users() -> TestResult {
    let test_state = test_state_with_handles();
    let room = require_ok(
        test_state
            .room_manager
            .serve_room(
                "issuer-disconnect",
                test_room_key(),
                &RoomConfig::default(),
                None,
            )
            .await,
        "test room should be served",
    )?;
    let alice_id = UserId::Integer(1);
    let bob_id = UserId::Integer(2);
    let (alice_tx, mut alice_rx) = test_outbound_sender(&test_state.state);
    let (bob_tx, _bob_rx) = test_outbound_sender(&test_state.state);

    require_ok(
        room.test_api()
            .join_user(alice_id.clone(), None, UserPermissions::default(), alice_tx)
            .await,
        "alice should join",
    )?;
    require_ok(
        room.test_api()
            .join_user(bob_id.clone(), None, UserPermissions::default(), bob_tx)
            .await,
        "bob should join",
    )?;

    let token = require_some(
        signed_disconnect_claims(BTreeMap::from([(
            room.uuid().to_owned(),
            vec![alice_id.clone()],
        )])),
        "disconnect JWT should sign",
    )?;
    route_status(
        &test_state.state,
        Request::post(route::v1::DISCONNECT),
        Body::from(token),
        StatusCode::OK,
        "disconnect request should complete",
    )
    .await?;

    match require_ok(alice_rx.try_recv(), "alice should receive runtime close")? {
        UserOutbound::Close(UserCloseReason::RemovedByRuntime) => {}
        other => return Err(anyhow!("alice should receive runtime close: {other:?}")),
    }
    assert!(
        !test_state
            .room_manager
            .test_api()
            .has_session(room.uuid(), &alice_id)
            .await
    );
    assert!(
        test_state
            .room_manager
            .test_api()
            .has_session(room.uuid(), &bob_id)
            .await
    );
    Ok(())
}

#[tokio::test]
async fn disconnect_route_updates_metrics_for_all_outcomes() -> TestResult {
    let state = test_state();

    route_status(
        &state,
        Request::post(route::v1::DISCONNECT),
        Body::from(vec![0xF0_u8, 0x28, 0x8C, 0x28]),
        StatusCode::BAD_REQUEST,
        "invalid UTF-8 disconnect request should complete",
    )
    .await?;

    route_status(
        &state,
        Request::post(route::v1::DISCONNECT),
        Body::from("invalid-token"),
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid-token disconnect request should complete",
    )
    .await?;

    let token = require_some(
        signed_disconnect_claims(BTreeMap::new()),
        "disconnect JWT should sign",
    )?;
    route_status(
        &state,
        Request::post(route::v1::DISCONNECT),
        Body::from(token),
        StatusCode::OK,
        "valid disconnect request should complete",
    )
    .await?;

    let metrics = state.metrics.snapshot();
    assert_eq!(metrics.http_disconnect_requests(), 3);
    assert_eq!(metrics.http_disconnect_bad_request(), 1);
    assert_eq!(metrics.http_disconnect_unprocessable_entity(), 1);
    assert_eq!(metrics.http_disconnect_success(), 1);
    Ok(())
}

#[tokio::test]
async fn disconnect_requires_unexpired_credentials() -> TestResult {
    for exp in [None, Some(0_u64)] {
        let mut claims = serde_json::json!({ "sessionIdsByChannel": {} });
        if let Some(exp) = exp {
            require_some(
                claims.as_object_mut(),
                "credential claims should be an object",
            )?
            .insert("exp".to_owned(), serde_json::json!(exp));
        }
        let token = auth::sign(&claims, &secrecy::SecretString::from(TEST_AUTH_KEY))?;
        route_status(
            &test_state(),
            Request::post(route::v1::DISCONNECT),
            Body::from(secrecy::ExposeSecret::expose_secret(&token).to_owned()),
            StatusCode::UNPROCESSABLE_ENTITY,
            "disconnect requires expiry",
        )
        .await?;
    }
    Ok(())
}
