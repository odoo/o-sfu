use super::fixtures::*;
use crate::core::server::{
    room::{UserCloseReason, UserOutbound},
    session::UserPermissions,
};

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
                TEST_ROOM_KEY.into(),
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
