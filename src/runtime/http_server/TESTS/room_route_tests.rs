use std::net::SocketAddr;

use axum::extract::ConnectInfo;

use super::fixtures::*;

fn room_token(
    issuer: Option<&str>,
    key: Option<&str>,
    key_seed: Option<&str>,
) -> TestResult<String> {
    require_some(
        signed_room_claims(issuer, key, key_seed),
        "room JWT should sign",
    )
}

fn room_builder(token: &str, scheme: &str) -> HttpRequestBuilder {
    Request::get(route::v1::CHANNEL)
        .header(header::HOST, "sfu.example.com")
        .header(header::AUTHORIZATION, format!("{scheme} {token}"))
}

async fn assert_room_status(builder: HttpRequestBuilder, expected: StatusCode) -> TestResult {
    route_status(
        &test_state(),
        builder,
        Body::empty(),
        expected,
        "room request should complete",
    )
    .await
}

async fn stats_first_remote_address(state: &RuntimeState) -> TestResult<String> {
    let payload: StatsResponse = route_json(
        state,
        Request::get(route::v1::STATS),
        Body::empty(),
        StatusCode::OK,
        "stats request should succeed",
    )
    .await?;
    assert_eq!(payload.len(), 1);
    Ok(
        require_some(payload.first(), "stats payload should contain one room")?
            .remote_address
            .clone(),
    )
}

#[tokio::test]
async fn room_rejects_unknown_authorization_scheme() -> TestResult {
    let token = room_token(Some("issuer-a"), Some(TEST_ROOM_KEY), None)?;
    assert_room_status(room_builder(&token, "Basic"), StatusCode::UNAUTHORIZED).await
}

#[tokio::test]
async fn room_accepts_legacy_jwt_authorization_scheme() -> TestResult {
    let token = room_token(Some("issuer-a"), Some("Y2hhbm5lbC1rZXk="), None)?;
    assert_room_status(room_builder(&token, "jwt"), StatusCode::OK).await
}

#[tokio::test]
async fn room_rejects_oversized_authorization_token() -> TestResult {
    let token = "a".repeat(auth::MAX_JWT_TOKEN_BYTES + 1);
    assert_room_status(room_builder(&token, "Bearer"), StatusCode::UNAUTHORIZED).await
}

#[tokio::test]
async fn room_requires_issuer_claim() -> TestResult {
    let token = room_token(None, None, None)?;
    assert_room_status(room_builder(&token, "Bearer"), StatusCode::FORBIDDEN).await
}

#[tokio::test]
async fn room_rejects_missing_key() -> TestResult {
    let token = room_token(Some("issuer-a"), None, None)?;
    assert_room_status(room_builder(&token, "Bearer"), StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn room_rejects_empty_seed_with_key() -> TestResult {
    let token = room_token(Some("issuer-a"), Some(TEST_ROOM_KEY), Some(""))?;
    assert_room_status(room_builder(&token, "Bearer"), StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn room_rejects_invalid_seed() -> TestResult {
    let token = room_token(Some("issuer-a"), None, Some("invalid-seed!"))?;
    assert_room_status(room_builder(&token, "Bearer"), StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn room_rejects_malformed_seed_with_key() -> TestResult {
    let token = room_token(Some("issuer-a"), Some(TEST_ROOM_KEY), Some("invalid-seed!"))?;
    assert_room_status(room_builder(&token, "Bearer"), StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn room_returns_uuid_and_request_base_url() -> TestResult {
    let token = room_token(Some("issuer-a"), Some("Y2hhbm5lbC1rZXk="), None)?;
    let payload: RoomResponse = route_json(
        &test_state(),
        room_builder(&token, "Bearer"),
        Body::empty(),
        StatusCode::OK,
        "room request should complete",
    )
    .await?;
    assert!(!payload.uuid.is_empty());
    assert_eq!(payload.url, "http://sfu.example.com");
    Ok(())
}

#[tokio::test]
async fn room_route_persists_query_config() -> TestResult {
    let token = room_token(Some("issuer-route-config"), Some(TEST_ROOM_KEY), None)?;
    let test_state = test_state_with_handles();
    let recording_address = "https://record.example.com/hook";
    let payload: RoomResponse = route_json(
        &test_state.state,
        Request::get(format!(
            "{}?webRTC=false&recordingAddress=https%3A%2F%2Frecord.example.com%2Fhook",
            route::v1::CHANNEL
        ))
        .header(header::HOST, "sfu.example.com")
        .header(header::AUTHORIZATION, format!("Bearer {token}")),
        Body::empty(),
        StatusCode::OK,
        "room request should complete",
    )
    .await?;
    let room = require_some(
        test_state.room_manager.get_by_uuid(&payload.uuid).await,
        "room should remain registered after route creation",
    )?;

    assert!(!room.web_rtc_enabled());
    assert_eq!(
        room.test_api().inspect().recording_address(),
        Some(recording_address)
    );
    let stats: StatsResponse = route_json(
        &test_state.state,
        Request::get(route::v1::STATS),
        Body::empty(),
        StatusCode::OK,
        "stats request should succeed",
    )
    .await?;
    let stats_room = require_some(
        stats.iter().find(|room| room.uuid == payload.uuid),
        "stats payload should contain created room",
    )?;
    assert!(!stats_room.web_rtc_enabled);
    Ok(())
}

#[tokio::test]
async fn room_ignores_forwarded_headers_when_proxy_trust_is_disabled() -> TestResult {
    let token = room_token(Some("issuer-a"), Some(TEST_ROOM_KEY), None)?;
    let state = test_state();
    let payload: RoomResponse = route_json(
        &state,
        room_builder(&token, "Bearer")
            .header("x-forwarded-for", "198.51.100.24, 10.0.0.1")
            .header(
                "x-forwarded-host",
                "proxy.example.com, internal.example.com",
            )
            .header("x-forwarded-proto", "https, http")
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 8070)))),
        Body::empty(),
        StatusCode::OK,
        "room request should complete",
    )
    .await?;
    assert_eq!(payload.url, "http://sfu.example.com");
    assert_eq!(stats_first_remote_address(&state).await?, "127.0.0.1");
    Ok(())
}

#[tokio::test]
async fn room_uses_forwarded_headers_when_proxy_trust_is_enabled() -> TestResult {
    let token = room_token(Some("issuer-a"), Some(TEST_ROOM_KEY), None)?;
    let mut state = test_state();
    state.config.http.trust_proxy_headers = true;
    let payload: RoomResponse = route_json(
        &state,
        room_builder(&token, "Bearer")
            .header(
                "x-forwarded-host",
                "proxy.example.com, internal.example.com",
            )
            .header("x-forwarded-proto", "https, http")
            .header("x-forwarded-for", "198.51.100.24, 10.0.0.1"),
        Body::empty(),
        StatusCode::OK,
        "room request should complete",
    )
    .await?;
    assert_eq!(payload.url, "https://proxy.example.com");
    assert_eq!(stats_first_remote_address(&state).await?, "198.51.100.24");
    Ok(())
}

#[tokio::test]
async fn room_route_updates_metrics_for_unauthorized_and_success_paths() -> TestResult {
    let state = test_state();
    route_status(
        &state,
        Request::get(route::v1::CHANNEL).header(header::HOST, "sfu.example.com"),
        Body::empty(),
        StatusCode::UNAUTHORIZED,
        "unauthorized room request should complete",
    )
    .await?;

    let token = room_token(Some("issuer-a"), Some(TEST_ROOM_KEY), None)?;
    route_status(
        &state,
        room_builder(&token, "Bearer"),
        Body::empty(),
        StatusCode::OK,
        "authorized room request should complete",
    )
    .await?;

    let metrics = state.metrics.snapshot();
    assert_eq!(metrics.http_room_requests(), 2);
    assert_eq!(metrics.http_room_unauthorized(), 1);
    assert_eq!(metrics.http_room_success(), 1);
    Ok(())
}

#[tokio::test]
async fn room_route_supports_key_seed_claim() -> TestResult {
    let token = room_token(Some("issuer-a"), None, Some("c2VlZC1rZXk="))?;
    let test_state = test_state_with_handles();
    let payload: RoomResponse = route_json(
        &test_state.state,
        room_builder(&token, "Bearer"),
        Body::empty(),
        StatusCode::OK,
        "room request should complete",
    )
    .await?;
    let room = require_some(
        test_state.room_manager.get_by_uuid(&payload.uuid).await,
        "room should remain registered after route creation",
    )?;
    let expected_key = require_some(
        auth::derive_key_from_seed(TEST_AUTH_KEY, "c2VlZC1rZXk=").ok(),
        "failed to derive expected key from seed",
    )?;
    assert_eq!(room.key(), expected_key);
    Ok(())
}

#[tokio::test]
async fn room_route_key_seed_claim_takes_precedence_over_key_claim() -> TestResult {
    let token = room_token(Some("issuer-a"), Some(TEST_ROOM_KEY), Some("c2VlZC1rZXk="))?;
    let test_state = test_state_with_handles();
    let payload: RoomResponse = route_json(
        &test_state.state,
        room_builder(&token, "Bearer"),
        Body::empty(),
        StatusCode::OK,
        "room request should complete",
    )
    .await?;
    let room = require_some(
        test_state.room_manager.get_by_uuid(&payload.uuid).await,
        "room should remain registered after route creation",
    )?;
    let expected_key = require_some(
        auth::derive_key_from_seed(TEST_AUTH_KEY, "c2VlZC1rZXk=").ok(),
        "failed to derive expected key from seed",
    )?;
    assert_eq!(room.key(), expected_key);
    Ok(())
}

#[tokio::test]
async fn room_route_returns_conflict_for_a_reservation_with_another_key() -> TestResult {
    let state = test_state();
    let token = room_token(Some("issuer-conflict-key"), Some(TEST_ROOM_KEY), None)?;
    route_status(
        &state,
        room_builder(&token, "Bearer"),
        Body::empty(),
        StatusCode::OK,
        "first room request should complete",
    )
    .await?;

    let other_key_token = room_token(Some("issuer-conflict-key"), None, Some("c2VlZC1rZXk="))?;
    route_status(
        &state,
        room_builder(&other_key_token, "Bearer"),
        Body::empty(),
        StatusCode::CONFLICT,
        "a second key for the same issuer should conflict",
    )
    .await?;

    let metrics = state.metrics.snapshot();
    assert_eq!(metrics.http_room_requests(), 2);
    assert_eq!(metrics.http_room_success(), 1);
    assert_eq!(metrics.http_room_conflict(), 1);
    Ok(())
}

#[tokio::test]
async fn room_route_returns_conflict_for_a_reservation_with_another_config() -> TestResult {
    let state = test_state();
    let token = room_token(Some("issuer-conflict-config"), Some(TEST_ROOM_KEY), None)?;
    route_status(
        &state,
        room_builder(&token, "Bearer"),
        Body::empty(),
        StatusCode::OK,
        "first room request should complete",
    )
    .await?;

    route_status(
        &state,
        Request::get(format!("{}?webRTC=false", route::v1::CHANNEL))
            .header(header::HOST, "sfu.example.com")
            .header(header::AUTHORIZATION, format!("Bearer {token}")),
        Body::empty(),
        StatusCode::CONFLICT,
        "a second config for the same issuer should conflict",
    )
    .await?;

    assert_eq!(state.metrics.snapshot().http_room_conflict(), 1);
    Ok(())
}

#[tokio::test]
async fn room_route_reuses_a_reservation_that_repeats_key_and_config() -> TestResult {
    let test_state = test_state_with_handles();
    let token = room_token(Some("issuer-repeat"), Some(TEST_ROOM_KEY), None)?;
    let first: RoomResponse = route_json(
        &test_state.state,
        room_builder(&token, "Bearer"),
        Body::empty(),
        StatusCode::OK,
        "first room request should complete",
    )
    .await?;
    let second: RoomResponse = route_json(
        &test_state.state,
        room_builder(&token, "Bearer"),
        Body::empty(),
        StatusCode::OK,
        "repeat room request should complete",
    )
    .await?;

    assert_eq!(
        first.uuid, second.uuid,
        "a repeat request with the same key and config should reuse the reservation"
    );
    assert_eq!(test_state.state.metrics.snapshot().http_room_conflict(), 0);
    Ok(())
}

#[tokio::test]
async fn malformed_room_route_query_counts_as_bad_request() -> TestResult {
    let state = test_state();
    let token = room_token(Some("issuer-malformed-query"), Some(TEST_ROOM_KEY), None)?;

    route_status(
        &state,
        Request::get(format!("{}?webRTC=invalid", route::v1::CHANNEL))
            .header(header::HOST, "sfu.example.com")
            .header(header::AUTHORIZATION, format!("Bearer {token}")),
        Body::empty(),
        StatusCode::BAD_REQUEST,
        "authorized room request with malformed query should fail",
    )
    .await?;

    let metrics = state.metrics.snapshot();
    assert_eq!(metrics.http_room_requests(), 1);
    assert_eq!(metrics.http_room_bad_request(), 1);
    Ok(())
}
