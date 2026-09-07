pub(super) use std::{collections::BTreeMap, fmt::Debug, result::Result as StdResult};

pub(super) use anyhow::{Result, anyhow};
pub(super) use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header, request::Builder as HttpRequestBuilder},
    response::Response as AxumResponse,
};
pub(super) use o_sfu_protocol::wire::UserId;
pub(super) use o_sfu_telemetry::diagnostics::{
    DiagnosticsRoomDetail, DiagnosticsRoomSummary, DiagnosticsSourceSelectionReason,
    DiagnosticsSummaryResponse, DiagnosticsUserDetail, DiagnosticsUserSummary,
    DiagnosticsWorkerSummary,
};
pub(super) use serde::de::DeserializeOwned;
pub(super) use tower::util::ServiceExt;

pub(super) use super::super::app;
pub(super) use crate::runtime::{
    RuntimeState,
    auth::{self, HttpDisconnectClaims, HttpRoomClaims, RegisteredJwtClaims},
    http_server::contract::{CreateRoomQuery, NoopResponse, RoomResponse, StatsResponse, route},
    media_transport::MediaTransport,
    room::{Room, RoomConfig, UserOutboundReceiver},
    test_support::{
        RuntimeMetricsSnapshotTestExt, RuntimeTestBuilder, RuntimeTestState, TEST_AUTH_KEY,
        TEST_ROOM_KEY, test_outbound_sender,
    },
};

pub(super) type TestResult<T = ()> = Result<T>;

pub(super) fn require_some<T>(value: Option<T>, context: &'static str) -> TestResult<T> {
    value.ok_or_else(|| anyhow!(context))
}

pub(super) fn require_ok<T, E>(value: StdResult<T, E>, context: &'static str) -> TestResult<T>
where
    E: Debug,
{
    value.map_err(|error| anyhow!("{context}: {error:?}"))
}

pub(super) fn test_state() -> RuntimeState {
    RuntimeTestBuilder::new().build_runtime_state()
}

pub(super) fn test_state_with_handles() -> RuntimeTestState {
    RuntimeTestBuilder::new().build_state()
}

pub(super) fn signed_room_claims(
    issuer: Option<&str>,
    key: Option<&str>,
    key_seed: Option<&str>,
) -> Option<String> {
    auth::sign(
        &HttpRoomClaims {
            registered: RegisteredJwtClaims {
                iss: issuer.map(str::to_owned),
                ..RegisteredJwtClaims::default()
            },
            key: key.map(str::to_owned),
            key_seed: key_seed.map(str::to_owned),
        },
        TEST_AUTH_KEY,
    )
    .ok()
}

pub(super) fn signed_disconnect_claims(
    user_ids_by_room: BTreeMap<String, Vec<UserId>>,
) -> Option<String> {
    auth::sign(
        &HttpDisconnectClaims {
            registered: RegisteredJwtClaims::default(),
            user_ids_by_room,
        },
        TEST_AUTH_KEY,
    )
    .ok()
}

pub(super) async fn route_response(
    state: &RuntimeState,
    builder: HttpRequestBuilder,
    body: Body,
    expected_status: StatusCode,
    context: &'static str,
) -> TestResult<AxumResponse> {
    let request = require_ok(builder.body(body), context)?;
    let response = require_ok(
        app(state.clone(), state.config.http.bind_address)
            .oneshot(request)
            .await,
        context,
    )?;
    assert_eq!(response.status(), expected_status);
    Ok(response)
}

pub(super) async fn route_status(
    state: &RuntimeState,
    builder: HttpRequestBuilder,
    body: Body,
    expected_status: StatusCode,
    context: &'static str,
) -> TestResult {
    route_response(state, builder, body, expected_status, context)
        .await
        .map(drop)
}

pub(super) async fn route_json<T>(
    state: &RuntimeState,
    builder: HttpRequestBuilder,
    body: Body,
    expected_status: StatusCode,
    context: &'static str,
) -> TestResult<T>
where
    T: DeserializeOwned,
{
    let response = route_response(state, builder, body, expected_status, context).await?;
    let bytes = require_ok(to_bytes(response.into_body(), usize::MAX).await, context)?;
    require_ok(serde_json::from_slice::<T>(&bytes), context)
}

pub(super) async fn response_text(
    response: AxumResponse,
    context: &'static str,
) -> TestResult<String> {
    let bytes = require_ok(to_bytes(response.into_body(), usize::MAX).await, context)?;
    require_ok(String::from_utf8(bytes.to_vec()), context)
}

pub(super) async fn create_transport_session_offer(
    room: &Room,
    user_id: &UserId,
    media_transport: &MediaTransport,
) -> Option<()> {
    let connection_id = room.test_api().user_connection_id(user_id).await?;
    let session_key = room.transport_user_key(user_id, connection_id).await;
    media_transport
        .create_initial_session_offer(room.uuid(), &session_key)
        .await
        .ok()?;
    Some(())
}
