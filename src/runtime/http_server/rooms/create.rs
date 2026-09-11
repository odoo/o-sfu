//! Room creation with verified credentials and proxy-aware request configuration.

use std::sync::Arc;

use axum::{
    extract::{FromRef, FromRequestParts, Query, State},
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Response},
    routing::{MethodRouter, get},
};
use o_sfu_core::server::room::RoomManagerServeError;
use o_sfu_rfc::jwt::RegisteredJwtClaims;
use tracing::Instrument;

use super::super::{
    access::room_authorization_token,
    contract::{CreateRoomQuery, RoomResponse},
};
use crate::runtime::{
    RuntimeMetrics, RuntimeState,
    auth::{self, HttpRoomClaims, derive_key_from_seed},
    request_origin::RequestOrigin,
    room::{RoomConfig, RoomManager},
    telemetry,
};

pub(super) fn route() -> MethodRouter<RuntimeState> {
    get(create)
}

#[derive(Debug, Clone)]
struct Services {
    room_manager: Arc<RoomManager>,
    metrics: Arc<RuntimeMetrics>,
}

impl FromRef<RuntimeState> for Services {
    fn from_ref(state: &RuntimeState) -> Self {
        Self {
            room_manager: Arc::clone(&state.room_manager),
            metrics: Arc::clone(&state.metrics),
        }
    }
}

/// Room credentials verified with the global auth key and a present issuer.
///
/// Accepts the `Bearer` and legacy `jwt` authorization schemes.
/// `keySeed` takes precedence over `key` and must be nonempty and decodable.
/// Query configuration and the proxy-aware [`RequestOrigin`] accompany the
/// resolved room key. Extraction does not create or reserve a room.
///
/// # Errors
///
/// Extraction returns a [`StatusCode`] and records its room rejection counter:
/// `400 Bad Request` for invalid query parameters, absent key claims or failed
/// seed resolution, `401 Unauthorized` for missing or unsupported authorization
/// headers or failed JWT verification and `403 Forbidden` for a missing issuer.
/// Query rejection takes precedence over credential rejection.
#[derive(Debug, Clone)]
struct VerifiedRoomRequest {
    issuer: String,
    room_key: String,
    config: RoomConfig,
    origin: RequestOrigin,
}

/// Room creation endpoint used by Odoo to bind a channel key to an SFU room.
///
/// [`VerifiedRoomRequest`] owns JWT verification and request-origin projection.
async fn create(State(services): State<Services>, request: VerifiedRoomRequest) -> Response {
    async {
        let serve_result = services
            .room_manager
            .serve_room(
                &request.issuer,
                &request.room_key,
                &request.config,
                Some(request.origin.remote_address.as_str()),
            )
            .await;
        match serve_result {
            Ok(room) => {
                services.metrics.record_http_room_success();
                (
                    StatusCode::OK,
                    axum::Json(RoomResponse {
                        uuid: room.uuid().to_owned(),
                        url: request.origin.base_url,
                    }),
                )
                    .into_response()
            }
            Err(RoomManagerServeError::ConflictingReservation) => {
                services.metrics.record_http_room_conflict();
                StatusCode::CONFLICT.into_response()
            }
        }
    }
    .instrument(telemetry::http_request_span("room"))
    .await
}

impl FromRequestParts<RuntimeState> for VerifiedRoomRequest {
    type Rejection = StatusCode;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &RuntimeState,
    ) -> Result<Self, Self::Rejection> {
        let origin = match RequestOrigin::from_request_parts(parts, state).await {
            Ok(origin) => origin,
            Err(error) => match error {},
        };
        let Query(query) = Query::<CreateRoomQuery>::from_request_parts(parts, state)
            .await
            .map_err(|_error| record_rejection(state, StatusCode::BAD_REQUEST))?;
        let Some(token) = room_authorization_token(&parts.headers) else {
            return Err(record_rejection(state, StatusCode::UNAUTHORIZED));
        };
        let HttpRoomClaims {
            registered: RegisteredJwtClaims { iss, .. },
            key,
            key_seed,
        } = auth::verify::<HttpRoomClaims>(token, &state.config.auth.key)
            .map_err(|_error| record_rejection(state, StatusCode::UNAUTHORIZED))?;
        let Some(issuer) = iss else {
            return Err(record_rejection(state, StatusCode::FORBIDDEN));
        };
        let room_key = match (key, key_seed) {
            (None, None) => {
                return Err(record_rejection(state, StatusCode::BAD_REQUEST));
            }
            (Some(key), None) => key,
            (_, Some(seed)) if seed.is_empty() => {
                return Err(record_rejection(state, StatusCode::BAD_REQUEST));
            }
            (_, Some(seed)) => derive_key_from_seed(&state.config.auth.key, seed.as_ref())
                .map_err(|_error| record_rejection(state, StatusCode::BAD_REQUEST))?,
        };
        Ok(Self {
            issuer,
            room_key,
            config: RoomConfig {
                web_rtc_enabled: query.web_rtc_enabled(),
                recording_address: query.recording_address,
            },
            origin,
        })
    }
}

fn record_rejection(state: &RuntimeState, status: StatusCode) -> StatusCode {
    match status {
        StatusCode::UNAUTHORIZED => state.metrics.record_http_room_unauthorized(),
        StatusCode::FORBIDDEN => state.metrics.record_http_room_forbidden(),
        StatusCode::BAD_REQUEST => state.metrics.record_http_room_bad_request(),
        _ => {}
    }
    status
}
