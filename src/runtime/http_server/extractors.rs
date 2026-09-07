use std::{net::SocketAddr, str, sync::Arc};

use axum::{
    body::Bytes,
    extract::{FromRef, FromRequest, FromRequestParts, Query, Request},
    http::{HeaderMap, StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
};
pub use o_sfu_rfc::jwt::RegisteredJwtClaims;

use crate::runtime::{
    MediaTransport, RuntimeMetrics, RuntimeState,
    auth::{self, HttpDisconnectClaims, HttpRoomClaims, derive_key_from_seed},
    http_server::contract::CreateRoomQuery,
    request_origin::RequestOrigin,
    room::{RoomConfig, RoomManager},
};

#[derive(Debug, Clone)]
pub(super) struct RoomServices {
    pub(super) room_manager: Arc<RoomManager>,
    pub(super) media_transport: MediaTransport,
    pub(super) metrics: Arc<RuntimeMetrics>,
}

#[derive(Debug, Clone)]
pub(super) struct DiagnosticsServices {
    pub(super) room_manager: Arc<RoomManager>,
    pub(super) media_transport: MediaTransport,
}

#[derive(Debug, Clone)]
pub(super) struct MetricsServices {
    pub(super) room_manager: Arc<RoomManager>,
    pub(super) metrics: Arc<RuntimeMetrics>,
}

#[derive(Debug, Clone)]
pub(super) struct VerifiedRoomRequest {
    pub(super) issuer: String,
    pub(super) room_key: String,
    pub(super) config: RoomConfig,
    pub(super) origin: RequestOrigin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct VerifiedDisconnectClaims(pub(super) HttpDisconnectClaims);

/// Operator authorization bound to the listener that serves the router.
#[derive(Clone)]
pub(super) struct OperatorAccessPolicy {
    auth_token: Option<Arc<str>>,
    listener_is_loopback: bool,
}

impl OperatorAccessPolicy {
    pub(super) fn new(auth_token: Option<&str>, listener_address: SocketAddr) -> Self {
        Self {
            auth_token: auth_token.map(Arc::from),
            listener_is_loopback: listener_address.ip().is_loopback(),
        }
    }
}

/// A configured token disables the loopback fallback.
pub(super) struct OperatorAccess;

impl FromRef<RuntimeState> for RoomServices {
    fn from_ref(state: &RuntimeState) -> Self {
        Self {
            room_manager: Arc::clone(&state.room_manager),
            media_transport: state.media_transport.clone(),
            metrics: Arc::clone(&state.metrics),
        }
    }
}

impl FromRef<RuntimeState> for DiagnosticsServices {
    fn from_ref(state: &RuntimeState) -> Self {
        Self {
            room_manager: Arc::clone(&state.room_manager),
            media_transport: state.media_transport.clone(),
        }
    }
}

impl FromRef<RuntimeState> for MetricsServices {
    fn from_ref(state: &RuntimeState) -> Self {
        Self {
            room_manager: Arc::clone(&state.room_manager),
            metrics: Arc::clone(&state.metrics),
        }
    }
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
            .map_err(|_error| record_room_rejection(state, StatusCode::BAD_REQUEST))?;
        let Some(token) = room_authorization_token(&parts.headers) else {
            return Err(record_room_rejection(state, StatusCode::UNAUTHORIZED));
        };
        let HttpRoomClaims {
            registered: RegisteredJwtClaims { iss, .. },
            key,
            key_seed,
        } = auth::verify::<HttpRoomClaims>(token, &state.config.auth.key)
            .map_err(|_error| record_room_rejection(state, StatusCode::UNAUTHORIZED))?;
        let Some(issuer) = iss else {
            return Err(record_room_rejection(state, StatusCode::FORBIDDEN));
        };
        let room_key = match (key, key_seed) {
            (None, None) => {
                return Err(record_room_rejection(state, StatusCode::BAD_REQUEST));
            }
            (Some(key), None) => key,
            (_, Some(seed)) if seed.is_empty() => {
                return Err(record_room_rejection(state, StatusCode::BAD_REQUEST));
            }
            (_, Some(seed)) => derive_key_from_seed(&state.config.auth.key, seed.as_ref())
                .map_err(|_error| record_room_rejection(state, StatusCode::BAD_REQUEST))?,
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

impl FromRequest<RuntimeState> for VerifiedDisconnectClaims {
    type Rejection = Response;

    async fn from_request(req: Request, state: &RuntimeState) -> Result<Self, Self::Rejection> {
        let body = Bytes::from_request(req, state)
            .await
            .map_err(IntoResponse::into_response)?;
        let token = str::from_utf8(&body)
            .map_err(|_error| record_disconnect_rejection(state, StatusCode::BAD_REQUEST))?;
        let mut claims = auth::verify::<HttpDisconnectClaims>(token, &state.config.auth.key)
            .map_err(|_error| {
                record_disconnect_rejection(state, StatusCode::UNPROCESSABLE_ENTITY)
            })?;
        claims.normalize_runtime_user_ids();
        Ok(Self(claims))
    }
}

impl FromRequestParts<OperatorAccessPolicy> for OperatorAccess {
    type Rejection = StatusCode;

    async fn from_request_parts(
        parts: &mut Parts,
        policy: &OperatorAccessPolicy,
    ) -> Result<Self, Self::Rejection> {
        if let Some(expected_token) = policy.auth_token.as_deref() {
            return match bearer_authorization_token(&parts.headers) {
                Some(actual_token) if tokens_match(actual_token, expected_token) => Ok(Self),
                _ => Err(StatusCode::UNAUTHORIZED),
            };
        }
        if policy.listener_is_loopback {
            Ok(Self)
        } else {
            Err(StatusCode::FORBIDDEN)
        }
    }
}

fn record_room_rejection(state: &RuntimeState, status: StatusCode) -> StatusCode {
    match status {
        StatusCode::UNAUTHORIZED => state.metrics.record_http_room_unauthorized(),
        StatusCode::FORBIDDEN => state.metrics.record_http_room_forbidden(),
        StatusCode::BAD_REQUEST => state.metrics.record_http_room_bad_request(),
        _ => {}
    }
    status
}

fn record_disconnect_rejection(state: &RuntimeState, status: StatusCode) -> Response {
    match status {
        StatusCode::BAD_REQUEST => state.metrics.record_http_disconnect_bad_request(),
        StatusCode::UNPROCESSABLE_ENTITY => {
            state.metrics.record_http_disconnect_unprocessable_entity();
        }
        _ => {}
    }
    status.into_response()
}

fn room_authorization_token(headers: &HeaderMap) -> Option<&str> {
    authorization_token(headers, &["Bearer", "jwt"])
}

fn bearer_authorization_token(headers: &HeaderMap) -> Option<&str> {
    authorization_token(headers, &["Bearer"])
}

fn authorization_token<'headers>(
    headers: &'headers HeaderMap,
    accepted_schemes: &[&str],
) -> Option<&'headers str> {
    let value = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())?;
    let (scheme, token) = value.split_once(' ')?;
    if !accepted_schemes
        .iter()
        .any(|accepted_scheme| scheme.eq_ignore_ascii_case(accepted_scheme))
    {
        return None;
    }
    let token = token.trim_start();
    if token.is_empty() {
        return None;
    }
    Some(token)
}

fn tokens_match(actual: &str, expected: &str) -> bool {
    let mut diff = actual.len() ^ expected.len();
    for (actual, expected) in actual.bytes().zip(expected.bytes()) {
        diff |= usize::from(actual ^ expected);
    }
    diff == 0
}
