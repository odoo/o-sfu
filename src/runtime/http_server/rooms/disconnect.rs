//! Bulk disconnect with bounded body JWTs and normalized runtime user IDs.

use std::{str, sync::Arc};

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, FromRef, FromRequest, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{MethodRouter, post},
};
use tracing::Instrument;

use crate::runtime::{
    MediaTransport, RuntimeMetrics, RuntimeState,
    auth::{self, HttpDisconnectClaims},
    room::RoomManager,
    telemetry,
};

const MAX_BODY_BYTES: usize = 16 * 1024;

pub(super) fn route() -> MethodRouter<RuntimeState> {
    post(disconnect).layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
}

#[derive(Debug, Clone)]
struct Services {
    room_manager: Arc<RoomManager>,
    media_transport: MediaTransport,
    metrics: Arc<RuntimeMetrics>,
}

impl FromRef<RuntimeState> for Services {
    fn from_ref(state: &RuntimeState) -> Self {
        Self {
            room_manager: Arc::clone(&state.room_manager),
            media_transport: state.media_transport.clone(),
            metrics: Arc::clone(&state.metrics),
        }
    }
}

/// Body JWT verified with the global auth key and runtime-normalized user IDs.
///
/// # Errors
///
/// Extraction returns a [`Response`]: `400 Bad Request` for non-UTF-8 bodies
/// or `422 Unprocessable Entity` when JWT verification fails. Both record the
/// corresponding disconnect rejection counter. Body buffering failures retain
/// Axum's rejection response, including `413 Payload Too Large` for the route's
/// body limit, without incrementing those counters.
#[derive(Debug, Clone, PartialEq, Eq)]
struct VerifiedDisconnectClaims(HttpDisconnectClaims);

/// Bulk-disconnect endpoint used by Odoo to remove users from active rooms.
///
/// [`VerifiedDisconnectClaims`] owns JWT verification and request-body decoding.
async fn disconnect(
    State(services): State<Services>,
    VerifiedDisconnectClaims(claims): VerifiedDisconnectClaims,
) -> Response {
    async {
        for (room_id, user_ids) in &claims.user_ids_by_room {
            services
                .room_manager
                .disconnect_users(room_id, user_ids, &services.media_transport)
                .await;
        }
        services.metrics.record_http_disconnect_success();
        StatusCode::OK.into_response()
    }
    .instrument(telemetry::http_request_span("disconnect"))
    .await
}

impl FromRequest<RuntimeState> for VerifiedDisconnectClaims {
    type Rejection = Response;

    async fn from_request(req: Request, state: &RuntimeState) -> Result<Self, Self::Rejection> {
        let body = Bytes::from_request(req, state)
            .await
            .map_err(IntoResponse::into_response)?;
        let token = str::from_utf8(&body)
            .map_err(|_error| record_rejection(state, StatusCode::BAD_REQUEST))?;
        let mut claims = auth::verify::<HttpDisconnectClaims>(token, &state.config.auth.key)
            .map_err(|_error| record_rejection(state, StatusCode::UNPROCESSABLE_ENTITY))?;
        claims.normalize_runtime_user_ids();
        Ok(Self(claims))
    }
}

fn record_rejection(state: &RuntimeState, status: StatusCode) -> Response {
    match status {
        StatusCode::BAD_REQUEST => state.metrics.record_http_disconnect_bad_request(),
        StatusCode::UNPROCESSABLE_ENTITY => {
            state.metrics.record_http_disconnect_unprocessable_entity();
        }
        _ => {}
    }
    status.into_response()
}
