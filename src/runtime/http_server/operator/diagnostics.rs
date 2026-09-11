//! HTTP response handlers for runtime diagnostics and node graphs.

use std::sync::Arc;

use axum::{
    extract::{FromRef, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::runtime::{MediaTransport, RuntimeState, diagnostics, room::RoomManager};

#[derive(Debug, Clone)]
pub(super) struct DiagnosticsServices {
    room_manager: Arc<RoomManager>,
    media_transport: MediaTransport,
}

impl FromRef<RuntimeState> for DiagnosticsServices {
    fn from_ref(state: &RuntimeState) -> Self {
        Self {
            room_manager: Arc::clone(&state.room_manager),
            media_transport: state.media_transport.clone(),
        }
    }
}

/// diagnostics overview for room, user and publication totals
pub(super) async fn summary(State(services): State<DiagnosticsServices>) -> Response {
    axum::Json(
        diagnostics::summary_response(&services.room_manager, &services.media_transport).await,
    )
    .into_response()
}

/// diagnostics inventory for active rooms
pub(super) async fn rooms(State(services): State<DiagnosticsServices>) -> Response {
    axum::Json(diagnostics::rooms_response(&services.room_manager, &services.media_transport).await)
        .into_response()
}

/// diagnostics inventory for media workers and load pressure
pub(super) async fn workers(State(services): State<DiagnosticsServices>) -> Response {
    axum::Json(
        diagnostics::workers_response(&services.room_manager, &services.media_transport).await,
    )
    .into_response()
}

/// room diagnostics with users and sources
pub(super) async fn room_detail(
    State(services): State<DiagnosticsServices>,
    Path(room_id): Path<String>,
) -> Response {
    let payload = diagnostics::room_detail_response(
        &services.room_manager,
        &services.media_transport,
        &room_id,
    )
    .await;
    optional_response(payload)
}

/// user rows for one room
pub(super) async fn room_users(
    State(services): State<DiagnosticsServices>,
    Path(room_id): Path<String>,
) -> Response {
    let payload = diagnostics::room_users_response(
        &services.room_manager,
        &services.media_transport,
        &room_id,
    )
    .await;
    optional_response(payload)
}

/// node-graph projection for one room diagnostics payload
pub(super) async fn room_graph(
    State(services): State<DiagnosticsServices>,
    Path(room_id): Path<String>,
) -> Response {
    let payload = diagnostics::room_detail_response(
        &services.room_manager,
        &services.media_transport,
        &room_id,
    )
    .await
    .map(|payload| diagnostics::build_graph(&payload));
    optional_response(payload)
}

/// node-graph projection rooted at one user in one room
pub(super) async fn user_graph(
    State(services): State<DiagnosticsServices>,
    Path((room_id, user_key)): Path<(String, String)>,
) -> Response {
    let payload = diagnostics::room_detail_response(
        &services.room_manager,
        &services.media_transport,
        &room_id,
    )
    .await
    .and_then(|payload| diagnostics::build_user_graph(&payload, &user_key));
    optional_response(payload)
}

/// diagnostics for one user in one room
pub(super) async fn user_detail(
    State(services): State<DiagnosticsServices>,
    Path((room_id, user_key)): Path<(String, String)>,
) -> Response {
    optional_response(
        diagnostics::user_detail_response(
            &services.room_manager,
            &services.media_transport,
            &room_id,
            &user_key,
        )
        .await,
    )
}

fn optional_response<T>(payload: Option<T>) -> Response
where
    axum::Json<T>: IntoResponse,
{
    payload.map_or_else(
        || StatusCode::NOT_FOUND.into_response(),
        |payload| axum::Json(payload).into_response(),
    )
}
