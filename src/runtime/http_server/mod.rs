//! HTTP control-plane families and WebSocket listener composition.
//!
//! [`rooms`] owns authenticated room operations and request limits.
//! [`operator`] owns protected stats, metrics and diagnostics routes.
//! Each family exposes its assembled router and keeps handlers private.
//!
//! [`server`] binds the listener and owns graceful shutdown. [`request_metrics`]
//! counts control requests, including rejections, while excluding diagnostics
//! and WebSocket upgrades. [`contract`] preserves public paths and payloads.

use std::{net::SocketAddr, sync::Arc};

use axum::{Router, middleware, response::IntoResponse, routing::get};
use tracing::Instrument;

use self::contract::{NoopResponse, route};
use super::{RuntimeState, telemetry, websocket_server};

#[cfg(test)]
#[expect(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
mod access;
pub(crate) mod contract;
mod operator;
mod request_metrics;
mod rooms;
mod server;

pub(crate) use self::server::{serve_http, serve_http_on};

/// Composes public liveness, authenticated HTTP families and WebSocket upgrades.
///
/// Operator authorization uses the actual listener address. WebSocket
/// authentication runs after the upgrade. Control request tracking wraps
/// authorization and input extraction so rejected requests remain counted.
pub(super) fn app(state: RuntimeState, listener_address: SocketAddr) -> Router {
    Router::new()
        .route(route::v1::NOOP, get(noop))
        .merge(rooms::routes())
        .merge(operator::routes(&state, listener_address))
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&state.metrics),
            request_metrics::track_request,
        ))
        .route(route::WEBSOCKET, get(websocket_server::upgrade))
        .with_state(state)
}

/// Liveness endpoint for a cheap control-plane round trip.
async fn noop() -> impl IntoResponse {
    async { axum::Json(NoopResponse::ok()) }
        .instrument(telemetry::http_request_span("noop"))
        .await
}
