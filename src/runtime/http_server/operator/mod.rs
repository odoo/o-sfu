//! Operator routes protected by the policy of the serving listener.

use std::net::SocketAddr;

use axum::{
    Router,
    extract::{Request, State},
    http::StatusCode,
    middleware,
    routing::get,
};

use super::{access::OperatorAccessPolicy, contract::route};
use crate::runtime::RuntimeState;

mod diagnostics;
mod metrics;
mod stats;

/// Stats, metrics and diagnostics with listener-bound operator authorization.
///
/// Stats and metrics reject unsupported methods before authorization. Diagnostics
/// authorizes every request to a registered path, including unsupported methods.
/// Unregistered paths retain the router's not-found response.
pub(super) fn routes(state: &RuntimeState, listener_address: SocketAddr) -> Router<RuntimeState> {
    let policy = OperatorAccessPolicy::new(
        state.config.diagnostics.auth_token.as_deref(),
        listener_address,
    );
    let authorization = middleware::map_request_with_state(policy, authorize_operator);
    let observation_routes = Router::new()
        .route(
            route::v1::STATS,
            get(stats::rooms).route_layer(authorization.clone()),
        )
        .route(
            route::METRICS,
            get(metrics::scrape).route_layer(authorization.clone()),
        );
    let diagnostics_routes = Router::new()
        .route(route::diagnostics::SUMMARY, get(diagnostics::summary))
        .route(route::diagnostics::ROOMS, get(diagnostics::rooms))
        .route(route::diagnostics::WORKERS, get(diagnostics::workers))
        .route(route::diagnostics::ROOM, get(diagnostics::room_detail))
        .route(route::diagnostics::ROOM_USERS, get(diagnostics::room_users))
        .route(route::diagnostics::ROOM_USER, get(diagnostics::user_detail))
        .route(route::diagnostics::ROOM_GRAPH, get(diagnostics::room_graph))
        .route(route::diagnostics::USER_GRAPH, get(diagnostics::user_graph))
        .route_layer(authorization);
    observation_routes.merge(diagnostics_routes)
}

/// Passes authorized requests without consuming their bodies.
///
/// # Errors
///
/// Returns the [`StatusCode`] rejection from [`OperatorAccessPolicy::authorize`].
async fn authorize_operator(
    State(policy): State<OperatorAccessPolicy>,
    request: Request,
) -> Result<Request, StatusCode> {
    policy.authorize(request.headers())?;
    Ok(request)
}
