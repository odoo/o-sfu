//! Operator routes protected by the policy of the serving listener.

use std::net::SocketAddr;

use axum::{
    Router,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
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
        state.config.diagnostics.auth_token.as_ref(),
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
        .route_layer(authorization);
    observation_routes.merge(diagnostics_routes)
}

/// Passes authorized requests without consuming their bodies.
///
/// # Errors
///
/// Returns an HTTP [`Response`] with the policy rejection status.
/// Unauthorized responses include the Bearer challenge required by RFC 9110.
async fn authorize_operator(
    State(policy): State<OperatorAccessPolicy>,
    request: Request,
) -> Result<Request, Response> {
    policy.authorize(request.headers()).map_err(|status| {
        let mut response = status.into_response();
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"o-sfu\""),
            );
        }
        response
    })?;
    Ok(request)
}
