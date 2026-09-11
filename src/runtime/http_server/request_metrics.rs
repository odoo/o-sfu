//! Control request counters and in-flight gauges.
//!
//! Request tracking wraps authorization and extraction to include rejected requests
//! and unsupported methods. Diagnostics paths are excluded from these counters.

use std::sync::Arc;

use axum::{
    extract::{MatchedPath, Request, State},
    middleware::Next,
    response::Response,
};

use super::contract::route;
use crate::runtime::metrics::{HttpRoute, RuntimeMetrics};

pub(super) async fn track_request(
    State(metrics): State<Arc<RuntimeMetrics>>,
    path: MatchedPath,
    request: Request,
    next: Next,
) -> Response {
    let route = match path.as_str() {
        route::v1::NOOP => HttpRoute::Noop,
        route::v1::STATS => HttpRoute::Stats,
        route::v1::CHANNEL => HttpRoute::Room,
        route::v1::DISCONNECT => HttpRoute::Disconnect,
        route::METRICS => HttpRoute::Metrics,
        _ => return next.run(request).await,
    };
    let _guard = metrics.track_http_request(route);
    next.run(request).await
}
