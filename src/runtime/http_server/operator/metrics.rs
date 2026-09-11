//! Prometheus HTTP projection of runtime counters and room gauges.

use std::sync::Arc;

use axum::{
    extract::{FromRef, State},
    http::header,
    response::IntoResponse,
};
use tracing::Instrument;

use crate::runtime::{
    RuntimeMetrics, RuntimeState,
    prometheus::{PROMETHEUS_CONTENT_TYPE, render_prometheus},
    room::RoomManager,
    telemetry,
};

#[derive(Debug, Clone)]
pub(super) struct MetricsServices {
    room_manager: Arc<RoomManager>,
    metrics: Arc<RuntimeMetrics>,
}

impl FromRef<RuntimeState> for MetricsServices {
    fn from_ref(state: &RuntimeState) -> Self {
        Self {
            room_manager: Arc::clone(&state.room_manager),
            metrics: Arc::clone(&state.metrics),
        }
    }
}

/// prometheus scrape endpoint for process, room, HTTP and media-transport metrics
pub(super) async fn scrape(State(services): State<MetricsServices>) -> impl IntoResponse {
    async {
        let room_gauges = services.room_manager.room_gauges().await;
        (
            [(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
            render_prometheus(&services.metrics, room_gauges),
        )
    }
    .instrument(telemetry::http_request_span("metrics"))
    .await
}
