//! telemetry setup, metric catalog and diagnostics schema
//!
//! `o-sfu-telemetry` keeps runtime observability contracts in one crate
//! the server initializes tracing from `TelemetryConfig`, records process-local
//! metrics through typed helpers and renders diagnostics or Prometheus output
//! from the metric catalog
//!
//! ```
//! use o_sfu_telemetry::{
//!     metrics::{HttpRoute, RoomGaugeValues, RuntimeMetrics},
//!     prometheus::render_prometheus,
//! };
//!
//! let metrics = RuntimeMetrics::default();
//! let request = metrics.track_http_request(HttpRoute::Noop);
//! let body = render_prometheus(&metrics, RoomGaugeValues::default());
//! drop(request);
//! assert!(body.contains("osfu_http_noop_requests_total"));
//! ```
//!
//! process-global tracing setup lives behind `init_tracing`
//! code that only needs counters, diagnostics or schema names can use those
//! modules without installing a tracing subscriber

mod config;
pub mod diagnostics;
pub mod metrics;
pub mod prometheus;
pub mod schema;
mod setup;

pub use config::{
    DEFAULT_MEDIA_QUALITY_INTERVAL, DEFAULT_TELEMETRY_DEPLOYMENT_ENVIRONMENT,
    DEFAULT_TELEMETRY_SERVICE_NAME, TelemetryConfig, TelemetryLogFormat, TelemetryResource,
    TraceExportConfig,
};
pub use setup::{
    TelemetryHandle, activated_span, http_request_span, init_tracing, ws_handshake_span,
    ws_upgrade_span,
};
