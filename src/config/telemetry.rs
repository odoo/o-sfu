use std::time::Duration;

use anyhow::{Result, anyhow};
pub use o_sfu_telemetry::{
    DEFAULT_MEDIA_QUALITY_INTERVAL, DEFAULT_TELEMETRY_DEPLOYMENT_ENVIRONMENT,
    DEFAULT_TELEMETRY_SERVICE_NAME, TelemetryConfig, TelemetryLogFormat, TelemetryResource,
    TraceExportConfig,
};

use super::env::{Env, EnvParse, EnvValue, non_empty};

const OTEL_TRACING_FEATURE_NAME: &str = "otel-tracing";
const MEDIA_QUALITY_INTERVAL_ENV: &str = "TELEMETRY_MEDIA_QUALITY_INTERVAL_MS";

impl EnvParse for TelemetryLogFormat {
    fn parse(value: EnvValue) -> Result<Self> {
        let key = value.key;
        match value.raw.as_str() {
            "compact" => Ok(Self::Compact),
            "json" => Ok(Self::Json),
            _ => Err(anyhow!("{key} must be either `compact` or `json`")),
        }
    }
}

pub(super) fn load_telemetry_config(env: &Env<'_>) -> Result<TelemetryConfig> {
    let log_format = env
        .var("TELEMETRY_LOG_FORMAT")
        .default(TelemetryLogFormat::default())?;
    let otlp_endpoint = env
        .var("TELEMETRY_OTLP_ENDPOINT")
        .check(non_empty)
        .optional()?;
    if !cfg!(feature = "otel-tracing") && otlp_endpoint.is_some() {
        return Err(anyhow!(
            "TELEMETRY_OTLP_ENDPOINT requires the `{OTEL_TRACING_FEATURE_NAME}` cargo feature"
        ));
    }
    let media_quality_interval_ms = env
        .var(MEDIA_QUALITY_INTERVAL_ENV)
        .default(u64::try_from(DEFAULT_MEDIA_QUALITY_INTERVAL.as_millis()).unwrap_or(5_000))?;
    let service_name = env
        .var("TELEMETRY_SERVICE_NAME")
        .check(non_empty)
        .optional()?;
    let deployment_environment = env
        .var("TELEMETRY_DEPLOYMENT_ENVIRONMENT")
        .check(non_empty)
        .optional()?;
    let service_instance_id = env
        .var("TELEMETRY_SERVICE_INSTANCE_ID")
        .check(non_empty)
        .optional()?;
    Ok(TelemetryConfig {
        log_format,
        resource: TelemetryResource {
            service_name: service_name.unwrap_or_else(|| DEFAULT_TELEMETRY_SERVICE_NAME.to_owned()),
            deployment_environment: deployment_environment
                .unwrap_or_else(|| DEFAULT_TELEMETRY_DEPLOYMENT_ENVIRONMENT.to_owned()),
            service_instance_id,
        },
        trace_export: TraceExportConfig { otlp_endpoint },
        media_quality_interval: (media_quality_interval_ms > 0)
            .then(|| Duration::from_millis(media_quality_interval_ms)),
    })
}

#[cfg(test)]
#[path = "TESTS/telemetry.rs"]
mod tests;
