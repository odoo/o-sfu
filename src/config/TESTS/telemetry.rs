use std::io;
#[cfg(feature = "otel-tracing")]
use std::time::Duration;

use super::{Env, TelemetryResource, load_telemetry_config};
#[cfg(feature = "otel-tracing")]
use super::{TelemetryConfig, TelemetryLogFormat, TraceExportConfig};

#[test]
fn telemetry_resource_resolves_process_fallback_instance_id() {
    let resource = TelemetryResource::default();
    assert_eq!(resource.resolved_instance_id(17), "pid-17");
}

#[cfg(feature = "otel-tracing")]
#[test]
fn load_telemetry_config_accepts_explicit_settings() {
    let config = load_telemetry_config(&Env::new(
        |key| match key {
            "TELEMETRY_LOG_FORMAT" => Some("json".to_owned()),
            "TELEMETRY_SERVICE_NAME" => Some("custom-o-sfu".to_owned()),
            "TELEMETRY_DEPLOYMENT_ENVIRONMENT" => Some("staging".to_owned()),
            "TELEMETRY_SERVICE_INSTANCE_ID" => Some("node-a-1".to_owned()),
            "TELEMETRY_OTLP_ENDPOINT" => Some("  http://collector:4317  ".to_owned()),
            _ => None,
        },
        |_| Err(io::Error::new(io::ErrorKind::NotFound, "file not found")),
    ));
    assert_eq!(
        config.ok(),
        Some(TelemetryConfig {
            log_format: TelemetryLogFormat::Json,
            resource: TelemetryResource {
                service_name: "custom-o-sfu".to_owned(),
                deployment_environment: "staging".to_owned(),
                service_instance_id: Some("node-a-1".to_owned()),
            },
            trace_export: TraceExportConfig {
                otlp_endpoint: Some("http://collector:4317".to_owned()),
            },
            media_quality_interval: Some(Duration::from_secs(5)),
        })
    );
}

#[cfg(not(feature = "otel-tracing"))]
#[test]
fn load_telemetry_config_reports_otlp_feature_error_before_later_errors() {
    let error = load_telemetry_config(&Env::new(
        |key| match key {
            "TELEMETRY_OTLP_ENDPOINT" => Some("http://collector:4318".to_owned()),
            "TELEMETRY_MEDIA_QUALITY_INTERVAL_MS" => Some("abc".to_owned()),
            "TELEMETRY_SERVICE_NAME" => Some("   ".to_owned()),
            _ => None,
        },
        |_| Err(io::Error::new(io::ErrorKind::NotFound, "file not found")),
    ))
    .err()
    .map(|error| error.to_string());

    assert_eq!(
        error.as_deref(),
        Some("TELEMETRY_OTLP_ENDPOINT requires the `otel-tracing` cargo feature")
    );
}

#[test]
fn load_telemetry_config_rejects_invalid_log_format() {
    let error = load_telemetry_config(&Env::new(
        |key| match key {
            "TELEMETRY_LOG_FORMAT" => Some("pretty".to_owned()),
            _ => None,
        },
        |_| Err(io::Error::new(io::ErrorKind::NotFound, "file not found")),
    ))
    .err()
    .map(|error| error.to_string());

    assert_eq!(
        error.as_deref(),
        Some("TELEMETRY_LOG_FORMAT must be either `compact` or `json`")
    );
}

#[test]
fn load_telemetry_config_rejects_empty_service_name() {
    let error = load_telemetry_config(&Env::new(
        |key| match key {
            "TELEMETRY_SERVICE_NAME" => Some("   ".to_owned()),
            _ => None,
        },
        |_| Err(io::Error::new(io::ErrorKind::NotFound, "file not found")),
    ))
    .err()
    .map(|error| error.to_string());

    assert_eq!(
        error.as_deref(),
        Some("TELEMETRY_SERVICE_NAME must not be empty")
    );
}

#[test]
fn load_telemetry_config_trims_resource_fields() -> anyhow::Result<()> {
    let config = load_telemetry_config(&Env::new(
        |key| match key {
            "TELEMETRY_SERVICE_NAME" => Some("  o-sfu-custom  ".to_owned()),
            "TELEMETRY_DEPLOYMENT_ENVIRONMENT" => Some("  staging  ".to_owned()),
            "TELEMETRY_SERVICE_INSTANCE_ID" => Some("  node-a  ".to_owned()),
            _ => None,
        },
        |_| Err(io::Error::new(io::ErrorKind::NotFound, "file not found")),
    ))?;
    assert_eq!(config.resource.service_name, "o-sfu-custom");
    assert_eq!(config.resource.deployment_environment, "staging");
    assert_eq!(
        config.resource.service_instance_id.as_deref(),
        Some("node-a")
    );
    Ok(())
}

#[test]
fn load_telemetry_config_reports_interval_parse_error() {
    let error = load_telemetry_config(&Env::new(
        |key| match key {
            "TELEMETRY_MEDIA_QUALITY_INTERVAL_MS" => Some("abc".to_owned()),
            _ => None,
        },
        |_| Err(io::Error::new(io::ErrorKind::NotFound, "file not found")),
    ))
    .err()
    .map(|error| error.to_string());

    assert_eq!(
        error.as_deref(),
        Some("TELEMETRY_MEDIA_QUALITY_INTERVAL_MS must be a valid u64")
    );
}

#[test]
fn load_telemetry_config_allows_disabling_media_quality_sampling() -> anyhow::Result<()> {
    let config = load_telemetry_config(&Env::new(
        |key| match key {
            "TELEMETRY_MEDIA_QUALITY_INTERVAL_MS" => Some("0".to_owned()),
            _ => None,
        },
        |_| Err(io::Error::new(io::ErrorKind::NotFound, "file not found")),
    ))?;

    assert_eq!(config.media_quality_interval, None);
    Ok(())
}
