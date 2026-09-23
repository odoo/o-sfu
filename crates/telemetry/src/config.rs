use std::time::Duration;

pub const DEFAULT_TELEMETRY_SERVICE_NAME: &str = "o-sfu";
pub const DEFAULT_TELEMETRY_DEPLOYMENT_ENVIRONMENT: &str = "local";
pub const DEFAULT_MEDIA_QUALITY_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryConfig {
    pub log_format: TelemetryLogFormat,
    pub resource: TelemetryResource,
    pub trace_export: TraceExportConfig,
    /// Sampling interval of at most 24 hours. `None` or zero disables sampling.
    ///
    /// The media transport validates programmatic values before starting workers.
    pub media_quality_interval: Option<Duration>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            log_format: TelemetryLogFormat::default(),
            resource: TelemetryResource::default(),
            trace_export: TraceExportConfig::default(),
            media_quality_interval: Some(DEFAULT_MEDIA_QUALITY_INTERVAL),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TelemetryLogFormat {
    #[default]
    Compact,
    Json,
}

impl TelemetryLogFormat {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Compact => "compact",
            Self::Json => "json",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryResource {
    pub service_name: String,
    pub deployment_environment: String,
    pub service_instance_id: Option<String>,
}

impl TelemetryResource {
    #[must_use]
    pub fn resolved_instance_id(&self, process_id: u32) -> String {
        self.service_instance_id
            .clone()
            .unwrap_or_else(|| format!("pid-{process_id}"))
    }
}

impl Default for TelemetryResource {
    fn default() -> Self {
        Self {
            service_name: DEFAULT_TELEMETRY_SERVICE_NAME.to_owned(),
            deployment_environment: DEFAULT_TELEMETRY_DEPLOYMENT_ENVIRONMENT.to_owned(),
            service_instance_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TraceExportConfig {
    pub otlp_endpoint: Option<String>,
}
