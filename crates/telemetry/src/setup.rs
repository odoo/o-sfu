use std::fmt;

use anyhow::Result;
use serde::{
    Serialize, Serializer,
    ser::{Error as _, SerializeSeq},
};
use serde_json::value::RawValue;
use time::format_description::well_known::Rfc3339;
use tracing::{Span, Subscriber, field};
use tracing_serde::fields::AsMap;
use tracing_subscriber::{
    EnvFilter, Registry,
    fmt::{
        FmtContext, FormattedFields,
        format::{FormatEvent, JsonFields, Writer},
        layer as fmt_layer,
    },
    layer::SubscriberExt,
    registry::{LookupSpan, Scope},
    util::SubscriberInitExt,
};
#[cfg(feature = "otel-tracing")]
use {
    opentelemetry::{
        KeyValue, global,
        trace::{TraceContextExt, TracerProvider as _},
    },
    opentelemetry_otlp::{Protocol, WithExportConfig},
    opentelemetry_sdk::{
        Resource,
        trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
    },
    std::sync::{Arc, OnceLock},
    tracing::dispatcher::WeakDispatch,
    tracing_opentelemetry::{OpenTelemetrySpanExt, get_otel_context},
    tracing_subscriber::Layer,
};

use crate::{TelemetryConfig, TelemetryLogFormat, schema};

const DEFAULT_ENV_FILTER: &str = "o_sfu=info,o_sfu_core=info,o_sfu_router=info";
#[cfg(feature = "otel-tracing")]
const PRODUCTION_ENVIRONMENT_NAME: &str = "production";
#[cfg(feature = "otel-tracing")]
const PRODUCTION_TRACE_SAMPLE_RATIO: f64 = 0.05;
#[cfg(feature = "otel-tracing")]
const TRACE_EXPORTER_NAME: &str = "o-sfu.runtime";

#[derive(Debug, Default)]
pub struct TelemetryHandle {
    #[cfg(feature = "otel-tracing")]
    tracer_provider: Option<SdkTracerProvider>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct TelemetryResourceFields {
    #[serde(rename = "service.name")]
    service_name: String,
    #[serde(rename = "service.version")]
    service_version: String,
    #[serde(rename = "service.instance.id")]
    service_instance_id: String,
    #[serde(rename = "deployment.environment")]
    deployment_environment: String,
}

#[derive(Debug, Clone)]
struct RuntimeJsonFormatter {
    resource: TelemetryResourceFields,
    #[cfg(feature = "otel-tracing")]
    dispatch: Arc<OnceLock<WeakDispatch>>,
}

#[derive(Serialize)]
struct JsonEvent<'a, E, S> {
    timestamp: String,
    level: &'static str,
    target: &'static str,
    #[serde(flatten)]
    resource: &'a TelemetryResourceFields,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_id: Option<String>,
    fields: E,
    spans: S,
}

#[derive(Serialize)]
struct JsonSpan<'a> {
    name: &'static str,
    fields: &'a RawValue,
}

struct JsonSpans<'a, 'context, S>(&'a FmtContext<'context, S, JsonFields>);

#[cfg(feature = "otel-tracing")]
impl Drop for TelemetryHandle {
    fn drop(&mut self) {
        if let Some(tracer_provider) = self.tracer_provider.take()
            && let Err(_error) = tracer_provider.shutdown()
        {
            // Drop cannot surface shutdown failures to a caller, and logging here would
            // recurse through the subscriber that is being torn down.
        }
    }
}

impl RuntimeJsonFormatter {
    fn new(resource: TelemetryResourceFields) -> Self {
        Self {
            resource,
            #[cfg(feature = "otel-tracing")]
            dispatch: Arc::default(),
        }
    }

    #[cfg(feature = "otel-tracing")]
    fn trace_id<S>(&self, ctx: &FmtContext<'_, S, JsonFields>) -> Option<String>
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        let dispatch = self.dispatch.get()?.upgrade()?;
        let parent = ctx.parent_span()?;
        let context = get_otel_context(&parent.id(), &dispatch)?;
        let span = context.span();
        let span_context = span.span_context();
        span_context
            .is_valid()
            .then(|| span_context.trace_id().to_string())
    }

    #[cfg(not(feature = "otel-tracing"))]
    fn trace_id<S>(&self, _ctx: &FmtContext<'_, S, JsonFields>) -> Option<String>
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        None
    }
}

#[cfg(feature = "otel-tracing")]
impl<S: Subscriber> Layer<S> for RuntimeJsonFormatter {
    fn on_register_dispatch(&self, dispatch: &tracing::Dispatch) {
        // Nested get_default calls hide the subscriber during event dispatch.
        // The formatter clone in this layer shares a weak reference with the fmt layer.
        let _ = self.dispatch.set(dispatch.downgrade());
    }
}

impl<S> FormatEvent<S, JsonFields> for RuntimeJsonFormatter
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, JsonFields>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> fmt::Result {
        let payload = JsonEvent {
            timestamp: time::OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .map_err(|_error| fmt::Error)?,
            level: event.metadata().level().as_str(),
            target: event.metadata().target(),
            resource: &self.resource,
            trace_id: self.trace_id(ctx),
            fields: event.field_map(),
            spans: JsonSpans(ctx),
        };
        let encoded = serde_json::to_string(&payload).map_err(|_error| fmt::Error)?;
        writeln!(writer, "{encoded}")
    }
}

impl<S> Serialize for JsonSpans<'_, '_, S>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn serialize<T: Serializer>(&self, serializer: T) -> Result<T::Ok, T::Error> {
        let mut sequence = serializer.serialize_seq(None)?;
        // Explicit-parent events can belong to a different scope than the entered span.
        for span in self.0.event_scope().into_iter().flat_map(Scope::from_root) {
            let extensions = span.extensions();
            if let Some(fields) = extensions.get::<FormattedFields<JsonFields>>() {
                let fields = serde_json::from_str::<&RawValue>(fields.fields.as_str())
                    .map_err(T::Error::custom)?;
                sequence.serialize_element(&JsonSpan {
                    name: span.name(),
                    fields,
                })?;
            }
        }
        sequence.end()
    }
}

/// Installs the configured tracing subscriber and retains its exporter until
/// the returned handle is dropped.
///
/// # Errors
///
/// Returns an [`anyhow::Error`] when subscriber initialization fails or when the
/// `otel-tracing` feature is enabled and OTLP exporter construction fails.
pub fn init_tracing(config: &TelemetryConfig, process_id: u32) -> Result<TelemetryHandle> {
    let env_filter = default_env_filter();
    let resource = telemetry_resource_fields(config, process_id);
    #[cfg(feature = "otel-tracing")]
    let tracer_provider = build_tracer_provider(config, &resource)?;
    #[cfg(feature = "otel-tracing")]
    let tracer = tracer_provider
        .as_ref()
        .map(|provider| provider.tracer(TRACE_EXPORTER_NAME));
    match config.log_format {
        TelemetryLogFormat::Compact => {
            let subscriber = Registry::default()
                .with(env_filter)
                .with(fmt_layer().with_target(false).compact());
            #[cfg(feature = "otel-tracing")]
            let subscriber = subscriber.with(
                tracer
                    .as_ref()
                    .map(|tracer| tracing_opentelemetry::layer().with_tracer(tracer.clone())),
            );
            subscriber.try_init()?;
        }
        TelemetryLogFormat::Json => {
            let formatter = RuntimeJsonFormatter::new(resource.clone());
            let subscriber = Registry::default().with(env_filter);
            #[cfg(feature = "otel-tracing")]
            let subscriber = subscriber.with(formatter.clone());
            let subscriber = subscriber.with(
                fmt_layer()
                    .fmt_fields(JsonFields::new())
                    .event_format(formatter)
                    .with_ansi(false),
            );
            #[cfg(feature = "otel-tracing")]
            let subscriber = subscriber.with(
                tracer
                    .as_ref()
                    .map(|tracer| tracing_opentelemetry::layer().with_tracer(tracer.clone())),
            );
            subscriber.try_init()?;
        }
    }
    #[cfg(feature = "otel-tracing")]
    let trace_export_otlp_endpoint = config
        .trace_export
        .otlp_endpoint
        .as_deref()
        .unwrap_or("disabled");
    #[cfg(not(feature = "otel-tracing"))]
    let trace_export_otlp_endpoint = "feature_disabled";
    tracing::info!(
        event = schema::event::RUNTIME_TELEMETRY_INITIALIZED,
        service_name = resource.service_name.as_str(),
        service_version = resource.service_version.as_str(),
        deployment_environment = resource.deployment_environment.as_str(),
        service_instance_id = resource.service_instance_id.as_str(),
        log_format = config.log_format.as_str(),
        common_fields = ?schema::COMMON_FIELD_NAMES,
        correlation_fields = ?schema::CORRELATION_FIELD_NAMES,
        trace_export_otlp_endpoint,
        "initialized runtime telemetry"
    );
    Ok(TelemetryHandle {
        #[cfg(feature = "otel-tracing")]
        tracer_provider,
    })
}

#[must_use]
pub fn http_request_span(route: &'static str) -> Span {
    activated_span(tracing::info_span!(
        "http.request",
        "otel.kind" = "server",
        route,
        room_id = field::Empty,
        user_id = field::Empty,
        connection_id = field::Empty,
        remote_address = field::Empty
    ))
}

#[must_use]
pub fn ws_upgrade_span() -> Span {
    activated_span(tracing::info_span!(
        "ws.upgrade",
        room_id = field::Empty,
        user_id = field::Empty,
        connection_id = field::Empty,
        remote_address = field::Empty
    ))
}

#[must_use]
pub fn ws_handshake_span() -> Span {
    activated_span(tracing::info_span!(
        "ws.handshake",
        room_id = field::Empty,
        user_id = field::Empty,
        connection_id = field::Empty,
        remote_address = field::Empty
    ))
}

#[cfg(feature = "otel-tracing")]
#[must_use]
pub fn activated_span(span: Span) -> Span {
    let _span_context = span.context();
    span
}

#[cfg(not(feature = "otel-tracing"))]
#[must_use]
pub fn activated_span(span: Span) -> Span {
    span
}

fn default_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_error| EnvFilter::new(DEFAULT_ENV_FILTER))
}

fn telemetry_resource_fields(config: &TelemetryConfig, process_id: u32) -> TelemetryResourceFields {
    TelemetryResourceFields {
        service_name: config.resource.service_name.clone(),
        service_version: env!("CARGO_PKG_VERSION").to_owned(),
        service_instance_id: config.resource.resolved_instance_id(process_id),
        deployment_environment: config.resource.deployment_environment.clone(),
    }
}

#[cfg(feature = "otel-tracing")]
fn build_tracer_provider(
    config: &TelemetryConfig,
    resource: &TelemetryResourceFields,
) -> Result<Option<SdkTracerProvider>> {
    let Some(endpoint) = config.trace_export.otlp_endpoint.as_deref() else {
        return Ok(None);
    };
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(normalize_trace_export_endpoint(endpoint))
        .build()?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(default_trace_sampler(
            resource.deployment_environment.as_str(),
        ))
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(
            Resource::builder_empty()
                .with_attributes([
                    KeyValue::new(schema::field::SERVICE_NAME, resource.service_name.clone()),
                    KeyValue::new(
                        schema::field::SERVICE_VERSION,
                        resource.service_version.clone(),
                    ),
                    KeyValue::new(
                        schema::field::SERVICE_INSTANCE_ID,
                        resource.service_instance_id.clone(),
                    ),
                    KeyValue::new(
                        schema::field::DEPLOYMENT_ENVIRONMENT,
                        resource.deployment_environment.clone(),
                    ),
                ])
                .build(),
        )
        .build();
    global::set_tracer_provider(tracer_provider.clone());
    Ok(Some(tracer_provider))
}

#[cfg(feature = "otel-tracing")]
fn default_trace_sampler(deployment_environment: &str) -> Sampler {
    if deployment_environment == PRODUCTION_ENVIRONMENT_NAME {
        Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            PRODUCTION_TRACE_SAMPLE_RATIO,
        )))
    } else {
        Sampler::AlwaysOn
    }
}

#[cfg(feature = "otel-tracing")]
fn normalize_trace_export_endpoint(endpoint: &str) -> String {
    if endpoint.ends_with("/v1/traces") {
        endpoint.to_owned()
    } else {
        format!("{}/v1/traces", endpoint.trim_end_matches('/'))
    }
}

#[cfg(test)]
#[path = "TESTS/setup.rs"]
mod tests;
