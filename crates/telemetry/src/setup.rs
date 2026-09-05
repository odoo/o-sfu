use std::{error::Error as StdError, fmt};

use anyhow::Result;
use serde_json::{Map, Number, Value};
use time::format_description::well_known::Rfc3339;
use tracing::{
    Span, Subscriber, field,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    fmt::{
        FmtContext,
        format::{FormatEvent, FormatFields, JsonFields, Writer},
        layer as fmt_layer,
    },
    layer::{Context as LayerContext, SubscriberExt},
    registry::LookupSpan,
    util::SubscriberInitExt,
};
#[cfg(feature = "otel-tracing")]
use {
    opentelemetry::{
        Context, KeyValue, global,
        trace::{TraceContextExt, TracerProvider as _},
    },
    opentelemetry_otlp::{Protocol, WithExportConfig},
    opentelemetry_sdk::{
        Resource,
        trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
    },
    tracing::dispatcher,
    tracing_opentelemetry::{OpenTelemetrySpanExt, get_otel_context},
    tracing_subscriber::registry::SpanRef,
};

use crate::{TelemetryConfig, TelemetryLogFormat, schema};

const DEFAULT_ENV_FILTER: &str = "o_sfu=info,o_sfu_core=info,o_sfu_router=info";
const INHERITED_CORRELATION_FIELDS: &[&str] = &[
    schema::field::CONNECTION_ID,
    schema::field::REMOTE_ADDRESS,
    schema::field::ROOM_ID,
    schema::field::USER_ID,
];
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct TelemetryResourceFields {
    service_name: String,
    service_version: String,
    service_instance_id: String,
    deployment_environment: String,
}

#[derive(Debug, Clone)]
struct RuntimeJsonFormatter {
    resource: TelemetryResourceFields,
}

#[derive(Debug, Default)]
struct JsonEventVisitor {
    fields: Map<String, Value>,
}

#[derive(Debug, Default)]
struct SpanFieldStore {
    fields: Map<String, Value>,
}

#[derive(Debug, Default)]
struct SpanFieldCaptureLayer;

impl TelemetryHandle {
    #[cfg(feature = "otel-tracing")]
    fn with_tracer_provider(tracer_provider: Option<SdkTracerProvider>) -> Self {
        Self { tracer_provider }
    }

    #[cfg(not(feature = "otel-tracing"))]
    const fn disabled() -> Self {
        Self {}
    }
}

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
        Self { resource }
    }
}

impl<S, N> FormatEvent<S, N> for RuntimeJsonFormatter
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'fields> FormatFields<'fields> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> fmt::Result {
        let mut visitor = JsonEventVisitor::default();
        event.record(&mut visitor);
        let mut payload = visitor.fields;
        for span in ctx.event_scope().into_iter().flatten() {
            if let Some(store) = span.extensions().get::<SpanFieldStore>() {
                for (key, value) in &store.fields {
                    payload.entry(key.clone()).or_insert_with(|| value.clone());
                }
            }
        }
        if payload.get(schema::field::EVENT).and_then(Value::as_str)
            == Some(schema::event::TRANSPORT_HEALTH_CHANGED)
        {
            payload.entry("from".to_owned()).or_insert(Value::Null);
        }
        payload.insert(
            schema::field::TIMESTAMP.to_owned(),
            Value::String(
                time::OffsetDateTime::now_utc()
                    .format(&Rfc3339)
                    .map_err(|_error| fmt::Error)?,
            ),
        );
        payload.insert(
            schema::field::LEVEL.to_owned(),
            Value::String(event.metadata().level().to_string()),
        );
        payload.insert(
            schema::field::TARGET.to_owned(),
            Value::String(event.metadata().target().to_owned()),
        );
        payload
            .entry(schema::field::EVENT.to_owned())
            .or_insert_with(|| Value::String(schema::event::RUNTIME_LOG.to_owned()));
        payload.insert(
            schema::field::SERVICE_NAME.to_owned(),
            Value::String(self.resource.service_name.clone()),
        );
        payload.insert(
            schema::field::SERVICE_VERSION.to_owned(),
            Value::String(self.resource.service_version.clone()),
        );
        payload.insert(
            schema::field::SERVICE_INSTANCE_ID.to_owned(),
            Value::String(self.resource.service_instance_id.clone()),
        );
        payload.insert(
            schema::field::DEPLOYMENT_ENVIRONMENT.to_owned(),
            Value::String(self.resource.deployment_environment.clone()),
        );
        if let Some(trace_id) = current_trace_id(ctx) {
            payload.insert(schema::field::TRACE_ID.to_owned(), Value::String(trace_id));
        }
        let encoded = serde_json::to_string(&payload).map_err(|_error| fmt::Error)?;
        writer.write_str(encoded.as_str())?;
        writeln!(writer)
    }
}

impl Visit for JsonEventVisitor {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_owned(), Value::Bool(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_owned(), Value::Number(Number::from(value)));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_owned(), Value::Number(Number::from(value)));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_owned(), Value::String(value.to_owned()));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn StdError + 'static)) {
        self.fields
            .insert(field.name().to_owned(), Value::String(value.to_string()));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        let json_value =
            Number::from_f64(value).map_or_else(|| Value::String(value.to_string()), Value::Number);
        self.fields.insert(field.name().to_owned(), json_value);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.fields
            .insert(field.name().to_owned(), Value::String(format!("{value:?}")));
    }
}

impl<S> Layer<S> for SpanFieldCaptureLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: LayerContext<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut visitor = JsonEventVisitor::default();
        attrs.record(&mut visitor);
        visitor
            .fields
            .retain(|name, _| INHERITED_CORRELATION_FIELDS.contains(&name.as_str()));
        span.extensions_mut().insert(SpanFieldStore {
            fields: visitor.fields,
        });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: LayerContext<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut visitor = JsonEventVisitor::default();
        values.record(&mut visitor);
        visitor
            .fields
            .retain(|name, _| INHERITED_CORRELATION_FIELDS.contains(&name.as_str()));
        if let Some(store) = span.extensions_mut().get_mut::<SpanFieldStore>() {
            store.fields.extend(visitor.fields);
        }
    }
}

#[cfg(feature = "otel-tracing")]
/// # Errors
///
/// Returns an error when subscriber initialization or OTLP exporter
/// construction fails.
pub fn init_tracing(config: &TelemetryConfig, process_id: u32) -> Result<TelemetryHandle> {
    let env_filter = default_env_filter();
    let resource = telemetry_resource_fields(config, process_id);
    let tracer_provider = build_tracer_provider(config, &resource)?;
    let tracer = tracer_provider
        .as_ref()
        .map(|provider| provider.tracer(TRACE_EXPORTER_NAME));
    match config.log_format {
        TelemetryLogFormat::Compact => Registry::default()
            .with(env_filter)
            .with(fmt_layer().with_target(false).compact())
            .with(
                tracer
                    .as_ref()
                    .map(|tracer| tracing_opentelemetry::layer().with_tracer(tracer.clone())),
            )
            .try_init()?,
        TelemetryLogFormat::Json => Registry::default()
            .with(env_filter)
            .with(SpanFieldCaptureLayer)
            .with(
                fmt_layer()
                    .fmt_fields(JsonFields::new())
                    .event_format(RuntimeJsonFormatter::new(resource.clone()))
                    .with_ansi(false),
            )
            .with(
                tracer
                    .as_ref()
                    .map(|tracer| tracing_opentelemetry::layer().with_tracer(tracer.clone())),
            )
            .try_init()?,
    }
    tracing::info!(
        event = schema::event::RUNTIME_TELEMETRY_INITIALIZED,
        service_name = resource.service_name.as_str(),
        service_version = resource.service_version.as_str(),
        deployment_environment = resource.deployment_environment.as_str(),
        service_instance_id = resource.service_instance_id.as_str(),
        log_format = config.log_format.as_str(),
        common_fields = ?schema::COMMON_FIELD_NAMES,
        correlation_fields = ?schema::CORRELATION_FIELD_NAMES,
        trace_export_otlp_endpoint = config
            .trace_export
            .otlp_endpoint
            .as_deref()
            .unwrap_or("disabled"),
        "initialized runtime telemetry"
    );
    Ok(TelemetryHandle::with_tracer_provider(tracer_provider))
}

#[cfg(not(feature = "otel-tracing"))]
/// # Errors
///
/// Returns an error when subscriber initialization fails.
pub fn init_tracing(config: &TelemetryConfig, process_id: u32) -> Result<TelemetryHandle> {
    let env_filter = default_env_filter();
    let resource = telemetry_resource_fields(config, process_id);
    match config.log_format {
        TelemetryLogFormat::Compact => Registry::default()
            .with(env_filter)
            .with(fmt_layer().with_target(false).compact())
            .try_init()?,
        TelemetryLogFormat::Json => Registry::default()
            .with(env_filter)
            .with(SpanFieldCaptureLayer)
            .with(
                fmt_layer()
                    .fmt_fields(JsonFields::new())
                    .event_format(RuntimeJsonFormatter::new(resource.clone()))
                    .with_ansi(false),
            )
            .try_init()?,
    }
    tracing::info!(
        event = schema::event::RUNTIME_TELEMETRY_INITIALIZED,
        service_name = resource.service_name.as_str(),
        service_version = resource.service_version.as_str(),
        deployment_environment = resource.deployment_environment.as_str(),
        service_instance_id = resource.service_instance_id.as_str(),
        log_format = config.log_format.as_str(),
        common_fields = ?schema::COMMON_FIELD_NAMES,
        correlation_fields = ?schema::CORRELATION_FIELD_NAMES,
        trace_export_otlp_endpoint = "feature_disabled",
        "initialized runtime telemetry"
    );
    Ok(TelemetryHandle::disabled())
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

#[cfg(feature = "otel-tracing")]
fn current_trace_id<S, N>(ctx: &FmtContext<'_, S, N>) -> Option<String>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'fields> FormatFields<'fields> + 'static,
{
    dispatcher::get_default(|dispatch| {
        ctx.event_scope()
            .and_then(|mut scope| scope.find_map(|span| trace_id_for_span(&span, dispatch)))
            .or_else(|| {
                ctx.lookup_current()
                    .and_then(|span| trace_id_for_span(&span, dispatch))
            })
    })
}

#[cfg(not(feature = "otel-tracing"))]
fn current_trace_id<S, N>(_ctx: &FmtContext<'_, S, N>) -> Option<String>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'fields> FormatFields<'fields> + 'static,
{
    None
}

#[cfg(feature = "otel-tracing")]
fn trace_id_for_span<S>(span: &SpanRef<'_, S>, dispatch: &tracing::Dispatch) -> Option<String>
where
    S: for<'lookup> LookupSpan<'lookup>,
{
    get_otel_context(&span.id(), dispatch)
        .and_then(|context| trace_id_from_context(&context))
        .or_else(|| trace_id_from_context(&Context::current()))
}

#[cfg(feature = "otel-tracing")]
fn trace_id_from_context(context: &Context) -> Option<String> {
    let span_context = context.span().span_context().clone();
    span_context
        .is_valid()
        .then(|| span_context.trace_id().to_string())
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
