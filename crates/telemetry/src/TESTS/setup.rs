use std::{
    error::Error,
    io,
    sync::{Arc, Mutex, PoisonError},
};

use o_sfu_model::UserId;
use tracing::{Subscriber, subscriber};
#[cfg(feature = "otel-tracing")]
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{fmt::MakeWriter, prelude::*};

use super::*;
use crate::{TelemetryLogFormat, TelemetryResource, TraceExportConfig};

#[derive(Clone, Debug, Default)]
struct SharedWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl<'writer> MakeWriter<'writer> for SharedWriter {
    type Writer = SharedBufferGuard;

    fn make_writer(&'writer self) -> Self::Writer {
        SharedBufferGuard {
            buffer: Arc::clone(&self.buffer),
        }
    }
}

#[derive(Debug)]
struct SharedBufferGuard {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for SharedBufferGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn json_values(writer: &SharedWriter) -> Result<Vec<Value>, Box<dyn Error>> {
    let buffer = writer
        .buffer
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    String::from_utf8(buffer)?
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

fn json_string<'value>(value: &'value Value, key: &str) -> Option<&'value str> {
    value.get(key).and_then(Value::as_str)
}

fn json_is_string(value: &Value, key: &str) -> bool {
    value.get(key).is_some_and(Value::is_string)
}

fn assert_json_string(value: &Value, key: &str, expected: &str) {
    assert_eq!(json_string(value, key), Some(expected));
}

#[cfg(feature = "otel-tracing")]
#[test]
fn normalize_trace_export_endpoint_appends_default_http_trace_path() {
    assert_eq!(
        normalize_trace_export_endpoint("http://collector:4318"),
        "http://collector:4318/v1/traces"
    );
    assert_eq!(
        normalize_trace_export_endpoint("http://collector:4318/v1/traces"),
        "http://collector:4318/v1/traces"
    );
}

#[test]
fn json_formatter_emits_common_fields() -> Result<(), Box<dyn Error>> {
    let writer = SharedWriter::default();
    let subscriber = json_test_subscriber(writer.clone());
    subscriber::with_default(subscriber, || {
        let span = activated_span(tracing::info_span!("ws.handshake", room_id = "room-a"));
        let _entered = span.enter();
        tracing::info!(
            event = schema::event::USER_JOINED,
            user_id = "user-1",
            message = "joined user"
        );
    });

    let values = json_values(&writer)?;
    let [value] = values.as_slice() else {
        return Err(io::Error::other("expected one JSON log").into());
    };

    assert_json_string(value, "event", schema::event::USER_JOINED);
    assert_json_string(value, "message", "joined user");
    assert_json_string(value, "service.name", "o-sfu-test");
    assert_json_string(value, "service.version", env!("CARGO_PKG_VERSION"));
    assert_json_string(value, "service.instance.id", "test-instance");
    assert_json_string(value, "deployment.environment", "test");
    assert_json_string(value, "user_id", "user-1");
    assert_json_string(value, "target", "o_sfu_telemetry::setup::tests");
    assert!(json_is_string(value, "timestamp"));
    #[cfg(feature = "otel-tracing")]
    assert!(json_is_string(value, "trace_id"));
    #[cfg(feature = "otel-tracing")]
    assert_ne!(
        json_string(value, "trace_id"),
        Some("00000000000000000000000000000000")
    );
    #[cfg(not(feature = "otel-tracing"))]
    assert!(value.get("trace_id").is_none());
    Ok(())
}

#[test]
fn json_formatter_preserves_initial_transport_health_origin() -> Result<(), Box<dyn Error>> {
    let writer = SharedWriter::default();
    let subscriber = json_test_subscriber(writer.clone());
    subscriber::with_default(subscriber, || {
        tracing::info!(
            target: "o_sfu_core::transport",
            event = schema::event::TRANSPORT_HEALTH_CHANGED,
            room_id = "room-a",
            user_id = "7",
            media_worker_id = 1_u64,
            from = Option::<&str>::None,
            to = "connected",
            "transport health changed"
        );
        tracing::info!(
            target: "o_sfu_core::transport",
            event = schema::event::TRANSPORT_HEALTH_CHANGED,
            room_id = "room-a",
            user_id = "7",
            media_worker_id = 1_u64,
            from = "connected",
            to = "disconnected",
            "transport health changed"
        );
    });

    let values = json_values(&writer)?;
    let [initial, transition] = values.as_slice() else {
        return Err(io::Error::other("expected two JSON logs").into());
    };

    assert!(initial.get("from").is_some_and(Value::is_null));
    assert_json_string(initial, "to", "connected");
    assert_json_string(transition, "from", "connected");
    assert_json_string(transition, "to", "disconnected");
    Ok(())
}

#[test]
fn json_formatter_inherits_span_correlation_fields() -> Result<(), Box<dyn Error>> {
    let writer = SharedWriter::default();
    let subscriber = json_test_subscriber(writer.clone());
    subscriber::with_default(subscriber, || {
        let outer = activated_span(tracing::info_span!(
            "ws.handshake",
            room_id = field::Empty,
            user_id = field::Empty,
            connection_id = field::Empty,
            remote_address = "remote-outer",
            stream_type = "webcam",
            active = field::Empty,
        ));
        // Late-record via Span::record, same shape as session.rs::record_current_span.
        outer.record("room_id", field::display("room-late"));
        outer.record("user_id", field::display(UserId::Integer(7).path_segment()));
        outer.record("connection_id", 42_u64);
        outer.record("active", true);
        let _outer_guard = outer.enter();

        tracing::info!(event = schema::event::USER_JOINED, "outer event");

        let inner = activated_span(tracing::info_span!(
            "room.join",
            user_id = "u-inner",
            source_count = 3_u64,
        ));
        let _inner_guard = inner.enter();

        tracing::info!(
            event = schema::event::USER_JOINED,
            room_id = "room-explicit",
            "inner event",
        );
    });

    let values = json_values(&writer)?;
    let [outer_event, inner_event] = values.as_slice() else {
        return Err(io::Error::other("expected two JSON logs").into());
    };

    assert_json_string(outer_event, "room_id", "room-late");
    assert_json_string(outer_event, "user_id", "7");
    assert_eq!(
        outer_event.get("connection_id").and_then(Value::as_u64),
        Some(42)
    );
    assert_json_string(outer_event, "remote_address", "remote-outer");

    assert_json_string(inner_event, "room_id", "room-explicit");
    assert_json_string(inner_event, "user_id", "u-inner");
    assert_eq!(
        inner_event.get("connection_id").and_then(Value::as_u64),
        Some(42)
    );
    assert_json_string(inner_event, "remote_address", "remote-outer");

    for event in [outer_event, inner_event] {
        assert!(event.get("stream_type").is_none());
        assert!(event.get("active").is_none());
        assert!(event.get("source_count").is_none());
    }
    Ok(())
}

fn json_test_config() -> TelemetryConfig {
    TelemetryConfig {
        log_format: TelemetryLogFormat::Json,
        resource: TelemetryResource {
            service_name: "o-sfu-test".to_owned(),
            deployment_environment: "test".to_owned(),
            service_instance_id: Some("test-instance".to_owned()),
        },
        trace_export: TraceExportConfig::default(),
        media_quality_interval: None,
    }
}

#[cfg(feature = "otel-tracing")]
fn json_test_subscriber(writer: SharedWriter) -> impl Subscriber + Send + Sync {
    let resource = telemetry_resource_fields(&json_test_config(), 7);
    let tracer_provider = SdkTracerProvider::builder().build();
    let tracer = tracer_provider.tracer(TRACE_EXPORTER_NAME);
    Registry::default()
        .with(EnvFilter::new(DEFAULT_ENV_FILTER))
        .with(SpanFieldCaptureLayer)
        .with(
            fmt_layer()
                .fmt_fields(JsonFields::new())
                .event_format(RuntimeJsonFormatter::new(resource))
                .with_ansi(false)
                .with_writer(writer),
        )
        .with(Some(OpenTelemetryLayer::new(tracer)))
}

#[cfg(not(feature = "otel-tracing"))]
fn json_test_subscriber(writer: SharedWriter) -> impl Subscriber + Send + Sync {
    let resource = telemetry_resource_fields(&json_test_config(), 7);
    Registry::default()
        .with(EnvFilter::new(DEFAULT_ENV_FILTER))
        .with(SpanFieldCaptureLayer)
        .with(
            fmt_layer()
                .fmt_fields(JsonFields::new())
                .event_format(RuntimeJsonFormatter::new(resource))
                .with_ansi(false)
                .with_writer(writer),
        )
}
