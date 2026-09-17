use std::{
    error::Error,
    io,
    sync::{Arc, Mutex, PoisonError},
};

use o_sfu_model::UserId;
use serde_json::Value;
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
    type Writer = Self;

    fn make_writer(&'writer self) -> Self::Writer {
        self.clone()
    }
}

impl io::Write for SharedWriter {
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

fn assert_json_string(value: &Value, pointer: &str, expected: &str) {
    assert_eq!(
        value.pointer(pointer).and_then(Value::as_str),
        Some(expected)
    );
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
fn json_formatter_separates_event_fields_from_metadata() -> Result<(), Box<dyn Error>> {
    let writer = SharedWriter::default();
    subscriber::with_default(json_test_subscriber(writer.clone()), || {
        tracing::info!(
            event = schema::event::USER_JOINED,
            target = "application-target",
            trace_id = "application-trace",
            timestamp = "application-time",
            connection_id = u64::MAX,
            active = true,
            value = 1.5,
            "joined user"
        );
    });
    let values = json_values(&writer)?;
    let [value] = values.as_slice() else {
        return Err(io::Error::other("expected one JSON log").into());
    };
    assert_json_string(value, "/fields/event", schema::event::USER_JOINED);
    assert_json_string(value, "/fields/message", "joined user");
    assert_json_string(value, "/service.name", "o-sfu-test");
    assert_json_string(value, "/service.version", env!("CARGO_PKG_VERSION"));
    assert_json_string(value, "/service.instance.id", "test-instance");
    assert_json_string(value, "/deployment.environment", "test");
    assert_json_string(value, "/target", "o_sfu_telemetry::setup::tests");
    assert_json_string(value, "/fields/target", "application-target");
    assert_json_string(value, "/fields/trace_id", "application-trace");
    assert_json_string(value, "/fields/timestamp", "application-time");
    assert!(value.get("timestamp").is_some_and(Value::is_string));
    assert!(value.get("trace_id").is_none());
    assert_eq!(
        value
            .pointer("/fields/connection_id")
            .and_then(Value::as_u64),
        Some(u64::MAX)
    );
    assert_eq!(
        value.pointer("/fields/active").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        value.pointer("/fields/value").and_then(Value::as_f64),
        Some(1.5)
    );
    assert_eq!(
        value.get("spans").and_then(Value::as_array).map(Vec::len),
        Some(0)
    );
    Ok(())
}

#[test]
fn json_formatter_preserves_optional_event_fields() -> Result<(), Box<dyn Error>> {
    let writer = SharedWriter::default();
    subscriber::with_default(json_test_subscriber(writer.clone()), || {
        tracing::info!(
            event = schema::event::TRANSPORT_HEALTH_CHANGED,
            from = Option::<&str>::None,
            to = "connected",
            "transport health changed"
        );
        tracing::info!("message without an event name");
    });
    let values = json_values(&writer)?;
    let [initial, unnamed] = values.as_slice() else {
        return Err(io::Error::other("expected two JSON logs").into());
    };
    assert!(initial.pointer("/fields/from").is_none());
    assert_json_string(initial, "/fields/to", "connected");
    assert!(unnamed.pointer("/fields/event").is_none());
    assert_json_string(unnamed, "/fields/message", "message without an event name");
    Ok(())
}

#[test]
fn json_formatter_preserves_span_fields_and_late_updates() -> Result<(), Box<dyn Error>> {
    let writer = SharedWriter::default();
    subscriber::with_default(json_test_subscriber(writer.clone()), || {
        let outer = activated_span(tracing::info_span!(
            "ws.handshake",
            room_id = field::Empty,
            user_id = field::Empty,
            connection_id = field::Empty,
            remote_address = "remote-outer",
            stream_type = "webcam",
            active = field::Empty,
        ));
        outer.record("room_id", "room-before");
        outer.record("room_id", "room-late");
        outer.record("user_id", field::display(UserId::Integer(7).path_segment()));
        outer.record("connection_id", 42_u64);
        outer.record("active", true);
        let _outer_guard = outer.enter();
        let inner = activated_span(tracing::info_span!(
            "room.join",
            user_id = "u-inner",
            source_count = 3_u64,
        ));
        let _inner_guard = inner.enter();
        tracing::info!(
            event = schema::event::USER_JOINED,
            room_id = "room-explicit",
            "inner event"
        );
    });
    let values = json_values(&writer)?;
    let [value] = values.as_slice() else {
        return Err(io::Error::other("expected one JSON log").into());
    };
    assert_json_string(value, "/fields/room_id", "room-explicit");
    assert!(value.pointer("/fields/user_id").is_none());
    assert!(value.get("room_id").is_none());
    assert_eq!(
        value.get("spans").and_then(Value::as_array).map(Vec::len),
        Some(2)
    );
    assert_json_string(value, "/spans/0/name", "ws.handshake");
    assert_json_string(value, "/spans/0/fields/room_id", "room-late");
    assert_json_string(value, "/spans/0/fields/user_id", "7");
    assert_json_string(value, "/spans/0/fields/remote_address", "remote-outer");
    assert_json_string(value, "/spans/0/fields/stream_type", "webcam");
    assert_eq!(
        value
            .pointer("/spans/0/fields/connection_id")
            .and_then(Value::as_u64),
        Some(42)
    );
    assert_eq!(
        value
            .pointer("/spans/0/fields/active")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_json_string(value, "/spans/1/name", "room.join");
    assert_json_string(value, "/spans/1/fields/user_id", "u-inner");
    assert_eq!(
        value
            .pointer("/spans/1/fields/source_count")
            .and_then(Value::as_u64),
        Some(3)
    );
    #[cfg(feature = "otel-tracing")]
    assert!(value.get("trace_id").is_some_and(|id| {
        id.as_str()
            .is_some_and(|id| id != "00000000000000000000000000000000")
    }));
    #[cfg(not(feature = "otel-tracing"))]
    assert!(value.get("trace_id").is_none());
    Ok(())
}

#[test]
fn json_formatter_uses_the_event_parent_scope() -> Result<(), Box<dyn Error>> {
    let writer = SharedWriter::default();
    let expected_trace_id = subscriber::with_default(json_test_subscriber(writer.clone()), || {
        let parent = tracing::info_span!(parent: None, "parent", room_id = "room-parent");
        let entered = tracing::info_span!(parent: None, "entered", room_id = "room-entered");
        let _guard = entered.enter();
        tracing::info!(parent: &parent, "explicit parent");
        tracing::info!(parent: None, "unparented event");
        #[cfg(feature = "otel-tracing")]
        let expected = Some(
            parent
                .context()
                .span()
                .span_context()
                .trace_id()
                .to_string(),
        );
        #[cfg(not(feature = "otel-tracing"))]
        let expected = None::<String>;
        expected
    });
    let values = json_values(&writer)?;
    let [parented, unparented] = values.as_slice() else {
        return Err(io::Error::other("expected two JSON logs").into());
    };
    assert_eq!(
        parented
            .get("spans")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(1)
    );
    assert_json_string(parented, "/spans/0/name", "parent");
    assert_json_string(parented, "/spans/0/fields/room_id", "room-parent");
    assert_eq!(
        parented.get("trace_id").and_then(Value::as_str),
        expected_trace_id.as_deref()
    );
    assert_eq!(
        unparented
            .get("spans")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );
    assert!(unparented.get("trace_id").is_none());
    Ok(())
}

fn json_test_subscriber(writer: SharedWriter) -> impl Subscriber + Send + Sync {
    let config = TelemetryConfig {
        log_format: TelemetryLogFormat::Json,
        resource: TelemetryResource {
            service_name: "o-sfu-test".to_owned(),
            deployment_environment: "test".to_owned(),
            service_instance_id: Some("test-instance".to_owned()),
        },
        trace_export: TraceExportConfig::default(),
        media_quality_interval: None,
    };
    let resource = telemetry_resource_fields(&config, 7);
    #[cfg(feature = "otel-tracing")]
    let tracer_provider = SdkTracerProvider::builder().build();
    #[cfg(feature = "otel-tracing")]
    let tracer = tracer_provider.tracer(TRACE_EXPORTER_NAME);
    let formatter = RuntimeJsonFormatter::new(resource);
    let subscriber = Registry::default().with(EnvFilter::new(DEFAULT_ENV_FILTER));
    #[cfg(feature = "otel-tracing")]
    let subscriber = subscriber.with(formatter.clone());
    let subscriber = subscriber.with(
        fmt_layer()
            .fmt_fields(JsonFields::new())
            .event_format(formatter)
            .with_ansi(false)
            .with_writer(writer),
    );
    #[cfg(feature = "otel-tracing")]
    let subscriber = subscriber.with(Some(OpenTelemetryLayer::new(tracer)));
    subscriber
}
