use std::{
    io::{self, Write},
    sync::{
        Arc, Mutex, Once, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
};

use o_sfu_telemetry::schema::field as telemetry_field;
use serde_json::{Map, Value};
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use tracing::{Level, subscriber::set_global_default};
use tracing_subscriber::{Layer, filter, fmt::layer as fmt_layer, layer::SubscriberExt};

static CAPTURE_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());
static CAPTURE_INIT: Once = Once::new();
static CAPTURING: AtomicBool = AtomicBool::new(false);
static EVENTS: Mutex<Vec<u8>> = Mutex::new(Vec::new());

struct EventWriter;

impl Write for &EventWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        EVENTS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) struct CaptureGuard {
    _lock: AsyncMutexGuard<'static, ()>,
}

pub(crate) async fn capture() -> CaptureGuard {
    CAPTURE_INIT.call_once(|| {
        // `tracing` caches callsite interest so changing `CAPTURING` requires a per-event filter.
        let gate = filter::dynamic_filter_fn(|metadata, _| {
            CAPTURING.load(Ordering::Acquire) && *metadata.level() <= Level::INFO
        })
        .with_max_level_hint(Level::INFO);
        set_global_default(
            tracing_subscriber::registry().with(
                fmt_layer()
                    .json()
                    .with_writer(Arc::new(EventWriter))
                    .with_filter(gate),
            ),
        )
        .expect("test tracing subscriber must be installed once");
    });
    let lock = CAPTURE_LOCK.lock().await;
    EVENTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clear();
    CAPTURING.store(true, Ordering::Release);
    CaptureGuard { _lock: lock }
}

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        CAPTURING.store(false, Ordering::Release);
    }
}

pub(crate) fn assert_exact(name: &str, fields: &[(&str, Value)]) {
    let mut expected = fields
        .iter()
        .map(|(field, value)| ((*field).to_owned(), value.clone()))
        .collect::<Map<_, _>>();
    expected.insert(
        telemetry_field::EVENT.to_owned(),
        Value::String(name.to_owned()),
    );
    let events = EVENTS.lock().unwrap_or_else(PoisonError::into_inner);
    let mut matches = 0;
    for line in events
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let event: Value = serde_json::from_slice(line).expect("tracing event should be JSON");
        let fields = event["fields"]
            .as_object()
            .expect("tracing event should contain fields");
        if expected
            .iter()
            .all(|(field, value)| fields.get(field) == Some(value))
        {
            matches += 1;
            assert_eq!(fields.len(), expected.len() + 1);
            assert!(fields.contains_key(telemetry_field::MESSAGE));
        }
    }
    drop(events);
    assert_eq!(
        matches, 1,
        "expected one tracing event {name} with {fields:?}"
    );
}

pub(crate) fn assert_no_event(name: &str, fields: &[(&str, Value)]) {
    let events = EVENTS.lock().unwrap_or_else(PoisonError::into_inner);
    for line in events
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let event: Value = serde_json::from_slice(line).expect("tracing event should be JSON");
        let event_fields = event["fields"]
            .as_object()
            .expect("tracing event should contain fields");
        let matches = event_fields
            .get(telemetry_field::EVENT)
            .and_then(Value::as_str)
            == Some(name)
            && fields
                .iter()
                .all(|(field, value)| event_fields.get(*field) == Some(value));
        assert!(!matches, "unexpected tracing event {name}: {event:?}");
    }
}

pub(crate) fn assert_user_exact(
    name: &str,
    room_id: &str,
    canonical_user_id: &str,
    connection_id: u64,
    media_worker_id: usize,
    extra_fields: &[(&str, Value)],
) {
    let mut fields = vec![
        ("room_id", Value::from(room_id)),
        ("user_id", Value::from(canonical_user_id)),
        ("connection_id", Value::from(connection_id)),
        ("media_worker_id", Value::from(media_worker_id)),
    ];
    fields.extend_from_slice(extra_fields);
    assert_exact(name, &fields);
}

#[tokio::test(flavor = "current_thread")]
async fn capture_follows_guard_lifetime() {
    let emit = || tracing::info!(event = "capture.test", "capture event");
    emit();
    let guard = capture().await;
    emit();
    assert!(
        !EVENTS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    );
    drop(guard);

    emit();
    assert_eq!(
        EVENTS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .count(),
        1
    );
}
