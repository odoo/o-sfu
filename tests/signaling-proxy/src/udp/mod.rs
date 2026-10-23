//! Connection-scoped candidate projection and bounded datagram relays.

pub mod policy;
mod relay;
mod sdp;

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

pub use policy::Config;
use rama_ws::handshake::mitm::WebSocketRelayMessage;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Notify;

use crate::{
    Result,
    events::{Event, Events},
};

const MAX_ENDPOINTS: usize = 8;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    received: u64,
    forwarded: u64,
    dropped: u64,
    delayed: u64,
    duplicated: u64,
    overflow: u64,
    pending: u64,
    active_flows: u64,
    rules: Vec<u64>,
}

pub struct Metrics {
    received: AtomicU64,
    forwarded: AtomicU64,
    dropped: AtomicU64,
    delayed: AtomicU64,
    duplicated: AtomicU64,
    overflow: AtomicU64,
    pending: AtomicU64,
    active_flows: AtomicU64,
    rules: Vec<AtomicU64>,
}

impl Metrics {
    pub fn new(rule_count: usize) -> Self {
        Self {
            received: AtomicU64::new(0),
            forwarded: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            delayed: AtomicU64::new(0),
            duplicated: AtomicU64::new(0),
            overflow: AtomicU64::new(0),
            pending: AtomicU64::new(0),
            active_flows: AtomicU64::new(0),
            rules: (0..rule_count).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    pub fn snapshot(&self) -> Stats {
        Stats {
            received: self.received.load(Ordering::Relaxed),
            forwarded: self.forwarded.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            delayed: self.delayed.load(Ordering::Relaxed),
            duplicated: self.duplicated.load(Ordering::Relaxed),
            overflow: self.overflow.load(Ordering::Relaxed),
            pending: self.pending.load(Ordering::Relaxed),
            active_flows: self.active_flows.load(Ordering::Relaxed),
            rules: self
                .rules
                .iter()
                .map(|count| count.load(Ordering::Relaxed))
                .collect(),
        }
    }
}

pub struct MediaSession {
    connection_id: u64,
    config: Arc<Config>,
    armed: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    events: Events,
    abort: Arc<Notify>,
    endpoints: HashMap<SocketAddr, relay::Endpoint>,
    closed: bool,
}

impl MediaSession {
    pub fn new(
        connection_id: u64,
        config: Arc<Config>,
        armed: Arc<AtomicBool>,
        metrics: Arc<Metrics>,
        events: Events,
        abort: Arc<Notify>,
    ) -> Self {
        Self {
            connection_id,
            config,
            armed,
            metrics,
            events,
            abort,
            endpoints: HashMap::new(),
            closed: false,
        }
    }

    /// Projects recognized server SDP requests. Returns `BoxError` for invalid
    /// candidates, endpoint limits, serialization failures or socket setup errors.
    pub async fn rewrite(&mut self, message: &mut WebSocketRelayMessage) -> Result<()> {
        if self.closed {
            return Err("UDP media session is closed".into());
        }
        let WebSocketRelayMessage::Text(text) = message else {
            return Ok(());
        };
        let Ok(Value::Array(mut envelopes)) = serde_json::from_str::<Value>(text.as_str()) else {
            return Ok(());
        };
        let mut changed = false;
        for envelope in &mut envelopes {
            if !matches!(
                envelope.get("t").and_then(Value::as_str),
                Some("offer" | "renegotiate")
            ) || envelope.get("q").and_then(Value::as_str).is_none()
            {
                continue;
            }
            let sdp = envelope
                .get_mut("p")
                .and_then(|payload| payload.get_mut("sdp"))
                .ok_or("UDP relay requires SDP in server negotiation requests")?;
            let plan =
                sdp::Projection::parse(sdp.as_str().ok_or("UDP relay requires string SDP")?)?;
            for target in plan.targets() {
                if self.endpoints.contains_key(&target) {
                    continue;
                }
                if self.endpoints.len() >= MAX_ENDPOINTS {
                    self.metrics.overflow.fetch_add(1, Ordering::Relaxed);
                    return Err("UDP relay endpoint limit exceeded".into());
                }
                let endpoint = relay::Endpoint::bind(
                    self.connection_id,
                    target,
                    Arc::clone(&self.config),
                    Arc::clone(&self.armed),
                    Arc::clone(&self.metrics),
                    Arc::clone(&self.abort),
                )
                .await?;
                self.events.emit(Event::UdpBound {
                    connection_id: self.connection_id,
                    upstream: target,
                    address: endpoint.address(),
                });
                self.endpoints.insert(target, endpoint);
            }
            *sdp = Value::String(
                plan.render(|target| self.endpoints.get(&target).map(relay::Endpoint::address))?,
            );
            changed = true;
        }
        if changed {
            *text = serde_json::to_string(&envelopes)?.into();
        }
        Ok(())
    }

    pub async fn close(&mut self) {
        // Shutdown can race middleware that has not observed the WebSocket abort.
        // Closing admission prevents that middleware from reopening UDP sockets.
        self.closed = true;
        for endpoint in self.endpoints.values() {
            endpoint.stop();
        }
        for (_, endpoint) in self.endpoints.drain() {
            endpoint.close().await;
        }
    }
}
