//! Shared activation, diagnostics and connection lifetimes for both transports.

use std::{
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use rama_core::telemetry::tracing;
use rama_ws::handshake::mitm::{
    WebSocketRelayDirection, WebSocketRelayEventInput, WebSocketRelayEventOutput,
};
use tokio::sync::{Mutex, Notify};

use crate::{
    Result,
    events::{Event, Events},
    fault::{Faults, Rule},
    udp::{self, MediaSession, Metrics},
};

#[derive(Default)]
struct Connections {
    closing: bool,
    sessions: HashMap<u64, Weak<Connection>>,
}

pub struct Scenario {
    faults: Arc<Faults>,
    udp: Option<Arc<udp::Config>>,
    armed: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    events: Events,
    connections: Mutex<Connections>,
}

impl Scenario {
    pub fn new(
        rule: Option<Rule>,
        udp: Option<udp::Config>,
        deferred: bool,
        events: Events,
    ) -> Self {
        let armed = Arc::new(AtomicBool::new(!deferred));
        let metrics = Arc::new(Metrics::new(
            udp.as_ref().map_or(0, |config| config.rules.len()),
        ));
        Self {
            faults: Arc::new(Faults::new(rule, Arc::clone(&armed), events.clone())),
            udp: udp.map(Arc::new),
            armed,
            metrics,
            events,
            connections: Mutex::new(Connections::default()),
        }
    }

    pub fn arm(&self) {
        self.armed.store(true, Ordering::Release);
        self.events.emit(Event::Armed);
    }

    pub async fn release(&self, id: u64) {
        self.faults.release(id).await;
    }

    pub fn stats(&self, request_id: u64) {
        self.events.emit(Event::Stats {
            request_id,
            udp: self.metrics.snapshot(),
        });
    }

    /// Returns `BoxError` if process shutdown has already closed admission.
    pub async fn connect(&self, id: u64, abort: Arc<Notify>) -> Result<Arc<Connection>> {
        let mut connections = self.connections.lock().await;
        if connections.closing {
            return Err("scenario is stopping".into());
        }
        let media = self.udp.as_ref().map(|config| {
            Mutex::new(MediaSession::new(
                id,
                Arc::clone(config),
                Arc::clone(&self.armed),
                Arc::clone(&self.metrics),
                self.events.clone(),
                Arc::clone(&abort),
            ))
        });
        let connection = Arc::new(Connection {
            id,
            faults: Arc::clone(&self.faults),
            media,
            abort,
        });
        connections.sessions.insert(id, Arc::downgrade(&connection));
        drop(connections);
        Ok(connection)
    }

    pub async fn disconnect(&self, connection: &Connection) {
        connection.close().await;
        self.connections
            .lock()
            .await
            .sessions
            .remove(&connection.id);
    }

    pub async fn shutdown(&self) {
        let sessions: Vec<_> = {
            let mut connections = self.connections.lock().await;
            connections.closing = true;
            connections
                .sessions
                .drain()
                .filter_map(|(_, session)| session.upgrade())
                .collect()
        };
        for session in &sessions {
            session.abort.notify_one();
        }
        for session in sessions {
            session.close().await;
        }
    }
}

pub struct Connection {
    id: u64,
    faults: Arc<Faults>,
    media: Option<Mutex<MediaSession>>,
    abort: Arc<Notify>,
}

impl Connection {
    /// Returns `BoxError` when a forwarded server offer cannot be routed through UDP.
    pub async fn apply(
        self: Arc<Self>,
        input: WebSocketRelayEventInput,
    ) -> Result<WebSocketRelayEventOutput> {
        // O-SFU is ICE-lite, so browsers initiate checks. Projecting server
        // offers routes both UDP directions without rewriting client answers.
        let project = matches!(input.direction, WebSocketRelayDirection::Egress);
        let mut output = self.faults.apply(input, self.id, &self.abort).await;
        if project && let Some(media) = &self.media {
            let mut media = media.lock().await;
            // Project only forwarded offers. A dropped offer must not create a media route.
            for message in &mut output.messages {
                if let Err(error) = media.rewrite(message).await {
                    tracing::warn!(connectionId = self.id, %error, "UDP candidate projection failed");
                    return Err(error);
                }
            }
        }
        Ok(output)
    }

    async fn close(&self) {
        if let Some(media) = &self.media {
            media.lock().await.close().await;
        }
        self.faults.connection_closed(self.id).await;
    }
}
