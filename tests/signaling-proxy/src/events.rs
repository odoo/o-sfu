use std::{net::SocketAddr, time::Duration};

use serde::Serialize;
use tokio::{
    io::{self, AsyncWriteExt},
    sync::mpsc,
    task::JoinHandle,
    time,
};

use crate::udp::Stats;

const EVENT_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Serialize)]
#[serde(
    tag = "event",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum Event {
    Ready {
        url: String,
    },
    Connected {
        connection_id: u64,
    },
    Armed,
    Blocked {
        connection_id: u64,
    },
    Released {
        connection_id: u64,
    },
    Disconnected {
        connection_id: u64,
    },
    Dropped {
        connection_id: u64,
    },
    Corrupted {
        connection_id: u64,
    },
    Closed {
        connection_id: u64,
        code: u16,
    },
    UdpBound {
        connection_id: u64,
        upstream: SocketAddr,
        address: SocketAddr,
    },
    Stats {
        request_id: u64,
        udp: Stats,
    },
}

#[derive(Clone)]
pub struct Events {
    sender: mpsc::UnboundedSender<Option<Event>>,
}

impl Events {
    pub fn emit(&self, event: Event) {
        let _ = self.sender.send(Some(event));
    }
}

pub struct EventWriter {
    events: Events,
    writer: JoinHandle<()>,
}

impl EventWriter {
    pub fn new() -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel::<Option<Event>>();
        let writer = tokio::spawn(async move {
            let mut stdout = io::stdout();
            while let Some(Some(event)) = receiver.recv().await {
                let Ok(mut line) = serde_json::to_vec(&event) else {
                    break;
                };
                line.push(b'\n');
                if stdout.write_all(&line).await.is_err() || stdout.flush().await.is_err() {
                    break;
                }
            }
        });
        Self {
            events: Events { sender },
            writer,
        }
    }

    pub fn sender(&self) -> Events {
        self.events.clone()
    }

    pub async fn finish(mut self) {
        // Held relays can retain senders. The marker drains earlier events
        // without waiting for those relays or interrupting a stdout write.
        let _ = self.events.sender.send(None);
        if time::timeout(EVENT_DRAIN_TIMEOUT, &mut self.writer)
            .await
            .is_err()
        {
            self.writer.abort();
        }
    }
}
