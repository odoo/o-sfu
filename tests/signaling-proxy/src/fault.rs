//! One-shot rule selection, hold ownership and Rama event effects.

use std::{
    future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use o_sfu_rfc::websocket;
use rama_ws::{
    handshake::mitm::{
        WebSocketRelayClose, WebSocketRelayDirection, WebSocketRelayEvent,
        WebSocketRelayEventInput, WebSocketRelayEventOutput, WebSocketRelayMessage,
    },
    protocol::{CloseFrame, frame::coding::CloseCode},
};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{Mutex, Notify, oneshot};

use crate::{
    Direction, Result,
    events::{Event, Events},
};

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Hold,
    Disconnect,
    Drop,
    Corrupt,
    Close {
        code: u16,
        #[serde(default)]
        reason: String,
    },
}

enum Decision {
    Hold(oneshot::Receiver<()>),
    Disconnect,
    Drop,
    Corrupt,
    Close(CloseFrame),
}

/// Applies one fault per process to a matching envelope's whole WebSocket message.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    direction: Direction,
    t: String,
    /// Require a string response ID in `r` when true or require its absence
    /// when false. Omit this field to match either routing form.
    response: Option<bool>,
    action: Action,
}

impl Rule {
    /// Returns `BoxError` for a forbidden close code or a reason exceeding
    /// [`websocket::MAX_CLOSE_REASON_BYTES`].
    pub fn validate(&self) -> Result<()> {
        if let Action::Close { code, reason } = &self.action
            && (!CloseCode::from(*code).is_allowed()
                || reason.len() > websocket::MAX_CLOSE_REASON_BYTES)
        {
            return Err(format!(
                "close requires an allowed WebSocket code and a reason of at most {} bytes",
                websocket::MAX_CLOSE_REASON_BYTES,
            )
            .into());
        }
        Ok(())
    }

    fn matches(&self, direction: Direction, message: &WebSocketRelayMessage) -> bool {
        if self.direction != direction {
            return false;
        }
        let WebSocketRelayMessage::Text(text) = message else {
            return false;
        };
        let Ok(Value::Array(envelopes)) = serde_json::from_str::<Value>(text.as_str()) else {
            return false;
        };
        envelopes.iter().any(|envelope| {
            envelope.get("t").and_then(Value::as_str) == Some(self.t.as_str())
                && self.response.is_none_or(|response| {
                    envelope.get("r").and_then(Value::as_str).is_some() == response
                })
        })
    }
}

struct Control {
    consumed: bool,
    held: Option<(u64, oneshot::Sender<()>)>,
}

pub struct Faults {
    rule: Option<Rule>,
    armed: Arc<AtomicBool>,
    control: Mutex<Control>,
    events: Events,
}

impl Faults {
    pub fn new(rule: Option<Rule>, armed: Arc<AtomicBool>, events: Events) -> Self {
        Self {
            rule,
            armed,
            control: Mutex::new(Control {
                consumed: false,
                held: None,
            }),
            events,
        }
    }

    pub async fn release(&self, connection_id: u64) {
        let mut control = self.control.lock().await;
        if let Some((_, release)) = control.held.take_if(|(id, _)| *id == connection_id) {
            let _ = release.send(());
        }
    }

    pub async fn connection_closed(&self, connection_id: u64) {
        self.control
            .lock()
            .await
            .held
            .take_if(|(id, _)| *id == connection_id);
    }

    pub async fn apply(
        &self,
        input: WebSocketRelayEventInput,
        id: u64,
        abort: &Notify,
    ) -> WebSocketRelayEventOutput {
        let direction = match input.direction {
            WebSocketRelayDirection::Ingress => Direction::ClientToServer,
            WebSocketRelayDirection::Egress => Direction::ServerToClient,
        };
        let decision = match &input.event {
            WebSocketRelayEvent::Data(message) => self.decide(id, direction, message).await,
            _ => None,
        };
        let mut output = WebSocketRelayEventOutput::from(input);
        match decision {
            Some(Decision::Hold(release)) => {
                if release.await.is_err() {
                    abort.notify_one();
                    return future::pending().await;
                }
                self.events.emit(Event::Released { connection_id: id });
            }
            Some(Decision::Disconnect) => {
                self.events.emit(Event::Disconnected { connection_id: id });
                abort.notify_one();
                // Keep this direction paused until the outer relay handles the abort.
                return future::pending().await;
            }
            Some(Decision::Drop) => {
                output.messages.clear();
                self.events.emit(Event::Dropped { connection_id: id });
            }
            Some(Decision::Corrupt) => {
                output.messages = vec![WebSocketRelayMessage::Text("{".into())];
                self.events.emit(Event::Corrupted { connection_id: id });
            }
            Some(Decision::Close(frame)) => {
                self.events.emit(Event::Closed {
                    connection_id: id,
                    code: u16::from(frame.code),
                });
                output.messages.clear();
                output.close = Some(WebSocketRelayClose::WithFrame(frame));
            }
            None => {}
        }
        output
    }

    async fn decide(
        &self,
        connection_id: u64,
        direction: Direction,
        message: &WebSocketRelayMessage,
    ) -> Option<Decision> {
        let rule = self.rule.as_ref()?;
        let mut control = self.control.lock().await;
        if !self.armed.load(Ordering::Acquire)
            || control.consumed
            || !rule.matches(direction, message)
        {
            return None;
        }
        // Consumption and hold registration must be atomic with release commands.
        control.consumed = true;
        Some(match &rule.action {
            Action::Hold => {
                let (tx, rx) = oneshot::channel();
                control.held = Some((connection_id, tx));
                self.events.emit(Event::Blocked { connection_id });
                // Queue the blocked event before a release can acquire control.
                drop(control);
                Decision::Hold(rx)
            }
            Action::Disconnect => Decision::Disconnect,
            Action::Drop => Decision::Drop,
            Action::Corrupt => Decision::Corrupt,
            Action::Close { code, reason } => Decision::Close(CloseFrame {
                code: (*code).into(),
                reason: reason.clone().into(),
            }),
        })
    }
}
