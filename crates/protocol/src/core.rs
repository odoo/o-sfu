//! Pure client-side signaling state machine for the `o-sfu` protocol.
//!
//! [`ProtocolCore`] performs no I/O. Each transition returns ordered [`Command`]
//! values for the host to execute before reporting follow-up events. This keeps
//! transitions deterministic and lets Wasm, native and test hosts share the
//! same lifecycle rules.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

mod connection_lifecycle;
mod outbound_batch;
mod request_flow;
mod request_tracker;
mod server_events;
mod sticky_replay;
mod timers;

use outbound_batch::{FlushMode, OutboundBatcher};
use request_tracker::RequestTracker;
use sticky_replay::StickyReplayState;
use timers::RequestTimeoutId;

use crate::{
    shared::{
        AvailableFeatures, DownloadStates, JsonPayload, RecordingState, RecordingStateUpdate,
        StreamType, UserId, UserInfo,
    },
    signaling::{
        AuthPayload, ClientBroadcastPayload, ClientEnvelope, ClientMessage, MAX_ENVELOPE_BATCH_LEN,
        NegotiationUploadSlot, PeerSnapshot, RequestId, ServerEnvelope, StreamIntentPayload,
        SubscribePayload, TrackBinding, WebSocketCloseCode, WelcomePayload, decode_envelope_batch,
    },
    wire::ServerMessage,
};

/// host-facing timer id used by the recovery backoff scheduler
pub const RECOVERY_TIMER_ID: u32 = 1;
const BATCH_FLUSH_TIMER_ID: u32 = 2;
const INITIAL_RECOVERY_DELAY_MS: u32 = 1_000;
const MAX_RECOVERY_DELAY_MS: u32 = 30_000;
const BATCH_FLUSH_DELAY_MS: u32 = 100;
const REQUEST_TIMEOUT_MS: u32 = 5_000;
const MAX_OUTBOUND_BATCH_LEN: usize = 16;

/// One ordered side effect for the host that drives [`ProtocolCore`].
///
/// The host must execute each returned vector before reporting follow-up events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Command {
    /// Send the already serialized JSON frame unchanged over the WebSocket.
    SendWebSocket {
        frame: String,
    },
    /// Apply a remote SDP offer to the local `RTCPeerConnection`.
    ApplyNegotiation {
        #[serde(rename = "requestId")]
        request_id: RequestId,
        #[serde(rename = "negotiationKind")]
        kind: NegotiationKind,
        sdp: String,
        #[serde(rename = "uploadSlots")]
        upload_slots: Vec<NegotiationUploadSlot>,
    },
    ClosePeerConnection,
    CloseWebSocket {
        code: u16,
    },
    /// Notify listeners of a connection-state transition, with an optional
    /// human-readable cause (e.g. `"kicked"`, `"full"`).
    EmitStateChange {
        state: ConnectionState,
        cause: Option<String>,
    },
    SetAvailableFeatures {
        features: AvailableFeatures,
    },
    SetRecordingState {
        state: RecordingState,
    },
    /// Emit a protocol-domain event for the host projection layer.
    #[serde(rename = "emitUpdate")]
    EmitEvent {
        #[serde(
            rename = "update",
            serialize_with = "crate::host_bridge::serialize_protocol_event"
        )]
        event: ProtocolEvent,
    },
    BeginPendingRequest {
        request: PendingRequest,
    },
    /// Cancel `timeout_timer_id` before resolving `request_id`.
    CompletePendingRequest {
        #[serde(rename = "requestId")]
        request_id: RequestId,
        #[serde(rename = "timeoutTimerId")]
        timeout_timer_id: u32,
        ok: bool,
    },
    /// Start a one-shot timer; the host must call [`ProtocolCore::on_timer`]
    /// when it fires.
    ScheduleTimer {
        id: u32,
        ms: u32,
    },
    CancelTimer {
        id: u32,
    },
    /// Open a new WebSocket to the given URL.
    Connect {
        url: String,
    },
}

pub(crate) type Commands = Vec<Command>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Authenticated,
    Connected,
    Recovering,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolEvent {
    PeerSnapshot {
        peers: Vec<PeerSnapshot>,
    },
    TrackSnapshot {
        bindings: Vec<TrackBinding>,
    },
    PeerInfo {
        user_id: UserId,
        info: UserInfo,
    },
    PeerLeft {
        user_id: UserId,
    },
    Broadcast {
        sender_id: UserId,
        message: JsonPayload,
    },
    RecordingStateChanged {
        state: RecordingStateUpdate,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NegotiationKind {
    Offer,
    Renegotiate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PendingRequestKind {
    StartRecording,
    StopRecording,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingRequest {
    pub request_id: RequestId,
    pub timeout_timer_id: u32,
    pub timeout_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConnectContext {
    url: String,
    jwt: String,
    room: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProtocolPhase {
    Disconnected,
    Connecting,
    Authenticated(Option<RequestId>),
    Connected(Option<RequestId>),
    Recovering,
    Closed,
}

impl ProtocolPhase {
    const fn connection_state(&self) -> ConnectionState {
        match self {
            Self::Disconnected => ConnectionState::Disconnected,
            Self::Connecting => ConnectionState::Connecting,
            Self::Authenticated(_) => ConnectionState::Authenticated,
            Self::Connected(_) => ConnectionState::Connected,
            Self::Recovering => ConnectionState::Recovering,
            Self::Closed => ConnectionState::Closed,
        }
    }

    const fn is_awaiting_welcome(&self) -> bool {
        matches!(self, Self::Connecting | Self::Recovering)
    }

    const fn can_send_client_messages(&self) -> bool {
        matches!(self, Self::Authenticated(_) | Self::Connected(_))
    }

    const fn can_enter_connected(&self) -> bool {
        matches!(self, Self::Authenticated(None))
    }

    fn accept_negotiation(
        &mut self,
        request_id: &RequestId,
        kind: NegotiationKind,
    ) -> Result<(), NegotiationRejection> {
        match (self, kind) {
            (Self::Authenticated(pending), NegotiationKind::Offer)
            | (Self::Connected(pending), NegotiationKind::Renegotiate) => {
                if pending.is_some() {
                    return Err(NegotiationRejection::ProtocolError);
                }
                *pending = Some(request_id.clone());
                Ok(())
            }
            (Self::Authenticated(_), NegotiationKind::Renegotiate)
            | (Self::Connected(_), NegotiationKind::Offer) => {
                Err(NegotiationRejection::ProtocolError)
            }
            (
                Self::Disconnected | Self::Connecting | Self::Recovering | Self::Closed,
                NegotiationKind::Offer | NegotiationKind::Renegotiate,
            ) => Err(NegotiationRejection::Ignored),
        }
    }

    fn resolve_negotiation(&mut self, request_id: &RequestId, kind: NegotiationKind) -> bool {
        let pending = match (self, kind) {
            (Self::Authenticated(pending), NegotiationKind::Offer)
            | (Self::Connected(pending), NegotiationKind::Renegotiate) => pending,
            (Self::Authenticated(_), NegotiationKind::Renegotiate)
            | (Self::Connected(_), NegotiationKind::Offer)
            | (
                Self::Disconnected | Self::Connecting | Self::Recovering | Self::Closed,
                NegotiationKind::Offer | NegotiationKind::Renegotiate,
            ) => return false,
        };
        if pending.as_ref() != Some(request_id) {
            return false;
        }
        *pending = None;
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NegotiationRejection {
    Ignored,
    ProtocolError,
}

/// The stored state falls into three groups:
///   - session state needed to interpret later protocol messages
///   - remembered client intent that should survive reconnects
///   - in-flight host work that must be cancelled or resolved during cleanup
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolCore {
    /// Lifecycle and server-driven negotiation state.
    phase: ProtocolPhase,
    /// Users retained by SDP MID for peer departure and teardown cleanup.
    ///
    /// Track snapshots replace the map and the last binding for a MID wins.
    /// Peer departures remove their entries. Disconnect and socket loss clear it.
    track_users_by_mid: BTreeMap<String, UserId>,
    /// Latest client intent that must be replayed after a recovered socket is
    /// authenticated.
    ///
    /// Publication, subscription and local user-info updates are kept here
    /// because they describe what the user still wants. One-off broadcasts and
    /// request-response operations are not sticky because replaying them later
    /// would change their meaning.
    sticky_replay: StickyReplayState,
    /// Saved admission context for the active connection attempt.
    ///
    /// Recovery reuses this URL, JWT and optional room to open the next socket.
    /// Explicit disconnects, terminal close codes and fresh connects clear or
    /// replace it so old credentials cannot revive a stopped session.
    connect_context: Option<ConnectContext>,
    /// Delay that will be used for the next recovery retry.
    ///
    /// The value is reset after a successful welcome or intentional lifecycle
    /// reset. Transient websocket loss consumes the current value when
    /// scheduling recovery, then increases it for the following retry.
    recovery_delay_ms: u32,
    /// Buffered outbound envelopes waiting for an immediate flush, size limit
    /// or batch timer.
    ///
    /// The batcher owns only serializable protocol envelopes and the knowledge
    /// that a flush timer is pending. The host still owns the actual timer and
    /// websocket write side effects emitted as commands.
    outbound_batch: OutboundBatcher,
    /// Tracks request-response operations that must resolve exactly once.
    ///
    /// Each live request is paired with one timeout timer. Responses and timer
    /// callbacks both flow through this tracker so stale, mismatched or racing
    /// events cannot resolve the wrong host promise.
    request_tracker: RequestTracker,
}

impl Default for ProtocolCore {
    fn default() -> Self {
        Self::new()
    }
}

impl ProtocolCore {
    /// Builds a fresh protocol state machine with no remembered user intent.
    ///
    /// Reconnect replay is opt-in through the mutating APIs below, so a new
    /// core starts from a fully fresh state instead of assuming any previous room,
    /// publication, or subscription state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            phase: ProtocolPhase::Disconnected,
            track_users_by_mid: BTreeMap::new(),
            sticky_replay: StickyReplayState::new(),
            connect_context: None,
            recovery_delay_ms: INITIAL_RECOVERY_DELAY_MS,
            outbound_batch: OutboundBatcher::new(),
            request_tracker: RequestTracker::new(),
        }
    }

    #[must_use]
    pub const fn state(&self) -> ConnectionState {
        self.phase.connection_state()
    }

    /// Authenticates a newly opened socket with the stored connect context.
    ///
    /// Recovery reuses the same JWT and optional room that [`ProtocolCore::connect`] captured,
    /// which keeps every socket attempt tied to one explicit admission context.
    pub fn on_ws_open(&mut self) -> Vec<Command> {
        if !self.phase.is_awaiting_welcome() {
            return Vec::new();
        }
        let Some(connect_context) = self.connect_context.as_ref() else {
            return Vec::new();
        };
        self.enqueue_client_message(
            ClientMessage::Auth(AuthPayload {
                jwt: connect_context.jwt.clone(),
                channel: connect_context.room.clone(),
            }),
            FlushMode::Immediate,
        )
    }

    /// handle ws message
    ///
    /// Malformed batches or envelopes are treated as protocol violations.
    /// The whole batch is decoded before any envelope is applied so partially
    /// applied server state cannot survive after a later decode error.
    pub fn on_ws_message(&mut self, frame: &str) -> Vec<Command> {
        let Ok(batch) = decode_envelope_batch(frame, MAX_ENVELOPE_BATCH_LEN) else {
            return close_for_protocol_error();
        };
        let Ok(envelopes) = batch
            .into_iter()
            .map(ServerEnvelope::decode)
            .collect::<Result<Vec<_>, _>>()
        else {
            return close_for_protocol_error();
        };
        let mut commands = Vec::new();
        for envelope in envelopes {
            match envelope {
                ServerEnvelope::Message(message) => {
                    if self.phase.is_awaiting_welcome()
                        && !matches!(message, ServerMessage::Welcome(_))
                    {
                        return close_for_protocol_error();
                    }
                    commands.extend(server_events::handle_server_message(self, message));
                }
                ServerEnvelope::Request {
                    request_id,
                    request,
                } => {
                    commands.extend(request_flow::handle_server_request(
                        self, request_id, request,
                    ));
                }
                ServerEnvelope::Response {
                    response_to,
                    response,
                } => {
                    commands.extend(request_flow::handle_server_response(
                        self,
                        &response_to,
                        response,
                    ));
                }
            }
        }
        commands
    }

    fn accept_welcome(&mut self, payload: WelcomePayload) -> Commands {
        if !self.phase.is_awaiting_welcome() {
            return Vec::new();
        }
        let WelcomePayload {
            features,
            recording,
            peers,
        } = payload;
        self.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
        self.phase = ProtocolPhase::Authenticated(None);

        let mut commands = vec![
            Command::SetAvailableFeatures { features },
            Command::SetRecordingState { state: recording },
            Command::EmitStateChange {
                state: self.phase.connection_state(),
                cause: None,
            },
        ];
        if !peers.is_empty() {
            commands.push(Command::EmitEvent {
                event: ProtocolEvent::PeerSnapshot { peers },
            });
        }
        commands.extend(self.replay_session_state());
        commands
    }

    /// Marks the local transport layer as ready after the initial negotiation.
    ///
    /// The host should call this only once the peer connection is usable for
    /// media, because it is what upgrades the core from authenticated signaling
    /// state to a fully connected user.
    pub fn on_transport_ready(&mut self) -> Vec<Command> {
        if !self.phase.can_enter_connected() {
            return Vec::new();
        }
        self.phase = ProtocolPhase::Connected(None);
        let mut commands = vec![Command::EmitStateChange {
            state: self.state(),
            cause: None,
        }];
        commands.extend(self.replay_publication_state());
        commands
    }

    /// Stores the desired publication state and sends it when the media transport is ready.
    ///
    /// Publish intent is sticky across reconnects, which lets UI toggles be issued
    /// before authentication completes without losing the latest desired state.
    pub fn publish(&mut self, stream_type: StreamType, active: bool) -> Vec<Command> {
        self.sticky_replay.set_publish_active(stream_type, active);
        if !matches!(&self.phase, ProtocolPhase::Connected(_)) {
            return Vec::new();
        }
        let message = if active {
            ClientMessage::Publish(StreamIntentPayload { stream_type })
        } else {
            ClientMessage::Unpublish(StreamIntentPayload { stream_type })
        };
        self.enqueue_client_message(message, FlushMode::Batched)
    }

    /// Remembers the latest per-peer subscription intent for reconnect replay.
    ///
    /// Repeated updates merge at the sticky layer, so callers can send partial
    /// audio/camera/screen adjustments without rebuilding the full preference set
    /// on every change or after recovery.
    pub fn subscribe(&mut self, user_id: UserId, states: DownloadStates) -> Vec<Command> {
        self.sticky_replay
            .remember_subscription_states(&user_id, &states);
        if !self.phase.can_send_client_messages() {
            return Vec::new();
        }
        self.enqueue_client_message(
            ClientMessage::Subscribe(SubscribePayload { user_id, states }),
            FlushMode::Batched,
        )
    }

    /// Persists the latest local user metadata patch for the current room.
    ///
    /// User info is replayed after reconnect so transient transport failures do
    /// not silently reset presence indicators such as mute, hand raise or camera
    /// state back to server defaults.
    pub fn update_info(&mut self, info: UserInfo) -> Vec<Command> {
        self.sticky_replay.remember_info(&info);
        if !self.phase.can_send_client_messages() {
            return Vec::new();
        }
        self.enqueue_client_message(ClientMessage::Info(info), FlushMode::Batched)
    }

    /// Sends a best-effort broadcast to the current room.
    ///
    /// Broadcast payloads are not sticky: if the client is not yet
    /// authenticated, the message is dropped instead of being replayed later out
    /// of its original conversational context.
    pub fn broadcast(&mut self, message: JsonPayload) -> Vec<Command> {
        if !self.phase.can_send_client_messages() {
            return Vec::new();
        }
        self.enqueue_client_message(
            ClientMessage::Broadcast(ClientBroadcastPayload { message }),
            FlushMode::Batched,
        )
    }

    /// Dispatches all timer callbacks through one entry point.
    ///
    /// Timer ids are part of the protocol-core contract: recovery, outbound batch
    /// flushing, and request timeouts each reserve their own namespace and must be
    /// routed back here by the host in the order they fire.
    pub fn on_timer(&mut self, timer_id: u32) -> Vec<Command> {
        if timer_id == RECOVERY_TIMER_ID {
            return connection_lifecycle::handle_recovery_timer(self);
        }
        if timer_id == BATCH_FLUSH_TIMER_ID {
            return self.outbound_batch.flush(false);
        }
        if let Some(commands) = RequestTimeoutId::try_from_raw(timer_id)
            .and_then(|timeout_id| self.request_tracker.resolve_timeout(timeout_id))
        {
            return commands;
        }
        Vec::new()
    }

    fn enqueue_client_message(&mut self, message: ClientMessage, mode: FlushMode) -> Commands {
        let Some(envelope) = ClientEnvelope::Message(message).into_envelope().ok() else {
            return Vec::new();
        };
        self.outbound_batch.enqueue(envelope, mode)
    }

    fn clear_runtime_state(&mut self) {
        self.track_users_by_mid.clear();
        self.outbound_batch.clear();
        self.request_tracker.clear();
    }

    /// Tears down runtime state while emitting the cleanup commands the host still owes.
    ///
    /// This is used on disconnect and terminal close paths where queued batches,
    /// timeout timers, and pending requests must be cancelled explicitly instead of
    /// being forgotten inside the pure state machine.
    fn teardown_runtime_state(&mut self) -> Commands {
        let mut commands = self.outbound_batch.discard_pending();
        commands.extend(self.request_tracker.fail_all());
        if !self.track_users_by_mid.is_empty() {
            self.track_users_by_mid.clear();
            commands.push(Command::EmitEvent {
                event: ProtocolEvent::TrackSnapshot {
                    bindings: Vec::new(),
                },
            });
        }
        commands
    }

    /// Flushes room-level intent immediately after the server snapshot is known.
    fn replay_session_state(&mut self) -> Commands {
        if !self.phase.can_send_client_messages() {
            return Vec::new();
        }
        let Some(replay_batch) = self.sticky_replay.replay_session_batch() else {
            return Vec::new();
        };
        self.outbound_batch.extend(replay_batch);
        self.outbound_batch.flush(true)
    }

    /// Flushes publish intent after the recovered media transport is ready.
    fn replay_publication_state(&mut self) -> Commands {
        if !self.phase.can_send_client_messages() {
            return Vec::new();
        }
        let replay_batch: Vec<_> = self
            .sticky_replay
            .active_publications()
            .filter_map(|stream_type| {
                ClientEnvelope::Message(ClientMessage::Publish(StreamIntentPayload { stream_type }))
                    .into_envelope()
                    .ok()
            })
            .collect();
        if replay_batch.is_empty() {
            return Vec::new();
        }
        self.outbound_batch.extend(replay_batch);
        self.outbound_batch.flush(true)
    }
}

fn empty_features() -> AvailableFeatures {
    AvailableFeatures {
        rtc: false,
        transcription: false,
        audio_recording: false,
        video_recording: false,
    }
}

fn close_for_protocol_error() -> Commands {
    vec![Command::CloseWebSocket {
        code: u16::from(WebSocketCloseCode::ProtocolError),
    }]
}

/// Grows reconnect delay by 1.5x while keeping the backoff bounded.
///
/// The sequence is modest so short-lived outages recover quickly,
/// but repeated failures still spread out retries and avoid hot-loop reconnects.
fn next_recovery_delay(current_delay_ms: u32) -> u32 {
    (current_delay_ms.saturating_mul(3) / 2).min(MAX_RECOVERY_DELAY_MS)
}

#[cfg(test)]
#[path = "core/TESTS/mod.rs"]
mod tests;
