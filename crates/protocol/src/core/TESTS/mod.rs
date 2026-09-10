pub(super) use super::{
    BATCH_FLUSH_TIMER_ID, Command, ConnectionState, NegotiationKind, ProtocolCore, ProtocolEvent,
    RECOVERY_TIMER_ID, REQUEST_TIMEOUT_MS, StickyReplayState,
};
pub(super) use crate::{
    shared::{
        AvailableFeatures, DownloadStates, RecordingState, RecordingStateUpdate, StopCode,
        StreamType, UserInfo, VideoLayoutIntent,
    },
    signaling::{
        AuthPayload, ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientRequest,
        ClientResponse, Envelope, EnvelopeBatch, PeerInfoPayload, PeerLeftPayload, PeerSnapshot,
        RecordingActionResult, RecordingOptions, RequestId, ServerBroadcastPayload, ServerEnvelope,
        ServerMessage, ServerRequest, ServerResponse, SessionDescriptionPayload,
        StreamIntentPayload, SubscribePayload, TrackBinding, WebSocketCloseCode, WelcomePayload,
    },
};

mod batching;
mod connection;
mod negotiation;
mod recovery;
mod requests;
mod server_messages;

pub(super) fn sample_welcome_payload() -> WelcomePayload {
    WelcomePayload {
        features: AvailableFeatures {
            rtc: true,
            transcription: false,
            audio_recording: false,
            video_recording: true,
        },
        recording: RecordingState {
            recording: Some(false),
            audio: Some(false),
            transcription: Some(false),
            video: Some(false),
        },
        peers: vec![PeerSnapshot {
            user_id: 7_i64.into(),
            info: UserInfo {
                is_talking: Some(true),
                ..UserInfo::default()
            },
        }],
    }
}

fn decode_sent_client_envelopes(commands: &[Command]) -> Result<Vec<ClientEnvelope>, String> {
    let mut frames = commands.iter().filter_map(|command| match command {
        Command::SendWebSocket { frame } => Some(frame),
        _ => None,
    });
    let (Some(frame), None) = (frames.next(), frames.next()) else {
        return Err(format!("expected one WebSocket frame, got {commands:?}"));
    };
    let batch: EnvelopeBatch =
        serde_json::from_str(frame).map_err(|error| format!("invalid sent batch: {error}"))?;
    batch
        .into_iter()
        .map(|envelope| {
            ClientEnvelope::decode(envelope)
                .map_err(|error| format!("invalid sent client envelope: {error:?}"))
        })
        .collect()
}

pub(super) fn assert_sent_client_envelopes(commands: &[Command], expected: Vec<ClientEnvelope>) {
    assert_eq!(decode_sent_client_envelopes(commands), Ok(expected));
}

pub(super) fn encode_server_batch(envelope: ServerEnvelope) -> Result<String, String> {
    let envelope = envelope
        .into_envelope()
        .map_err(|error| error.to_string())?;
    serde_json::to_string(&[envelope]).map_err(|error| error.to_string())
}
use serde_json::json;
