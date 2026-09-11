use o_sfu_model::RecordingOptions;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::{
    AuthPayload, ClientBroadcastPayload, Envelope, PeerInfoPayload, PeerLeftPayload,
    RecordingActionResult, RequestId, ServerBroadcastPayload, SessionDescriptionPayload,
    SourceDescriptor, StreamIntentPayload, SubscribePayload, TrackBinding, WelcomePayload,
};
use crate::shared::{RecordingStateUpdate, UserInfo};

const AUTH: &str = "auth";
const BROADCAST: &str = "broadcast";
const INFO: &str = "info";
const OFFER: &str = "offer";
const PEER_INFO: &str = "peerinfo";
const PEER_JOINED: &str = "peerjoined";
const PEER_LEFT: &str = "peerleft";
const PUBLISH: &str = "publish";
const RECORDING_CHANGE: &str = "recordingchange";
const RENEGOTIATE: &str = "renegotiate";
const START_RECORDING: &str = "startrecording";
const STOP_RECORDING: &str = "stoprecording";
const SUBSCRIBE: &str = "subscribe";
const SOURCES: &str = "sources";
const TRACKS: &str = "tracks";
const UNPUBLISH: &str = "unpublish";
const WELCOME: &str = "welcome";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeDecodeError {
    #[error("unknown envelope tag: {0}")]
    UnknownTag(String),
    #[error("invalid payload for envelope tag: {0}")]
    InvalidPayload(String),
    #[error("unexpected payload for envelope tag: {0}")]
    UnexpectedPayload(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage {
    Auth(AuthPayload),
    Publish(StreamIntentPayload),
    Unpublish(StreamIntentPayload),
    Subscribe(SubscribePayload),
    Info(UserInfo),
    Broadcast(ClientBroadcastPayload),
}

impl ClientMessage {
    pub(crate) fn into_envelope(self) -> Result<Envelope, serde_json::Error> {
        match self {
            Self::Auth(payload) => encode_message(AUTH, payload),
            Self::Publish(payload) => encode_message(PUBLISH, payload),
            Self::Unpublish(payload) => encode_message(UNPUBLISH, payload),
            Self::Subscribe(payload) => encode_message(SUBSCRIBE, payload),
            Self::Info(payload) => encode_message(INFO, payload),
            Self::Broadcast(payload) => encode_message(BROADCAST, payload),
        }
    }

    pub(crate) fn decode(tag: &str, payload: Option<Value>) -> Result<Self, EnvelopeDecodeError> {
        match tag {
            AUTH => parse_payload(tag, payload).map(Self::Auth),
            PUBLISH => parse_payload(tag, payload).map(Self::Publish),
            UNPUBLISH => parse_payload(tag, payload).map(Self::Unpublish),
            SUBSCRIBE => parse_payload(tag, payload).map(Self::Subscribe),
            INFO => parse_payload(tag, payload).map(Self::Info),
            BROADCAST => parse_payload(tag, payload).map(Self::Broadcast),
            _ => Err(EnvelopeDecodeError::UnknownTag(tag.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientRequest {
    StartRecording(RecordingOptions),
    StopRecording,
}

impl ClientRequest {
    pub(crate) fn into_envelope(
        self,
        request_id: RequestId,
    ) -> Result<Envelope, serde_json::Error> {
        match self {
            Self::StartRecording(payload) => encode_request(START_RECORDING, request_id, payload),
            Self::StopRecording => Ok(Envelope::request(STOP_RECORDING, request_id, None)),
        }
    }

    pub(crate) fn decode(tag: &str, payload: Option<Value>) -> Result<Self, EnvelopeDecodeError> {
        match tag {
            START_RECORDING => parse_payload(tag, payload).map(Self::StartRecording),
            STOP_RECORDING => {
                ensure_empty_payload(tag, payload.as_ref())?;
                Ok(Self::StopRecording)
            }
            _ => Err(EnvelopeDecodeError::UnknownTag(tag.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerRequest {
    Offer(SessionDescriptionPayload),
    Renegotiate(SessionDescriptionPayload),
}

impl ServerRequest {
    /// encode one server-authored request into the Odoo wire envelope
    ///
    /// # Errors
    ///
    /// returns an error when the typed payload cannot be serialized into the
    /// JSON envelope payload
    pub fn into_envelope(self, request_id: RequestId) -> Result<Envelope, serde_json::Error> {
        match self {
            Self::Offer(payload) => encode_request(OFFER, request_id, payload),
            Self::Renegotiate(payload) => encode_request(RENEGOTIATE, request_id, payload),
        }
    }

    pub(crate) fn decode(tag: &str, payload: Option<Value>) -> Result<Self, EnvelopeDecodeError> {
        match tag {
            OFFER => parse_payload(tag, payload).map(Self::Offer),
            RENEGOTIATE => parse_payload(tag, payload).map(Self::Renegotiate),
            _ => Err(EnvelopeDecodeError::UnknownTag(tag.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientResponse {
    Offer(SessionDescriptionPayload),
    Renegotiate(SessionDescriptionPayload),
}

impl ClientResponse {
    pub(crate) fn into_envelope(
        self,
        response_to: RequestId,
    ) -> Result<Envelope, serde_json::Error> {
        match self {
            Self::Offer(payload) => encode_response(OFFER, response_to, payload),
            Self::Renegotiate(payload) => encode_response(RENEGOTIATE, response_to, payload),
        }
    }

    pub(crate) fn decode(tag: &str, payload: Option<Value>) -> Result<Self, EnvelopeDecodeError> {
        match tag {
            OFFER => parse_payload(tag, payload).map(Self::Offer),
            RENEGOTIATE => parse_payload(tag, payload).map(Self::Renegotiate),
            _ => Err(EnvelopeDecodeError::UnknownTag(tag.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMessage {
    Welcome(WelcomePayload),
    Tracks(Vec<TrackBinding>),
    Sources(Vec<SourceDescriptor>),
    PeerInfo(PeerInfoPayload),
    PeerJoined(PeerInfoPayload),
    PeerLeft(PeerLeftPayload),
    Broadcast(ServerBroadcastPayload),
    RecordingChange(RecordingStateUpdate),
}

impl ServerMessage {
    /// encode one server message into the Odoo wire envelope
    ///
    /// # Errors
    ///
    /// returns an error when the typed payload cannot be serialized into the
    /// JSON envelope payload
    pub fn into_envelope(self) -> Result<Envelope, serde_json::Error> {
        match self {
            Self::Welcome(payload) => encode_message(WELCOME, payload),
            Self::Tracks(payload) => encode_message(TRACKS, payload),
            Self::Sources(payload) => encode_message(SOURCES, payload),
            Self::PeerInfo(payload) => encode_message(PEER_INFO, payload),
            Self::PeerJoined(payload) => encode_message(PEER_JOINED, payload),
            Self::PeerLeft(payload) => encode_message(PEER_LEFT, payload),
            Self::Broadcast(payload) => encode_message(BROADCAST, payload),
            Self::RecordingChange(payload) => encode_message(RECORDING_CHANGE, payload),
        }
    }

    pub(crate) fn decode(tag: &str, payload: Option<Value>) -> Result<Self, EnvelopeDecodeError> {
        match tag {
            WELCOME => parse_payload(tag, payload).map(Self::Welcome),
            TRACKS => parse_payload(tag, payload).map(Self::Tracks),
            SOURCES => parse_payload(tag, payload).map(Self::Sources),
            PEER_INFO => parse_payload(tag, payload).map(Self::PeerInfo),
            PEER_JOINED => parse_payload(tag, payload).map(Self::PeerJoined),
            PEER_LEFT => parse_payload(tag, payload).map(Self::PeerLeft),
            BROADCAST => parse_payload(tag, payload).map(Self::Broadcast),
            RECORDING_CHANGE => parse_payload(tag, payload).map(Self::RecordingChange),
            _ => Err(EnvelopeDecodeError::UnknownTag(tag.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerResponse {
    StartRecording(RecordingActionResult),
    StopRecording(RecordingActionResult),
}

impl ServerResponse {
    /// encode one server response into the Odoo wire envelope
    ///
    /// # Errors
    ///
    /// returns an error when the typed payload cannot be serialized into the
    /// JSON envelope payload
    pub fn into_envelope(self, response_to: RequestId) -> Result<Envelope, serde_json::Error> {
        match self {
            Self::StartRecording(payload) => encode_response(START_RECORDING, response_to, payload),
            Self::StopRecording(payload) => encode_response(STOP_RECORDING, response_to, payload),
        }
    }

    pub(crate) fn decode(tag: &str, payload: Option<Value>) -> Result<Self, EnvelopeDecodeError> {
        match tag {
            START_RECORDING => parse_payload(tag, payload).map(Self::StartRecording),
            STOP_RECORDING => parse_payload(tag, payload).map(Self::StopRecording),
            _ => Err(EnvelopeDecodeError::UnknownTag(tag.to_owned())),
        }
    }
}

fn encode_message<T: Serialize>(tag: &str, payload: T) -> Result<Envelope, serde_json::Error> {
    Ok(Envelope::message(tag, Some(serde_json::to_value(payload)?)))
}

fn encode_request<T: Serialize>(
    tag: &str,
    request_id: RequestId,
    payload: T,
) -> Result<Envelope, serde_json::Error> {
    Ok(Envelope::request(
        tag,
        request_id,
        Some(serde_json::to_value(payload)?),
    ))
}

fn encode_response<T: Serialize>(
    tag: &str,
    response_to: RequestId,
    payload: T,
) -> Result<Envelope, serde_json::Error> {
    Ok(Envelope::response(
        tag,
        response_to,
        Some(serde_json::to_value(payload)?),
    ))
}

fn parse_payload<T: DeserializeOwned>(
    tag: &str,
    payload: Option<Value>,
) -> Result<T, EnvelopeDecodeError> {
    serde_json::from_value(
        payload.ok_or_else(|| EnvelopeDecodeError::InvalidPayload(tag.to_owned()))?,
    )
    .map_err(|_error| EnvelopeDecodeError::InvalidPayload(tag.to_owned()))
}

fn ensure_empty_payload(tag: &str, payload: Option<&Value>) -> Result<(), EnvelopeDecodeError> {
    if payload.is_some() {
        return Err(EnvelopeDecodeError::UnexpectedPayload(tag.to_owned()));
    }
    Ok(())
}
