use std::num::NonZeroU16;

use o_sfu_rfc::webrtc::MediaKind;
use serde::{Deserialize, Serialize};

use crate::shared::{
    AvailableFeatures, DownloadStates, JsonPayload, PeerSnapshot, RecordingState, StreamType,
    UserId, UserInfo,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthPayload {
    pub jwt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WelcomePayload {
    pub features: AvailableFeatures,
    pub recording: RecordingState,
    pub peers: Vec<PeerSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionDescriptionPayload {
    pub sdp: String,
    #[serde(default, rename = "uploadSlots", skip_serializing_if = "Vec::is_empty")]
    pub upload_slots: Vec<NegotiationUploadSlot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NegotiationUploadSlot {
    pub mid: String,
    pub kind: MediaKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub codecs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub simulcast_encodings: Vec<NegotiationUploadEncoding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NegotiationUploadEncoding {
    pub rid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bitrate: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution_scale: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_framerate: Option<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum UploadLayerPolicyRole {
    Featured,
    Thumbnail,
    DegradedThumbnail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamIntentPayload {
    #[serde(rename = "type")]
    pub stream_type: StreamType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribePayload {
    /// Wire shape is flat: `{ sessionId, audio?, camera?, screen? }`.
    /// Adding fields to `DownloadStates` implicitly changes the subscribe payload shape.
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
    #[serde(flatten)]
    pub states: DownloadStates,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackBinding {
    pub mid: String,
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
    #[serde(rename = "type")]
    pub stream_type: StreamType,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceDescriptor {
    pub source_id: String,
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
    #[serde(rename = "type")]
    pub stream_type: StreamType,
    pub active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mid: Option<String>,
    pub encodings: Vec<SourceEncodingDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceEncodingDescriptor {
    pub encoding_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bitrate: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution_scale: Option<NonZeroU16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_framerate: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_role: Option<UploadLayerPolicyRole>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerInfoPayload {
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
    pub info: UserInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerLeftPayload {
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientBroadcastPayload {
    pub message: JsonPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerBroadcastPayload {
    pub sender_id: UserId,
    pub message: JsonPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingActionResult {
    pub ok: bool,
}
