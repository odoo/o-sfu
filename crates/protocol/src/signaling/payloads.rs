use std::num::NonZeroU16;

use o_sfu_rfc::webrtc::MediaKind;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

use crate::shared::{
    AvailableFeatures, DownloadStates, JsonPayload, PeerSnapshot, RecordingState, StreamType,
    UserId, UserInfo,
};

/// The client's bearer JWT, carried in the clear (JWTs are only base64url
/// encoded, not encrypted) as `SecretString` so it auto-redacts from `Debug`
/// and zeroizes on drop instead of lingering in process memory as a plain
/// `String`.
///
/// Every embedder of [`crate::host::ProtocolCore`] (browser, native, or test
/// host) plays the client role and needs to serialize this payload to send
/// its own token, which the `client` feature (on by default) provides.
/// The one consumer that only ever decodes an `AuthPayload` it received is
/// the SFU server itself, whose own dependency on this crate disables
/// default features, so its build never compiles in the capability to
/// serialize this type at all.
#[derive(Debug, Clone, Deserialize)]
pub struct AuthPayload {
    pub jwt: SecretString,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
}

impl PartialEq for AuthPayload {
    fn eq(&self, other: &Self) -> bool {
        self.jwt.expose_secret() == other.jwt.expose_secret() && self.channel == other.channel
    }
}

impl Eq for AuthPayload {}

#[cfg(feature = "client")]
impl Serialize for AuthPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct WireAuthPayload<'a> {
            jwt: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            channel: Option<&'a str>,
        }
        WireAuthPayload {
            jwt: self.jwt.expose_secret(),
            channel: self.channel.as_deref(),
        }
        .serialize(serializer)
    }
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

/// Sparse receiver preferences for the protocol's audio, camera and screen
/// streams. Unknown fields are ignored and never become retained stream ids.
/// An empty preference update leaves every existing stream preference unchanged.
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
