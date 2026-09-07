//! shared application model for the Odoo Discuss SFU contract
//!
//! this crate defines the Odoo Discuss call concepts that multiple `o-sfu`
//! crates must interpret identically
//! they are more specific than RFC vocabulary but less specific than any one
//! runtime subsystem
//!
//! the model crate depends only on serialization support
//! sockets, async work, media transports, router topology, metrics registries,
//! server configuration and JSON envelope parsing stay in the runtime, core,
//! router, telemetry and protocol crates
//!
//! # Compatibility
//!
//! several types preserve the old SFU and Odoo browser contract
//! they should
//! remain small data types with explicit serde shapes and local normalization
//! helpers
//! runtime callers should normalize compatibility input at ingress before
//! storing it in room state, diagnostics indexes or subscription maps

use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// opaque compatibility payload carried through legacy broadcast paths
///
/// prefer explicit application structs for new flows
/// this alias exists where Odoo owns the shape and the SFU only relays the JSON
/// value
pub type JsonPayload = Value;

/// user identity as accepted by the Odoo-facing call contract
///
/// Odoo normally uses integer user ids, while legacy and test callers may send
/// string ids
/// the runtime canonicalizes numeric strings before indexing room
/// state so `"42"` and `42` cannot become two live users in the same call
///
/// non-numeric strings remain valid compatibility ids
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UserId {
    Integer(i64),
    String(String),
}

impl From<i64> for UserId {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<&str> for UserId {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for UserId {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl UserId {
    /// return the path representation used by diagnostics and bundle keys
    /// string ids retain their raw representation
    #[must_use]
    pub fn path_segment(&self) -> Cow<'_, str> {
        match self {
            Self::Integer(value) => Cow::Owned(value.to_string()),
            Self::String(value) => Cow::Borrowed(value),
        }
    }

    /// return the runtime key form for this user id
    ///
    /// numeric strings are parsed into [`Self::Integer`] so all room state,
    /// diagnostics lookup, disconnect handling and subscription logic use one
    /// canonical key
    /// non-numeric strings are preserved as compatibility identities
    #[must_use]
    pub fn normalized_for_runtime(self) -> Self {
        match self {
            Self::String(value) => value
                .parse::<i64>()
                .map_or(Self::String(value), Self::Integer),
            Self::Integer(value) => Self::Integer(value),
        }
    }

    /// borrowing variant of [`Self::normalized_for_runtime`]
    ///
    /// use this when the caller owns a borrowed auth or protocol payload and
    /// needs the canonical runtime key without consuming that payload
    #[must_use]
    pub fn runtime_normalized(&self) -> Self {
        self.clone().normalized_for_runtime()
    }
}

/// room capabilities advertised to a newly connected browser client
///
/// these are call capabilities, not permission checks
/// the room advertises
/// which features exist for the call, then per-user permissions decide who may
/// actually start or change a restricted feature
#[expect(
    clippy::struct_excessive_bools,
    reason = "feature flags mirror the compatibility startup surface with explicit optional room capabilities"
)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AvailableFeatures {
    /// `false` keeps compatibility with websocket-relay rooms
    pub rtc: bool,
    pub transcription: bool,
    pub audio_recording: bool,
    pub video_recording: bool,
}

/// current room recording state as shown to call participants
///
/// fields are optional because the compatibility surface may carry sparse
/// updates
/// room snapshots should fill known fields
/// consumers must treat a missing field as "not asserted by this payload"
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordingState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recording: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video: Option<bool>,
}

/// business reason attached to a recording stop update
///
/// this code is shown to clients and diagnostics as the reason recording became
/// inactive
/// it does not describe transport failures or upload service details
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StopCode {
    #[serde(rename = "user_request")]
    UserRequest,
    #[serde(rename = "channel_closed")]
    ChannelClosed,
    #[serde(rename = "recording_timeout")]
    RecordingTimeout,
    #[serde(rename = "recording_failed")]
    RecordingFailed,
    #[serde(rename = "disk_space_exhausted")]
    DiskSpaceExhausted,
}

/// recording state update emitted to clients and observers
///
/// `stop_code` is present only when the update explains why a recording session
/// stopped
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingStateUpdate {
    pub state: RecordingState,
    /// present only when a recording became inactive
    #[serde(rename = "stopCode", skip_serializing_if = "Option::is_none")]
    pub stop_code: Option<StopCode>,
}

/// user-level permissions supplied by the Odoo authentication path
///
/// missing values are denied by the room runtime so omitted permissions never
/// grant access by accident
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserPermissions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_recording: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_recording: Option<bool>,
}

/// presence and call UI state associated with one room participant
///
/// this is participant state visible to other clients
/// it does not include media routing, transport health or source identity
///
/// fields are optional so callers can send partial updates
/// use [`Self::snapshot_complete`] when serializing a full room snapshot
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_talking: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_featured: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_camera_on: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_screen_sharing_on: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_self_muted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_deaf: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_raising_hand: Option<bool>,
}

impl UserInfo {
    /// fill missing presence fields with `false` for snapshot emission
    ///
    /// partial updates keep `None` to mean "unchanged"
    /// full room snapshots use this so receivers can render without merging
    /// against stale local data
    #[must_use]
    pub fn snapshot_complete(self) -> Self {
        Self {
            is_talking: Some(self.is_talking.unwrap_or(false)),
            is_featured: Some(self.is_featured.unwrap_or(false)),
            is_camera_on: Some(self.is_camera_on.unwrap_or(false)),
            is_screen_sharing_on: Some(self.is_screen_sharing_on.unwrap_or(false)),
            is_self_muted: Some(self.is_self_muted.unwrap_or(false)),
            is_deaf: Some(self.is_deaf.unwrap_or(false)),
            is_raising_hand: Some(self.is_raising_hand.unwrap_or(false)),
        }
    }

    /// merge a partial presence update into the current stored value
    ///
    /// `None` means "unchanged", matching the wire contract for incremental
    /// user-info updates
    pub fn apply_partial_update(&mut self, update: &Self) {
        *self = Self {
            is_talking: update.is_talking.or(self.is_talking),
            is_featured: update.is_featured.or(self.is_featured),
            is_camera_on: update.is_camera_on.or(self.is_camera_on),
            is_screen_sharing_on: update.is_screen_sharing_on.or(self.is_screen_sharing_on),
            is_self_muted: update.is_self_muted.or(self.is_self_muted),
            is_deaf: update.is_deaf.or(self.is_deaf),
            is_raising_hand: update.is_raising_hand.or(self.is_raising_hand),
        };
    }

    /// return this presence payload with the room-layout featured flag applied
    #[must_use]
    pub fn with_featured(mut self, is_featured: Option<bool>) -> Self {
        self.is_featured = is_featured;
        self
    }
}

/// full peer entry sent when a client needs the current room membership view
///
/// the serialized `sessionId` field is the Odoo-facing user identity
/// runtime connection ids are absent because reconnection and replacement are
/// server-local concerns
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerSnapshot {
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
    #[serde(default)]
    pub info: UserInfo,
}

/// receiver intent for which streams to download from one peer
///
/// this is client intent, not a transport subscription object
/// missing fields mean the current receiver preference for that stream or
/// layout should be left unchanged
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadStates {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub camera: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub screen: Option<bool>,
    #[serde(rename = "cameraLayout", skip_serializing_if = "Option::is_none")]
    pub camera_layout: Option<VideoLayoutIntent>,
    #[serde(rename = "screenLayout", skip_serializing_if = "Option::is_none")]
    pub screen_layout: Option<VideoLayoutIntent>,
}

impl DownloadStates {
    pub fn apply_partial_update(&mut self, update: &Self) {
        *self = Self {
            audio: update.audio.or(self.audio),
            camera: update.camera.or(self.camera),
            screen: update.screen.or(self.screen),
            camera_layout: update.camera_layout.or(self.camera_layout),
            screen_layout: update.screen_layout.or(self.screen_layout),
        };
    }
}

/// receiver-side layout role for a video stream
///
/// the room uses this layout hint to prioritize selected video layers under
/// bandwidth pressure
/// it does not name an RTP encoding, simulcast RID or concrete packet gate
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VideoLayoutIntent {
    /// main speaker or call focus
    Featured,
    /// user-pinned stream protected more strongly than ordinary thumbnails
    Pinned,
    /// thumbnail that is currently visible in the client layout
    VisibleThumbnail,
    /// stream hidden by the client layout
    Hidden,
    /// stream outside the currently visible layout range
    ///
    /// currently the same as hidden
    /// the distinct value leaves room for more granular client layout policy
    Overflow,
}

/// stream category exposed to Odoo clients
///
/// this is smaller than the internal source model
/// the source model may contain encodings, RTP metadata and transport-local
/// media ids, while `StreamType` only names the user-facing stream
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum StreamType {
    #[serde(rename = "audio")]
    Audio,
    #[serde(rename = "camera")]
    Camera,
    #[serde(rename = "screen")]
    Screen,
}

/// recording modes requested by a user
///
/// missing fields mean the caller did not request that mode
/// the room combines these options with feature flags, current recording state
/// and [`UserPermissions`] before mutating recording state
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video: Option<bool>,
}

/// websocket close code vocabulary shared by server and browser protocol code
///
/// standard codes keep their RFC meaning
/// custom codes mirror the legacy Odoo SFU websocket close vocabulary used by
/// browser clients and low-cardinality telemetry
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum WebSocketCloseCode {
    /// normal websocket closure
    Clean = 1000,
    /// the peer is leaving
    Leaving = 1001,
    /// the peer sent a malformed or invalid protocol message
    ProtocolError = 1002,
    /// the server hit an internal error while handling the socket
    Error = 1011,

    /// authentication failed
    AuthFailed = 4106,
    /// the client did not authenticate before the server timeout
    AuthTimeout = 4107,
    /// the runtime removed this client from the room
    Kicked = 4108,
    /// admission failed because the room cannot accept another user
    RoomFull = 4109,
}

impl WebSocketCloseCode {
    /// decode a raw websocket close code if it belongs to the shared vocabulary
    ///
    /// unknown codes return `None` so the caller can keep foreign websocket
    /// close reasons out of application telemetry labels and protocol state
    /// machines
    #[must_use]
    pub const fn from_u16(value: u16) -> Option<Self> {
        match value {
            1000 => Some(Self::Clean),
            1001 => Some(Self::Leaving),
            1002 => Some(Self::ProtocolError),
            1011 => Some(Self::Error),
            4106 => Some(Self::AuthFailed),
            4107 => Some(Self::AuthTimeout),
            4108 => Some(Self::Kicked),
            4109 => Some(Self::RoomFull),
            _ => None,
        }
    }
}

impl From<WebSocketCloseCode> for u16 {
    fn from(value: WebSocketCloseCode) -> Self {
        match value {
            WebSocketCloseCode::Clean => 1000,
            WebSocketCloseCode::Leaving => 1001,
            WebSocketCloseCode::ProtocolError => 1002,
            WebSocketCloseCode::Error => 1011,
            WebSocketCloseCode::AuthFailed => 4106,
            WebSocketCloseCode::AuthTimeout => 4107,
            WebSocketCloseCode::Kicked => 4108,
            WebSocketCloseCode::RoomFull => 4109,
        }
    }
}

#[cfg(test)]
#[path = "TESTS/lib.rs"]
mod tests;
