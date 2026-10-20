//! rust payloads for odoo browser bundle state and updates
//!
//! callable compatibility methods stay on `SfuClient`
//! host projection emits these DTOs through the public [`crate::bundle`] facade

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub use crate::core::ConnectionState as BundleConnectionState;
use crate::{
    shared::{JsonPayload, RecordingState, RecordingStateUpdate, StopCode, UserId, UserInfo},
    signaling::TrackBinding,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleStateChange {
    pub state: BundleConnectionState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
}

/// `info_change` payloads use string keys from [`bundle_session_info_key`]
/// integer and string user ids that stringify to the same key collide, so the
/// later entry wins
pub type BundleSessionInfoSnapshotById = BTreeMap<String, UserInfo>;

#[must_use]
pub fn bundle_session_info_key(user_id: &UserId) -> String {
    user_id.path_segment().into_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleBroadcastUpdate {
    #[serde(rename = "senderId")]
    pub sender_id: UserId,
    pub message: JsonPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleDisconnectUpdate {
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleRemoteMediaUpdate {
    pub bindings: Vec<TrackBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleRecordingFlags {
    pub audio: bool,
    pub video: bool,
    pub transcription: bool,
}

impl From<&RecordingState> for BundleRecordingFlags {
    fn from(state: &RecordingState) -> Self {
        // Browser updates require full snapshots. Missing legacy flags default to false.
        Self {
            audio: state.audio.unwrap_or(false),
            video: state.video.unwrap_or(false),
            transcription: state.transcription.unwrap_or(false),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleRecordingStateUpdate {
    pub state: BundleRecordingFlags,
    #[serde(rename = "stopCode", skip_serializing_if = "Option::is_none")]
    pub stop_code: Option<StopCode>,
}

impl From<&RecordingStateUpdate> for BundleRecordingStateUpdate {
    fn from(update: &RecordingStateUpdate) -> Self {
        Self {
            state: (&update.state).into(),
            stop_code: update.stop_code,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "name", content = "payload")]
pub enum BundleUpdate {
    #[serde(rename = "remote_media")]
    RemoteMedia(BundleRemoteMediaUpdate),
    #[serde(rename = "broadcast")]
    Broadcast(BundleBroadcastUpdate),
    #[serde(rename = "disconnect")]
    Disconnect(BundleDisconnectUpdate),
    #[serde(rename = "info_change")]
    SessionInfoChange(BundleSessionInfoSnapshotById),
    #[serde(rename = "channel_info_change")]
    ChannelInfoChange(BundleRecordingStateUpdate),
}

#[cfg(test)]
#[path = "bundle_api/TESTS/mod.rs"]
mod tests;
