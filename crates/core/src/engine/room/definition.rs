//! Room identity and immutable runtime policy.
//!
//! A room has two identities:
//!
//! ```text
//! Odoo-facing identity        Runtime-local placement
//! issuer, uuid, key           instance, router, media worker
//! ```
//!
//! The first identity is visible at the HTTP and websocket edge. The second
//! identity is process-local and drives transport ownership, diagnostics and
//! room topology. Mutable committed routing directories live in `RoomState`
//! rather than in this immutable definition.

use secrecy::{ExposeSecret, SecretString};
use uuid::Uuid;

use super::{RoomConfig, RoomRuntimeContext, RoomRuntimePolicy};
use crate::{
    RoomWorkerPolicy, RuntimeFeatureFlags,
    engine::{AvailableFeatures, RoomInstanceId},
};

/// Central gate for exposing recording as a production room capability.
///
/// This must become true only when accepted recording controls can produce the
/// promised persistent artifact or handoff.
const fn persistent_recording_backend_available() -> bool {
    false
}

#[derive(Debug, Clone)]
struct RoomIdentity {
    uuid: String,
    issuer: String,
    key: SecretString,
}

impl RoomIdentity {
    fn new(issuer: String, key: SecretString) -> Self {
        Self {
            uuid: Uuid::new_v4().to_string(),
            issuer,
            key,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RoomDefinition {
    /// Live room instance id used by runtime diagnostics and transport keys.
    ///
    /// Recreating a room for the same issuer allocates a fresh instance id so
    /// stale transport work cannot be confused with the new room lifetime.
    instance_id: RoomInstanceId,
    room_worker_policy: RoomWorkerPolicy,
    identity: RoomIdentity,
    config: RoomConfig,
    feature_flags: RuntimeFeatureFlags,
}

impl RoomDefinition {
    #[must_use]
    pub(crate) fn new(
        runtime_context: &RoomRuntimeContext,
        runtime_policy: &RoomRuntimePolicy,
        issuer: String,
        key: SecretString,
        config: RoomConfig,
    ) -> Self {
        Self {
            instance_id: runtime_context.instance(),
            room_worker_policy: runtime_policy.room_worker_policy,
            identity: RoomIdentity::new(issuer, key),
            config,
            feature_flags: runtime_policy.feature_flags,
        }
    }

    #[must_use]
    pub(crate) fn matches_reservation(&self, key: &SecretString, config: &RoomConfig) -> bool {
        self.identity.key.expose_secret() == key.expose_secret() && self.config == *config
    }

    #[must_use]
    pub(crate) fn uuid(&self) -> &str {
        &self.identity.uuid
    }

    #[must_use]
    pub(crate) fn issuer(&self) -> &str {
        &self.identity.issuer
    }

    #[must_use]
    pub(crate) fn key(&self) -> &SecretString {
        &self.identity.key
    }

    #[must_use]
    pub(crate) fn available_features(&self) -> AvailableFeatures {
        let recording_available = self.recording_available();
        AvailableFeatures {
            rtc: self.config.web_rtc_enabled,
            transcription: recording_available && self.feature_flags.transcription,
            audio_recording: recording_available && self.feature_flags.audio_recording,
            video_recording: recording_available && self.feature_flags.video_recording,
        }
    }

    #[must_use]
    pub(crate) const fn web_rtc_enabled(&self) -> bool {
        self.config.web_rtc_enabled
    }

    #[must_use]
    pub(crate) const fn recording_available(&self) -> bool {
        if self.config.recording_address.is_none() {
            return false;
        }
        persistent_recording_backend_available()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub fn recording_address(&self) -> Option<&str> {
        self.config.recording_address.as_deref()
    }

    #[must_use]
    pub(crate) fn room_worker_policy(&self) -> RoomWorkerPolicy {
        self.room_worker_policy
    }

    #[must_use]
    pub(crate) const fn instance_id(&self) -> RoomInstanceId {
        self.instance_id
    }
}
