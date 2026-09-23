use secrecy::SecretSlice;

use super::auth::{AuthenticationError, decode_signing_key};
use crate::config::{
    Config, DeadlineDuration, DiagnosticsConfig, HttpConfig, RuntimeFeatureFlags, UserConfig,
};

#[derive(Debug, Clone)]
pub(crate) struct RuntimeConfig {
    pub(crate) auth: RuntimeAuthConfig,
    pub(crate) http: HttpConfig,
    pub(crate) user: UserConfig,
    pub(crate) diagnostics: DiagnosticsConfig,
}

/// Decoded credentials and admission limits used after configuration loading.
#[derive(Debug, Clone)]
pub(crate) struct RuntimeAuthConfig {
    pub(crate) key: SecretSlice<u8>,
    pub(crate) authentication_timeout: DeadlineDuration,
    pub(crate) max_pre_auth_websocket_sessions: usize,
    pub(crate) max_pre_auth_websocket_sessions_per_origin: usize,
}

impl RuntimeConfig {
    pub(crate) fn from_config(config: &Config) -> Result<Self, AuthenticationError> {
        Ok(Self {
            auth: RuntimeAuthConfig {
                key: decode_signing_key(&config.auth.key)?,
                authentication_timeout: config.auth.authentication_timeout,
                max_pre_auth_websocket_sessions: config.auth.max_pre_auth_websocket_sessions,
                max_pre_auth_websocket_sessions_per_origin: config
                    .auth
                    .max_pre_auth_websocket_sessions_per_origin,
            },
            http: config.http.clone(),
            user: config.user,
            diagnostics: config.diagnostics.clone(),
        })
    }
}

pub(crate) const fn effective_feature_flags(features: RuntimeFeatureFlags) -> RuntimeFeatureFlags {
    RuntimeFeatureFlags {
        transcription: features.transcription
            && (features.audio_recording || features.video_recording),
        audio_recording: features.audio_recording,
        video_recording: features.video_recording,
    }
}

#[cfg(test)]
#[path = "TESTS/options.rs"]
mod tests;
