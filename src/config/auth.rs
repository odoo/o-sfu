use anyhow::{Result, anyhow};
use secrecy::SecretString;

use super::{
    AuthConfig, DEFAULT_AUTHENTICATION_TIMEOUT_MS, DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
    DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN,
    env::{Env, positive},
};
use crate::runtime::auth::decode_signing_key;

impl AuthConfig {
    pub(super) fn from_env(env: &Env<'_>) -> Result<Self> {
        Ok(Self {
            key: env
                .var("AUTH_KEY")
                .or_load_from_file("AUTH_KEY_FILE")
                .check(validate_auth_key)
                .required()?,
            authentication_timeout_ms: env
                .var("AUTHENTICATION_TIMEOUT_MS")
                .check(positive)
                .default(DEFAULT_AUTHENTICATION_TIMEOUT_MS)?,
            max_pre_auth_websocket_sessions: env
                .var("MAX_PRE_AUTH_WEBSOCKET_SESSIONS")
                .check(positive)
                .default(DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS)?,
            max_pre_auth_websocket_sessions_per_origin: env
                .var("MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN")
                .check(positive)
                .default(DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN)?,
        })
    }
}

fn validate_auth_key(key: &'static str, value: SecretString) -> Result<SecretString> {
    decode_signing_key(&value).map_err(|error| anyhow!("{key}: {error}"))?;
    Ok(value)
}
