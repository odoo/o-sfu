use anyhow::{Result, anyhow, ensure};
use o_sfu_rfc::jwt::HS256_MIN_KEY_BYTES;

use super::{
    AuthConfig, DEFAULT_AUTHENTICATION_TIMEOUT_MS, DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
    DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN,
    env::{Env, positive},
};
use crate::runtime::auth::decode_key;

impl AuthConfig {
    pub(super) fn from_env(env: &Env<'_>) -> Result<Self> {
        Ok(Self {
            key: env.var("AUTH_KEY").check(validate_auth_key).required()?,
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

fn validate_auth_key(key: &'static str, value: String) -> Result<String> {
    let key_len = decode_key(&value)
        .map_err(|_error| anyhow!("{key} must be valid base64"))?
        .len();
    ensure!(
        key_len >= HS256_MIN_KEY_BYTES,
        "{key} must decode to at least {HS256_MIN_KEY_BYTES} bytes"
    );
    Ok(value)
}
