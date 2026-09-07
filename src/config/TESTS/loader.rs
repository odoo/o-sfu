use std::time::Duration;

use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};

use crate::{
    config::{
        CodecPreferences, Config, DEFAULT_AUTHENTICATION_TIMEOUT_MS,
        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN, DiagnosticsConfig, MediaCodecFlags,
        RuntimeFeatureFlags, TelemetryConfig,
    },
    core::server::room::{
        DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
    },
};

const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

fn config_from(overrides: &[(&str, &str)]) -> anyhow::Result<Config> {
    Config::from_var_lookup(|key| {
        overrides
            .iter()
            .find(|(name, _value)| *name == key)
            .map(|(_name, value)| (*value).to_owned())
            .or_else(|| match key {
                "AUTH_KEY" => Some(TEST_AUTH_KEY.to_owned()),
                "ANNOUNCED_IP" => Some("127.0.0.1".to_owned()),
                _ => None,
            })
    })
}

fn config_error_from(overrides: &[(&str, &str)]) -> Option<String> {
    config_from(overrides).err().map(|error| error.to_string())
}

#[test]
fn config_requires_auth_key() {
    let error = Config::from_var_lookup(|key| match key {
        "ANNOUNCED_IP" => Some("127.0.0.1".to_owned()),
        _ => None,
    })
    .err()
    .map(|error| error.to_string());

    assert_eq!(error.as_deref(), Some("AUTH_KEY env variable is required"));
}

#[test]
fn config_validates_auth_key_material() -> anyhow::Result<()> {
    let short = STANDARD.encode([0xff; 31]);
    let error = config_error_from(&[("AUTH_KEY", &short)]);
    assert_eq!(
        error.as_deref(),
        Some("AUTH_KEY must decode to at least 32 bytes")
    );

    let standard = STANDARD.encode([0xff; 32]);
    assert_eq!(config_from(&[("AUTH_KEY", &standard)])?.auth.key, standard);

    let jose = URL_SAFE_NO_PAD.encode([0xff; 32]);
    assert_eq!(config_from(&[("AUTH_KEY", &jose)])?.auth.key, jose);

    let error = config_error_from(&[("AUTH_KEY", "not base64")]);
    assert_eq!(error.as_deref(), Some("AUTH_KEY must be valid base64"));
    Ok(())
}

#[test]
fn config_uses_defaults_and_explicit_values() -> anyhow::Result<()> {
    let config = config_from(&[])?;
    assert_eq!(config.http.bind_address.to_string(), "0.0.0.0:8070");
    assert_eq!(config.http.shutdown_timeout_ms, 10_000);
    assert_eq!(config.auth.key, TEST_AUTH_KEY);
    assert_eq!(
        config.auth.authentication_timeout_ms,
        DEFAULT_AUTHENTICATION_TIMEOUT_MS
    );
    assert_eq!(
        config.auth.max_pre_auth_websocket_sessions,
        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS
    );
    assert_eq!(
        config.auth.max_pre_auth_websocket_sessions_per_origin,
        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN
    );
    assert_eq!(config.user.room_size, 100);
    assert_eq!(config.user.timeout_ms, 10_000);
    assert_eq!(config.user.ping_interval_ms, 60_000);
    assert_eq!(
        config.user.outbound_queue_capacity,
        DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY
    );
    assert_eq!(
        config.user.outbound_queue_byte_capacity,
        DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY
    );
    assert_eq!(config.user.room_reservation_ttl, Duration::from_mins(1));
    assert!(!config.http.trust_proxy_headers);
    assert_eq!(config.features, RuntimeFeatureFlags::default());
    assert_eq!(config.codecs.flags, MediaCodecFlags::default());
    assert_eq!(config.codecs.preferences, CodecPreferences::default());
    assert_eq!(config.diagnostics, DiagnosticsConfig::default());
    assert_eq!(config.telemetry, TelemetryConfig::default());
    assert_eq!(config.transport.announced_ip.to_string(), "127.0.0.1");
    Ok(())
}

#[test]
fn config_accepts_explicit_http_auth_and_user_settings() -> anyhow::Result<()> {
    let config = config_from(&[
        ("BIND_ADDRESS", "127.0.0.1:9000"),
        ("PROXY", "true"),
        ("SHUTDOWN_TIMEOUT_MS", "2500"),
        ("AUTHENTICATION_TIMEOUT_MS", "1500"),
        ("MAX_PRE_AUTH_WEBSOCKET_SESSIONS", "12"),
        ("MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN", "3"),
        ("ROOM_SIZE", "4"),
        ("USER_TIMEOUT_MS", "5000"),
        ("PING_INTERVAL_MS", "1000"),
        ("USER_OUTBOUND_QUEUE_CAPACITY", "16"),
        ("USER_OUTBOUND_QUEUE_BYTE_CAPACITY", "8192"),
        ("ROOM_RESERVATION_TTL", "120"),
    ])?;
    assert_eq!(config.http.bind_address.to_string(), "127.0.0.1:9000");
    assert_eq!(config.http.shutdown_timeout_ms, 2500);
    assert!(config.http.trust_proxy_headers);
    assert_eq!(config.auth.authentication_timeout_ms, 1500);
    assert_eq!(config.auth.max_pre_auth_websocket_sessions, 12);
    assert_eq!(config.auth.max_pre_auth_websocket_sessions_per_origin, 3);
    assert_eq!(config.user.room_size, 4);
    assert_eq!(config.user.timeout_ms, 5000);
    assert_eq!(config.user.ping_interval_ms, 1000);
    assert_eq!(config.user.outbound_queue_capacity, 16);
    assert_eq!(config.user.outbound_queue_byte_capacity, 8192);
    assert_eq!(config.user.room_reservation_ttl, Duration::from_mins(2));
    Ok(())
}

#[test]
fn config_rejects_invalid_proxy_flag() {
    let error = config_error_from(&[("PROXY", "maybe")]);

    assert_eq!(
        error.as_deref(),
        Some("PROXY must be either `true` or `false`")
    );
}

#[test]
fn config_rejects_zero_runtime_limits() {
    let cases = [
        "SHUTDOWN_TIMEOUT_MS",
        "ROOM_SIZE",
        "USER_TIMEOUT_MS",
        "PING_INTERVAL_MS",
        "MAX_PRE_AUTH_WEBSOCKET_SESSIONS",
        "MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN",
        "USER_OUTBOUND_QUEUE_CAPACITY",
        "USER_OUTBOUND_QUEUE_BYTE_CAPACITY",
    ];

    for key in cases {
        let error = config_error_from(&[(key, "0")]);
        assert_eq!(
            error.as_deref(),
            Some(format!("{key} must be greater than zero").as_str()),
            "{key}"
        );
    }
}
