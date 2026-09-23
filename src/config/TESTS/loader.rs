use std::time::Duration;

use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use secrecy::ExposeSecret;

use crate::{
    config::{
        CodecPreferences, Config, DEFAULT_AUTHENTICATION_TIMEOUT_MS,
        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN, MediaCodecFlags, RuntimeFeatureFlags,
        TelemetryConfig,
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
        Some("AUTH_KEY: HS256 key must contain at least 32 decoded bytes")
    );

    let standard = STANDARD.encode([0xff; 32]);
    assert_eq!(
        config_from(&[("AUTH_KEY", &standard)])?
            .auth
            .key
            .expose_secret(),
        standard
    );

    let jose = URL_SAFE_NO_PAD.encode([0xff; 32]);
    assert_eq!(
        config_from(&[("AUTH_KEY", &jose)])?
            .auth
            .key
            .expose_secret(),
        jose
    );

    let error = config_error_from(&[("AUTH_KEY", "not base64")]);
    assert_eq!(error.as_deref(), Some("AUTH_KEY: invalid base64 encoding"));
    Ok(())
}

#[test]
fn config_uses_defaults_and_explicit_values() -> anyhow::Result<()> {
    let config = config_from(&[])?;
    assert_eq!(config.http.bind_address.to_string(), "0.0.0.0:8070");
    assert_eq!(config.http.shutdown_timeout_ms, 10_000);
    assert_eq!(config.auth.key.expose_secret(), TEST_AUTH_KEY);
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
    assert_eq!(config.user.departure_grace, Duration::from_mins(1));
    assert!(!config.http.trust_proxy_headers);
    assert_eq!(config.features, RuntimeFeatureFlags::default());
    assert_eq!(config.codecs.flags, MediaCodecFlags::default());
    assert_eq!(config.codecs.preferences, CodecPreferences::default());
    assert!(config.diagnostics.auth_token.is_none());
    assert_eq!(config.telemetry, TelemetryConfig::default());
    assert_eq!(config.transport.announced_ip.to_string(), "127.0.0.1");
    Ok(())
}

#[test]
fn config_accepts_explicit_http_auth_and_user_settings() -> anyhow::Result<()> {
    let config = config_from(&[
        ("BIND_ADDRESS", "127.0.0.1:9000"),
        ("PROXY", "true"),
        ("TRUSTED_PROXIES", "127.0.0.1/32, ::1/128"),
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
        ("ROOM_DEPARTURE_GRACE", "0"),
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
    // zero is a supported setting: it restores immediate empty-room removal
    assert_eq!(config.user.departure_grace, Duration::ZERO);
    Ok(())
}

#[test]
fn config_rejects_invalid_room_lifecycle_durations() {
    for key in ["ROOM_RESERVATION_TTL", "ROOM_DEPARTURE_GRACE"] {
        let error = config_error_from(&[(key, "1m")]);
        assert_eq!(
            error.as_deref(),
            Some(format!("{key} must be a valid duration in seconds").as_str()),
            "{key}"
        );
    }
}

#[test]
fn config_rejects_room_lifecycle_durations_above_the_maximum() {
    for key in ["ROOM_RESERVATION_TTL", "ROOM_DEPARTURE_GRACE"] {
        // the second value would overflow the deadline computed from `Instant::now()`
        for raw in ["86401", "10000000000000000000"] {
            let error = config_error_from(&[(key, raw)]);
            assert_eq!(
                error.as_deref(),
                Some(format!("{key} must not exceed 86400 seconds").as_str()),
                "{key}={raw}"
            );
        }
    }
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

#[test]
fn proxy_mode_requires_an_explicit_trusted_proxy_network() -> anyhow::Result<()> {
    assert!(config_error_from(&[("PROXY", "true")]).is_some());
    for value in [
        "",
        " ",
        "127.0.0.1",
        "127.0.0.1/33",
        "127.0.0.1/32,",
        "localhost/32",
    ] {
        assert!(
            config_error_from(&[("PROXY", "true"), ("TRUSTED_PROXIES", value)]).is_some(),
            "{value}"
        );
    }
    let config = config_from(&[
        ("PROXY", "true"),
        ("TRUSTED_PROXIES", "127.0.0.1/32, ::1/128"),
    ])?;
    assert_eq!(config.http.trusted_proxies.len(), 2);
    assert_eq!(config.auth.authentication_timeout_ms, 10_000);
    Ok(())
}
