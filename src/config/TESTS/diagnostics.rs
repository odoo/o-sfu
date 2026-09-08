use std::io;

use super::{DiagnosticsConfig, Env};

#[test]
fn load_diagnostics_config_accepts_trimmed_bearer_token() -> anyhow::Result<()> {
    let config = DiagnosticsConfig::from_env(&Env::new(
        |key| match key {
            "DIAGNOSTICS_AUTH_TOKEN" => Some("  bearer-token  \n".to_owned()),
            _ => None,
        },
        |_| Err(io::Error::new(io::ErrorKind::NotFound, "file not found")),
    ))?;
    assert_eq!(config.auth_token.as_deref(), Some("bearer-token"));
    Ok(())
}

#[test]
fn load_diagnostics_config_rejects_empty_token() {
    let error = DiagnosticsConfig::from_env(&Env::new(
        |key| match key {
            "DIAGNOSTICS_AUTH_TOKEN" => Some("   ".to_owned()),
            _ => None,
        },
        |_| Err(io::Error::new(io::ErrorKind::NotFound, "file not found")),
    ))
    .err()
    .map(|error| error.to_string());

    assert_eq!(
        error.as_deref(),
        Some("DIAGNOSTICS_AUTH_TOKEN must not be empty")
    );
}

#[expect(
    clippy::non_ascii_literal,
    reason = "test-only Unicode inputs are easier to read and never enter the production interface"
)]
#[test]
fn load_diagnostics_config_rejects_invalid_header_value_tokens() {
    let invalid_tokens = [
        "你好 杭州,rust",
        "token_with_emoji_🔒",
        "crème_brûlée",
        "token\r\nwith_newline",
        "token\nwith_line_feed",
        "token\rwith_carriage_return",
        "token\0with_null",
        "token\x1b[31mwith_ansi",
    ];

    for invalid_token in invalid_tokens {
        let env = Env::new(
            |key| match key {
                "DIAGNOSTICS_AUTH_TOKEN" => Some(invalid_token.to_owned()),
                _ => None,
            },
            |_| Err(io::Error::new(io::ErrorKind::NotFound, "file not found")),
        );

        let error = DiagnosticsConfig::from_env(&env)
            .err()
            .map(|error| error.to_string());

        assert_eq!(
            error.as_deref(),
            Some("DIAGNOSTICS_AUTH_TOKEN contains invalid HTTP header-value characters"),
        );
    }
}

#[test]
fn load_diagnostics_config_accepts_valid_tokens() {
    let valid_tokens = [
        "simpletoken123",
        "bearer_token-v1.0.0",
        "Secret_Auth_Token!@#$%^&*",
        "550e8400-e29b-41d4-a716-446655440000",
        "aW52YWxpZF90b2tlbl9leGFtcGxl==",
        "token\twith_tab",
    ];
    for valid_token in valid_tokens {
        let env = Env::new(
            |key| match key {
                "DIAGNOSTICS_AUTH_TOKEN" => Some(valid_token.to_owned()),
                _ => None,
            },
            |_| Err(io::Error::new(io::ErrorKind::NotFound, "file not found")),
        );
        let config = DiagnosticsConfig::from_env(&env).ok();
        let actual_token = config.as_ref().and_then(|c| c.auth_token.as_deref());
        assert_eq!(actual_token, Some(valid_token));
    }
}
