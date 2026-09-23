use std::{cell::Cell, io, path::Path};

use secrecy::ExposeSecret;

use super::{DiagnosticsConfig, Env};

const TOKEN: &str = "operator-secret-with-at-least-32-bytes";

fn no_file(_path: &Path) -> io::Result<String> {
    Err(io::Error::new(io::ErrorKind::NotFound, "file not found"))
}

#[test]
fn load_diagnostics_config_accepts_trimmed_bearer_token() -> anyhow::Result<()> {
    let config = DiagnosticsConfig::from_env(&Env::new(
        |key| (key == "DIAGNOSTICS_AUTH_TOKEN").then(|| format!("  {TOKEN}  \n")),
        no_file,
    ))?;
    assert_eq!(
        config.auth_token.as_ref().map(ExposeSecret::expose_secret),
        Some(TOKEN)
    );
    Ok(())
}

#[test]
fn load_diagnostics_config_without_token_keeps_loopback_fallback() -> anyhow::Result<()> {
    let config = DiagnosticsConfig::from_env(&Env::new(|_| None, no_file))?;
    assert!(config.auth_token.is_none());
    Ok(())
}

#[test]
fn load_diagnostics_config_rejects_empty_and_short_tokens() {
    for token in [
        "",
        "   ",
        "short-secret",
        "1234567890123456789012345678901", // DevSkim: ignore DS173237 (synthetic test token)
    ] {
        let error = DiagnosticsConfig::from_env(&Env::new(
            |key| (key == "DIAGNOSTICS_AUTH_TOKEN").then(|| token.to_owned()),
            no_file,
        ))
        .err()
        .map(|error| error.to_string());
        let expected = if token.trim().is_empty() {
            "DIAGNOSTICS_AUTH_TOKEN must not be empty"
        } else {
            "DIAGNOSTICS_AUTH_TOKEN must contain at least 32 bytes"
        };
        assert_eq!(error.as_deref(), Some(expected));
    }
}

#[expect(
    clippy::non_ascii_literal,
    reason = "test-only Unicode inputs cover the HTTP header-value rejection boundary"
)]
#[test]
fn load_diagnostics_config_rejects_invalid_header_value_tokens() {
    for invalid in [
        "crème_brûlée",
        "token\r\nwith_newline",
        "token\nwith_line_feed",
        "token\rwith_carriage_return",
        "token\0with_null",
        "token\x1b[31mwith_ansi",
        "token\x7fwith_delete",
    ] {
        let error = DiagnosticsConfig::from_env(&Env::new(
            |key| (key == "DIAGNOSTICS_AUTH_TOKEN").then(|| format!("{TOKEN}{invalid}")),
            no_file,
        ))
        .err()
        .map(|error| error.to_string());
        assert_eq!(
            error.as_deref(),
            Some("DIAGNOSTICS_AUTH_TOKEN contains invalid HTTP header-value characters"),
        );
    }
}

#[test]
fn load_diagnostics_config_accepts_valid_tokens() -> anyhow::Result<()> {
    for token in [
        "12345678901234567890123456789012", // DevSkim: ignore DS173237 (synthetic test token)
        TOKEN,
        "Secret_Auth_Token!@#$%^&*0123456789",
        "550e8400-e29b-41d4-a716-446655440000",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        "a token with whitespace\tand at least 32 bytes",
    ] {
        let config = DiagnosticsConfig::from_env(&Env::new(
            |key| (key == "DIAGNOSTICS_AUTH_TOKEN").then(|| token.to_owned()),
            no_file,
        ))?;
        assert_eq!(
            config.auth_token.as_ref().map(ExposeSecret::expose_secret),
            Some(token)
        );
    }
    Ok(())
}

#[test]
fn diagnostics_config_debug_redacts_token() -> anyhow::Result<()> {
    let config = DiagnosticsConfig::from_env(&Env::new(
        |key| (key == "DIAGNOSTICS_AUTH_TOKEN").then(|| TOKEN.to_owned()),
        no_file,
    ))?;
    assert!(!format!("{config:?}").contains(TOKEN));
    Ok(())
}

#[test]
fn load_diagnostics_config_loads_trimmed_file_token() -> anyhow::Result<()> {
    let config = DiagnosticsConfig::from_env(&Env::new(
        |key| (key == "DIAGNOSTICS_AUTH_TOKEN_FILE").then(|| "/run/secrets/token".to_owned()),
        |path| {
            assert_eq!(path, Path::new("/run/secrets/token"));
            Ok(format!("  {TOKEN}\n"))
        },
    ))?;
    assert_eq!(
        config.auth_token.as_ref().map(ExposeSecret::expose_secret),
        Some(TOKEN)
    );
    assert!(!format!("{config:?}").contains(TOKEN));
    Ok(())
}

#[test]
fn load_diagnostics_config_rejects_conflicting_sources_before_reading_file() {
    let read = Cell::new(false);
    let error = DiagnosticsConfig::from_env(&Env::new(
        |key| match key {
            "DIAGNOSTICS_AUTH_TOKEN" => Some(TOKEN.to_owned()),
            "DIAGNOSTICS_AUTH_TOKEN_FILE" => Some("/run/secrets/token".to_owned()),
            _ => None,
        },
        |_| {
            read.set(true);
            Ok(TOKEN.to_owned())
        },
    ))
    .err()
    .map(|error| error.to_string());
    assert_eq!(
        error.as_deref(),
        Some("DIAGNOSTICS_AUTH_TOKEN conflicts with DIAGNOSTICS_AUTH_TOKEN_FILE")
    );
    assert!(!read.get());
}

#[test]
fn load_diagnostics_config_preserves_file_errors_and_validation() {
    for (contents, expected) in [
        (
            "short-secret".to_owned(),
            "DIAGNOSTICS_AUTH_TOKEN_FILE must contain at least 32 bytes",
        ),
        (
            "   \n".to_owned(),
            "DIAGNOSTICS_AUTH_TOKEN_FILE must not be empty",
        ),
        (
            format!("{TOKEN}\0"),
            "DIAGNOSTICS_AUTH_TOKEN_FILE contains invalid HTTP header-value characters",
        ),
    ] {
        let error = DiagnosticsConfig::from_env(&Env::new(
            |key| (key == "DIAGNOSTICS_AUTH_TOKEN_FILE").then(|| "/run/secrets/token".to_owned()),
            |_| Ok(contents.clone()),
        ))
        .err()
        .map(|error| error.to_string());
        assert_eq!(error.as_deref(), Some(expected));
    }
    let error = DiagnosticsConfig::from_env(&Env::new(
        |key| (key == "DIAGNOSTICS_AUTH_TOKEN_FILE").then(|| "/run/secrets/token".to_owned()),
        |_| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
    ))
    .err()
    .map(|error| error.to_string());
    assert_eq!(
        error.as_deref(),
        Some(
            "DIAGNOSTICS_AUTH_TOKEN_FILE points to \"/run/secrets/token\" which could not be read"
        )
    );
}
