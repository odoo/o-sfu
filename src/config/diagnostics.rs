use anyhow::{Result, ensure};
use secrecy::{ExposeSecret, SecretString};

use super::env::Env;

const MIN_TOKEN_BYTES: usize = 32;

#[derive(Debug, Clone, Default)]
pub struct DiagnosticsConfig {
    /// Bearer token required on every listener when configured.
    ///
    /// Surrounding whitespace is ignored. Requires at least 32 bytes after trimming.
    /// The token must be generated independently from cryptographically random bytes.
    pub auth_token: Option<SecretString>,
}

impl DiagnosticsConfig {
    pub(super) fn from_env(env: &Env<'_>) -> Result<Self> {
        let auth_token = env
            .var("DIAGNOSTICS_AUTH_TOKEN")
            .exclusive_file("DIAGNOSTICS_AUTH_TOKEN_FILE")
            .check(|key, value: SecretString| {
                let token = value.expose_secret().trim();
                validate_token(key, token)?;
                if token.len() == value.expose_secret().len() {
                    Ok(value)
                } else {
                    Ok(SecretString::from(token))
                }
            })
            .optional()?;
        Ok(Self { auth_token })
    }

    /// Applies credential rules to configuration supplied directly by library callers.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] for an empty token, invalid HTTP header bytes
    /// or fewer than 32 bytes after trimming whitespace. Error text excludes the token.
    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(token) = &self.auth_token {
            validate_token("DIAGNOSTICS_AUTH_TOKEN", token.expose_secret().trim())?;
        }
        Ok(())
    }
}

fn validate_token(key: &'static str, token: &str) -> Result<()> {
    ensure!(!token.is_empty(), "{key} must not be empty");
    // Checking bytes avoids a plaintext HeaderValue allocation outside SecretString.
    ensure!(
        token
            .bytes()
            .all(|byte| byte == b'\t' || (b' '..=b'~').contains(&byte)),
        "{key} contains invalid HTTP header-value characters"
    );
    ensure!(
        token.len() >= MIN_TOKEN_BYTES,
        "{key} must contain at least {MIN_TOKEN_BYTES} bytes"
    );
    Ok(())
}

#[cfg(test)]
#[path = "TESTS/diagnostics.rs"]
mod tests;
