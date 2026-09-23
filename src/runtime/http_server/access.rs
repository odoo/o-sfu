//! HTTP authorization schemes and operator access bound to the serving listener.

use std::net::SocketAddr;

use axum::http::{HeaderMap, StatusCode, header};
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Operator authorization bound to the listener that serves the router.
#[derive(Clone)]
pub(super) struct OperatorAccessPolicy {
    auth_digest: Option<[u8; 32]>,
    listener_is_loopback: bool,
}

impl OperatorAccessPolicy {
    pub(super) fn new(auth_token: Option<&SecretString>, listener_address: SocketAddr) -> Self {
        Self {
            auth_digest: auth_token
                .map(|token| Sha256::digest(token.expose_secret().trim()).into()),
            listener_is_loopback: listener_address.ip().is_loopback(),
        }
    }

    /// Authorizes operator requests with the configured token or loopback listener.
    ///
    /// A configured token requires the `Bearer` authorization scheme and disables
    /// the loopback fallback.
    ///
    /// # Errors
    ///
    /// Returns [`StatusCode::UNAUTHORIZED`] when a configured token is missing or
    /// does not match. Without a configured token, a non-loopback listener returns
    /// [`StatusCode::FORBIDDEN`].
    pub(super) fn authorize(&self, headers: &HeaderMap) -> Result<(), StatusCode> {
        if let Some(expected_digest) = self.auth_digest.as_ref() {
            return match bearer_authorization_token(headers) {
                Some(actual_token) if token_matches(actual_token, expected_digest) => Ok(()),
                _ => Err(StatusCode::UNAUTHORIZED),
            };
        }
        if self.listener_is_loopback {
            Ok(())
        } else {
            Err(StatusCode::FORBIDDEN)
        }
    }
}

pub(super) fn room_authorization_token(headers: &HeaderMap) -> Option<&str> {
    authorization_token(headers, &["Bearer", "jwt"])
}

fn bearer_authorization_token(headers: &HeaderMap) -> Option<&str> {
    authorization_token(headers, &["Bearer"])
}

fn authorization_token<'headers>(
    headers: &'headers HeaderMap,
    accepted_schemes: &[&str],
) -> Option<&'headers str> {
    let value = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())?;
    let (scheme, token) = value.split_once(' ')?;
    if !accepted_schemes
        .iter()
        .any(|accepted_scheme| scheme.eq_ignore_ascii_case(accepted_scheme))
    {
        return None;
    }
    let token = token.trim_start();
    if token.is_empty() {
        return None;
    }
    Some(token)
}

/// Compares fixed-size digests without secret-length or matching-prefix shortcuts.
///
/// Hashing and request parsing still depend on the supplied token length.
/// See <https://docs.rs/subtle/latest/subtle/trait.ConstantTimeEq.html>.
fn token_matches(actual: &str, expected: &[u8; 32]) -> bool {
    let actual: [u8; 32] = Sha256::digest(actual).into();
    bool::from(actual.ct_eq(expected))
}
