//! HTTP authorization schemes and operator access bound to the serving listener.

use std::{net::SocketAddr, sync::Arc};

use axum::http::{HeaderMap, StatusCode, header};

/// Operator authorization bound to the listener that serves the router.
#[derive(Clone)]
pub(super) struct OperatorAccessPolicy {
    auth_token: Option<Arc<str>>,
    listener_is_loopback: bool,
}

impl OperatorAccessPolicy {
    pub(super) fn new(auth_token: Option<&str>, listener_address: SocketAddr) -> Self {
        Self {
            auth_token: auth_token.map(Arc::from),
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
        if let Some(expected_token) = self.auth_token.as_deref() {
            return match bearer_authorization_token(headers) {
                Some(actual_token) if tokens_match(actual_token, expected_token) => Ok(()),
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

fn tokens_match(actual: &str, expected: &str) -> bool {
    let mut diff = actual.len() ^ expected.len();
    for (actual, expected) in actual.bytes().zip(expected.bytes()) {
        diff |= usize::from(actual ^ expected);
    }
    diff == 0
}
