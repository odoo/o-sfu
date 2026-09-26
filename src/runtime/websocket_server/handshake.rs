//! Authenticates the first WebSocket envelope before room admission.
//!
//! Room selection precedes JWT verification for legacy Odoo tokens. Decoded
//! claims select only a candidate room and become trusted after verification
//! with that room's key.

use std::{borrow::Cow, fmt::Display, str, sync::Arc};

use axum::extract::ws::{Message, WebSocket};
use o_sfu_protocol::wire::{AuthPayload, ClientEnvelope, ClientMessage, WebSocketCloseCode};
use thiserror::Error;
use tokio::time::timeout;
use tracing::info;

use super::{
    WsWriter, admission::admit_rejection_log, controller::WebSocketServices,
    io::close_writer_bounded,
};
use crate::{
    core::server::room::Room,
    runtime::{
        auth::{self, AuthProof, AuthenticationError, WebSocketConnectClaims},
        telemetry::schema::event as telemetry_event,
        websocket_server::{MAX_CLIENT_FRAME_BYTES, decode_client_batch},
    },
};

/// Proves the selected room authenticated this WebSocket join.
pub(super) struct WebSocketAuth(AuthProof);

pub(super) struct AuthenticatedJoin {
    pub(super) room: Arc<Room>,
    pub(super) claims: WebSocketConnectClaims,
    pub(super) proof: WebSocketAuth,
}

#[derive(Debug, Error)]
pub(super) enum HandshakeError {
    #[error("peer closed before authentication")]
    PeerClosed,
    #[error("websocket handshake rejected: {0:?}")]
    Rejected(WebSocketCloseCode),
    #[error(transparent)]
    Authentication(#[from] AuthenticationError),
    #[error("authentication referenced an unknown room")]
    UnknownRoom,
    #[error("server is shutting down")]
    Shutdown,
}

impl HandshakeError {
    pub(super) fn close_code(&self) -> WebSocketCloseCode {
        match self {
            Self::PeerClosed => WebSocketCloseCode::Clean,
            Self::Rejected(code) => *code,
            Self::Authentication(_) | Self::UnknownRoom => WebSocketCloseCode::AuthFailed,
            Self::Shutdown => WebSocketCloseCode::Leaving,
        }
    }
}

/// returns the authenticated room join intent without admitting the user
pub(super) async fn authenticate(
    state: &WebSocketServices,
    socket: &mut WebSocket,
) -> Result<AuthenticatedJoin, HandshakeError> {
    let auth = receive_auth(state, socket).await;
    if state.shutdown.is_cancelled() {
        return Err(HandshakeError::Shutdown);
    }
    let auth = auth?;
    state.metrics.record_ws_handshake_credentials_received();
    let auth = verify_auth_payload(state, &auth).await;
    if state.shutdown.is_cancelled() {
        return Err(HandshakeError::Shutdown);
    }
    auth
}

async fn receive_auth(
    state: &WebSocketServices,
    socket: &mut WebSocket,
) -> Result<AuthPayload, HandshakeError> {
    tokio::select! {
        biased;
        () = state.shutdown.cancelled() => Err(HandshakeError::Shutdown),
        result = timeout(
            state.authentication_timeout.as_duration(),
            socket.recv(),
        ) => match result {
            Err(_) => Err(HandshakeError::Rejected(WebSocketCloseCode::AuthTimeout)),
            Ok(None) => Err(HandshakeError::PeerClosed),
            Ok(Some(Err(_error))) => {
                Err(HandshakeError::Rejected(WebSocketCloseCode::Error))
            }
            Ok(Some(Ok(message))) => parse_auth_payload(message).map_err(HandshakeError::Rejected),
        }
    }
}

fn parse_auth_payload(message: Message) -> Result<AuthPayload, WebSocketCloseCode> {
    match message {
        Message::Text(payload) if payload.len() <= MAX_CLIENT_FRAME_BYTES => {
            decode_auth_payload_text(&payload)
        }
        Message::Binary(payload) if payload.len() <= MAX_CLIENT_FRAME_BYTES => {
            str::from_utf8(&payload)
                .map_err(|_error| WebSocketCloseCode::ProtocolError)
                .and_then(decode_auth_payload_text)
        }
        Message::Close(_) => Err(WebSocketCloseCode::Clean),
        _ => Err(WebSocketCloseCode::ProtocolError),
    }
}

/// Decodes the single auth envelope required as the first WebSocket frame.
///
/// # Errors
///
/// Returns the close code for an invalid authentication batch.
pub fn decode_auth_payload_text(payload: &str) -> Result<AuthPayload, WebSocketCloseCode> {
    let batch = decode_client_batch(payload).map_err(|_error| WebSocketCloseCode::ProtocolError)?;
    let [envelope] = batch
        .try_into()
        .map_err(|_batch: Vec<ClientEnvelope>| WebSocketCloseCode::ProtocolError)?;
    let ClientEnvelope::Message(ClientMessage::Auth(auth_payload)) = envelope else {
        return Err(WebSocketCloseCode::ProtocolError);
    };
    Ok(auth_payload)
}

async fn verify_auth_payload(
    state: &WebSocketServices,
    auth_payload: &AuthPayload,
) -> Result<AuthenticatedJoin, HandshakeError> {
    let room_id = match &auth_payload.channel {
        Some(room_id) => Cow::Borrowed(room_id.as_str()),
        None => Cow::Owned(auth::unverified_websocket_room(&auth_payload.jwt)?),
    };
    let room = state
        .room_manager
        .get_by_uuid(&room_id)
        .await
        .ok_or(HandshakeError::UnknownRoom)?;
    let (claims, proof) =
        auth::verify_websocket_claims(&auth_payload.jwt, room.key(), room.uuid())?;
    Ok(AuthenticatedJoin {
        room,
        claims,
        proof: WebSocketAuth(proof),
    })
}

pub(super) async fn reject(
    state: &WebSocketServices,
    writer: &mut WsWriter,
    code: WebSocketCloseCode,
    remote_address: &str,
    reason: impl Display,
) {
    state.metrics.record_ws_handshake_rejection(Some(code));
    if let Some(suppressed_rejections) = admit_rejection_log() {
        info!(
            event = telemetry_event::WS_HANDSHAKE_REJECTED,
            close_code = u16::from(code),
            remote_address,
            suppressed_rejections,
            reason = %reason,
            "rejecting websocket handshake"
        );
    }
    close_writer_bounded(writer, code).await;
}

#[cfg(test)]
#[path = "TESTS/handshake.rs"]
mod tests;
