//! Authenticates the first WebSocket envelope before room admission.
//!
//! Room selection precedes JWT verification for legacy Odoo tokens. Decoded
//! claims select only a candidate room and become trusted after verification
//! with that room's key.

use std::{str, sync::Arc, time::Duration};

use axum::extract::ws::{Message, WebSocket};
use o_sfu_protocol::wire::{AuthPayload, ClientEnvelope, ClientMessage, WebSocketCloseCode};
use secrecy::SecretString;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use super::{WsWriter, controller::WebSocketServices, io::close_writer_bounded};
use crate::{
    core::server::room::Room,
    runtime::{
        auth::{self, AuthProof, WebSocketConnectClaims, WebSocketWireClaims},
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

pub(super) enum HandshakeError {
    PeerClosed,
    Rejected(WebSocketCloseCode),
    Shutdown,
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
    auth.map_err(HandshakeError::Rejected)
}

async fn receive_auth(
    state: &WebSocketServices,
    socket: &mut WebSocket,
) -> Result<AuthPayload, HandshakeError> {
    tokio::select! {
        biased;
        () = state.shutdown.cancelled() => Err(HandshakeError::Shutdown),
        result = timeout(
            Duration::from_millis(state.auth.authentication_timeout_ms),
            socket.recv(),
        ) => match result {
            Err(_) => {
                debug!("timed out waiting for initial websocket auth payload");
                Err(HandshakeError::Rejected(WebSocketCloseCode::AuthTimeout))
            }
            Ok(None) => Err(HandshakeError::PeerClosed),
            Ok(Some(Err(_error))) => {
                debug!("websocket reader returned an error before authentication completed");
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
    let [envelope] = batch.try_into().map_err(|batch: Vec<ClientEnvelope>| {
        warn!(
            batch_len = batch.len(),
            "authentication batch must contain exactly one envelope"
        );
        WebSocketCloseCode::ProtocolError
    })?;
    let ClientEnvelope::Message(ClientMessage::Auth(auth_payload)) = envelope else {
        debug!("first websocket envelope was not an auth message");
        return Err(WebSocketCloseCode::ProtocolError);
    };
    Ok(auth_payload)
}

async fn verify_auth_payload(
    state: &WebSocketServices,
    auth_payload: &AuthPayload,
) -> Result<AuthenticatedJoin, WebSocketCloseCode> {
    let token = &auth_payload.jwt;
    let room = resolve_handshake_room(state, auth_payload, token).await?;
    let (claims, proof) = authenticate_room_scoped_claims(token, &room)?;
    Ok(AuthenticatedJoin {
        room,
        claims,
        proof: WebSocketAuth(proof),
    })
}

/// Selects the candidate room without trusting decoded claims.
async fn resolve_handshake_room(
    state: &WebSocketServices,
    auth_payload: &AuthPayload,
    token: &SecretString,
) -> Result<Arc<Room>, WebSocketCloseCode> {
    let Some(explicit_room_id) = auth_payload.channel.as_deref() else {
        // The decoded room id is only a lookup hint until room-key verification.
        let unverified_claims = auth::decode_unverified_claims::<WebSocketWireClaims>(token)
            .map_err(|_error| {
                debug!("authentication payload did not select a room");
                WebSocketCloseCode::AuthFailed
            })?;
        let room_id = unverified_claims
            .room_id
            .ok_or(WebSocketCloseCode::AuthFailed)?;
        return resolve_room_by_id(state, &room_id).await;
    };
    resolve_room_by_id(state, explicit_room_id).await
}

async fn resolve_room_by_id(
    state: &WebSocketServices,
    room_id: &str,
) -> Result<Arc<Room>, WebSocketCloseCode> {
    state
        .room_manager
        .get_by_uuid(room_id)
        .await
        .ok_or_else(|| {
            debug!(
                room_id,
                "authentication referenced an unknown explicit room"
            );
            WebSocketCloseCode::AuthFailed
        })
}

fn authenticate_room_scoped_claims(
    token: &SecretString,
    room: &Room,
) -> Result<(WebSocketConnectClaims, AuthProof), WebSocketCloseCode> {
    let (claims, proof) = auth::verify_with_proof::<WebSocketWireClaims>(token, room.key())
        .map_err(|_error| WebSocketCloseCode::AuthFailed)?;
    if claims
        .room_id
        .as_deref()
        .is_some_and(|room_id| room_id != room.uuid())
    {
        debug!(
            expected_room_id = room.uuid(),
            claimed_room_id = claims.room_id,
            "room-scoped websocket token targeted the wrong room"
        );
        return Err(WebSocketCloseCode::AuthFailed);
    }
    let mut claims = claims
        .into_claims(room.uuid().to_owned())
        .map_err(|_error| WebSocketCloseCode::AuthFailed)?;
    claims.normalize_runtime_user_id();
    Ok((claims, proof))
}

pub(super) async fn reject(
    state: &WebSocketServices,
    writer: &mut WsWriter,
    code: WebSocketCloseCode,
    remote_address: &str,
    message: &str,
) {
    state.metrics.record_ws_handshake_rejection(Some(code));
    info!(
        event = telemetry_event::WS_HANDSHAKE_REJECTED,
        close_code = u16::from(code),
        remote_address,
        "{message}"
    );
    close_writer_bounded(writer, code).await;
}

#[cfg(test)]
#[path = "TESTS/handshake.rs"]
mod tests;
