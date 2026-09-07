use std::{collections::VecDeque, future::Future, time::Duration};

use futures_util::SinkExt;
use o_sfu_protocol::wire::{
    AuthPayload, ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientResponse, RequestId,
    ServerEnvelope, ServerMessage, ServerRequest, UserId, WelcomePayload,
};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::{self, protocol::frame::coding::CloseCode};

use super::{
    fake_rtc_peer::FakeRtcPeer,
    harness::{
        TestServer, TestWebSocket, connect_websocket, decode_protocol_welcome_batch,
        read_close_code, read_text_message,
    },
    protocol_wire::{encode_client_batch, read_protocol_batch, send_server_request_response},
};

pub struct ProtocolWebSocketClient {
    websocket: TestWebSocket,
    rtc_peer: FakeRtcPeer,
    pending_server_envelopes: VecDeque<ServerEnvelope>,
}

impl ProtocolWebSocketClient {
    pub async fn authenticate_with_jwt(server: &TestServer, token: &str) -> Option<Self> {
        Self::authenticate(
            server,
            AuthPayload {
                jwt: token.to_owned(),
                channel: None,
            },
        )
        .await
    }

    pub async fn authenticate_with_room(
        server: &TestServer,
        token: &str,
        room_id: &str,
    ) -> Option<Self> {
        Self::authenticate(
            server,
            AuthPayload {
                jwt: token.to_owned(),
                channel: Some(room_id.to_owned()),
            },
        )
        .await
    }

    pub async fn authenticate_and_negotiate(
        server: &TestServer,
        token: &str,
    ) -> Option<(Self, WelcomePayload)> {
        let mut client = Self::authenticate_with_jwt(server, token).await?;
        let welcome = client.read_welcome().await?;
        client.finish_initial_negotiation().await?;
        Some((client, welcome))
    }

    async fn authenticate(server: &TestServer, auth_payload: AuthPayload) -> Option<Self> {
        let mut websocket = connect_websocket(server).await?;
        websocket
            .send(tungstenite::Message::Text(
                encode_auth(auth_payload)?.into(),
            ))
            .await
            .ok()?;
        Some(Self {
            websocket,
            rtc_peer: FakeRtcPeer::bind(0).await?,
            pending_server_envelopes: VecDeque::new(),
        })
    }

    pub async fn read_welcome(&mut self) -> Option<WelcomePayload> {
        let payload = read_text_message(&mut self.websocket).await?;
        decode_protocol_welcome_batch(&payload)
    }

    pub async fn finish_initial_negotiation(&mut self) -> Option<()> {
        let (request_id, request) = self.read_server_request().await?;
        let ServerRequest::Offer(_) = request else {
            return None;
        };
        send_server_request_response(&mut self.websocket, &mut self.rtc_peer, request_id, request)
            .await
    }

    pub async fn finish_initial_negotiation_without_candidates(&mut self) -> Option<()> {
        let (response_to, request) = self.read_server_request().await?;
        let ServerRequest::Offer(payload) = request else {
            return None;
        };
        let answer = self
            .rtc_peer
            .answer_offer_without_candidates(&payload.sdp)?;
        self.websocket
            .send(tungstenite::Message::Text(
                encode_client_batch(vec![ClientEnvelope::Response {
                    response_to,
                    response: ClientResponse::Offer(answer),
                }])?
                .into(),
            ))
            .await
            .ok()
    }

    pub async fn wait_until_connected(&mut self, timeout_window: Duration) -> Option<()> {
        self.rtc_peer.wait_until_connected(timeout_window).await
    }

    pub async fn send_message(&mut self, message: ClientMessage) -> Option<()> {
        self.send_messages(vec![message]).await
    }

    pub async fn send_messages(&mut self, messages: Vec<ClientMessage>) -> Option<()> {
        self.websocket
            .send(tungstenite::Message::Text(
                encode_client_batch(messages.into_iter().map(ClientEnvelope::Message).collect())?
                    .into(),
            ))
            .await
            .ok()?;
        Some(())
    }

    pub async fn send_broadcast(&mut self, message: serde_json::Value) -> Option<()> {
        self.send_message(ClientMessage::Broadcast(ClientBroadcastPayload { message }))
            .await
    }

    pub async fn read_server_message(&mut self) -> Option<ServerMessage> {
        match self.read_server_envelope().await? {
            ServerEnvelope::Message(message) => Some(message),
            ServerEnvelope::Request { .. } | ServerEnvelope::Response { .. } => None,
        }
    }

    pub async fn read_server_request(&mut self) -> Option<(RequestId, ServerRequest)> {
        match self.read_server_envelope().await? {
            ServerEnvelope::Request {
                request_id,
                request,
            } => Some((request_id, request)),
            ServerEnvelope::Message(_) | ServerEnvelope::Response { .. } => None,
        }
    }

    pub async fn read_server_message_with_timeout(
        &mut self,
        duration: Duration,
    ) -> Option<ServerMessage> {
        timeout(duration, self.read_server_message()).await.ok()?
    }

    pub async fn read_close_code(&mut self) -> Option<CloseCode> {
        read_close_code(&mut self.websocket).await
    }

    pub fn close(self) -> impl Future<Output = Option<()>> {
        let mut websocket = self.websocket;
        async move {
            websocket.close(None).await.ok()?;
            Some(())
        }
    }

    async fn read_server_envelope(&mut self) -> Option<ServerEnvelope> {
        if self.pending_server_envelopes.is_empty() {
            // A rejected batch must not leave a decoded prefix for later reads.
            self.pending_server_envelopes = read_protocol_batch(&mut self.websocket)
                .await?
                .into_iter()
                .map(ServerEnvelope::decode)
                .collect::<Result<_, _>>()
                .ok()?;
        }
        self.pending_server_envelopes.pop_front()
    }
}

fn encode_auth(auth_payload: AuthPayload) -> Option<String> {
    encode_client_batch(vec![ClientEnvelope::Message(ClientMessage::Auth(
        auth_payload,
    ))])
}

pub async fn read_until_server_message(
    client: &mut ProtocolWebSocketClient,
    timeout_duration: Duration,
    predicate: impl Fn(&ServerMessage) -> bool,
) -> Option<ServerMessage> {
    loop {
        let message = client
            .read_server_message_with_timeout(timeout_duration)
            .await?;
        if predicate(&message) {
            return Some(message);
        }
    }
}

pub async fn connect_protocol_pair(
    server: &TestServer,
    first_token: &str,
    second_token: &str,
    second_user_id: UserId,
) -> Option<(ProtocolWebSocketClient, ProtocolWebSocketClient)> {
    let (mut first, _welcome) = Box::pin(ProtocolWebSocketClient::authenticate_and_negotiate(
        server,
        first_token,
    ))
    .await?;
    let (second, _welcome) = Box::pin(ProtocolWebSocketClient::authenticate_and_negotiate(
        server,
        second_token,
    ))
    .await?;
    Box::pin(read_until_server_message(
        &mut first,
        Duration::from_secs(1),
        |message| {
            matches!(message, ServerMessage::PeerJoined(payload) if payload.user_id == second_user_id)
        },
    ))
    .await?;
    Some((first, second))
}

#[cfg(test)]
#[path = "TESTS/protocol_harness.rs"]
mod tests;
