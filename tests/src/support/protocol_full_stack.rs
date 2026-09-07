#![allow(
    dead_code,
    reason = "the protocol full-stack harness is shared by multiple RTC integration scenarios"
)]

use std::{collections::VecDeque, future::Future, pin::Pin, time::Duration};

use futures_util::SinkExt;
use o_sfu_protocol::wire::{
    AuthPayload, ClientEnvelope, ClientMessage, ClientRequest, DownloadStates, RequestId,
    ServerEnvelope, ServerMessage, ServerRequest, ServerResponse, StreamIntentPayload, StreamType,
    SubscribePayload, UserId, UserInfo, WelcomePayload,
};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::{self, protocol::frame::coding::CloseCode};

use super::{
    TEST_ROOM_KEY, TestServer, TestWebSocket, connect_websocket, decode_protocol_welcome_batch,
    fake_media::FakeMediaSource,
    fake_rtc_peer::FakeRtcPeer,
    protocol_wire::{encode_client_batch, read_protocol_batch, send_server_request_response},
    read_close_code, read_text_message, signed_connect_claims,
};

pub type ProtocolFakePeerFuture<'a, T = ProtocolFakePeer> =
    Pin<Box<dyn Future<Output = Option<T>> + 'a>>;

#[must_use]
pub fn connect_two_fake_peers<'a>(
    server: &'a TestServer,
    room_id: &'a str,
    first_user_id: UserId,
    second_user_id: UserId,
) -> ProtocolFakePeerFuture<'a, (ProtocolFakePeer, ProtocolFakePeer)> {
    Box::pin(async move {
        let first = connect_fake_peer(server, room_id, first_user_id, TEST_ROOM_KEY).await?;
        let second = connect_fake_peer(server, room_id, second_user_id, TEST_ROOM_KEY).await?;
        Some((first, second))
    })
}

#[must_use]
pub fn connect_two_rtc_ready_fake_peers<'a>(
    server: &'a TestServer,
    room_id: &'a str,
    first_user_id: UserId,
    second_user_id: UserId,
    timeout_window: Duration,
) -> ProtocolFakePeerFuture<'a, (ProtocolFakePeer, ProtocolFakePeer)> {
    Box::pin(async move {
        let (mut first, mut second) =
            connect_two_fake_peers(server, room_id, first_user_id, second_user_id).await?;
        first.rtc().wait_until_connected(timeout_window).await?;
        second.rtc().wait_until_connected(timeout_window).await?;
        Some((first, second))
    })
}

#[must_use]
pub fn connect_fake_peer<'a>(
    server: &'a TestServer,
    room_id: &'a str,
    user_id: UserId,
    key: &'a str,
) -> ProtocolFakePeerFuture<'a> {
    connect_fake_peer_with_video_answer(server, room_id, user_id, key, false)
}

#[must_use]
pub fn connect_ridless_video_fake_peer<'a>(
    server: &'a TestServer,
    room_id: &'a str,
    user_id: UserId,
    key: &'a str,
) -> ProtocolFakePeerFuture<'a> {
    connect_fake_peer_with_video_answer(server, room_id, user_id, key, true)
}

fn connect_fake_peer_with_video_answer<'a>(
    server: &'a TestServer,
    room_id: &'a str,
    user_id: UserId,
    key: &'a str,
    ridless_video_fid: bool,
) -> ProtocolFakePeerFuture<'a> {
    Box::pin(async move {
        let token = signed_connect_claims(key, room_id, user_id.clone())?;
        let mut websocket = connect_websocket(server).await?;
        websocket
            .send(tungstenite::Message::Text(
                encode_client_batch(vec![ClientEnvelope::Message(ClientMessage::Auth(
                    AuthPayload {
                        jwt: token,
                        channel: Some(room_id.to_owned()),
                    },
                ))])?
                .into(),
            ))
            .await
            .ok()?;

        let welcome = decode_protocol_welcome_batch(&read_text_message(&mut websocket).await?)?;
        let mut rtc_peer = FakeRtcPeer::bind(0).await?;
        if ridless_video_fid {
            rtc_peer.answer_video_with_ridless_fid();
        }
        let mut peer = ProtocolFakePeer {
            user_id,
            websocket,
            welcome,
            rtc_peer,
            pending_server_messages: VecDeque::new(),
        };
        peer.complete_next_negotiation().await?;
        Some(peer)
    })
}

pub struct ProtocolFakePeer {
    user_id: UserId,
    websocket: TestWebSocket,
    welcome: WelcomePayload,
    rtc_peer: FakeRtcPeer,
    pending_server_messages: VecDeque<ServerMessage>,
}

impl ProtocolFakePeer {
    #[must_use]
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }

    #[must_use]
    pub fn welcome(&self) -> &WelcomePayload {
        &self.welcome
    }

    pub async fn publish_track(&mut self, source: &FakeMediaSource) -> Option<()> {
        self.send_message(ClientMessage::Publish(StreamIntentPayload {
            stream_type: source.stream_type(),
        }))
        .await
    }

    pub async fn set_publication_active(
        &mut self,
        stream_type: StreamType,
        active: bool,
    ) -> Option<()> {
        let message = if active {
            ClientMessage::Publish(StreamIntentPayload { stream_type })
        } else {
            ClientMessage::Unpublish(StreamIntentPayload { stream_type })
        };
        self.send_message(message).await
    }

    pub async fn update_subscription(
        &mut self,
        target_user_id: UserId,
        states: DownloadStates,
    ) -> Option<()> {
        self.send_message(ClientMessage::Subscribe(SubscribePayload {
            user_id: target_user_id,
            states,
        }))
        .await
    }

    pub async fn send_info(&mut self, info: UserInfo) -> Option<()> {
        self.send_message(ClientMessage::Info(info)).await
    }

    pub async fn request_recording(&mut self, recording_request: ClientRequest) -> Option<bool> {
        let recording_request_id = RequestId::new("recording");
        self.send_envelope(ClientEnvelope::Request {
            request_id: recording_request_id.clone(),
            request: recording_request,
        })
        .await?;
        loop {
            for envelope in read_protocol_batch(&mut self.websocket).await? {
                match ServerEnvelope::decode(envelope).ok()? {
                    ServerEnvelope::Message(message) => {
                        self.pending_server_messages.push_back(message);
                    }
                    ServerEnvelope::Request {
                        request_id: server_request_id,
                        request: server_request,
                    } => {
                        self.respond_to_server_request(server_request_id, server_request)
                            .await?;
                    }
                    ServerEnvelope::Response {
                        response_to,
                        response,
                    } if response_to == recording_request_id => {
                        return match response {
                            ServerResponse::StartRecording(result)
                            | ServerResponse::StopRecording(result) => Some(result.ok),
                        };
                    }
                    ServerEnvelope::Response { .. } => {}
                }
            }
        }
    }

    pub async fn read_next_server_message(&mut self) -> Option<ServerMessage> {
        loop {
            if let Some(message) = self.pending_server_messages.pop_front() {
                return Some(message);
            }
            let batch = read_protocol_batch(&mut self.websocket).await?;
            for envelope in batch {
                match ServerEnvelope::decode(envelope).ok()? {
                    ServerEnvelope::Message(message) => {
                        self.pending_server_messages.push_back(message);
                    }
                    ServerEnvelope::Request {
                        request_id,
                        request,
                    } => {
                        self.respond_to_server_request(request_id, request).await?;
                    }
                    ServerEnvelope::Response { .. } => {}
                }
            }
        }
    }

    pub async fn read_next_server_request(&mut self) -> Option<(RequestId, ServerRequest)> {
        loop {
            let batch = read_protocol_batch(&mut self.websocket).await?;
            for envelope in batch {
                match ServerEnvelope::decode(envelope).ok()? {
                    ServerEnvelope::Request {
                        request_id,
                        request,
                    } => return Some((request_id, request)),
                    ServerEnvelope::Message(message) => {
                        self.pending_server_messages.push_back(message);
                    }
                    ServerEnvelope::Response { .. } => {}
                }
            }
        }
    }

    pub async fn read_server_message_with_timeout(
        &mut self,
        duration: Duration,
    ) -> Option<ServerMessage> {
        timeout(duration, self.read_next_server_message())
            .await
            .ok()?
    }

    pub async fn complete_next_negotiation(&mut self) -> Option<()> {
        let (request_id, request) = self.read_next_server_request().await?;
        self.respond_to_server_request(request_id, request).await
    }

    pub fn close(self) -> impl Future<Output = Option<()>> {
        let mut websocket = self.websocket;
        async move {
            websocket.close(None).await.ok()?;
            Some(())
        }
    }

    /// Provides the RTC fixture without processing WebSocket signaling.
    pub fn rtc(&mut self) -> &mut FakeRtcPeer {
        &mut self.rtc_peer
    }

    pub async fn read_close_code(&mut self) -> Option<CloseCode> {
        read_close_code(&mut self.websocket).await
    }

    async fn send_message(&mut self, message: ClientMessage) -> Option<()> {
        self.send_envelope(ClientEnvelope::Message(message)).await
    }

    async fn send_envelope(&mut self, envelope: ClientEnvelope) -> Option<()> {
        self.websocket
            .send(tungstenite::Message::Text(
                encode_client_batch(vec![envelope])?.into(),
            ))
            .await
            .ok()?;
        Some(())
    }

    pub async fn respond_to_server_request(
        &mut self,
        request_id: RequestId,
        request: ServerRequest,
    ) -> Option<()> {
        send_server_request_response(&mut self.websocket, &mut self.rtc_peer, request_id, request)
            .await
    }
}
