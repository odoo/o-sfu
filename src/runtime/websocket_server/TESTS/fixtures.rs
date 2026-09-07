pub(super) use std::{
    fmt::Debug,
    net::SocketAddr,
    result::Result as StdResult,
    sync::{
        Arc,
        atomic::{AtomicU16, Ordering},
    },
    time::{Duration, Instant},
};

pub(super) use anyhow::{Result, anyhow};
pub(super) use futures_util::{SinkExt, StreamExt};
pub(super) use o_sfu_protocol::wire::{
    AuthPayload, ClientEnvelope, ClientMessage, ClientResponse, EnvelopeBatch, RequestId,
    ServerEnvelope, ServerMessage, ServerRequest, SessionDescriptionPayload, StreamType, UserId,
    UserPermissions, WelcomePayload,
};
use str0m::{Candidate, Rtc, change::SdpOffer};
pub(super) use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::{sleep, timeout},
};
pub(super) use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        self, client::IntoClientRequest, http::HeaderValue, protocol::frame::coding::CloseCode,
    },
};

pub(super) use crate::{
    application::stream_catalog::{
        source_publish_intent_for_stream_type, stream_id_for_stream_type,
    },
    config::RuntimeFeatureFlags,
    runtime::{
        RuntimeState,
        auth::{RegisteredJwtClaims, WebSocketConnectClaims, sign},
        http_server::app,
        media_transport::MediaTransport,
        room::{
            JoinPlacementTestGate, JoinUserRequest, Room, RoomConfig, RoomManager,
            UserOutboundQueueLimits, UserOutboundReceiver, UserOutboundSender,
        },
        test_support::{RuntimeMetricsSnapshotTestExt, RuntimeTestBuilder},
    },
};

pub(super) const TEST_ROOM_KEY: &str = "Y2hhbm5lbC1rZXk=";
pub(super) const OTHER_ROOM_KEY: &str = "b3RoZXItcm9vbS1rZXk=";
static NEXT_WEBSOCKET_TEST_PEER_PORT: AtomicU16 = AtomicU16::new(58_000);
pub(super) type TestWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;
pub(super) type CreateRoomQuery = RoomConfig;
pub(super) type TestResult<T = ()> = Result<T>;

pub(super) fn require_some<T>(value: Option<T>, context: &'static str) -> TestResult<T> {
    value.ok_or_else(|| anyhow!(context))
}

pub(super) fn require_ok<T, E>(value: StdResult<T, E>, context: &'static str) -> TestResult<T>
where
    E: Debug,
{
    value.map_err(|error| anyhow!("{context}: {error:?}"))
}

/// raw websocket peer for tests that must prove the server handles a silent client
///
/// `tokio_tungstenite` is the right fixture for normal client behavior, but it
/// queues automatic pong replies when reads observe ping frames, so timeout
/// tests need a peer that can authenticate and read server frames without ever
/// acknowledging liveness probes
pub(super) struct SilentWebSocket {
    stream: TcpStream,
}

/// server-frame subset needed by the silent peer
///
/// the fixture decodes only text frames, pings and close frames because those
/// are enough to authenticate, reach steady state and observe timeout shutdown
enum RawWebSocketFrame {
    /// JSON protocol batch from the server
    Text(String),
    /// server liveness probe that this fixture deliberately ignores
    Ping,
    /// server close frame plus optional RFC close code
    Close(Option<CloseCode>),
    /// any frame that has no value for the timeout assertions
    Other,
}

/// server text frame opcode used while reading welcome and offer batches
const RAW_TEXT_FRAME_OPCODE: u8 = 0x1;
/// server close frame opcode used to assert the timeout close code
const RAW_CLOSE_FRAME_OPCODE: u8 = 0x8;
/// server ping frame opcode that must not trigger a pong in this fixture
const RAW_PING_FRAME_OPCODE: u8 = 0x9;
/// client-to-server websocket frames must be masked
const RAW_FRAME_MASK_BIT: u8 = 0x80;
/// websocket payload marker for two-byte extended lengths
const RAW_EXTENDED_16_BIT_LENGTH: u8 = 0x7e;
/// websocket payload marker for eight-byte extended lengths
const RAW_EXTENDED_64_BIT_LENGTH: u8 = 0x7f;

/// App-level WebSocket subsystem fixture.
///
/// This serves the Axum app over a local listener while keeping direct access to
/// private room and media-transport state for server-crate tests. Full
/// `Runtime::serve` startup coverage belongs to the integration test crate.
pub(super) struct TestServer {
    pub(super) addr: SocketAddr,
    pub(super) handle: JoinHandle<()>,
    pub(super) room_manager: Arc<RoomManager>,
    pub(super) media_transport: MediaTransport,
    pub(super) state: RuntimeState,
}

impl TestServer {
    pub(super) fn url(&self) -> String {
        format!("ws://{}/", self.addr)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

pub(super) struct TestServerBuilder {
    runtime: RuntimeTestBuilder,
}

impl TestServerBuilder {
    pub(super) fn new() -> Self {
        Self {
            runtime: RuntimeTestBuilder::new(),
        }
    }

    pub(super) fn authentication_timeout_ms(mut self, value: u64) -> Self {
        self.runtime = self.runtime.authentication_timeout_ms(value);
        self
    }

    pub(super) fn user_timeout_ms(mut self, value: u64) -> Self {
        self.runtime = self.runtime.user_timeout_ms(value);
        self
    }

    pub(super) fn ping_interval_ms(mut self, value: u64) -> Self {
        self.runtime = self.runtime.ping_interval_ms(value);
        self
    }

    pub(super) fn room_size(mut self, value: usize) -> Self {
        self.runtime = self.runtime.room_size(value);
        self
    }

    pub(super) fn pre_auth_capacity(mut self, total: usize, per_origin: usize) -> Self {
        self.runtime = self.runtime.pre_auth_capacity(total, per_origin);
        self
    }

    pub(super) fn trust_proxy_headers(mut self, value: bool) -> Self {
        self.runtime = self.runtime.trust_proxy_headers(value);
        self
    }

    pub(super) fn feature_flags(mut self, value: RuntimeFeatureFlags) -> Self {
        self.runtime = self.runtime.feature_flags(value);
        self
    }

    pub(super) async fn spawn(self) -> Option<TestServer> {
        let bind_address = self.runtime.config().http.bind_address;
        let runtime = self.runtime.build_state();
        let state_for_server = runtime.state.clone();
        let listener = require_ok(
            TcpListener::bind(bind_address).await,
            "test listener should bind",
        )
        .ok()?;
        let addr = require_ok(
            listener.local_addr(),
            "test listener address should resolve",
        )
        .ok()?;
        let handle = tokio::spawn(async move {
            let result = axum::serve(listener, app(state_for_server, addr)).await;
            assert!(
                result.is_ok(),
                "test server should stop cleanly: {result:?}"
            );
        });
        Some(TestServer {
            addr,
            handle,
            room_manager: runtime.room_manager,
            media_transport: runtime.media_transport,
            state: runtime.state,
        })
    }

    pub(super) async fn spawn_required(self) -> TestResult<TestServer> {
        require_some(self.spawn().await, "test websocket server should start")
    }
}

impl Default for TestServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn next_websocket_test_peer_addr() -> SocketAddr {
    SocketAddr::from((
        [127, 0, 0, 1],
        NEXT_WEBSOCKET_TEST_PEER_PORT.fetch_add(1, Ordering::Relaxed),
    ))
}

pub(super) async fn connect_websocket(server: &TestServer) -> Option<TestWebSocket> {
    let websocket = connect_async(server.url()).await.ok()?;
    Some(websocket.0)
}

pub(super) async fn connect_websocket_with_forwarded_for(
    server: &TestServer,
    forwarded_for: &str,
) -> Option<TestWebSocket> {
    let mut request = server.url().into_client_request().ok()?;
    let forwarded_for = HeaderValue::from_str(forwarded_for).ok()?;
    request
        .headers_mut()
        .insert("x-forwarded-for", forwarded_for);
    let websocket = connect_async(request).await.ok()?;
    Some(websocket.0)
}

/// opens a real upgraded websocket while keeping direct control of raw frames
///
/// this keeps the test on the actual Axum upgrade path but avoids handing
/// liveness frames to a client library that would answer them automatically
pub(super) async fn connect_silent_websocket(server: &TestServer) -> Option<SilentWebSocket> {
    let mut stream = TcpStream::connect(server.addr).await.ok()?;
    let request = format!(
        "GET / HTTP/1.1\r\n\
         Host: {}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Sec-WebSocket-Version: 13\r\n\
         \r\n",
        server.addr
    );
    stream.write_all(request.as_bytes()).await.ok()?;
    read_raw_upgrade_response(&mut stream).await?;
    Some(SilentWebSocket { stream })
}

/// verifies the HTTP upgrade without installing websocket client behavior
///
/// the silent fixture only needs to know that the server accepted the upgrade
/// before it starts writing raw masked frames
async fn read_raw_upgrade_response(stream: &mut TcpStream) -> Option<()> {
    let mut response = Vec::new();
    let mut byte = [0_u8; 1];
    while !response.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).await.ok()?;
        response.push(byte[0]);
        if response.len() > 8192 {
            return None;
        }
    }
    let response = String::from_utf8(response).ok()?;
    response.starts_with("HTTP/1.1 101").then_some(())
}

/// authenticates through the normal protocol envelope on the raw peer
///
/// this keeps auth and room admission identical to regular websocket tests
/// while preserving the silent behavior needed after startup
pub(super) async fn authenticate_silent_with_jwt(
    server: &TestServer,
    token: &str,
) -> Option<SilentWebSocket> {
    let mut websocket = connect_silent_websocket(server).await?;
    let payload = encode_protocol_auth(AuthPayload {
        jwt: token.to_owned(),
        channel: None,
    })?;
    websocket.send_text(&payload).await?;
    Some(websocket)
}

pub(super) fn signed_connect_claims(key: &str, room_id: &str, user_id: UserId) -> Option<String> {
    signed_connect_claims_with_permissions(key, room_id, user_id, None)
}

pub(super) fn signed_connect_claims_with_permissions(
    key: &str,
    room_id: &str,
    user_id: UserId,
    permissions: Option<UserPermissions>,
) -> Option<String> {
    sign(
        &WebSocketConnectClaims {
            registered: RegisteredJwtClaims::default(),
            room_id: room_id.to_owned(),
            user_id,
            label: Some("Alice".to_owned()),
            permissions,
        },
        key,
    )
    .ok()
}

pub(super) fn signed_legacy_channel_scoped_connect_claims(
    key: &str,
    user_id: UserId,
    permissions: Option<UserPermissions>,
) -> Option<String> {
    #[derive(serde::Serialize)]
    struct LegacyClaims {
        #[serde(flatten)]
        registered: RegisteredJwtClaims,
        #[serde(rename = "session_id")]
        user_id: UserId,
        #[serde(skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        permissions: Option<UserPermissions>,
    }

    sign(
        &LegacyClaims {
            registered: RegisteredJwtClaims::default(),
            user_id,
            label: Some("Alice".to_owned()),
            permissions,
        },
        key,
    )
    .ok()
}

pub(super) async fn create_room(
    server: &TestServer,
    issuer: &str,
    config: RoomConfig,
) -> Option<Arc<Room>> {
    server
        .room_manager
        .serve_room(issuer, TEST_ROOM_KEY, &config, None)
        .await
        .ok()
}

pub(super) async fn authenticate_with_jwt(
    server: &TestServer,
    token: &str,
) -> Option<TestWebSocket> {
    authenticate_with_room(server, token, None).await
}

pub(super) async fn authenticate_with_room(
    server: &TestServer,
    token: &str,
    room_id: Option<&str>,
) -> Option<TestWebSocket> {
    let mut websocket = connect_websocket(server).await?;
    let payload = encode_protocol_auth(AuthPayload {
        jwt: token.to_owned(),
        channel: room_id.map(str::to_owned),
    })?;
    websocket
        .send(tungstenite::Message::Text(payload.into()))
        .await
        .ok()?;
    Some(websocket)
}

pub(super) fn encode_protocol_auth(auth_payload: AuthPayload) -> Option<String> {
    let envelope = ClientEnvelope::Message(ClientMessage::Auth(auth_payload))
        .into_envelope()
        .ok()?;
    serde_json::to_string(&vec![envelope]).ok()
}

pub(super) async fn authenticate_and_read_welcome(
    server: &TestServer,
    token: &str,
) -> Option<(TestWebSocket, WelcomePayload)> {
    let mut websocket = authenticate_with_jwt(server, token).await?;
    let welcome = read_welcome(&mut websocket).await?;
    Some((websocket, welcome))
}

pub(super) async fn complete_initial_negotiation(
    websocket: &mut TestWebSocket,
    sdp: &str,
) -> Option<()> {
    let (request_id, request) = wait_for_protocol_server_request(websocket).await?;
    respond_to_protocol_negotiation_request_with_test_rtc(websocket, request_id, request, sdp).await
}

pub(super) async fn setup_negotiated_session(
    server: &TestServer,
    room: &Arc<Room>,
    user_id: UserId,
) -> Option<TestWebSocket> {
    let token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), user_id)?;
    let (mut websocket, _welcome) = authenticate_and_read_welcome(server, &token).await?;
    complete_initial_negotiation(&mut websocket, "v=0\r\ns=test-answer\r\n").await?;
    Some(websocket)
}

pub(super) async fn close_socket_and_wait_for_session_cleanup(
    websocket: &mut TestWebSocket,
    room: &Room,
    user_id: &UserId,
) -> Option<()> {
    websocket.close(None).await.ok()?;
    wait_for_session_cleanup(room, user_id).await
}

pub(super) async fn wait_for_session_cleanup(room: &Room, user_id: &UserId) -> Option<()> {
    timeout(Duration::from_secs(1), async {
        loop {
            if !room.test_api().has_session(user_id).await {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .ok()
}

pub(super) async fn read_welcome(websocket: &mut TestWebSocket) -> Option<WelcomePayload> {
    let payload = read_text_message(websocket).await?;
    decode_protocol_welcome_batch(&payload)
}

/// reads the welcome batch through the shared protocol decoder
///
/// this proves the raw peer reached the same authenticated state as a normal
/// test websocket before the liveness-specific assertions begin
pub(super) async fn read_silent_welcome(websocket: &mut SilentWebSocket) -> Option<WelcomePayload> {
    let payload = websocket.read_text().await?;
    decode_protocol_welcome_batch(&payload)
}

fn decode_protocol_welcome_batch(payload: &str) -> Option<WelcomePayload> {
    let batch = serde_json::from_str::<EnvelopeBatch>(payload).ok()?;
    let envelope = batch.first()?.clone();
    match ServerEnvelope::decode(envelope).ok()? {
        ServerEnvelope::Message(ServerMessage::Welcome(welcome)) => Some(welcome),
        ServerEnvelope::Message(_)
        | ServerEnvelope::Request { .. }
        | ServerEnvelope::Response { .. } => None,
    }
}

pub(super) async fn read_protocol_server_batch(
    websocket: &mut TestWebSocket,
) -> Option<EnvelopeBatch> {
    let payload = timeout(Duration::from_secs(1), read_text_message(websocket))
        .await
        .ok()??;
    serde_json::from_str(&payload).ok()
}

pub(super) async fn wait_for_protocol_server_request(
    websocket: &mut TestWebSocket,
) -> Option<(RequestId, ServerRequest)> {
    loop {
        let batch = read_protocol_server_batch(websocket).await?;
        if let Some(request) = first_protocol_server_request(&batch) {
            return Some(request);
        }
    }
}

/// waits for the first server request without enabling automatic pong behavior
///
/// the ping-timeout test needs the session to reach steady state before the
/// peer goes silent, otherwise it would cover startup failure instead
pub(super) async fn wait_for_silent_protocol_server_request(
    websocket: &mut SilentWebSocket,
) -> Option<(RequestId, ServerRequest)> {
    loop {
        let payload = timeout(Duration::from_secs(1), websocket.read_text())
            .await
            .ok()??;
        let batch = serde_json::from_str::<EnvelopeBatch>(&payload).ok()?;
        if let Some(request) = first_protocol_server_request(&batch) {
            return Some(request);
        }
    }
}

pub(super) fn first_protocol_server_request(
    batch: &EnvelopeBatch,
) -> Option<(RequestId, ServerRequest)> {
    for envelope in batch.iter().cloned() {
        if let ServerEnvelope::Request {
            request_id,
            request,
        } = ServerEnvelope::decode(envelope).ok()?
        {
            return Some((request_id, request));
        }
    }
    None
}

pub(super) fn protocol_server_messages(batch: &EnvelopeBatch) -> Option<Vec<ServerMessage>> {
    let mut messages = Vec::new();
    for envelope in batch.clone() {
        match ServerEnvelope::decode(envelope).ok()? {
            ServerEnvelope::Message(message) => messages.push(message),
            ServerEnvelope::Request { .. } | ServerEnvelope::Response { .. } => return None,
        }
    }
    Some(messages)
}

pub(super) async fn respond_to_protocol_negotiation_request(
    websocket: &mut TestWebSocket,
    response_to: RequestId,
    request: ServerRequest,
    sdp: &str,
) -> Option<()> {
    let response = match request {
        ServerRequest::Offer(_) => ClientResponse::Offer(SessionDescriptionPayload {
            sdp: sdp.to_owned(),
            upload_slots: Vec::new(),
        }),
        ServerRequest::Renegotiate(_) => ClientResponse::Renegotiate(SessionDescriptionPayload {
            sdp: sdp.to_owned(),
            upload_slots: Vec::new(),
        }),
    };
    let frame = serde_json::to_string(&vec![
        ClientEnvelope::Response {
            response_to,
            response,
        }
        .into_envelope()
        .ok()?,
    ])
    .ok()?;
    websocket
        .send(tungstenite::Message::Text(frame.into()))
        .await
        .ok()?;
    Some(())
}

pub(super) async fn respond_to_protocol_negotiation_request_with_test_rtc(
    websocket: &mut TestWebSocket,
    response_to: RequestId,
    request: ServerRequest,
    fallback_sdp: &str,
) -> Option<()> {
    let sdp = test_rtc_answer_sdp(&request).unwrap_or_else(|| fallback_sdp.to_owned());
    respond_to_protocol_negotiation_request(websocket, response_to, request, &sdp).await
}

pub(super) fn test_rtc_answer_sdp(request: &ServerRequest) -> Option<String> {
    let offer_sdp = match request {
        ServerRequest::Offer(payload) | ServerRequest::Renegotiate(payload) => &payload.sdp,
    };
    let mut rtc = Rtc::new(Instant::now());
    rtc.add_local_candidate(Candidate::host(next_websocket_test_peer_addr(), "udp").ok()?)?;
    let answer = rtc
        .sdp_api()
        .accept_offer(SdpOffer::from_sdp_string(offer_sdp).ok()?)
        .ok()?;
    Some(answer.to_sdp_string())
}

pub(super) async fn read_websocket_ping(websocket: &mut TestWebSocket) -> Option<Vec<u8>> {
    loop {
        let message = websocket.next().await?;
        match message.ok()? {
            tungstenite::Message::Ping(payload) => return Some(payload.to_vec()),
            tungstenite::Message::Pong(_) => {}
            tungstenite::Message::Text(_)
            | tungstenite::Message::Binary(_)
            | tungstenite::Message::Close(_)
            | tungstenite::Message::Frame(_) => return None,
        }
    }
}

pub(super) async fn read_text_message(websocket: &mut TestWebSocket) -> Option<String> {
    loop {
        let message = websocket.next().await?;
        match message.ok()? {
            tungstenite::Message::Text(payload) => return Some(payload.to_string()),
            tungstenite::Message::Ping(payload) => {
                websocket
                    .send(tungstenite::Message::Pong(payload))
                    .await
                    .ok()?;
            }
            tungstenite::Message::Pong(_) => {}
            tungstenite::Message::Binary(_)
            | tungstenite::Message::Close(_)
            | tungstenite::Message::Frame(_) => return None,
        }
    }
}

pub(super) async fn read_close_code(websocket: &mut TestWebSocket) -> Option<CloseCode> {
    loop {
        let message = websocket.next().await?;
        match message.ok()? {
            tungstenite::Message::Close(frame) => {
                let code = frame.map(|frame| frame.code);
                let _ = websocket.close(None).await;
                return code;
            }
            tungstenite::Message::Ping(payload) => {
                websocket
                    .send(tungstenite::Message::Pong(payload))
                    .await
                    .ok()?;
            }
            tungstenite::Message::Pong(_)
            | tungstenite::Message::Text(_)
            | tungstenite::Message::Binary(_)
            | tungstenite::Message::Frame(_) => {}
        }
    }
}

pub(super) async fn read_close_code_promptly(websocket: &mut TestWebSocket) -> Option<CloseCode> {
    timeout(Duration::from_secs(1), read_close_code(websocket))
        .await
        .ok()
        .flatten()
}

/// reads until the timeout close frame arrives without answering ping frames
///
/// this is the assertion boundary that the old tungstenite helper could not
/// provide because its reads could queue automatic pong replies
pub(super) async fn read_silent_close_code(websocket: &mut SilentWebSocket) -> Option<CloseCode> {
    loop {
        match websocket.read_frame().await? {
            RawWebSocketFrame::Close(code) => return code,
            RawWebSocketFrame::Ping | RawWebSocketFrame::Text(_) | RawWebSocketFrame::Other => {}
        }
    }
}

impl SilentWebSocket {
    /// writes one masked client text frame
    ///
    /// client masking is required by the websocket protocol, so the raw peer
    /// must perform it explicitly when sending auth and protocol responses
    async fn send_text(&mut self, payload: &str) -> Option<()> {
        let payload = payload.as_bytes();
        let mut frame = Vec::with_capacity(payload.len().saturating_add(8));
        frame.push(0x81);
        if payload.len() < 126 {
            frame.push(RAW_FRAME_MASK_BIT | u8::try_from(payload.len()).ok()?);
        } else {
            frame.push(RAW_FRAME_MASK_BIT | RAW_EXTENDED_16_BIT_LENGTH);
            frame.extend_from_slice(&u16::try_from(payload.len()).ok()?.to_be_bytes());
        }
        let mask = [1_u8, 2, 3, 4];
        frame.extend_from_slice(&mask);
        frame.extend(
            payload
                .iter()
                .zip(mask.iter().copied().cycle())
                .map(|(byte, mask_byte)| byte ^ mask_byte),
        );
        self.stream.write_all(&frame).await.ok()?;
        Some(())
    }

    /// reads server text frames while deliberately ignoring pings
    ///
    /// this lets startup use normal JSON batches without changing the fixture
    /// into an active liveness participant
    async fn read_text(&mut self) -> Option<String> {
        loop {
            match self.read_frame().await? {
                RawWebSocketFrame::Text(payload) => return Some(payload),
                RawWebSocketFrame::Ping | RawWebSocketFrame::Other => {}
                RawWebSocketFrame::Close(_) => return None,
            }
        }
    }

    /// decodes the minimal server-to-client websocket frame shape used by tests
    ///
    /// keeping this parser local avoids expanding the production websocket
    /// surface just to model one silent test peer
    async fn read_frame(&mut self) -> Option<RawWebSocketFrame> {
        let mut header = [0_u8; 2];
        self.stream.read_exact(&mut header).await.ok()?;
        let opcode = header[0] & 0x0f;
        let masked = header[1] & RAW_FRAME_MASK_BIT != 0;
        let mut payload_len = u64::from(header[1] & 0x7f);
        if payload_len == u64::from(RAW_EXTENDED_16_BIT_LENGTH) {
            let mut extended = [0_u8; 2];
            self.stream.read_exact(&mut extended).await.ok()?;
            payload_len = u64::from(u16::from_be_bytes(extended));
        } else if payload_len == u64::from(RAW_EXTENDED_64_BIT_LENGTH) {
            let mut extended = [0_u8; 8];
            self.stream.read_exact(&mut extended).await.ok()?;
            payload_len = u64::from_be_bytes(extended);
        }
        let mask = if masked {
            let mut mask = [0_u8; 4];
            self.stream.read_exact(&mut mask).await.ok()?;
            Some(mask)
        } else {
            None
        };
        let mut payload = vec![0_u8; usize::try_from(payload_len).ok()?];
        self.stream.read_exact(&mut payload).await.ok()?;
        if let Some(mask) = mask {
            for (byte, mask_byte) in payload.iter_mut().zip(mask.iter().copied().cycle()) {
                *byte ^= mask_byte;
            }
        }
        match opcode {
            RAW_TEXT_FRAME_OPCODE => {
                Some(RawWebSocketFrame::Text(String::from_utf8(payload).ok()?))
            }
            RAW_CLOSE_FRAME_OPCODE => Some(RawWebSocketFrame::Close(raw_close_code(&payload))),
            RAW_PING_FRAME_OPCODE => Some(RawWebSocketFrame::Ping),
            _ => Some(RawWebSocketFrame::Other),
        }
    }
}

/// extracts the optional close code from a raw close payload
///
/// empty close payloads are valid, but the ping-timeout assertion expects the
/// server to send the explicit internal-error code
fn raw_close_code(payload: &[u8]) -> Option<CloseCode> {
    let bytes = payload.get(0..2)?.try_into().ok()?;
    Some(CloseCode::from(u16::from_be_bytes(bytes)))
}
