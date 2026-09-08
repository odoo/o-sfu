#![allow(
    dead_code,
    reason = "shared integration-test support is compiled by multiple test targets, each of which uses only a subset of the helpers"
)]

use std::{
    collections::BTreeMap,
    future::Future,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};

use anyhow::{Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use o_sfu::{
    Runtime, ServeError,
    auth::{HttpDisconnectClaims, RegisteredJwtClaims, WebSocketConnectClaims, sign},
    config::{
        AuthConfig, Bitrate, CodecConfig, CodecPreferences, Config,
        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN, DiagnosticsConfig, HttpConfig,
        MediaCodecFlags, RoomMediaLimits, RoomWorkerPolicy, RtcUdpIoBackend, RuntimeFeatureFlags,
        TelemetryConfig, TransportConfig, UserConfig, VideoAdaptationTuning, VideoBitrateLimits,
    },
    core::server::room::{
        DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
    },
    http::{CreateRoomQuery, RoomResponse, StatsResponse, route},
};
use o_sfu_core::server::transport::{MediaTransport, test_support::test_rtc_port_range};
use o_sfu_protocol::wire::{
    EnvelopeBatch, ServerEnvelope, ServerMessage, StreamType, UserId, UserPermissions,
    WelcomePayload,
};
use o_sfu_telemetry::diagnostics::{
    DiagnosticsActiveSpeaker, DiagnosticsActiveSpeakerReason, DiagnosticsActiveSpeakerState,
    DiagnosticsRoomDetail, DiagnosticsRouteState, DiagnosticsSubscription,
    DiagnosticsVideoLayoutRole,
};
use reqwest::StatusCode;
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use tokio::{
    net::{TcpListener, TcpStream},
    task::yield_now,
    time::timeout,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, protocol::frame::coding::CloseCode},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

pub type TestWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

pub const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";
pub const TEST_ROOM_KEY: &str = "Y2hhbm5lbC1rZXk=";

pub type TestResult<T = ()> = Result<T>;

/// Convert a required optional test fixture value into a contextual test error.
///
/// # Errors
///
/// Returns an error when the required value is absent.
pub fn require_some<T>(value: Option<T>, context: &'static str) -> Result<T> {
    value.ok_or_else(|| anyhow!(context))
}

/// Test-only server handle used by integration tests to exercise the real HTTP and WS entry points.
#[derive(Debug)]
pub struct TestServer {
    addr: SocketAddr,
    handle: AbortOnDropHandle<Result<(), ServeError>>,
    media_transport: MediaTransport,
    shutdown: CancellationToken,
}

const TEST_POLL_DEADLINE: Duration = Duration::from_secs(5);

impl TestServer {
    #[must_use]
    pub fn ws_url(&self) -> String {
        format!("ws://{}/", self.addr)
    }

    #[must_use]
    pub fn http_base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn stop(&self) {
        self.shutdown.cancel();
    }

    pub fn set_packet_loop_delays_ms(&self, delays_ms: Vec<Option<u64>>) {
        self.media_transport
            .test_api()
            .set_packet_loop_delays_ms(delays_ms);
    }

    /// # Errors
    ///
    /// Returns an error when the server fails, panics or is cancelled.
    pub async fn join(self) -> Result<()> {
        self.shutdown.cancel();
        self.handle
            .await
            .map_err(|error| anyhow!("test server task failed: {error}"))?
            .map_err(Into::into)
    }

    pub async fn wait_for_room_absence(&self, room_id: &str) -> bool {
        wait_for_test_predicate(|| async { self.room_absent(room_id).await.then_some(()) }).await
    }

    pub async fn wait_for_consumer_route_active(
        &self,
        room_id: &str,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_type: StreamType,
    ) -> bool {
        self.wait_for_consumer_route_state(
            room_id,
            consumer_user_id,
            producer_user_id,
            stream_type,
            ExpectedRouteState::Active,
        )
        .await
    }

    pub async fn wait_for_consumer_route_inactive(
        &self,
        room_id: &str,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_type: StreamType,
    ) -> bool {
        self.wait_for_consumer_route_state(
            room_id,
            consumer_user_id,
            producer_user_id,
            stream_type,
            ExpectedRouteState::Inactive,
        )
        .await
    }

    pub async fn wait_for_consumer_route_absence(
        &self,
        room_id: &str,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_type: StreamType,
    ) -> bool {
        self.wait_for_consumer_route_state(
            room_id,
            consumer_user_id,
            producer_user_id,
            stream_type,
            ExpectedRouteState::Absent,
        )
        .await
    }

    pub async fn wait_for_audio_source_active_speaker(
        &self,
        room_id: &str,
        owner_user_id: &UserId,
        expected_state: DiagnosticsActiveSpeakerState,
        expected_reason: DiagnosticsActiveSpeakerReason,
        expected_last_audio_level_dbov: Option<i8>,
    ) -> bool {
        wait_for_test_predicate(|| async {
            let room = self.room_detail(room_id).await?;
            let active_speaker = audio_source_active_speaker(&room, owner_user_id)?;
            (active_speaker.state == expected_state
                && active_speaker.reason == expected_reason
                && active_speaker.last_audio_level_dbov == expected_last_audio_level_dbov)
                .then_some(())
        })
        .await
    }

    pub async fn wait_for_video_subscription_selected_rid(
        &self,
        room_id: &str,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        expected_rid: &str,
    ) -> bool {
        wait_for_test_predicate(|| async {
            let room = self.room_detail(room_id).await?;
            let selected_rid =
                video_subscription_selected_rid(&room, consumer_user_id, producer_user_id)?;
            (selected_rid == expected_rid).then_some(())
        })
        .await
    }

    pub async fn wait_for_video_subscription_layout(
        &self,
        room_id: &str,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        expected_layout: DiagnosticsVideoLayoutRole,
    ) -> bool {
        wait_for_test_predicate(|| async {
            let room = self.room_detail(room_id).await?;
            let subscription = video_subscription(&room, consumer_user_id, producer_user_id)?;
            (subscription.layout_role == Some(expected_layout)).then_some(())
        })
        .await
    }

    pub async fn wait_for_user_media_worker(
        &self,
        room_id: &str,
        user_id: &UserId,
        expected_media_worker_id: usize,
    ) -> bool {
        wait_for_test_predicate(|| async {
            let room = self.room_detail(room_id).await?;
            (user_media_worker_id(&room, user_id) == Some(expected_media_worker_id)).then_some(())
        })
        .await
    }

    async fn wait_for_consumer_route_state(
        &self,
        room_id: &str,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_type: StreamType,
        expected_state: ExpectedRouteState,
    ) -> bool {
        wait_for_test_predicate(|| async {
            let room = self.room_detail(room_id).await?;
            expected_state
                .matches(route_state(
                    &room,
                    consumer_user_id,
                    producer_user_id,
                    stream_type,
                ))
                .then_some(())
        })
        .await
    }

    async fn room_absent(&self, room_id: &str) -> bool {
        reqwest::Client::new()
            .get(format!(
                "{}{}/{}",
                self.http_base_url(),
                route::diagnostics::ROOMS,
                room_id
            ))
            .send()
            .await
            .is_ok_and(|response| response.status() == StatusCode::NOT_FOUND)
    }

    async fn room_detail(&self, room_id: &str) -> Option<DiagnosticsRoomDetail> {
        let response = reqwest::Client::new()
            .get(format!(
                "{}{}/{}",
                self.http_base_url(),
                route::diagnostics::ROOMS,
                room_id
            ))
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        response.json::<DiagnosticsRoomDetail>().await.ok()
    }
}

fn audio_source_active_speaker<'room>(
    room: &'room DiagnosticsRoomDetail,
    owner_user_id: &UserId,
) -> Option<&'room DiagnosticsActiveSpeaker> {
    room.sources
        .iter()
        .find(|source| {
            source.owner_user_id == *owner_user_id
                && source.stream_id == stream_id_for_stream_type(StreamType::Audio)
        })?
        .active_speaker
        .as_ref()
}

fn user_media_worker_id(room: &DiagnosticsRoomDetail, user_id: &UserId) -> Option<usize> {
    room.users
        .iter()
        .find(|user| user.user_id == *user_id)
        .map(|user| user.transport.media_worker_id)
}

fn video_subscription_selected_rid<'room>(
    room: &'room DiagnosticsRoomDetail,
    consumer_user_id: &UserId,
    producer_user_id: &UserId,
) -> Option<&'room str> {
    // Layout intent does not prove that the receiver policy selected an encoding.
    video_subscription(room, consumer_user_id, producer_user_id)?
        .selection
        .selected_rid
        .as_deref()
}

fn video_subscription<'room>(
    room: &'room DiagnosticsRoomDetail,
    consumer_user_id: &UserId,
    producer_user_id: &UserId,
) -> Option<&'room DiagnosticsSubscription> {
    room.users
        .iter()
        .find(|user| user.user_id == *consumer_user_id)?
        .subscriptions
        .iter()
        .find(|subscription| {
            subscription.producer_user_id == *producer_user_id
                && subscription.stream_id == stream_id_for_stream_type(StreamType::Camera)
        })
}

#[derive(Debug, Clone, Copy)]
enum ExpectedRouteState {
    Active,
    Inactive,
    Absent,
}

impl ExpectedRouteState {
    fn matches(self, actual: Option<&DiagnosticsRouteState>) -> bool {
        match self {
            Self::Active => actual == Some(&DiagnosticsRouteState::Active),
            Self::Inactive => actual == Some(&DiagnosticsRouteState::Inactive),
            Self::Absent => actual.is_none(),
        }
    }
}

fn route_state<'room>(
    room: &'room DiagnosticsRoomDetail,
    consumer_user_id: &UserId,
    producer_user_id: &UserId,
    stream_type: StreamType,
) -> Option<&'room DiagnosticsRouteState> {
    room.users
        .iter()
        .find(|user| user.user_id == *consumer_user_id)?
        .subscriptions
        .iter()
        .find(|subscription| {
            subscription.producer_user_id == *producer_user_id
                && subscription.stream_id == stream_id_for_stream_type(stream_type)
        })
        .map(|subscription| &subscription.state)
}

fn stream_id_for_stream_type(stream_type: StreamType) -> &'static str {
    match stream_type {
        StreamType::Audio => "audio",
        StreamType::Camera => "camera",
        StreamType::Screen => "screen",
    }
}

/// Spawns the production server on an ephemeral port.
///
/// # Errors
///
/// Returns an error when runtime construction or listener binding fails.
pub async fn spawn_test_server(config: Config) -> Result<TestServer> {
    let listener = TcpListener::bind(config.http.bind_address).await?;
    spawn_test_server_with_listener(&config, listener)
}

/// Spawns the production server with a caller-provided listener.
///
/// # Errors
///
/// Returns an error when runtime construction or listener inspection fails.
pub fn spawn_test_server_with_listener(
    config: &Config,
    listener: TcpListener,
) -> Result<TestServer> {
    let runtime = Runtime::new(config)?;
    let media_transport = runtime.media_transport_for_test();
    let mut client_addr = listener
        .local_addr()
        .map_err(|error| anyhow!("failed to read test listener address: {error}"))?;
    client_addr.set_ip(match client_addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => Ipv4Addr::LOCALHOST.into(),
        IpAddr::V6(ip) if ip.is_unspecified() => Ipv6Addr::LOCALHOST.into(),
        ip => ip,
    });
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(runtime.serve_listener(listener, shutdown.clone().cancelled_owned()));
    Ok(TestServer {
        addr: client_addr,
        handle: AbortOnDropHandle::new(handle),
        media_transport,
        shutdown,
    })
}

pub async fn spawn_room_server_with_config(
    config: Config,
    issuer: &str,
    key: &str,
) -> Option<(TestServer, String)> {
    let server = spawn_test_server(config).await.ok()?;
    let room_id = create_room(&server, issuer, key).await?;
    Some((server, room_id))
}

#[must_use]
pub fn test_config(authentication_timeout_ms: u64, room_size: usize) -> Config {
    Config {
        auth: AuthConfig {
            key: SecretString::from(TEST_AUTH_KEY.to_owned()),
            authentication_timeout_ms,
            max_pre_auth_websocket_sessions: DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
            max_pre_auth_websocket_sessions_per_origin:
                DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN,
        },
        http: HttpConfig {
            bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
            trust_proxy_headers: true,
            shutdown_timeout_ms: 10_000,
        },
        user: UserConfig {
            room_size,
            timeout_ms: 10_000,
            ping_interval_ms: 60_000,
            outbound_queue_capacity: DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
            outbound_queue_byte_capacity: DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
            // this window must stay open far longer than the slowest room-create-to-first-join path
            room_reservation_ttl: Duration::from_hours(1),
        },
        transport: TransportConfig {
            announced_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            rtc_port_range: test_rtc_port_range(),
            max_bitrate_in: Bitrate::from_mbps(8),
            max_bitrate_out: Bitrate::from_mbps(10),
            video_bitrate_limits: VideoBitrateLimits::default(),
            rtc_udp_io_backend: RtcUdpIoBackend::Tokio,
            rtc_media_worker_count: 1,
            room_worker_policy: RoomWorkerPolicy::strict_single_router(),
            room_media_limits: RoomMediaLimits::default(),
            video_adaptation_tuning: VideoAdaptationTuning::default(),
        },
        codecs: CodecConfig {
            flags: MediaCodecFlags::default(),
            preferences: CodecPreferences::default(),
        },
        features: RuntimeFeatureFlags::default(),
        telemetry: TelemetryConfig::default(),
        diagnostics: DiagnosticsConfig::default(),
    }
}

#[must_use]
pub fn signed_connect_claims(key: &str, room_id: &str, user_id: UserId) -> Option<String> {
    sign(
        &WebSocketConnectClaims {
            registered: RegisteredJwtClaims::default(),
            room_id: room_id.to_owned(),
            user_id,
            label: Some("Alice".to_owned()),
            permissions: Some(UserPermissions::default()),
        },
        &SecretString::from(key.to_owned()),
    )
    .ok()
}

#[derive(Serialize)]
struct TestHttpRoomClaims {
    #[serde(flatten)]
    registered: RegisteredJwtClaims,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_secret_key"
    )]
    key: Option<SecretString>,
    #[serde(
        rename = "keySeed",
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_secret_key"
    )]
    key_seed: Option<SecretString>,
}

#[allow(
    clippy::ref_option,
    reason = "specific to testing and required by serde for serialization"
)]
fn serialize_secret_key<S>(key: &Option<SecretString>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match key {
        Some(secret) => serializer.serialize_str(secret.expose_secret()),
        None => serializer.serialize_none(),
    }
}

#[must_use]
pub fn signed_room_claims(issuer: &str, key: &str) -> Option<String> {
    sign(
        &TestHttpRoomClaims {
            registered: RegisteredJwtClaims {
                iss: Some(issuer.to_owned()),
                ..RegisteredJwtClaims::default()
            },
            key: Some(SecretString::from(key.to_owned())),
            key_seed: None,
        },
        &SecretString::from(TEST_AUTH_KEY.to_owned()),
    )
    .ok()
}

#[must_use]
pub fn signed_disconnect_claims(user_ids_by_room: BTreeMap<String, Vec<UserId>>) -> Option<String> {
    sign(
        &HttpDisconnectClaims {
            registered: RegisteredJwtClaims::default(),
            user_ids_by_room,
        },
        &SecretString::from(TEST_AUTH_KEY.to_owned()),
    )
    .ok()
}

pub async fn create_room(server: &TestServer, issuer: &str, key: &str) -> Option<String> {
    let token = signed_room_claims(issuer, key)?;
    let response = reqwest::Client::new()
        .get(format!("{}{}", server.http_base_url(), route::v1::CHANNEL))
        .bearer_auth(token)
        .header("x-forwarded-for", "127.0.0.1")
        .query(&CreateRoomQuery::default())
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let payload = response.json::<RoomResponse>().await.ok()?;
    Some(payload.uuid)
}

pub async fn disconnect_sessions_via_http(
    server: &TestServer,
    user_ids_by_room: BTreeMap<String, Vec<UserId>>,
) -> Option<StatusCode> {
    let token = signed_disconnect_claims(user_ids_by_room)?;
    let response = reqwest::Client::new()
        .post(format!(
            "{}{}",
            server.http_base_url(),
            route::v1::DISCONNECT
        ))
        .body(token)
        .send()
        .await
        .ok()?;
    Some(response.status())
}

pub async fn metrics_text(server: &TestServer) -> Option<String> {
    let response = reqwest::Client::new()
        .get(format!("{}{}", server.http_base_url(), route::METRICS))
        .send()
        .await
        .ok()?;
    response.text().await.ok()
}

pub async fn stats(server: &TestServer) -> Option<StatsResponse> {
    let response = reqwest::Client::new()
        .get(format!("{}{}", server.http_base_url(), route::v1::STATS))
        .send()
        .await
        .ok()?;
    response.json::<StatsResponse>().await.ok()
}

pub async fn connect_websocket(server: &TestServer) -> Option<TestWebSocket> {
    let websocket = connect_async(server.ws_url()).await.ok()?;
    Some(websocket.0)
}

pub async fn read_text_message(websocket: &mut TestWebSocket) -> Option<String> {
    loop {
        let message = websocket.next().await?;
        match message.ok()? {
            Message::Text(payload) => return Some(payload.to_string()),
            Message::Ping(payload) => {
                websocket.send(Message::Pong(payload)).await.ok()?;
            }
            Message::Pong(_) => {}
            Message::Binary(_) | Message::Close(_) | Message::Frame(_) => return None,
        }
    }
}

pub async fn read_close_code(websocket: &mut TestWebSocket) -> Option<CloseCode> {
    loop {
        let message = websocket.next().await?;
        match message.ok()? {
            Message::Close(frame) => {
                let code = frame.map(|frame| frame.code);
                let _ = websocket.close(None).await;
                return code;
            }
            Message::Ping(payload) => {
                websocket.send(Message::Pong(payload)).await.ok()?;
            }
            Message::Pong(_) | Message::Text(_) | Message::Binary(_) | Message::Frame(_) => {}
        }
    }
}

#[must_use]
pub fn decode_protocol_welcome_batch(payload: &str) -> Option<WelcomePayload> {
    let batch = serde_json::from_str::<EnvelopeBatch>(payload).ok()?;
    let envelope = batch.first()?.clone();
    match ServerEnvelope::decode(envelope).ok()? {
        ServerEnvelope::Message(ServerMessage::Welcome(welcome)) => Some(welcome),
        ServerEnvelope::Message(_)
        | ServerEnvelope::Request { .. }
        | ServerEnvelope::Response { .. } => None,
    }
}

async fn wait_for_test_predicate<F, Fut>(mut predicate: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<()>>,
{
    timeout(TEST_POLL_DEADLINE, async {
        loop {
            if predicate().await.is_some() {
                return Some(());
            }
            yield_now().await;
        }
    })
    .await
    .ok()
    .flatten()
    .is_some()
}

#[cfg(test)]
#[path = "TESTS/harness.rs"]
mod tests;
