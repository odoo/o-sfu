use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use o_sfu_rfc::jwt::RegisteredJwtClaims;
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub(super) use crate::runtime::metrics::test_support::RuntimeMetricsSnapshotTestExt;
use crate::{
    auth::HttpRoomClaims,
    config::{
        AuthConfig, Bitrate, CodecConfig, CodecPreferences, Config,
        DEFAULT_AUTHENTICATION_TIMEOUT_MS, DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN, DiagnosticsConfig, HttpConfig,
        MediaCodecFlags, RoomMediaLimits, RoomWorkerPolicy, RtcUdpIoBackend, RuntimeFeatureFlags,
        TelemetryConfig, TransportConfig, UserConfig, VideoAdaptationTuning, VideoBitrateLimits,
    },
    runtime::{
        MediaTransport, RuntimeServices, RuntimeState, build_media_transport, build_room_manager,
        build_room_runtime_policy,
        media_transport::test_support::test_rtc_port_range,
        options::RuntimeConfig,
        room::{
            DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
            RoomManager, UserOutboundReceiver, UserOutboundSender,
        },
    },
};

pub(super) const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";
pub(super) const TEST_ROOM_KEY: &str = "Y2hhbm5lbC1rZXk=";

pub(super) struct RuntimeTestState {
    pub(super) state: RuntimeState,
    pub(super) room_manager: Arc<RoomManager>,
    pub(super) media_transport: MediaTransport,
}

pub(super) struct RuntimeTestBuilder {
    config: Config,
}

impl RuntimeTestBuilder {
    pub(super) fn new() -> Self {
        Self {
            config: Config {
                auth: AuthConfig {
                    key: SecretString::from(TEST_AUTH_KEY.to_owned()),
                    authentication_timeout_ms: DEFAULT_AUTHENTICATION_TIMEOUT_MS,
                    max_pre_auth_websocket_sessions: DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
                    max_pre_auth_websocket_sessions_per_origin:
                        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN,
                },
                http: HttpConfig {
                    bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
                    trust_proxy_headers: false,
                    shutdown_timeout_ms: 10_000,
                },
                user: UserConfig {
                    room_size: 100,
                    timeout_ms: 10_000,
                    ping_interval_ms: 60_000,
                    outbound_queue_capacity: DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
                    outbound_queue_byte_capacity: DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
                    room_reservation_ttl: Duration::from_secs(5),
                },
                transport: TransportConfig {
                    announced_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
                    rtc_port_range: test_rtc_port_range(),
                    max_bitrate_in: Bitrate::from_mbps(8),
                    max_bitrate_out: Bitrate::from_mbps(10),
                    video_bitrate_limits: VideoBitrateLimits::default(),
                    rtc_media_worker_count: 1,
                    room_worker_policy: RoomWorkerPolicy::strict_single_router(),
                    room_media_limits: RoomMediaLimits::default(),
                    video_adaptation_tuning: VideoAdaptationTuning::default(),
                    rtc_udp_io_backend: RtcUdpIoBackend::Tokio,
                },
                codecs: CodecConfig {
                    flags: MediaCodecFlags::default(),
                    preferences: CodecPreferences::default(),
                },
                features: RuntimeFeatureFlags::default(),
                telemetry: TelemetryConfig::default(),
                diagnostics: DiagnosticsConfig::default(),
            },
        }
    }

    pub(super) const fn config(&self) -> &Config {
        &self.config
    }

    pub(super) fn authentication_timeout_ms(mut self, value: u64) -> Self {
        self.config.auth.authentication_timeout_ms = value;
        self
    }

    pub(super) fn user_timeout_ms(mut self, value: u64) -> Self {
        self.config.user.timeout_ms = value;
        self
    }

    pub(super) fn ping_interval_ms(mut self, value: u64) -> Self {
        self.config.user.ping_interval_ms = value;
        self
    }

    pub(super) fn room_size(mut self, value: usize) -> Self {
        self.config.user.room_size = value;
        self
    }

    pub(super) fn room_reservation_ttl(mut self, value: Duration) -> Self {
        self.config.user.room_reservation_ttl = value;
        self
    }

    pub(super) fn pre_auth_capacity(mut self, total: usize, per_origin: usize) -> Self {
        self.config.auth.max_pre_auth_websocket_sessions = total;
        self.config.auth.max_pre_auth_websocket_sessions_per_origin = per_origin;
        self
    }

    pub(super) fn trust_proxy_headers(mut self, value: bool) -> Self {
        self.config.http.trust_proxy_headers = value;
        self
    }

    pub(super) fn feature_flags(mut self, value: RuntimeFeatureFlags) -> Self {
        self.config.features = value;
        self
    }

    #[expect(
        clippy::panic,
        reason = "runtime tests use validated in-process RTC fixtures and should fail loudly if construction becomes invalid"
    )]
    pub(super) fn build_state(self) -> RuntimeTestState {
        let services = RuntimeServices::default();
        let media_transport = match build_media_transport(&self.config, &services) {
            Ok(transport) => transport,
            Err(error) => panic!("runtime test RTC transport config should be valid: {error}"),
        };
        let room_manager = build_room_manager(
            build_room_runtime_policy(&self.config, &media_transport),
            &services,
            self.config.user.room_reservation_ttl,
        );
        let runtime_config = RuntimeConfig::from_config(&self.config);
        let state = RuntimeState::from_parts(
            runtime_config,
            Arc::clone(&room_manager),
            Arc::clone(&services.metrics),
            media_transport.clone(),
            CancellationToken::new(),
            TaskTracker::new(),
        );
        RuntimeTestState {
            state,
            room_manager,
            media_transport,
        }
    }

    pub(super) fn build_runtime_state(self) -> RuntimeState {
        self.build_state().state
    }
}

pub(super) fn test_outbound_sender(
    state: &RuntimeState,
) -> (UserOutboundSender, UserOutboundReceiver) {
    UserOutboundSender::channel(
        state.config.user.outbound_queue_capacity,
        Arc::clone(&state.metrics),
    )
}

// This struct is used for testing serialization of HttpRoomClaims while keeping the initial struct
// secure. It allows us to serialize the SecretString as a string for testing purposes.
#[derive(Debug, Serialize)]
pub struct TestHttpRoomClaims {
    #[serde(flatten)]
    pub registered: RegisteredJwtClaims,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_secret_key"
    )]
    pub key: Option<SecretString>,
    #[serde(
        rename = "keySeed",
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_secret_key"
    )]
    pub key_seed: Option<SecretString>,
}

impl From<HttpRoomClaims> for TestHttpRoomClaims {
    fn from(claims: HttpRoomClaims) -> Self {
        Self {
            registered: claims.registered,
            key: claims.key,
            key_seed: claims.key_seed,
        }
    }
}

impl From<&Self> for TestHttpRoomClaims {
    fn from(claims: &Self) -> Self {
        Self {
            registered: claims.registered.clone(),
            key: claims.key.clone(),
            key_seed: claims.key_seed.clone(),
        }
    }
}

impl From<&HttpRoomClaims> for TestHttpRoomClaims {
    fn from(claims: &HttpRoomClaims) -> Self {
        Self {
            registered: claims.registered.clone(),
            key: claims.key.clone(),
            key_seed: claims.key_seed.clone(),
        }
    }
}

fn equal_test_http_room_claims<T, U>(a: &T, b: &U) -> bool
where
    T: Into<TestHttpRoomClaims> + Clone,
    U: Into<TestHttpRoomClaims> + Clone,
{
    let a_test: TestHttpRoomClaims = a.clone().into();
    let b_test: TestHttpRoomClaims = b.clone().into();
    a_test == b_test
}

impl PartialEq<HttpRoomClaims> for TestHttpRoomClaims {
    fn eq(&self, other: &HttpRoomClaims) -> bool {
        equal_test_http_room_claims(&self, other)
    }
}

impl PartialEq<TestHttpRoomClaims> for HttpRoomClaims {
    fn eq(&self, other: &TestHttpRoomClaims) -> bool {
        self.registered == other.registered
            && self.key.as_ref().map(SecretString::expose_secret)
                == other.key.as_ref().map(SecretString::expose_secret)
            && self.key_seed.as_ref().map(SecretString::expose_secret)
                == other.key_seed.as_ref().map(SecretString::expose_secret)
    }
}

impl PartialEq<Self> for TestHttpRoomClaims {
    fn eq(&self, other: &Self) -> bool {
        self.registered == other.registered
            && self.key.as_ref().map(SecretString::expose_secret)
                == other.key.as_ref().map(SecretString::expose_secret)
            && self.key_seed.as_ref().map(SecretString::expose_secret)
                == other.key_seed.as_ref().map(SecretString::expose_secret)
    }
}

impl Eq for TestHttpRoomClaims {}

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
