use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use secrecy::SecretString;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub use crate::runtime::auth::test_support::TestHttpRoomClaims;
pub(super) use crate::runtime::metrics::test_support::RuntimeMetricsSnapshotTestExt;
use crate::{
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
pub(super) const TEST_ROOM_KEY: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";

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
                    key: SecretString::from(TEST_AUTH_KEY),
                    authentication_timeout_ms: DEFAULT_AUTHENTICATION_TIMEOUT_MS,
                    max_pre_auth_websocket_sessions: DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
                    max_pre_auth_websocket_sessions_per_origin:
                        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN,
                },
                http: HttpConfig {
                    bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
                    trust_proxy_headers: false,
                    trusted_proxies: Vec::new(),
                    shutdown_timeout_ms: 10_000,
                },
                user: UserConfig {
                    room_size: 100,
                    timeout_ms: 10_000,
                    ping_interval_ms: 60_000,
                    outbound_queue_capacity: DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
                    outbound_queue_byte_capacity: DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
                    room_reservation_ttl: Duration::from_secs(5),
                    departure_grace: Duration::from_mins(1),
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
        self.config.http.trusted_proxies = vec![IpAddr::V4(Ipv4Addr::LOCALHOST).into()];
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
            self.config.user.departure_grace,
        );
        let runtime_config = match RuntimeConfig::from_config(&self.config) {
            Ok(config) => config,
            Err(error) => panic!("runtime test auth config should be valid: {error}"),
        };
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

#[expect(
    clippy::expect_used,
    reason = "the shared room fixture key is valid HS256 material"
)]
pub(super) fn test_room_key() -> secrecy::SecretSlice<u8> {
    super::auth::decode_signing_key(&TEST_ROOM_KEY.into())
        .expect("shared test room key should be valid")
}
