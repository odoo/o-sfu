use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use o_sfu_core::prelude::Bitrate;
use secrecy::{ExposeSecret, SecretString};

use super::{
    CodecPreferences, MediaCodecFlags, RoomMediaLimits, RoomWorkerPolicy, RtcPortRange,
    RtcUdpIoBackend, VideoAdaptationTuning, VideoBitrateLimits, diagnostics::DiagnosticsConfig,
    feature_flags::RuntimeFeatureFlags, telemetry::TelemetryConfig,
};

pub const DEFAULT_AUTHENTICATION_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS: usize = 512;
pub const DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub auth: AuthConfig,
    pub http: HttpConfig,
    pub user: UserConfig,
    pub transport: TransportConfig,
    pub codecs: CodecConfig,
    pub features: RuntimeFeatureFlags,
    pub telemetry: TelemetryConfig,
    pub diagnostics: DiagnosticsConfig,
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub key: SecretString,
    pub authentication_timeout_ms: u64,
    pub max_pre_auth_websocket_sessions: usize,
    pub max_pre_auth_websocket_sessions_per_origin: usize,
}

impl PartialEq for AuthConfig {
    fn eq(&self, other: &Self) -> bool {
        self.key.expose_secret() == other.key.expose_secret()
            && self.authentication_timeout_ms == other.authentication_timeout_ms
            && self.max_pre_auth_websocket_sessions == other.max_pre_auth_websocket_sessions
            && self.max_pre_auth_websocket_sessions_per_origin
                == other.max_pre_auth_websocket_sessions_per_origin
    }
}

impl Eq for AuthConfig {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpConfig {
    pub bind_address: SocketAddr,
    pub trust_proxy_headers: bool,
    /// Positive deadline in milliseconds for listener, session, background and RTC worker drainage.
    /// Loaded from `SHUTDOWN_TIMEOUT_MS` with a `10_000` default.
    pub shutdown_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserConfig {
    pub room_size: usize,
    pub timeout_ms: u64,
    pub ping_interval_ms: u64,
    pub outbound_queue_capacity: usize,
    pub outbound_queue_byte_capacity: usize,
    pub room_reservation_ttl: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportConfig {
    pub announced_ip: IpAddr,
    pub max_bitrate_in: Bitrate,
    pub max_bitrate_out: Bitrate,
    pub video_bitrate_limits: VideoBitrateLimits,
    pub rtc_port_range: RtcPortRange,
    pub rtc_udp_io_backend: RtcUdpIoBackend,
    pub rtc_media_worker_count: usize,
    pub room_worker_policy: RoomWorkerPolicy,
    pub room_media_limits: RoomMediaLimits,
    pub video_adaptation_tuning: VideoAdaptationTuning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecConfig {
    pub flags: MediaCodecFlags,
    pub preferences: CodecPreferences,
}
