use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use ipnet::IpNet;
use o_sfu_core::prelude::Bitrate;
use secrecy::SecretString;

use super::{
    CodecPreferences, MediaCodecFlags, RoomMediaLimits, RoomWorkerPolicy, RtcPortRange,
    RtcUdpIoBackend, VideoAdaptationTuning, VideoBitrateLimits, diagnostics::DiagnosticsConfig,
    feature_flags::RuntimeFeatureFlags, telemetry::TelemetryConfig,
};

pub const DEFAULT_AUTHENTICATION_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS: usize = 512;
pub const DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN: usize = 16;

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpConfig {
    pub bind_address: SocketAddr,
    pub trust_proxy_headers: bool,
    /// TCP peers permitted to supply forwarded request metadata when proxy mode is enabled.
    pub trusted_proxies: Vec<IpNet>,
    /// Maximum accepted HTTP sockets, including upgraded WebSocket connections.
    /// Must be positive and no greater than [`tokio::sync::Semaphore::MAX_PERMITS`].
    pub max_http_connections: usize,
    /// Acceptance-to-first-header and subsequent HTTP/1 header deadline.
    /// Must be between one second and one day.
    pub header_read_timeout: Duration,
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
    pub departure_grace: Duration,
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
