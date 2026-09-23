//! Construction inputs for the media transport boundary.
//!
//! These types are cold-path configuration and dependency carriers. They are
//! consumed when the runtime builds the transport service and must not be
//! consulted by packet-loop code after startup. The split keeps startup policy
//! separate from long-lived service handles:
//! transport config describes operator policy, while transport deps describe
//! process services shared with metrics and recording.

use std::{net::IpAddr, sync::Arc, time::Duration};

use crate::{
    CodecPreferences, MediaCodecFlags, RtcPortRange, RtcUdpIoBackend, SessionBitrateLimits,
    VideoBitrateLimits,
    engine::{metrics::RuntimeMetrics, packet_sink_registry::RoomPacketSinkRegistry},
};

/// operator-facing RTC transport policy used to build each RTC worker
///
/// values are immutable after construction with each worker receiving only its
/// UDP sub-range while sharing bitrate, codec and announced IP policy
#[derive(Copy, Clone, Debug)]
pub struct MediaTransportConfig {
    /// number of process-local RTC workers owned by this transport
    pub worker_count: usize,
    /// client-routable address advertised in ICE candidates
    ///
    /// This is deployment policy, not a local bind address. A wrong value can
    /// make otherwise valid sessions unreachable from browsers.
    pub announced_ip: IpAddr,
    /// Per-user ingress and egress bitrate ceilings enforced by the transport.
    pub bitrate_limits: SessionBitrateLimits,
    /// Default video bitrate policy used while building negotiated offers.
    pub video_bitrate_limits: VideoBitrateLimits,
    /// UDP port range available to this transport config.
    ///
    /// The top-level config covers the whole process. Worker configs carry only
    /// the sub-range assigned to one media worker.
    pub rtc_port_range: RtcPortRange,
    /// UDP I/O implementation used by RTC packet-loop workers.
    pub rtc_udp_io_backend: RtcUdpIoBackend,
    /// Enabled codec set for offer generation and capability projection.
    pub codec_flags: MediaCodecFlags,
    /// Codec preference order preserved while constructing router capabilities.
    pub codec_preferences: CodecPreferences,
    /// str0m stats interval for transport-quality events, at most 24 hours.
    ///
    /// `None` or zero disables sampling. Construction rejects larger intervals.
    pub media_quality_interval: Option<Duration>,
}

impl MediaTransportConfig {
    /// Largest sampling interval accepted before constructing str0m timers.
    pub const MAX_MEDIA_QUALITY_INTERVAL: Duration = Duration::from_hours(24);
}

/// Long-lived services injected into media transport construction.
///
/// # Resource split
///
/// The transport owns no global telemetry registry or recording service by
/// itself. It receives handles to the process stores it must update while
/// executing media work.
#[derive(Debug, Clone)]
pub struct MediaTransportDeps {
    /// Room packet-sink registry used by the packet path to fan out recording
    /// and non-local destinations.
    pub packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    /// Process-local metrics catalog updated by transport lifecycle and media
    /// counters.
    pub metrics: Arc<RuntimeMetrics>,
}
