//! Room state, routing and media transport orchestration.
//!
//! `o-sfu-core` bridges the server runtime, the pure `o-sfu-router` state machine
//! and the `str0m`-backed media transport. It keeps room admission, user media
//! intent and RTC worker details behind [`SfuCore`](prelude::SfuCore) and
//! [`MediaSession`](prelude::MediaSession).
//!
//! # Public Surface
//!
//! - [`prelude`] contains caller-facing configuration, media intent,
//!   [`SfuCore`](prelude::SfuCore) and [`MediaSession`](prelude::MediaSession).
//! - [`server`] contains runtime construction, room integration, transport,
//!   diagnostics and metrics.
//! - Fundamental identifiers and [`Bitrate`] remain at the crate root.
//!
//! # Architecture
//!
//! ```text
//! server runtime
//!   -> SfuCore::admit_user
//!   -> MediaSession
//!   -> room state and source policy
//!   -> MediaTransport
//!   -> RTC workers
//! ```
//!
//! [`MediaTransport`](server::transport::MediaTransport) starts the worker threads
//! and binds their UDP sockets. Room operations release state locks before awaiting
//! transport work. Source policy maps layout intent, active-speaker observations
//! and receiver bandwidth to route activity and packet gates. Worker-local packet
//! loops then demultiplex UDP and forward RTP through those gates. The private
//! `rtc::codec` boundary contains capability projection plus codec-specific packet
//! inspection and rewrite, so source policy does not branch on payload details.
//!
//! # Server Construction
//!
//! The server builds one [`MediaTransport`](server::transport::MediaTransport)
//! from owner configuration and shared process services.
//!
//! ```no_run
//! use std::{
//!     net::{IpAddr, Ipv4Addr},
//!     sync::Arc,
//! };
//!
//! use o_sfu_core::{
//!     prelude::{
//!         Bitrate, CodecPreferences, MediaCodecFlags, RtcPortRange, RtcUdpIoBackend,
//!         SessionBitrateLimits, VideoBitrateLimits,
//!     },
//!     server::{
//!         metrics::RuntimeMetrics,
//!         packet_sinks::RoomPacketSinkRegistry,
//!         transport::{MediaTransport, MediaTransportConfig, MediaTransportDeps},
//!     },
//! };
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let config = MediaTransportConfig {
//!     worker_count: 1,
//!     announced_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
//!     bitrate_limits: SessionBitrateLimits::new(
//!         Bitrate::from_mbps(3),
//!         Bitrate::from_mbps(3),
//!     ),
//!     video_bitrate_limits: VideoBitrateLimits::default(),
//!     rtc_port_range: RtcPortRange::try_new(40_000, 40_099)?,
//!     rtc_udp_io_backend: RtcUdpIoBackend::Tokio,
//!     codec_flags: MediaCodecFlags::default(),
//!     codec_preferences: CodecPreferences::default(),
//!     media_quality_interval: None,
//! };
//! let deps = MediaTransportDeps {
//!     packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
//!     metrics: Arc::new(RuntimeMetrics::default()),
//! };
//!
//! let transport = MediaTransport::build(config, deps)?;
//!
//! # let _transport = transport;
//! # Ok(())
//! # }
//! ```
//!
//! [`MediaTransport::build`](server::transport::MediaTransport::build) returns
//! after every worker runtime has bound its UDP socket. Session-local RTC state
//! remains lazy.
//!
//! # Session Negotiation
//!
//! Negotiation is serialized through `&mut MediaSession`. A publish received
//! while an offer is pending is queued. Applying the answer returns a follow-up
//! offer when that intent needs another SDP round.
//!
//! ```no_run
//! use o_sfu_core::prelude::{
//!     MediaSession, NegotiationOffer, SessionError, SourcePublishIntent,
//! };
//!
//! # async fn exchange(_: NegotiationOffer) -> String { String::new() }
//! async fn publish_source(
//!     mut session: MediaSession,
//!     intent: SourcePublishIntent,
//! ) -> Result<(), SessionError> {
//!     let Some(initial_offer) = session.establish().await? else {
//!         return Ok(());
//!     };
//!     let initial_answer = exchange(initial_offer).await;
//!
//!     // Publish before answering so this intent queues behind the in-flight SDP round.
//!     let _queued_without_offer = session.publish(intent).await?;
//!
//!     let Some(follow_up_offer) = session.answer(&initial_answer).await? else {
//!         return Ok(());
//!     };
//!
//!     let follow_up_answer = exchange(follow_up_offer).await;
//!
//!     let _next_offer = session.answer(&follow_up_answer).await?;
//!     Ok(())
//! }
//! ```

use std::fmt::{self, Display, Formatter};

pub use o_sfu_router::{ConnectionId, MediaWorkerId};

mod engine;
mod options;
pub mod prelude;
pub mod server;
mod sfu;

pub(crate) use options::{
    AudioCodecPreference, CodecPreferences, MediaCodecFlags, RoomMediaLimits, RoomWorkerPolicy,
    RtcPortRange, RtcUdpIoBackend, RuntimeFeatureFlags, SessionBitrateLimits,
    VideoAdaptationTuning, VideoBitrateLimits, VideoCodecPreference,
};

/// Media bitrate stored as bits per second (not bytes per second).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bitrate(u64);

impl Bitrate {
    #[must_use]
    pub const fn from_bps(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn from_kbps(value: u64) -> Self {
        Self(value.saturating_mul(1_000))
    }

    #[must_use]
    pub const fn from_mbps(value: u64) -> Self {
        Self(value.saturating_mul(1_000_000))
    }

    #[must_use]
    pub const fn zero() -> Self {
        Self(0)
    }

    #[must_use]
    pub const fn as_bps(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }

    #[must_use]
    pub const fn saturating_sub(self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }

    #[must_use]
    pub const fn divided_by(self, divisor: u64) -> Self {
        match self.0.checked_div(divisor) {
            Some(value) => Self(value),
            None => Self::zero(),
        }
    }
}

/// Process-local generation tag for one room lifecycle.
///
/// The tag separates one runtime allocation from its application room identity
/// in transport keys and telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoomInstanceId(u64);

impl RoomInstanceId {
    /// Allocates the next room generation tag.
    ///
    /// The counter saturates, so repeated calls return `u64::MAX` after
    /// exhaustion.
    #[must_use]
    pub fn allocate(next_room_instance_id: &mut u64) -> Self {
        let room_instance_id = Self(*next_room_instance_id);
        *next_room_instance_id = next_room_instance_id.saturating_add(1);
        room_instance_id
    }

    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for RoomInstanceId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
