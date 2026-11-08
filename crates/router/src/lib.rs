//! Pure topology and capability negotiation engine for O-SFU.
//!
//! `o-sfu-router` is the isolated, deterministic brain of the SFU. It models room
//! membership, multi-worker router placements, producer-consumer dependency graphs,
//! and typed RTP codec negotiation without touching networks, threads, or raw wire protocols.
//!
//! # Why is the Router Isolated?
//!
//! Traditional SFU architectures often interleave signaling protocols (SDP offer/answer),
//! routing topology, and RTP packet loops into monolithic async engines. `o-sfu-router`
//! enforces a strict architectural boundary:
//!
//! 1. **Complete Determinism (Zero I/O)**: Contains zero async runtimes, zero threads,
//!    and zero socket syscalls. All state transitions are synchronous and deterministic.
//! 2. **SDP-Free Domain Modeling**: Sits cleanly behind `o-sfu-core`'s SDP edge. It does not
//!    parse raw SDP text or manage ICE candidates; it operates on strongly-typed topology
//!    and RTP models ([`rtp::MediaStream`], [`rtp::MediaCapabilities`]).
//! 3. **Isolated Placement & Teardowns**: Tracks cross-router subscriptions via foreign session
//!    shadows. When a client reconnects or leaves, the router cleans its dependent graph
//!    without affecting other active publishers.
//! 4. **Direct Testability**: Because the crate has zero I/O, complex multi-router reconnects,
//!    cascading disconnects, and codec negotiation edge cases can be tested directly.
//!
//! # System Architecture
//!
//! ```text
//!                       +------------------------------------------+
//!                       |       Signaling Edge / Clients           |
//!                       | (HTTP, WebSocket, SDP Offer/Answer Bags) |
//!                       +------------------------------------------+
//!                                            |
//!                     core adapts SDP to     |  request placement,
//!                     MediaStream / Caps     |  publish, subscribe
//!                                            v
//! +===================================================================================+
//! |                              o-sfu-router (Pure Core)                             |
//! |                                                                                   |
//! |  * 100% Synchronous & Deterministic (Zero async, Zero I/O, Zero RTP transport)    |
//! |                                                                                   |
//! |  +-------------------------------------+   +------------------------------------+ |
//! |  | Multi-Router Routing Topology       |   | Typed RTP Capability Matching      | |
//! |  | - User -> Connection -> Home Router |   | - Ingress normalization            | |
//! |  | - Producer -> Dependent Consumers   |   | - Egress codec intersection        | |
//! |  | - Cross-Router Session Shadows      |   | - RFC 4588 RTX `apt` remapping     | |
//! |  +-------------------------------------+   +------------------------------------+ |
//! +===================================================================================+
//!                                            |
//!                  routed identities, worker |  deterministic
//!                     lookups, RTP specs     |  graph mutations
//!                                            v
//! +-----------------------------------------------------------------------------------+
//! |                            o-sfu-core (Runtime & Engine)                          |
//! |                                                                                   |
//! |  * Async Tokio Runtimes, Media Transport Workers, UDP Demuxing, str0m Packet Loop |
//! +-----------------------------------------------------------------------------------+
//! ```
//!
//! The [`Router`] struct is the sole stateful facade. It owns the exact user-to-connection
//! placement relation and manages local and foreign session graphs across attached routers.
//!
//! # Examples
//!
//! ```
//! # use o_sfu_model::UserId;
//! # use o_sfu_router::{
//! #     ConnectionId, ConsumerId, MediaWorkerId, ProducerId, Router, RouterError, RouterId,
//! #     rtp::MediaCapabilities,
//! #     topology::{RouterPlacement, RouterPlacements},
//! # };
//!
//! # fn main() -> Result<(), RouterError> {
//! # fn placement(router: u64, worker: usize) -> RouterPlacement {
//! #     RouterPlacement {
//! #         router: RouterId(router),
//! #         media_worker: MediaWorkerId::from_raw(worker),
//! #     }
//! # }
//! let source = placement(1, 0);
//! let first_receiver = placement(2, 1);
//! let next_receiver = placement(3, 2);
//! let placements = RouterPlacements::new(source, vec![first_receiver, next_receiver]);
//! let mut router = Router::with_placements(placements, MediaCapabilities::default());
//! let publisher = UserId::from(1_i64);
//! let receiver = UserId::from(2_i64);
//! let publisher_connection = ConnectionId::from_raw(10);
//! let first_receiver_connection = ConnectionId::from_raw(20);
//! router.commit_session_placement(&publisher, publisher_connection, source)?;
//! router.commit_session_placement(&receiver, first_receiver_connection, first_receiver)?;
//! let producer = router.add_producer(&publisher, ProducerId(30))?;
//! let stale = router.add_consumer(&receiver, ConsumerId(40), producer)?;
//!
//! assert_eq!(stale.router_id(), producer.router_id());
//! assert_eq!(stale.connection_id(), first_receiver_connection);
//!
//! let next_receiver_connection = ConnectionId::from_raw(21);
//! router.commit_session_placement(&receiver, next_receiver_connection, next_receiver)?;
//! assert_eq!(
//!     router.remove_consumer(stale),
//!     Err(RouterError::MissingConsumer(stale)),
//! );
//!
//! let current = router.add_consumer(&receiver, ConsumerId(41), producer)?;
//! assert_eq!(current.router_id(), producer.router_id());
//! assert_eq!(current.connection_id(), next_receiver_connection);
//! # Ok(())
//! # }
//! ```

mod model;
#[cfg(any(test, feature = "test-support"))]
#[path = "TESTS/test_support/mod.rs"]
pub mod test_support;

/// typed router and media identifiers
pub mod ids {
    pub use crate::model::ids::*;
}

/// typed RTP values used at the router boundary
pub mod rtp {
    pub use crate::model::{MediaKind, rtp::*};
}

/// producer and consumer RTP negotiation
///
/// Producer parameters are first normalized against router capabilities. The
/// consumer is then negotiated from that router-visible stream
///
/// # Examples
///
/// ```
/// # use o_sfu_rfc::rtp::{
/// #     CodecName, RTP_VIDEO_CLOCK_RATE_HZ as VIDEO_CLOCK_RATE_HZ, codec_name, fmtp,
/// # };
/// # use o_sfu_router::{
/// #     MediaKind,
/// #     negotiation::{
/// #         RtpNegotiationError, derive_consumable_rtp_parameters,
/// #         negotiate_consumer_rtp_parameters,
/// #     },
/// #     rtp::{
/// #         MediaCapabilities, MediaCodecCapability, MediaFormat, MediaStream, PayloadType,
/// #         RtcpFeedback, RtcpFeedbackKind,
/// #     },
/// # };
/// # fn main() -> Result<(), RtpNegotiationError> {
/// let producer = MediaStream::new(
///     vec![
///         MediaFormat::new(MediaKind::Video, CodecName::Rtx, PayloadType::new(97), VIDEO_CLOCK_RATE_HZ)
///             .with_parameter(fmtp::RTX_ASSOCIATION, "96"),
///         MediaFormat::new(MediaKind::Video, CodecName::Vp8, PayloadType::new(96), VIDEO_CLOCK_RATE_HZ)
///             .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None)),
///     ],
///     vec![],
///     vec![],
/// );
/// let router = MediaCapabilities::new(
///     vec![
///         MediaCodecCapability::new(MediaKind::Video, CodecName::Vp8, VIDEO_CLOCK_RATE_HZ)
///             .with_payload_type(PayloadType::new(100))
///             .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None)),
///         MediaCodecCapability::new(MediaKind::Video, CodecName::Rtx, VIDEO_CLOCK_RATE_HZ)
///             .with_payload_type(PayloadType::new(101))
///             .with_parameter(fmtp::RTX_ASSOCIATION, "100"),
///     ],
///     vec![],
/// );
/// let consumer = MediaCapabilities::new(
///     vec![
///         MediaCodecCapability::new(MediaKind::Video, CodecName::Vp8, VIDEO_CLOCK_RATE_HZ)
///             .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None)),
///         MediaCodecCapability::new(MediaKind::Video, CodecName::Rtx, VIDEO_CLOCK_RATE_HZ)
///             .with_parameter(fmtp::RTX_ASSOCIATION, "100"),
///     ],
///     vec![],
/// );
///
/// let consumable = derive_consumable_rtp_parameters(&producer, &router)?;
/// let negotiated = negotiate_consumer_rtp_parameters(&consumable, &consumer)?;
///
/// assert_eq!(
///     consumable
///         .formats()
///         .map(|format| (
///             format.codec_name(),
///             format.payload_type(),
///             format.rtx_associated_payload_type(),
///         ))
///         .collect::<Vec<_>>(),
///     vec![(codec_name::VP8, 100, None), (codec_name::RTX, 101, Some(100))],
/// );
/// assert_eq!(negotiated, consumable);
/// # Ok(())
/// # }
/// ```
pub mod negotiation {
    #[cfg(any(test, feature = "test-support"))]
    pub use crate::model::diagnostic::*;
    pub use crate::model::rtp_negotiation::*;
}

/// placement and routed media identifiers used by [`Router`]
pub mod topology {
    pub use crate::model::topology::{
        PlacementSnapshot, RoutedConsumerId, RoutedProducerId, RouterPlacement, RouterPlacements,
    };
}

pub use ids::{ConnectionId, ConsumerId, MediaWorkerId, ProducerId, RouterId};
pub use model::{MediaKind, Router, RouterError};
