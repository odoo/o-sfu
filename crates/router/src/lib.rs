//! Sans-IO room-local media topology and RTP capability negotiation.
//!
//! Alice, Bob and Carol join a call. The caller registers their connections and worker
//! placements with [`Router`]. Alice publishes camera video, so the caller adds one
//! producer for Alice and a consumer for each viewer, Bob and Carol. The router records
//! these dependencies and returns routed IDs for the publication and subscriptions.
//!
//! If Bob stops watching, removing his consumer leaves Carol subscribed. Removing
//! Alice's session removes the producer and any consumers that depend on it.
//!
//! The [`negotiation`] module in this crate derives RTP parameters for the publication,
//! then matches them against receiver capabilities separately for Bob and Carol.
//! Each successful match returns a [`MediaStream`](rtp::MediaStream) describing compatible
//! RTP parameters. Adding producers and consumers to the topology does not run negotiation.
//!
//! Both APIs are synchronous and perform no I/O. The caller supplies identifiers,
//! chooses media workers and uses the returned IDs and RTP parameters to set up packet
//! forwarding. Transport cleanup when removing topology entries is up to the caller too.
//!
//! ```text
//!     IDs, placements and           MediaStream and
//!     media dependencies            MediaCapabilities
//!              |                          |
//!              v                          v
//! +--------------------- o-sfu-router ---------------------+
//! |            Router                 negotiation          |
//! |       connection topology       RTP compatibility      |
//! +------------|--------------------------|----------------+
//!              v                          v
//!    routed IDs and placements        MediaStream
//! ```
//!
//! # Placements and media identities
//!
//! One [`Router`] manages topology for a room across multiple logical [`RouterId`]s.
//! A [`RouterPlacement`](topology::RouterPlacement) maps a logical router to a
//! [`MediaWorkerId`]. Worker selection and load measurements belong to the caller.
//!
//! Each admitted user has a current [`ConnectionId`] and a home placement. Producers
//! live on that home router, while consumers live on their producer's router even when
//! receivers sit on another worker. For those receivers, the source router keeps a foreign
//! session containing their consumer dependencies.
//!
//! [`RoutedProducerId`](topology::RoutedProducerId) combines the source router, publisher
//! connection and producer ID. [`RoutedConsumerId`](topology::RoutedConsumerId) combines
//! the source router, receiver connection and consumer ID. To locate a receiver's worker,
//! use [`Router::committed_media_worker_id`] with its user and connection IDs.
//! The consumer's `router_id()` identifies the source topology, not the receiver's worker.
//!
//! # Use the topology API
//!
//! Start with [`Router::with_placements`] when worker assignments are known.
//! [`Router::new`] accepts only a primary router ID and RTP capabilities, so its first
//! session commit must assign that primary router to a worker. Later commits can add
//! placements, but cannot change an existing router's worker assignment.
//!
//! | Operation | Use |
//! | --- | --- |
//! | [`Router::commit_session_placement`] | Register a user connection and its placement before adding media. A new connection for an existing user replaces its previous session and media dependencies. |
//! | [`Router::add_producer`] | Register published media for a committed user and keep the returned routed producer ID. |
//! | [`Router::add_consumer`] | Connect a committed receiver to a routed producer and keep the returned routed consumer ID. |
//! | [`Router::remove_consumer`] | Remove a subscription by routed ID. The producer remains registered. |
//! | [`Router::retire_committed_placement`] | Remove a session only if its connection is still current. Returns `None` for an obsolete connection. |
//! | [`Router::remove_session`] | Remove the current session by user ID, including its producers and their dependent consumers. |
//! | [`Router::placement_snapshot`], [`Router::primary_worker`] | Read assigned placements for caller-side worker selection. |
//! | [`Router::rtp_capabilities`] | Read the capability baseline supplied at construction for RTP negotiation. |
//!
//! Mutations fail with [`RouterError`] for duplicate identities, missing sessions or
//! media, or inconsistent placement assignments. IDs come from the caller. Use a fresh
//! [`ConnectionId`] for each reconnect and keep routed IDs rather than reconstructing
//! them from bare producer or consumer numbers.
//!
//! For delayed disconnect handling, pass the original connection ID to
//! [`Router::retire_committed_placement`]. [`Router::remove_session`] targets whichever
//! connection is current. Neither operation closes transport sessions or returns a
//! transport cleanup plan, so callers must save the information needed for cleanup
//! before mutating topology.
//!
//! ## A receiver reconnects
//!
//! Replacing a connection removes its publications and consumer dependencies across routers.
//! In this example the receiver publishes nothing, so its replacement leaves the source
//! producer available. An obsolete connection cannot retire the replacement session:
//!
//! ```
//! use o_sfu_model::UserId;
//! use o_sfu_router::{
//!     ConnectionId, ConsumerId, MediaWorkerId, ProducerId, Router, RouterError, RouterId,
//!     rtp::MediaCapabilities,
//!     topology::{RouterPlacement, RouterPlacements},
//! };
//!
//! # fn main() -> Result<(), RouterError> {
//! let source = RouterPlacement {
//!     router: RouterId(1),
//!     media_worker: MediaWorkerId::from_raw(0),
//! };
//! let destination = RouterPlacement {
//!     router: RouterId(2),
//!     media_worker: MediaWorkerId::from_raw(1),
//! };
//! let mut router = Router::with_placements(
//!     RouterPlacements::new(source, vec![destination]),
//!     MediaCapabilities::default(),
//! );
//! let publisher = UserId::from(1_i64);
//! let receiver = UserId::from(2_i64);
//! let old_connection = ConnectionId::from_raw(20);
//! router.commit_session_placement(&publisher, ConnectionId::from_raw(10), source)?;
//! router.commit_session_placement(&receiver, old_connection, destination)?;
//! let producer = router.add_producer(&publisher, ProducerId(30))?;
//! let consumer = router.add_consumer(&receiver, ConsumerId(40), producer)?;
//!
//! assert_eq!(consumer.router_id(), source.router);
//! assert_eq!(consumer.connection_id(), old_connection);
//! assert_eq!(
//!     router.committed_media_worker_id(&receiver, old_connection),
//!     Some(destination.media_worker),
//! );
//!
//! let new_connection = ConnectionId::from_raw(21);
//! router.commit_session_placement(&receiver, new_connection, destination)?;
//! assert_eq!(router.retire_committed_placement(&receiver, old_connection), None);
//! assert_eq!(router.remove_consumer(consumer), Err(RouterError::MissingConsumer(consumer)));
//!
//! let replacement = router.add_consumer(&receiver, ConsumerId(41), producer)?;
//! assert_eq!(replacement.connection_id(), new_connection);
//! # Ok(())
//! # }
//! ```
//!
//! The empty capabilities above are enough for topology operations. Media forwarding
//! also requires negotiated RTP parameters and transport setup.
//!
//! # Negotiate RTP parameters
//!
//! [`rtp::MediaCapabilities`] describes supported codecs and header extensions.
//! [`rtp::MediaStream`] describes concrete formats, payload types and stream bindings.
//! Convert transport negotiation results into these types before calling the matching APIs:
//!
//! 1. [`derive_consumable_rtp_parameters`](negotiation::derive_consumable_rtp_parameters)
//!    matches producer formats against router capabilities and maps payload types into the
//!    router's forwarding model. Keep this consumable stream with the publication.
//! 2. [`negotiate_consumer_rtp_parameters`](negotiation::negotiate_consumer_rtp_parameters)
//!    selects compatible formats and feedback for each receiver. Pass the consumable stream
//!    from step 1 and the receiver's capabilities. The result preserves consumable payload types.
//!
//! Negotiation fails with [`RtpNegotiationError`](negotiation::RtpNegotiationError).
//! Producer derivation rejects unsupported media codecs and invalid repair associations.
//! Consumer negotiation rejects receivers with no compatible media codec. See
//! [`negotiation`] for an example with payload-type remapping and retransmission formats.

mod model;
#[cfg(any(test, feature = "test-support"))]
#[path = "TESTS/test_support/mod.rs"]
pub mod test_support;

/// Logical router, worker, connection and media identifiers.
///
/// These values identify topology entries. Creating an ID allocates no transport
/// resources. Keep [`topology::RoutedProducerId`] and [`topology::RoutedConsumerId`]
/// from router mutations when an operation needs to name a specific connection.
pub mod ids {
    pub use crate::model::ids::*;
}

/// Codec capabilities and concrete media parameters accepted by [`negotiation`].
///
/// [`MediaCapabilities`](rtp::MediaCapabilities) describes supported codecs and
/// extensions. [`MediaStream`](rtp::MediaStream) describes formats and bindings
/// for negotiated media. SDP parsing and packet rewriting belong to the caller.
pub mod rtp {
    pub use crate::model::{MediaKind, rtp::*};
}

/// Match producer media to router capabilities, then to receiver capabilities.
///
/// Call [`derive_consumable_rtp_parameters`](negotiation::derive_consumable_rtp_parameters)
/// once for a publication, then
/// [`negotiate_consumer_rtp_parameters`](negotiation::negotiate_consumer_rtp_parameters)
/// for each receiver using that consumable stream. These functions return media
/// descriptions without modifying [`Router`] or doing transport I/O.
///
/// # Payload-type remapping
///
/// Here the producer uses payload types 96 and 97, while the router uses 100 and
/// 101. Derivation resolves the repair association even though RTX appears before
/// VP8 in the producer formats. Consumer negotiation preserves the mapped pair.
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

/// Worker assignments and connection-scoped media identities used by [`Router`].
///
/// [`RouterPlacement`](topology::RouterPlacement) maps a logical router to a worker.
/// Both routed media IDs name the producer's router. A routed consumer names the
/// receiver's connection, whose worker can differ from the producer's worker.
pub mod topology {
    pub use crate::model::topology::{
        PlacementSnapshot, RoutedConsumerId, RoutedProducerId, RouterPlacement, RouterPlacements,
    };
}

pub use ids::{ConnectionId, ConsumerId, MediaWorkerId, ProducerId, RouterId};
pub use model::{MediaKind, Router, RouterError};
