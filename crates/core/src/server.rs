//! Server-runtime integration surface.
//!
//! The top-level runtime uses these facades for metrics, room management,
//! transport construction and packet sinks. The caller-facing session API
//! remains under [`crate::prelude`].
//!
//! Each name is exported once and only when the runtime, a test crate or a
//! public signature uses it. Diagnostics response types come from
//! `o_sfu_telemetry::diagnostics`.

/// process-local metric catalog and typed recorders
///
/// runtime edges record through this facade instead of assembling metric names
/// or label sets manually
/// the Prometheus renderer reads the catalog declared by these types
pub mod metrics {
    pub use o_sfu_telemetry::metrics::{
        HttpRoute, MetricName, RoomGaugeValues, RuntimeMetrics, WsSessionLoopExitReason,
    };
}

/// room packet-sink registry shared by transport workers
///
/// packet sinks are looked up by room when media has to fan out to recording or
/// other non-local destinations
/// the registry keeps that routing concern out of packet-loop callers
pub mod packet_sinks {
    pub use crate::engine::packet_sink_registry::{
        PacketSink, PacketSinkKind, RoomPacketSinkRegistry,
    };
}

/// room manager, room runtime and user-session integration types
///
/// this facade is the server crate's entry point for admitting users, sending
/// outbound messages and reading room statistics
/// pure routing and transport details remain behind the room API
///
/// # Slow-consumer shutdown
///
/// [`room::UserOutboundSender::send`] queues output without waiting for
/// capacity
/// when the queue is full it also records an overflow sentinel
/// [`room::UserOutboundReceiver::recv_event`] returns that sentinel before
/// output that was already queued
/// the caller should stop normal draining after
/// [`room::UserOutboundEvent::Overflow`]
///
/// ```
/// # use std::sync::Arc;
/// # use o_sfu_core::server::{
/// #     metrics::RuntimeMetrics,
/// #     room::{
/// #         RoomEventMessage, UserOutbound, UserOutboundEvent, UserOutboundSendError,
/// #         UserOutboundSender,
/// #     },
/// #     session::UserId,
/// # };
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let (sender, mut receiver) =
///     UserOutboundSender::channel(1, Arc::new(RuntimeMetrics::default()));
/// let departed = |id| {
///     UserOutbound::Message(RoomEventMessage::UserDeparted {
///         user_id: UserId::Integer(id),
///     })
/// };
/// let queued_output = departed(1);
/// assert!(sender.send(queued_output).is_ok());
///
/// assert!(matches!(
///     sender.send(departed(2)),
///     Err(UserOutboundSendError::Full(_))
/// ));
/// assert!(matches!(
///     receiver.recv_event().await,
///     UserOutboundEvent::Overflow(_)
/// ));
/// # }
/// ```
pub mod room {
    #[cfg(any(test, feature = "testing-transport"))]
    pub mod test_support {
        pub use crate::engine::{
            room::test_support::{
                ConsumerRouteState, JoinPlacementTestGate, NegotiatedPublish, RoomManagerTestApi,
                RoomTestApi,
            },
            source_model::test_support::{
                TestSourceKind, TestSubscriptionStates, stream_id_for_source,
                subscription_intents_from_test_states,
            },
        };
    }

    #[cfg(feature = "internal-benchmarks")]
    pub mod benchmark_support {
        pub use crate::engine::room::run_source_policy_turn_for_benchmark;
    }

    pub use crate::engine::room::{
        BroadcastPayload, BroadcastPayloadError, DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
        DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY, IncomingBitrateSnapshot, JoinUserRequest,
        RemoteTrackProjection, RemoteTrackSnapshot, Room, RoomAdmissionPolicy, RoomConfig,
        RoomDetailCapture, RoomEventMessage, RoomJoinError, RoomManager, RoomManagerJoinError,
        RoomManagerServeError, RoomMediaCounts, RoomOverviewCapture, RoomRuntimePolicy,
        RoomUserAdmission, RoomUserCapture, RoomUserStatsSnapshot, RoomUsersCapture,
        RuntimeRoomDirectorySnapshot, RuntimeRoomStatsSnapshot, UserCloseReason, UserOutbound,
        UserOutboundEvent, UserOutboundOverflow, UserOutboundOverflowKind, UserOutboundQueueLimits,
        UserOutboundReceiver, UserOutboundSendError, UserOutboundSender,
    };
}

/// `o-sfu-model` user types that room and session signatures take
pub mod session {
    pub use o_sfu_model::{UserId, UserInfo, UserPermissions, VideoLayoutIntent};
}

/// media transport construction and extension boundary
///
/// the runtime builds one `MediaTransport` from owner configuration and process
/// services then room code uses the opaque handle for media operations
///
/// code above this module should not branch on RTC worker internals
pub mod transport {
    #[cfg(feature = "internal-benchmarks")]
    pub use crate::engine::media_transport::benchmark_support;
    #[cfg(any(test, fuzzing))]
    pub use crate::engine::media_transport::fuzz_support;
    #[cfg(any(test, feature = "testing-transport"))]
    pub use crate::engine::media_transport::test_support;
    pub use crate::engine::media_transport::{
        ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
        ActiveSpeakerSourceDiagnostic, AppliedProducer, AppliedSessionAnswer, MediaTransport,
        MediaTransportBuildError, MediaTransportConfig, MediaTransportDeps,
        ReceiverBandwidthSnapshot, SourcePolicyUpdateSubscription, TransportAdapterError,
        TransportBitrateSnapshot, TransportHealthSnapshot, TransportMediaId,
        TransportQualitySample, TransportSessionKey, TransportSourceDiagnosticsSnapshot,
        TransportSourceKey, TransportWorkerPressureSnapshot,
    };
}
