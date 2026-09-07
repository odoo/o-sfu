use std::time::Duration;

use o_sfu_model::WebSocketCloseCode;

use super::counter::{ExportedMetricLabel, HistogramBucketLabel, MetricBucketLabel, MetricLabel};

macro_rules! impl_metric_label {
    ($visibility:vis enum $label:ident { $($variant:ident => $value:tt),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        $visibility enum $label {
            $($variant),+
        }

        impl_metric_label!($label { $($variant => $value),+ });
    };
    ($label:ty { $($variant:ident => $index:expr),+ $(,)? }) => {
        impl MetricLabel for $label {
            const VARIANTS: &'static [Self] = &[$(Self::$variant),+];
            const COUNT: usize = Self::VARIANTS.len();

            fn as_index(self) -> usize {
                match self {
                    $(Self::$variant => $index),+
                }
            }
        }
    };
}

macro_rules! impl_exported_metric_label {
    ($visibility:vis enum $label:ident { $($variant:ident => $value:tt),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        $visibility enum $label {
            $($variant),+
        }

        impl_exported_metric_label!($label { $($variant => $value),+ });
    };
    ($label:ty { $($variant:ident => ($index:expr, $label_value:literal)),+ $(,)? }) => {
        impl_metric_label!($label {
            $($variant => $index),+
        });

        impl ExportedMetricLabel for $label {
            fn label_value(self) -> &'static str {
                match self {
                    $(Self::$variant => $label_value),+
                }
            }
        }
    };
}

macro_rules! impl_exported_metric_label_pair {
    ($visibility:vis enum $label:ident { $($variant:ident => $value:tt),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        $visibility enum $label {
            $($variant),+
        }

        impl_exported_metric_label_pair!($label { $($variant => $value),+ });
    };
    ($label:ty { $($variant:ident => ($index:expr, [($first_name:literal, $first_value:literal), ($second_name:literal, $second_value:literal)])),+ $(,)? }) => {
        impl_metric_label!($label {
            $($variant => $index),+
        });

        impl ExportedMetricLabelPair for $label {
            fn label_pair(self) -> [(&'static str, &'static str); 2] {
                match self {
                    $(Self::$variant => [($first_name, $first_value), ($second_name, $second_value)]),+
                }
            }
        }
    };
}

pub(super) trait ExportedMetricLabelPair: MetricLabel {
    fn label_pair(self) -> [(&'static str, &'static str); 2];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsSessionLoopExitReason {
    RuntimeShutdown,
    UserClosed,
    ReaderError,
    BusBreak,
    PingTimeout,
    TransportDisconnected,
    OutboundChannelClosed,
    OutboundCloseSignal,
    OutboundMessageSendFailure,
    OutboundQueueOverflow,
}

impl RtcRelayEnqueueResult {
    #[must_use]
    pub const fn target_label(self) -> &'static str {
        match self {
            Self::IntraNodeEnqueued | Self::IntraNodeOverloaded | Self::IntraNodeClosed => {
                "intra_node_relay"
            }
        }
    }

    #[must_use]
    pub const fn outcome_label(self) -> &'static str {
        match self {
            Self::IntraNodeEnqueued => "enqueued",
            Self::IntraNodeOverloaded => "overloaded",
            Self::IntraNodeClosed => "closed",
        }
    }
}

impl_exported_metric_label!(pub enum HttpRoute {
    Noop => (0, "noop"),
    Stats => (1, "stats"),
    Room => (2, "room"),
    Disconnect => (3, "disconnect"),
    Metrics => (4, "metrics"),
});

impl_exported_metric_label!(pub(super) enum HttpRoomResponseStatus {
    Success => (0, "success"),
    Unauthorized => (1, "unauthorized"),
    Forbidden => (2, "forbidden"),
    BadRequest => (3, "bad_request"),
    Conflict => (4, "conflict"),
});

impl_exported_metric_label!(pub(super) enum HttpDisconnectResponseStatus {
    Success => (0, "success"),
    BadRequest => (1, "bad_request"),
    UnprocessableEntity => (2, "unprocessable_entity"),
});

impl_metric_label!(pub(super) enum ControlPlaneDurationBucket {
    Le10Millis => 0,
    Le50Millis => 1,
    Le100Millis => 2,
    Le250Millis => 3,
    Le500Millis => 4,
    Le1Second => 5,
    Le5Seconds => 6,
});

impl MetricBucketLabel for ControlPlaneDurationBucket {
    fn upper_bound(self) -> &'static str {
        match self {
            Self::Le10Millis => "0.01",
            Self::Le50Millis => "0.05",
            Self::Le100Millis => "0.1",
            Self::Le250Millis => "0.25",
            Self::Le500Millis => "0.5",
            Self::Le1Second => "1",
            Self::Le5Seconds => "5",
        }
    }
}

impl HistogramBucketLabel for ControlPlaneDurationBucket {
    fn from_duration(duration: Duration) -> Self {
        if duration <= Duration::from_millis(10) {
            return Self::Le10Millis;
        }
        if duration <= Duration::from_millis(50) {
            return Self::Le50Millis;
        }
        if duration <= Duration::from_millis(100) {
            return Self::Le100Millis;
        }
        if duration <= Duration::from_millis(250) {
            return Self::Le250Millis;
        }
        if duration <= Duration::from_millis(500) {
            return Self::Le500Millis;
        }
        if duration <= Duration::from_secs(1) {
            return Self::Le1Second;
        }
        Self::Le5Seconds
    }
}

impl_exported_metric_label!(pub(super) enum WsConnectionStage {
    Accepted => (0, "accepted"),
    CredentialsReceived => (1, "credentials_received"),
    Joined => (2, "joined"),
});

impl_exported_metric_label!(WebSocketCloseCode {
    AuthTimeout => (0, "auth_timeout"),
    AuthFailed => (1, "auth_failed"),
    ProtocolError => (2, "protocol_error"),
    RoomFull => (3, "room_full"),
    Error => (4, "error"),
    Clean => (5, "clean"),
    Leaving => (6, "leaving"),
    Kicked => (7, "kicked"),
});

impl_exported_metric_label!(pub(super) enum WsStartupFailureKind {
    StartupSend => (0, "startup_send"),
    SessionInitialize => (1, "user_initialize"),
});

impl_exported_metric_label!(WsSessionLoopExitReason {
    UserClosed => (0, "user_closed"),
    ReaderError => (1, "reader_error"),
    BusBreak => (2, "bus_break"),
    PingTimeout => (3, "ping_timeout"),
    TransportDisconnected => (4, "transport_disconnected"),
    OutboundChannelClosed => (5, "outbound_room_closed"),
    OutboundCloseSignal => (6, "outbound_close_signal"),
    OutboundMessageSendFailure => (7, "outbound_message_send_failure"),
    OutboundQueueOverflow => (8, "outbound_queue_overflow"),
    RuntimeShutdown => (9, "runtime_shutdown"),
});

impl_exported_metric_label!(pub(super) enum WsBusDirection {
    Received => (0, "received"),
    Sent => (1, "sent"),
});

impl_exported_metric_label!(pub(super) enum WsBusFailureKind {
    InvalidInput => (0, "invalid_input"),
    UnsupportedFeature => (1, "unsupported_feature"),
    Send => (2, "send"),
});

impl_exported_metric_label!(pub(super) enum WsBusClientFrameKind {
    Request => (0, "request"),
    Message => (1, "message"),
});

impl_exported_metric_label!(pub(super) enum RtpFlowDirection {
    Ingress => (0, "ingress"),
    Egress => (1, "egress"),
});

impl_exported_metric_label!(pub enum RtpForwardDestinationKind {
    LocalRtc => (0, "local_rtc"),
    Recording => (1, "recording"),
    IntraNodeRelay => (2, "intra_node_relay"),
});

impl_exported_metric_label!(pub enum RtpDecoderRefreshScope {
    Rid => (0, "rid"),
    Source => (1, "source"),
});

impl_exported_metric_label!(pub enum RtpRelayDropKind {
    IntraNodeRelay => (0, "intra_node_relay"),
});

impl_exported_metric_label!(pub enum RtcDatagramRoutePath {
    Indexed => (0, "indexed"),
    Scan => (1, "scan"),
});

impl_exported_metric_label!(pub enum RtcDatagramDropReason {
    RecentMissCache => (0, "recent_miss_cache"),
    SourceRateLimited => (1, "source_rate_limited"),
    NoUser => (2, "no_user"),
    Malformed => (3, "malformed"),
});

impl_exported_metric_label!(pub enum RtcNackDirection {
    SentToPublisher => (0, "sent_to_publisher"),
    ReceivedFromSubscriber => (1, "received_from_subscriber"),
});

impl_exported_metric_label!(pub enum RtcOutputBudgetLimit {
    Packets => (0, "packets"),
    PayloadBytes => (1, "payload_bytes"),
    PacketsAndPayloadBytes => (2, "packets_and_payload_bytes"),
});

impl_exported_metric_label!(pub enum RtcRouteControlOutcome {
    Absorbed => (0, "absorbed"),
    Forwarded => (1, "forwarded"),
    RouteGatedRelayDrop => (2, "route_gated_relay_drop"),
    LayerAllowed => (3, "layer_allowed"),
    LayerDropped => (4, "layer_dropped"),
});

impl_exported_metric_label!(pub enum RtcKeyframeRequestOutcome {
    Forwarded => (0, "forwarded"),
    Absorbed => (1, "absorbed"),
    Retry => (2, "retry"),
    Cleared => (3, "cleared"),
});

impl_metric_label!(pub enum RtcRelayEnqueueResult {
    IntraNodeEnqueued => 0,
    IntraNodeOverloaded => 1,
    IntraNodeClosed => 2,
});

impl ExportedMetricLabelPair for RtcRelayEnqueueResult {
    fn label_pair(self) -> [(&'static str, &'static str); 2] {
        [
            ("target", self.target_label()),
            ("outcome", self.outcome_label()),
        ]
    }
}

impl_exported_metric_label!(pub enum RtcRemoteControlDropKind {
    Keyframe => (0, "keyframe"),
    PacketGate => (1, "packet_gate"),
});

impl_exported_metric_label!(pub enum RtcRemotePacketGateConvergence {
    Retry => (0, "retry"),
    Flushed => (1, "flushed"),
});

impl_exported_metric_label!(pub enum SourceSelectionKind {
    Open => (0, "open"),
    Encoding => (1, "encoding"),
});

impl_exported_metric_label!(pub enum BudgetSolverOutcome {
    Degraded => (0, "degraded"),
    Paused => (1, "paused"),
    Resumed => (2, "resumed"),
});

impl_exported_metric_label!(pub enum TransportIceState {
    New => (0, "new"),
    Checking => (1, "checking"),
    Connected => (2, "connected"),
    Completed => (3, "completed"),
    Disconnected => (4, "disconnected"),
});

impl_exported_metric_label!(pub enum TransportHealthState {
    Connected => (0, "connected"),
    Disconnected => (1, "disconnected"),
});

impl_exported_metric_label_pair!(pub(super) enum TransportHealthTransition {
    UnsetToConnected => (0, [("from", "unset"), ("to", "connected")]),
    UnsetToDisconnected => (1, [("from", "unset"), ("to", "disconnected")]),
    ConnectedToDisconnected => (2, [("from", "connected"), ("to", "disconnected")]),
    DisconnectedToConnected => (3, [("from", "disconnected"), ("to", "connected")]),
    ConnectedToUnset => (4, [("from", "connected"), ("to", "unset")]),
    DisconnectedToUnset => (5, [("from", "disconnected"), ("to", "unset")]),
});

impl_metric_label!(pub(super) enum TransportUserLifetimeBucket {
    Le1Second => 0,
    Le10Seconds => 1,
    Le60Seconds => 2,
    Le300Seconds => 3,
});

impl MetricBucketLabel for TransportUserLifetimeBucket {
    fn upper_bound(self) -> &'static str {
        match self {
            Self::Le1Second => "1",
            Self::Le10Seconds => "10",
            Self::Le60Seconds => "60",
            Self::Le300Seconds => "300",
        }
    }
}

impl_exported_metric_label!(pub enum MediaQualitySample {
    Peer => (0, "peer"),
    MediaIngress => (1, "media_ingress"),
    MediaEgress => (2, "media_egress"),
});

impl_exported_metric_label!(pub enum MediaQualityLossDirection {
    Ingress => (0, "ingress"),
    Egress => (1, "egress"),
});

impl_metric_label!(pub(super) enum MediaQualityRttBucket {
    Le50Millis => 0,
    Le100Millis => 1,
    Le250Millis => 2,
    Le500Millis => 3,
    Le1Second => 4,
    Le2Seconds => 5,
    Le5Seconds => 6,
});

impl MetricBucketLabel for MediaQualityRttBucket {
    fn upper_bound(self) -> &'static str {
        match self {
            Self::Le50Millis => "0.05",
            Self::Le100Millis => "0.1",
            Self::Le250Millis => "0.25",
            Self::Le500Millis => "0.5",
            Self::Le1Second => "1",
            Self::Le2Seconds => "2",
            Self::Le5Seconds => "5",
        }
    }
}

impl HistogramBucketLabel for MediaQualityRttBucket {
    fn from_duration(duration: Duration) -> Self {
        if duration <= Duration::from_millis(50) {
            return Self::Le50Millis;
        }
        if duration <= Duration::from_millis(100) {
            return Self::Le100Millis;
        }
        if duration <= Duration::from_millis(250) {
            return Self::Le250Millis;
        }
        if duration <= Duration::from_millis(500) {
            return Self::Le500Millis;
        }
        if duration <= Duration::from_secs(1) {
            return Self::Le1Second;
        }
        if duration <= Duration::from_secs(2) {
            return Self::Le2Seconds;
        }
        Self::Le5Seconds
    }
}

impl_exported_metric_label_pair!(pub(super) enum RecordingActionOutcome {
    StartAccepted => (0, [("action", "start"), ("outcome", "accepted")]),
    StartRejected => (1, [("action", "start"), ("outcome", "rejected")]),
    StopAccepted => (2, [("action", "stop"), ("outcome", "accepted")]),
    StopRejected => (3, [("action", "stop"), ("outcome", "rejected")]),
});
