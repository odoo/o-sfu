mod catalog;
mod counter;
mod descriptor;
mod labels;
mod rtc;
mod rtp;

#[cfg(any(test, feature = "test-support"))]
#[path = "TESTS/snapshot.rs"]
mod snapshot;

#[cfg(any(test, feature = "test-support"))]
#[path = "TESTS/test_support.rs"]
pub mod test_support;

#[cfg(test)]
#[expect(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;

#[cfg(test)]
pub(crate) use descriptor::METRIC_FAMILY_COUNT;
pub(crate) use descriptor::render_prometheus_text;
#[cfg(any(test, feature = "test-support"))]
pub use snapshot::RuntimeMetricsSnapshot;

pub use self::{
    catalog::RuntimeMetrics,
    descriptor::{MetricName, RoomGaugeValues},
    labels::{
        BudgetSolverOutcome, HttpRoute, MediaQualityLossDirection, MediaQualitySample,
        RtcDatagramDropReason, RtcDatagramRoutePath, RtcKeyframeRequestOutcome, RtcNackDirection,
        RtcOutputBudgetLimit, RtcProducerSsrcBindingOutcome, RtcRelayEnqueueResult,
        RtcRemoteControlDropKind, RtcRemotePacketGateConvergence, RtcRouteControlOutcome,
        RtpDecoderRefreshScope, RtpForwardDestinationKind, RtpRelayDropKind, SourceSelectionKind,
        TransportHealthState, TransportIceState, WsSessionLoopExitReason,
    },
    rtc::RtcMetricsRecorder,
    rtp::RtpMetricsRecorder,
};
