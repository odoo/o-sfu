use std::{
    mem::take,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};

use super::{
    counter::{MetricLabel, PaddedCounter, PaddedCounterFamily},
    labels::{
        RtcDatagramDropReason, RtcDatagramRoutePath, RtcDrainFailureStage, RtcInputFailure,
        RtcKeyframeRequestOutcome, RtcNackDirection, RtcOutputBudgetLimit,
        RtcProducerSsrcBindingOutcome, RtcRelayEnqueueResult, RtcRemoteControlDropKind,
        RtcRemotePacketGateConvergence, RtcRouteControlOutcome, RtcTransportIoFailure,
        RtcWorkerObservationKind,
    },
};

const RTC_DATAGRAM_ROUTE_PATH_COUNT: usize = <RtcDatagramRoutePath as MetricLabel>::COUNT;
const RTC_DATAGRAM_DROP_REASON_COUNT: usize = <RtcDatagramDropReason as MetricLabel>::COUNT;
const RTC_NACK_DIRECTION_COUNT: usize = <RtcNackDirection as MetricLabel>::COUNT;
const RTC_TRANSPORT_IO_FAILURE_COUNT: usize = <RtcTransportIoFailure as MetricLabel>::COUNT;
const RTC_INPUT_FAILURE_COUNT: usize = <RtcInputFailure as MetricLabel>::COUNT;
const RTC_DRAIN_FAILURE_STAGE_COUNT: usize = <RtcDrainFailureStage as MetricLabel>::COUNT;
const RTC_WORKER_OBSERVATION_KIND_COUNT: usize = <RtcWorkerObservationKind as MetricLabel>::COUNT;
const RTC_OUTPUT_BUDGET_LIMIT_COUNT: usize = <RtcOutputBudgetLimit as MetricLabel>::COUNT;
const RTC_PRODUCER_SSRC_BINDING_OUTCOME_COUNT: usize =
    <RtcProducerSsrcBindingOutcome as MetricLabel>::COUNT;
const RTC_ROUTE_CONTROL_OUTCOME_COUNT: usize = <RtcRouteControlOutcome as MetricLabel>::COUNT;
const RTC_KEYFRAME_REQUEST_OUTCOME_COUNT: usize = <RtcKeyframeRequestOutcome as MetricLabel>::COUNT;
const RTC_RELAY_ENQUEUE_RESULT_COUNT: usize = <RtcRelayEnqueueResult as MetricLabel>::COUNT;
const RTC_REMOTE_CONTROL_DROP_KIND_COUNT: usize = <RtcRemoteControlDropKind as MetricLabel>::COUNT;
const RTC_REMOTE_PACKET_GATE_CONVERGENCE_COUNT: usize =
    <RtcRemotePacketGateConvergence as MetricLabel>::COUNT;

const FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Default)]
struct FailureLogSlot {
    last_log: Option<Instant>,
    suppressed: u64,
}

impl FailureLogSlot {
    fn observe(&mut self, now: Instant) -> Option<u64> {
        if self
            .last_log
            .is_some_and(|last| now.saturating_duration_since(last) < FAILURE_LOG_INTERVAL)
        {
            self.suppressed = self.suppressed.saturating_add(1);
            return None;
        }
        let suppressed = take(&mut self.suppressed);
        self.last_log = Some(now);
        Some(suppressed)
    }
}

/// Worker-local RTC packet-loop metric recorder.
///
/// Packet loops keep one recorder for their full worker lifetime. Datagram and
/// route-control updates touch only this worker's padded atomics while
/// `RuntimeMetrics` aggregates all registered recorders during scrape capture.
#[derive(Debug, Default)]
pub struct RtcMetricsRecorder {
    datagram_routes: PaddedCounterFamily<RtcDatagramRoutePath>,
    datagram_drops: PaddedCounterFamily<RtcDatagramDropReason>,
    datagram_fallback_scans: PaddedCounter,
    datagram_scan_users: PaddedCounter,
    nacks: PaddedCounterFamily<RtcNackDirection>,
    rtx_packets_received_from_publisher: PaddedCounter,
    rtx_payload_bytes_received_from_publisher: PaddedCounter,
    rtcp_ingress_budget_drops: PaddedCounter,
    transport_io_failures: PaddedCounterFamily<RtcTransportIoFailure>,
    transport_io_log: Mutex<[FailureLogSlot; RTC_TRANSPORT_IO_FAILURE_COUNT]>,
    input_failures: PaddedCounterFamily<RtcInputFailure>,
    input_log: Mutex<[FailureLogSlot; RTC_INPUT_FAILURE_COUNT]>,
    drain_failures: PaddedCounterFamily<RtcDrainFailureStage>,
    worker_terminal_failures: PaddedCounter,
    worker_observation_timeouts: PaddedCounterFamily<RtcWorkerObservationKind>,
    output_budget_exhaustions: PaddedCounterFamily<RtcOutputBudgetLimit>,
    output_budget_session_closes: PaddedCounter,
    producer_ssrc_bindings: PaddedCounterFamily<RtcProducerSsrcBindingOutcome>,
    route_control: PaddedCounterFamily<RtcRouteControlOutcome>,
    keyframe_requests: PaddedCounterFamily<RtcKeyframeRequestOutcome>,
    relay_enqueues: PaddedCounterFamily<RtcRelayEnqueueResult>,
    relay_mailbox_depth_samples: PaddedCounter,
    relay_mailbox_depth_total: PaddedCounter,
    relay_drain_batches: PaddedCounter,
    relay_drained_packets: PaddedCounter,
    relay_drain_cap_hits: PaddedCounter,
    remote_control_drops: PaddedCounterFamily<RtcRemoteControlDropKind>,
    remote_packet_gate_convergence: PaddedCounterFamily<RtcRemotePacketGateConvergence>,
}

impl RtcMetricsRecorder {
    pub fn record_rtc_datagram_route(&self, path: RtcDatagramRoutePath) {
        self.datagram_routes.increment(path);
    }

    pub fn record_rtc_datagram_drop(&self, reason: RtcDatagramDropReason) {
        self.datagram_drops.increment(reason);
    }

    pub fn record_rtc_datagram_fallback_scan(&self, examined_sessions: usize) {
        self.datagram_fallback_scans.increment();
        self.datagram_scan_users.add(examined_sessions);
    }

    pub fn record_rtc_nacks(&self, direction: RtcNackDirection, count: u64) {
        self.nacks.add_u64(direction, count);
    }

    pub fn record_rtc_rtx_received_from_publisher(&self, payload_bytes: usize) {
        self.rtx_packets_received_from_publisher.increment();
        self.rtx_payload_bytes_received_from_publisher
            .add(payload_bytes);
    }

    pub fn record_rtc_rtcp_ingress_budget_drop(&self) {
        self.rtcp_ingress_budget_drops.increment();
    }

    /// Counts every failure and returns suppressed occurrences when this category may log.
    pub fn record_rtc_transport_io_failure(&self, failure: RtcTransportIoFailure) -> Option<u64> {
        self.transport_io_failures.increment(failure);
        let mut slots = self
            .transport_io_log
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        slots.get_mut(failure.as_index())?.observe(Instant::now())
    }

    /// Counts every failure and returns suppressed occurrences when this category may log.
    pub fn record_rtc_input_failure(&self, failure: RtcInputFailure) -> Option<u64> {
        self.input_failures.increment(failure);
        let mut slots = self
            .input_log
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        slots.get_mut(failure.as_index())?.observe(Instant::now())
    }

    pub fn record_rtc_drain_failure(&self, stage: RtcDrainFailureStage) {
        self.drain_failures.increment(stage);
    }

    pub fn record_rtc_worker_terminal_failure(&self) {
        self.worker_terminal_failures.increment();
    }

    pub fn record_rtc_worker_observation_timeout(&self, kind: RtcWorkerObservationKind) {
        self.worker_observation_timeouts.increment(kind);
    }

    pub fn record_rtc_output_budget_exhaustion(&self, limit: RtcOutputBudgetLimit) {
        self.output_budget_exhaustions.increment(limit);
    }

    pub fn record_rtc_output_budget_session_close(&self) {
        self.output_budget_session_closes.increment();
    }

    /// Records one producer binding change or rejection after RTP authentication.
    ///
    /// Repeated packets matching an established binding do not change it and
    /// should not increment this counter.
    pub fn record_rtc_producer_ssrc_binding(&self, outcome: RtcProducerSsrcBindingOutcome) {
        self.producer_ssrc_bindings.increment(outcome);
    }

    pub fn record_rtc_route_control(&self, outcome: RtcRouteControlOutcome) {
        self.route_control.increment(outcome);
    }

    pub fn record_rtc_keyframe_request(&self, outcome: RtcKeyframeRequestOutcome) {
        self.keyframe_requests.increment(outcome);
    }

    pub fn record_rtc_relay_enqueue(&self, result: RtcRelayEnqueueResult) {
        self.relay_enqueues.increment(result);
    }

    pub fn record_rtc_relay_mailbox_depth(&self, depth: usize) {
        self.relay_mailbox_depth_samples.increment();
        self.relay_mailbox_depth_total.add(depth);
    }

    pub fn record_rtc_relay_drain_batch(&self, drained_packets: usize, cap_hit: bool) {
        if drained_packets == 0 {
            return;
        }
        self.relay_drain_batches.increment();
        self.relay_drained_packets.add(drained_packets);
        if cap_hit {
            self.relay_drain_cap_hits.increment();
        }
    }

    pub fn record_rtc_remote_control_drop(&self, kind: RtcRemoteControlDropKind) {
        self.remote_control_drops.increment(kind);
    }

    pub fn record_rtc_remote_packet_gate_convergence(
        &self,
        outcome: RtcRemotePacketGateConvergence,
    ) {
        self.remote_packet_gate_convergence.increment(outcome);
    }
}

#[derive(Debug, Default)]
pub(super) struct RtcMetrics {
    worker_recorders: Mutex<Vec<Arc<RtcMetricsRecorder>>>,
}

impl RtcMetrics {
    pub(super) fn register_worker(&self) -> Arc<RtcMetricsRecorder> {
        let recorder = Arc::new(RtcMetricsRecorder::default());
        {
            let mut workers = match self.worker_recorders.lock() {
                Ok(workers) => workers,
                Err(poisoned) => poisoned.into_inner(),
            };
            workers.push(Arc::clone(&recorder));
        }
        recorder
    }

    pub(super) fn snapshot(&self) -> RtcMetricsSnapshot {
        let mut snapshot = RtcMetricsSnapshot::default();
        {
            let workers = match self.worker_recorders.lock() {
                Ok(workers) => workers,
                Err(poisoned) => poisoned.into_inner(),
            };
            for recorder in workers.iter() {
                snapshot.add_recorder(recorder);
            }
        }
        snapshot
    }
}

#[derive(Debug, Default)]
pub(super) struct RtcMetricsSnapshot {
    datagram_routes: [u64; RTC_DATAGRAM_ROUTE_PATH_COUNT],
    datagram_drops: [u64; RTC_DATAGRAM_DROP_REASON_COUNT],
    datagram_fallback_scans: u64,
    datagram_scan_users: u64,
    nacks: [u64; RTC_NACK_DIRECTION_COUNT],
    rtx_packets_received_from_publisher: u64,
    rtx_payload_bytes_received_from_publisher: u64,
    rtcp_ingress_budget_drops: u64,
    transport_io_failures: [u64; RTC_TRANSPORT_IO_FAILURE_COUNT],
    input_failures: [u64; RTC_INPUT_FAILURE_COUNT],
    drain_failures: [u64; RTC_DRAIN_FAILURE_STAGE_COUNT],
    worker_terminal_failures: u64,
    worker_observation_timeouts: [u64; RTC_WORKER_OBSERVATION_KIND_COUNT],
    output_budget_exhaustions: [u64; RTC_OUTPUT_BUDGET_LIMIT_COUNT],
    output_budget_session_closes: u64,
    producer_ssrc_bindings: [u64; RTC_PRODUCER_SSRC_BINDING_OUTCOME_COUNT],
    route_control: [u64; RTC_ROUTE_CONTROL_OUTCOME_COUNT],
    keyframe_requests: [u64; RTC_KEYFRAME_REQUEST_OUTCOME_COUNT],
    relay_enqueues: [u64; RTC_RELAY_ENQUEUE_RESULT_COUNT],
    relay_mailbox_depth_samples: u64,
    relay_mailbox_depth_total: u64,
    relay_drain_batches: u64,
    relay_drained_packets: u64,
    relay_drain_cap_hits: u64,
    remote_control_drops: [u64; RTC_REMOTE_CONTROL_DROP_KIND_COUNT],
    remote_packet_gate_convergence: [u64; RTC_REMOTE_PACKET_GATE_CONVERGENCE_COUNT],
}

impl RtcMetricsSnapshot {
    pub(super) fn datagram_routes(&self, path: RtcDatagramRoutePath) -> u64 {
        self.datagram_routes
            .get(path.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn datagram_drops(&self, reason: RtcDatagramDropReason) -> u64 {
        self.datagram_drops
            .get(reason.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) const fn datagram_fallback_scans(&self) -> u64 {
        self.datagram_fallback_scans
    }

    pub(super) const fn datagram_scan_users(&self) -> u64 {
        self.datagram_scan_users
    }

    pub(super) fn nacks(&self, direction: RtcNackDirection) -> u64 {
        self.nacks.get(direction.as_index()).copied().unwrap_or(0)
    }

    pub(super) const fn rtx_packets_received_from_publisher(&self) -> u64 {
        self.rtx_packets_received_from_publisher
    }

    pub(super) const fn rtx_payload_bytes_received_from_publisher(&self) -> u64 {
        self.rtx_payload_bytes_received_from_publisher
    }

    pub(super) const fn rtcp_ingress_budget_drops(&self) -> u64 {
        self.rtcp_ingress_budget_drops
    }

    pub(super) fn transport_io_failures(&self, failure: RtcTransportIoFailure) -> u64 {
        self.transport_io_failures
            .get(failure.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn input_failures(&self, failure: RtcInputFailure) -> u64 {
        self.input_failures
            .get(failure.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn drain_failures(&self, stage: RtcDrainFailureStage) -> u64 {
        self.drain_failures
            .get(stage.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) const fn worker_terminal_failures(&self) -> u64 {
        self.worker_terminal_failures
    }

    pub(super) fn worker_observation_timeouts(&self, kind: RtcWorkerObservationKind) -> u64 {
        self.worker_observation_timeouts
            .get(kind.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn output_budget_exhaustions(&self, limit: RtcOutputBudgetLimit) -> u64 {
        self.output_budget_exhaustions
            .get(limit.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) const fn output_budget_session_closes(&self) -> u64 {
        self.output_budget_session_closes
    }

    pub(super) fn producer_ssrc_bindings(&self, outcome: RtcProducerSsrcBindingOutcome) -> u64 {
        self.producer_ssrc_bindings
            .get(outcome.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn route_control(&self, outcome: RtcRouteControlOutcome) -> u64 {
        self.route_control
            .get(outcome.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn keyframe_requests(&self, outcome: RtcKeyframeRequestOutcome) -> u64 {
        self.keyframe_requests
            .get(outcome.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn relay_enqueues(&self, result: RtcRelayEnqueueResult) -> u64 {
        self.relay_enqueues
            .get(result.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) const fn relay_mailbox_depth_samples(&self) -> u64 {
        self.relay_mailbox_depth_samples
    }

    pub(super) const fn relay_mailbox_depth_total(&self) -> u64 {
        self.relay_mailbox_depth_total
    }

    pub(super) const fn relay_drain_batches(&self) -> u64 {
        self.relay_drain_batches
    }

    pub(super) const fn relay_drained_packets(&self) -> u64 {
        self.relay_drained_packets
    }

    pub(super) const fn relay_drain_cap_hits(&self) -> u64 {
        self.relay_drain_cap_hits
    }

    pub(super) fn remote_control_drops(&self, kind: RtcRemoteControlDropKind) -> u64 {
        self.remote_control_drops
            .get(kind.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn remote_packet_gate_convergence(
        &self,
        outcome: RtcRemotePacketGateConvergence,
    ) -> u64 {
        self.remote_packet_gate_convergence
            .get(outcome.as_index())
            .copied()
            .unwrap_or(0)
    }

    fn add_recorder(&mut self, recorder: &RtcMetricsRecorder) {
        recorder
            .datagram_routes
            .accumulate_into(&mut self.datagram_routes);
        recorder
            .datagram_drops
            .accumulate_into(&mut self.datagram_drops);
        self.datagram_fallback_scans = self
            .datagram_fallback_scans
            .saturating_add(recorder.datagram_fallback_scans.load());
        self.datagram_scan_users = self
            .datagram_scan_users
            .saturating_add(recorder.datagram_scan_users.load());
        recorder.nacks.accumulate_into(&mut self.nacks);
        self.rtx_packets_received_from_publisher = self
            .rtx_packets_received_from_publisher
            .saturating_add(recorder.rtx_packets_received_from_publisher.load());
        self.rtx_payload_bytes_received_from_publisher = self
            .rtx_payload_bytes_received_from_publisher
            .saturating_add(recorder.rtx_payload_bytes_received_from_publisher.load());
        self.rtcp_ingress_budget_drops = self
            .rtcp_ingress_budget_drops
            .saturating_add(recorder.rtcp_ingress_budget_drops.load());
        recorder
            .transport_io_failures
            .accumulate_into(&mut self.transport_io_failures);
        recorder
            .input_failures
            .accumulate_into(&mut self.input_failures);
        recorder
            .drain_failures
            .accumulate_into(&mut self.drain_failures);
        self.worker_terminal_failures = self
            .worker_terminal_failures
            .saturating_add(recorder.worker_terminal_failures.load());
        recorder
            .worker_observation_timeouts
            .accumulate_into(&mut self.worker_observation_timeouts);
        recorder
            .output_budget_exhaustions
            .accumulate_into(&mut self.output_budget_exhaustions);
        self.output_budget_session_closes = self
            .output_budget_session_closes
            .saturating_add(recorder.output_budget_session_closes.load());
        recorder
            .producer_ssrc_bindings
            .accumulate_into(&mut self.producer_ssrc_bindings);
        recorder
            .route_control
            .accumulate_into(&mut self.route_control);
        recorder
            .keyframe_requests
            .accumulate_into(&mut self.keyframe_requests);
        recorder
            .relay_enqueues
            .accumulate_into(&mut self.relay_enqueues);
        self.relay_mailbox_depth_samples = self
            .relay_mailbox_depth_samples
            .saturating_add(recorder.relay_mailbox_depth_samples.load());
        self.relay_mailbox_depth_total = self
            .relay_mailbox_depth_total
            .saturating_add(recorder.relay_mailbox_depth_total.load());
        self.relay_drain_batches = self
            .relay_drain_batches
            .saturating_add(recorder.relay_drain_batches.load());
        self.relay_drained_packets = self
            .relay_drained_packets
            .saturating_add(recorder.relay_drained_packets.load());
        self.relay_drain_cap_hits = self
            .relay_drain_cap_hits
            .saturating_add(recorder.relay_drain_cap_hits.load());
        recorder
            .remote_control_drops
            .accumulate_into(&mut self.remote_control_drops);
        recorder
            .remote_packet_gate_convergence
            .accumulate_into(&mut self.remote_packet_gate_convergence);
    }
}

#[cfg(test)]
#[path = "TESTS/failure_log.rs"]
mod failure_log_tests;
