use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use super::{
    counter::{MetricLabel, PaddedCounterFamily},
    labels::{RtpDecoderRefreshScope, RtpFlowDirection, RtpForwardDestinationKind},
};

const RTP_FLOW_DIRECTION_COUNT: usize = <RtpFlowDirection as MetricLabel>::COUNT;
const RTP_FORWARD_DESTINATION_COUNT: usize = <RtpForwardDestinationKind as MetricLabel>::COUNT;
const RTP_DECODER_REFRESH_SCOPE_COUNT: usize = <RtpDecoderRefreshScope as MetricLabel>::COUNT;

/// Worker-local RTP packet metric recorder.
///
/// Packet loops keep one recorder for their full worker lifetime. Updates touch
/// only this worker's padded atomics while `RuntimeMetrics` aggregates all
/// registered recorders during scrape capture.
#[derive(Debug, Default)]
pub struct RtpMetricsRecorder {
    packets: PaddedCounterFamily<RtpFlowDirection>,
    payload_bytes: PaddedCounterFamily<RtpFlowDirection>,
    forwarded_packets: PaddedCounterFamily<RtpForwardDestinationKind>,
    forwarded_payload_bytes: PaddedCounterFamily<RtpForwardDestinationKind>,
    decoder_refreshes: PaddedCounterFamily<RtpDecoderRefreshScope>,
}

impl RtpMetricsRecorder {
    pub fn record_ingress(&self, payload_bytes: usize) {
        self.packets.increment(RtpFlowDirection::Ingress);
        self.payload_bytes
            .add(RtpFlowDirection::Ingress, payload_bytes);
    }

    pub fn record_egress(&self, payload_bytes: usize) {
        self.packets.increment(RtpFlowDirection::Egress);
        self.payload_bytes
            .add(RtpFlowDirection::Egress, payload_bytes);
    }

    pub fn record_forwarded(&self, destination: RtpForwardDestinationKind, payload_bytes: usize) {
        self.forwarded_packets.increment(destination);
        self.forwarded_payload_bytes.add(destination, payload_bytes);
    }

    pub fn record_decoder_refresh(&self, scope: RtpDecoderRefreshScope) {
        self.decoder_refreshes.increment(scope);
    }
}

#[derive(Debug, Default)]
pub(super) struct RtpMetrics {
    worker_recorders: Mutex<Vec<RtpWorkerMetricsRecorder>>,
}

impl RtpMetrics {
    pub(super) fn register_worker(
        &self,
        media_worker_id: Option<usize>,
    ) -> Arc<RtpMetricsRecorder> {
        let recorder = Arc::new(RtpMetricsRecorder::default());
        {
            let mut workers = match self.worker_recorders.lock() {
                Ok(workers) => workers,
                Err(poisoned) => poisoned.into_inner(),
            };
            workers.push(RtpWorkerMetricsRecorder {
                media_worker_id,
                recorder: Arc::clone(&recorder),
            });
        }
        recorder
    }

    pub(super) fn snapshot(&self) -> RtpMetricsSnapshot {
        let mut snapshot = RtpMetricsSnapshot::default();
        {
            let workers = match self.worker_recorders.lock() {
                Ok(workers) => workers,
                Err(poisoned) => poisoned.into_inner(),
            };
            let mut worker_snapshots = BTreeMap::<usize, RtpWorkerMetricsSnapshot>::new();
            for worker in workers.iter() {
                snapshot.add_recorder(&worker.recorder);
                if let Some(media_worker_id) = worker.media_worker_id {
                    worker_snapshots
                        .entry(media_worker_id)
                        .or_insert_with(|| RtpWorkerMetricsSnapshot::new(media_worker_id))
                        .traffic
                        .add_recorder(&worker.recorder);
                }
            }
            drop(workers);
            snapshot.worker_snapshots = worker_snapshots.into_values().collect();
        }
        snapshot
    }
}

#[derive(Debug)]
struct RtpWorkerMetricsRecorder {
    media_worker_id: Option<usize>,
    recorder: Arc<RtpMetricsRecorder>,
}

#[derive(Debug, Default)]
pub(super) struct RtpMetricsSnapshot {
    pub(super) traffic: RtpTrafficSnapshot,
    decoder_refreshes: [u64; RTP_DECODER_REFRESH_SCOPE_COUNT],
    worker_snapshots: Vec<RtpWorkerMetricsSnapshot>,
}

impl RtpMetricsSnapshot {
    pub(super) fn decoder_refreshes(&self, scope: RtpDecoderRefreshScope) -> u64 {
        self.decoder_refreshes
            .get(scope.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn worker_snapshots(&self) -> &[RtpWorkerMetricsSnapshot] {
        &self.worker_snapshots
    }

    fn add_recorder(&mut self, recorder: &RtpMetricsRecorder) {
        self.traffic.add_recorder(recorder);
        for scope in <RtpDecoderRefreshScope as MetricLabel>::VARIANTS {
            self.add_decoder_refresh(*scope, recorder.decoder_refreshes.load(*scope));
        }
    }

    fn add_decoder_refresh(&mut self, scope: RtpDecoderRefreshScope, refreshes: u64) {
        if let Some(counter) = self.decoder_refreshes.get_mut(scope.as_index()) {
            *counter = counter.saturating_add(refreshes);
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct RtpWorkerMetricsSnapshot {
    media_worker_id: usize,
    pub(super) traffic: RtpTrafficSnapshot,
}

impl RtpWorkerMetricsSnapshot {
    fn new(media_worker_id: usize) -> Self {
        Self {
            media_worker_id,
            ..Self::default()
        }
    }

    pub(super) const fn media_worker_id(&self) -> usize {
        self.media_worker_id
    }
}

#[derive(Debug, Default)]
pub(super) struct RtpTrafficSnapshot {
    packets: [u64; RTP_FLOW_DIRECTION_COUNT],
    payload_bytes: [u64; RTP_FLOW_DIRECTION_COUNT],
    forwarded_packets: [u64; RTP_FORWARD_DESTINATION_COUNT],
    forwarded_payload_bytes: [u64; RTP_FORWARD_DESTINATION_COUNT],
}

impl RtpTrafficSnapshot {
    pub(super) fn packets(&self, direction: RtpFlowDirection) -> u64 {
        self.packets.get(direction.as_index()).copied().unwrap_or(0)
    }

    pub(super) fn payload_bytes(&self, direction: RtpFlowDirection) -> u64 {
        self.payload_bytes
            .get(direction.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn forwarded_packets(&self, destination: RtpForwardDestinationKind) -> u64 {
        self.forwarded_packets
            .get(destination.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn forwarded_payload_bytes(&self, destination: RtpForwardDestinationKind) -> u64 {
        self.forwarded_payload_bytes
            .get(destination.as_index())
            .copied()
            .unwrap_or(0)
    }

    fn add_recorder(&mut self, recorder: &RtpMetricsRecorder) {
        for direction in <RtpFlowDirection as MetricLabel>::VARIANTS {
            self.add_flow(
                *direction,
                recorder.packets.load(*direction),
                recorder.payload_bytes.load(*direction),
            );
        }
        for destination in <RtpForwardDestinationKind as MetricLabel>::VARIANTS {
            self.add_forwarded(
                *destination,
                recorder.forwarded_packets.load(*destination),
                recorder.forwarded_payload_bytes.load(*destination),
            );
        }
    }

    fn add_flow(&mut self, direction: RtpFlowDirection, packets: u64, payload_bytes: u64) {
        if let Some(counter) = self.packets.get_mut(direction.as_index()) {
            *counter = counter.saturating_add(packets);
        }
        if let Some(counter) = self.payload_bytes.get_mut(direction.as_index()) {
            *counter = counter.saturating_add(payload_bytes);
        }
    }

    fn add_forwarded(
        &mut self,
        destination: RtpForwardDestinationKind,
        packets: u64,
        payload_bytes: u64,
    ) {
        if let Some(counter) = self.forwarded_packets.get_mut(destination.as_index()) {
            *counter = counter.saturating_add(packets);
        }
        if let Some(counter) = self.forwarded_payload_bytes.get_mut(destination.as_index()) {
            *counter = counter.saturating_add(payload_bytes);
        }
    }
}
