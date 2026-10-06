use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use super::{
    counter::{MetricLabel, PaddedCounter, PaddedCounterFamily},
    labels::{RtpDecoderRefreshScope, RtpFlowDirection, RtpForwardDestinationKind},
};

const RTP_FORWARD_DESTINATION_COUNT: usize = <RtpForwardDestinationKind as MetricLabel>::COUNT;
const RTP_DECODER_REFRESH_SCOPE_COUNT: usize = <RtpDecoderRefreshScope as MetricLabel>::COUNT;

/// Worker-local RTP packet metric recorder.
///
/// Packet loops keep one recorder for their full worker lifetime. Updates touch
/// only this worker's padded atomics while `RuntimeMetrics` aggregates all
/// registered recorders during scrape capture.
#[derive(Debug, Default)]
pub struct RtpMetricsRecorder {
    ingress_packets: PaddedCounter,
    ingress_payload_bytes: PaddedCounter,
    forwarded_packets: PaddedCounterFamily<RtpForwardDestinationKind>,
    forwarded_payload_bytes: PaddedCounterFamily<RtpForwardDestinationKind>,
    decoder_refreshes: PaddedCounterFamily<RtpDecoderRefreshScope>,
}

impl RtpMetricsRecorder {
    pub fn record_ingress(&self, payload_bytes: usize) {
        self.ingress_packets.increment();
        self.ingress_payload_bytes.add(payload_bytes);
    }

    /// Records one RTP forwarding event supplied by the caller.
    ///
    /// `LocalRtc` means successful local RTC queuing and also supplies the
    /// egress series. Pass the payload length returned by the local send.
    /// Socket delivery is not counted.
    /// Updates are additive and safe for concurrent callers.
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
        recorder
            .decoder_refreshes
            .accumulate_into(&mut self.decoder_refreshes);
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
    ingress_packets: u64,
    ingress_payload_bytes: u64,
    forwarded_packets: [u64; RTP_FORWARD_DESTINATION_COUNT],
    forwarded_payload_bytes: [u64; RTP_FORWARD_DESTINATION_COUNT],
}

impl RtpTrafficSnapshot {
    pub(super) fn packets(&self, direction: RtpFlowDirection) -> u64 {
        match direction {
            RtpFlowDirection::Ingress => self.ingress_packets,
            RtpFlowDirection::Egress => self.forwarded_packets(RtpForwardDestinationKind::LocalRtc),
        }
    }

    pub(super) fn payload_bytes(&self, direction: RtpFlowDirection) -> u64 {
        match direction {
            RtpFlowDirection::Ingress => self.ingress_payload_bytes,
            RtpFlowDirection::Egress => {
                self.forwarded_payload_bytes(RtpForwardDestinationKind::LocalRtc)
            }
        }
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
        self.ingress_packets = self
            .ingress_packets
            .saturating_add(recorder.ingress_packets.load());
        self.ingress_payload_bytes = self
            .ingress_payload_bytes
            .saturating_add(recorder.ingress_payload_bytes.load());
        recorder
            .forwarded_packets
            .accumulate_into(&mut self.forwarded_packets);
        recorder
            .forwarded_payload_bytes
            .accumulate_into(&mut self.forwarded_payload_bytes);
    }
}
