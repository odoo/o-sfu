use std::{mem::take, sync::Arc, time::Instant};

use str0m::media::Mid;

use super::super::{
    commands::WorkerMediaControlBatch,
    control::apply_media_control_batch,
    state::route_control::PacketLayerGate,
    test_support::{MediaWorkerScenario, prepare_source_session, test_transport_session_key},
};
use crate::{
    Bitrate,
    engine::{
        UserId,
        media_transport::{
            TransportConsumerRoute, TransportMediaId, TransportSessionKey, TransportSourceKey,
            rtc::state::PacketLoopState,
        },
        metrics::{RtcMetricsRecorder, RuntimeMetrics},
    },
};

/// fixed consumer packet-gate batch fixture for route-control benchmarks
///
/// setup installs one source with a caller-selected number of consumer
/// destinations. the measured path applies one source-scoped selected-RID gate
/// batch through the production worker route-control helper
pub struct ConsumerGateBatchBenchFixture {
    state: PacketLoopState,
    metrics: Arc<RtcMetricsRecorder>,
    source: Option<TransportSourceKey>,
    updates: Vec<(usize, TransportConsumerRoute, PacketLayerGate)>,
    src_media: TransportMediaId,
    now: Instant,
}

impl ConsumerGateBatchBenchFixture {
    #[must_use]
    pub fn consumers_64() -> Self {
        Self::with_consumers(64)
    }

    #[must_use]
    pub fn consumers_256() -> Self {
        Self::with_consumers(256)
    }

    fn with_consumers(destination_count: usize) -> Self {
        let src_key = test_transport_session_key(121, 0, 122, UserId::Integer(123));
        let dst_key = test_transport_session_key(121, 0, 124, UserId::Integer(125));
        let mut state = PacketLoopState::default();
        let src_media = prepare_source_session(&mut state, &src_key, Mid::from("cam-up"), 88_889);
        let mut scenario = MediaWorkerScenario::new(&mut state);
        let source = TransportSourceKey::new(src_key, src_media);
        let updates = route_gate_updates(
            &mut scenario,
            src_media,
            &source,
            &dst_key,
            destination_count,
        );

        Self {
            state,
            metrics: RuntimeMetrics::default().register_rtc_worker(),
            source: Some(source),
            updates,
            src_media,
            now: Instant::now(),
        }
    }

    #[must_use]
    pub fn apply_updates(mut self) -> Self {
        if let Some(source) = self.source.take() {
            let updates = take(&mut self.updates);
            let _ = apply_media_control_batch(
                &mut self.state,
                &self.metrics,
                Bitrate::from_mbps(10),
                self.now,
                WorkerMediaControlBatch::ConsumerGates { source, updates },
            );
        }
        self
    }

    #[must_use]
    pub fn updates_applied(self) -> bool {
        let route = self.state.routes.local_route(self.src_media);
        route.is_some_and(|r| {
            r.destinations
                .iter()
                .all(|dst| dst.delivery.pending_gate().is_some())
        })
    }
}

fn route_gate_updates(
    scenario: &mut MediaWorkerScenario<'_>,
    src_media: TransportMediaId,
    source: &TransportSourceKey,
    consumer_session: &TransportSessionKey,
    destination_count: usize,
) -> Vec<(usize, TransportConsumerRoute, PacketLayerGate)> {
    let mut updates = Vec::with_capacity(destination_count);
    for destination_idx in 0..destination_count {
        let mid = Mid::from(format!("cam-down-{destination_idx}").as_str());
        let consumer_media = scenario.destination(src_media, consumer_session.clone(), mid);
        updates.push((
            destination_idx,
            TransportConsumerRoute::new(consumer_session.clone(), consumer_media, source.clone()),
            PacketLayerGate::Rid("hi".into()),
        ));
    }
    updates
}
