use std::sync::Arc;

use str0m::media::Mid;
use tokio::sync::mpsc;

use super::super::{
    commands::RtcWorkerCommand,
    state::{
        PacketLoopState, relay_registry::RelayTargetId, route_control::PacketLayerGate,
        source_route::RemoteSourceRegistration,
    },
    test_support::{
        MediaWorkerScenario, register_saturated_remote_source, test_transport_session_key,
    },
};
use crate::engine::{UserId, media_transport::TransportMediaId, metrics::RuntimeMetrics};

pub const REMOTE_GATE_RETRY_TURNS: usize = 8;

/// fixed remote packet-gate retry fixture for route-table benchmarks
///
/// setup installs remote sources whose source-worker control mailboxes are
/// already full
/// the measured path publishes fresh packet gates and flushes the pending retry
/// queue while pressure remains in place
pub struct RemoteGateRetryBenchFixture {
    state: PacketLoopState,
    sources: Vec<TransportMediaId>,
    _metrics: RuntimeMetrics,
    _control_rxs: Vec<mpsc::Receiver<RtcWorkerCommand>>,
}

impl RemoteGateRetryBenchFixture {
    #[must_use]
    pub fn sources_64() -> Self {
        Self::with_sources(64)
    }

    #[must_use]
    pub fn sources_256() -> Self {
        Self::with_sources(256)
    }

    fn with_sources(source_count: usize) -> Self {
        let mut state = PacketLoopState::default();
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();
        let mut sources = Vec::with_capacity(source_count);
        let mut registrations = Vec::with_capacity(source_count);
        let consumer_session = test_transport_session_key(171, 1, 172, UserId::Integer(173));

        {
            let mut scenario = MediaWorkerScenario::new(&mut state);
            for source_idx in 0..source_count {
                let source_offset = u64::try_from(source_idx).unwrap_or(0);
                let user_offset = i64::try_from(source_idx).unwrap_or(0);
                let source_session = test_transport_session_key(
                    171,
                    0,
                    10_000 + source_offset,
                    UserId::Integer(20_000 + user_offset),
                );
                let source_mid = Mid::from(format!("cam-up-{source_idx}").as_str());
                let src_media = scenario.source(source_session.clone(), source_mid);
                let consumer_mid = Mid::from(format!("cam-down-{source_idx}").as_str());
                scenario.destination(src_media, consumer_session.clone(), consumer_mid);
                let target_id = RelayTargetId::new(30_000 + source_offset);
                registrations.push((source_session, src_media, target_id));
                sources.push(src_media);
            }
        }

        let mut control_rxs = Vec::with_capacity(source_count);
        for (source_session, src_media, target_id) in registrations {
            control_rxs.push(register_saturated_remote_source(
                &mut state,
                src_media,
                &source_session,
                target_id,
                Arc::clone(&rtc_metrics),
            ));
        }

        Self {
            state,
            sources,
            _metrics: metrics,
            _control_rxs: control_rxs,
        }
    }

    #[must_use]
    pub fn retry_under_pressure(&mut self) -> usize {
        for turn in 0..REMOTE_GATE_RETRY_TURNS {
            let gate = if turn % 2 == 0 {
                PacketLayerGate::Block
            } else {
                PacketLayerGate::Open
            };
            for source_id in &self.sources {
                self.state.routes.publish_remote_pkt_gate(*source_id, gate);
            }
            self.state.routes.flush_remote_pkt_gates();
        }

        self.sources
            .iter()
            .filter(|source_id| {
                self.state
                    .routes
                    .remote_source(**source_id)
                    .is_some_and(RemoteSourceRegistration::has_pending_gate)
            })
            .count()
    }
}
