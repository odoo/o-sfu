use std::sync::Arc;

use str0m::media::Mid;

use super::super::{
    packet_loop::{
        forwarded_packet::ForwardedPacket,
        forwarding_destination::ForwardingDestination,
        forwarding_planner::{PacketGateDecision, plan_forwards},
    },
    state::PacketLoopState,
    test_support::{MediaWorkerScenario, sample_forwarded_packet, test_transport_session_key},
};
use crate::engine::{
    UserId,
    metrics::{RtcMetricsRecorder, RtcRouteControlOutcome, RuntimeMetrics},
    packet_sink_registry::PacketSinkRouteCache,
};

pub const ROUTE_PLANNING_TURNS: usize = 1024;

/// fixed local-fanout topology for packet-loop route-planning benchmarks
///
/// setup registers one producer, a caller-selected number of local consumers,
/// one prebuilt RTP packet and warmed source facts
/// the measured method clears
/// and reuses the destination buffer across fixed turns so Callgrind sees the
/// production planner work instead of fixture allocation
pub struct FanoutBenchTopology {
    state: PacketLoopState,
    packet_sinks: PacketSinkRouteCache,
    metrics: Arc<RtcMetricsRecorder>,
    pending_packets: Vec<ForwardedPacket>,
    forwards: Vec<ForwardingDestination>,
}

impl FanoutBenchTopology {
    #[must_use]
    pub fn with_local_destinations(destination_count: usize) -> Self {
        let destination_count = destination_count.max(1);
        let producer_session = test_transport_session_key(1, 0, 1, UserId::Integer(1));
        let consumer_session = test_transport_session_key(1, 0, 2, UserId::Integer(2));
        let mut state = PacketLoopState::default();
        let mut scenario = MediaWorkerScenario::new(&mut state);
        let src_media = scenario.source(producer_session.clone(), Mid::from("cam-up"));
        for _ in 0..destination_count {
            scenario.destination(src_media, consumer_session.clone(), Mid::from("cam-down"));
        }
        let pending_packets = vec![sample_forwarded_packet(
            producer_session,
            "cam-up",
            b"payload",
        )];
        let mut topology = Self {
            state,
            packet_sinks: PacketSinkRouteCache::default(),
            metrics: RuntimeMetrics::default().register_rtc_worker(),
            pending_packets,
            forwards: Vec::with_capacity(destination_count),
        };
        topology.warm_route_facts();
        topology.plan_single_turn();
        topology.forwards.clear();
        topology
    }

    #[must_use]
    pub fn plan_route_turns(&mut self) -> usize {
        let mut planned_forwards = 0;
        for _ in 0..ROUTE_PLANNING_TURNS {
            planned_forwards += self.plan_packet_send();
        }
        planned_forwards
    }

    #[must_use]
    pub fn plan_packet_send(&mut self) -> usize {
        self.forwards.clear();
        self.plan_single_turn()
    }

    fn warm_route_facts(&mut self) {
        for packet in &mut self.pending_packets {
            let _ = packet.resolve_facts(&self.state);
        }
    }

    #[inline(never)]
    fn plan_single_turn(&mut self) -> usize {
        for packet in &mut self.pending_packets {
            let visits_origin = packet.visits_origin_sinks();
            let Some(facts) = packet.resolve_facts(&self.state) else {
                continue;
            };
            if let Some(decision) = plan_forwards(
                facts,
                visits_origin,
                &self.state.routes,
                &self.packet_sinks,
                &mut self.forwards,
            ) {
                self.metrics.record_rtc_route_control(match decision {
                    PacketGateDecision::Allowed => RtcRouteControlOutcome::LayerAllowed,
                    PacketGateDecision::Dropped => RtcRouteControlOutcome::LayerDropped,
                });
            }
        }
        self.forwards.len()
    }
}
