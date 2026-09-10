use std::sync::Arc;

use str0m::media::Mid;
use tokio::sync::mpsc;

use super::super::{
    packet_loop::{
        forwarded_packet::ForwardedPacket,
        forwarding_destination::ForwardingDestination,
        forwarding_planner::{PacketGateDecision, plan_forwards},
    },
    state::{
        PacketLoopState,
        relay_registry::{RelayEnqueueOutcome, RelayPacketMailbox, RelayTargetId},
        route_control::PacketLayerGate,
    },
    test_support::{
        MediaWorkerScenario, sample_forwarded_packet,
        sample_forwarded_packet_with_rid_and_audio_activity, test_transport_session_key,
    },
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

/// Relay planning with open, matching, mismatched and blocked target gates.
///
/// The packet and destination storage are warmed before measurement. Receivers
/// stay open so every retained target represents a usable relay mailbox.
pub struct RelayFanoutBenchFixture {
    topology: FanoutBenchTopology,
    receivers: Vec<mpsc::Receiver<ForwardedPacket>>,
}

impl RelayFanoutBenchFixture {
    #[must_use]
    pub fn mixed_gates() -> Self {
        let producer_session = test_transport_session_key(2, 0, 3, UserId::Integer(4));
        let mut state = PacketLoopState::default();
        let src_media = MediaWorkerScenario::new(&mut state)
            .source(producer_session.clone(), Mid::from("cam-up"));
        let gates = [
            PacketLayerGate::Open,
            PacketLayerGate::Rid("hi".into()),
            PacketLayerGate::Rid("lo".into()),
            PacketLayerGate::Block,
        ];
        let mut receivers = Vec::with_capacity(gates.len());
        for (index, gate) in (0..).zip(gates) {
            let target = RelayTargetId::new(index);
            let (sender, receiver) = mpsc::channel(1);
            state
                .routes
                .add_relay_target(src_media, target, RelayPacketMailbox::new(sender));
            state
                .routes
                .set_relay_target_active(src_media, target, true);
            state.routes.set_relay_pkt_gate(src_media, target, gate);
            receivers.push(receiver);
        }
        let mut topology = FanoutBenchTopology {
            state,
            packet_sinks: PacketSinkRouteCache::default(),
            metrics: RuntimeMetrics::default().register_rtc_worker(),
            pending_packets: vec![sample_forwarded_packet_with_rid_and_audio_activity(
                producer_session,
                "cam-up",
                Some("hi"),
                None,
                None,
                b"payload",
            )],
            forwards: Vec::with_capacity(receivers.len()),
        };
        topology.warm_route_facts();
        topology.plan_single_turn();
        topology.forwards.clear();
        Self {
            topology,
            receivers,
        }
    }

    #[must_use]
    pub fn plan_route_turns(&mut self) -> usize {
        self.topology.plan_route_turns()
    }

    /// Verifies the planner selects only the open and matching relay in order.
    ///
    /// # Panics
    ///
    /// Panics if planning or delivery no longer follows the fixture's target gates.
    #[expect(
        clippy::panic,
        reason = "fixture validation must reject a non-relay destination"
    )]
    pub fn assert_gate_selection(&mut self) {
        assert_eq!(self.topology.plan_packet_send(), 2);
        let [packet] = self.topology.pending_packets.as_slice() else {
            panic!("relay fixture should contain exactly one packet");
        };
        for (destination, expected_receiver) in self.topology.forwards.iter().zip([0, 1]) {
            let ForwardingDestination::Relay(destination) = destination else {
                panic!("relay fixture planned a non-relay destination");
            };
            assert!(
                destination
                    .send(&self.topology.state, packet)
                    .is_some_and(|report| report.outcome == RelayEnqueueOutcome::Enqueued)
            );
            for (index, receiver) in self.receivers.iter_mut().enumerate() {
                assert_eq!(receiver.try_recv().is_ok(), index == expected_receiver);
            }
        }
    }
}
