use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use str0m::media::Mid;

use super::super::{
    packet_loop::{
        ForwardingDestination, PacketGateDecision, flush_packet_forwards, plan_forwards,
    },
    state::{PacketLoopState, media_registry::RegisteredMediaHandle},
    test_support::{sample_forwarded_packet, test_transport_session_key},
    worker::PacketLoopBuffers,
};
use crate::engine::{
    UserId,
    media_transport::{TransportMediaId, TransportSessionKey},
    metrics::{
        RtcMetricsRecorder, RtcRouteControlOutcome, RtpForwardDestinationKind, RtpMetricsRecorder,
        RuntimeMetrics,
    },
    packet_sink_registry::{PacketSink, PacketSinkRouteCache, RoomPacketSinkRegistry},
};

pub const PACKET_SINK_FANOUT_TURNS: usize = 512;

struct CountingPacketSink {
    packets: AtomicUsize,
}

impl CountingPacketSink {
    fn new() -> Self {
        Self {
            packets: AtomicUsize::new(0),
        }
    }

    fn packets(&self) -> usize {
        self.packets.load(Ordering::Relaxed)
    }

    fn reset(&self) {
        self.packets.store(0, Ordering::Relaxed);
    }
}

impl PacketSink for CountingPacketSink {
    fn record_packet(
        &self,
        _session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
        _received_at: Instant,
        _payload: &[u8],
    ) {
        self.packets.fetch_add(1, Ordering::Relaxed);
    }
}

/// fixed recording-sink fixture for packet-loop packet-sink fanout benchmarks
///
/// setup registers one producer and one active room sink
/// the measured method reuses one packet and one destination buffer while
/// exercising production route planning plus flush delivery into the sink
pub struct PacketSinkFanoutBenchFixture {
    state: PacketLoopState,
    packet_sinks: PacketSinkRouteCache,
    sink: Arc<CountingPacketSink>,
    metrics: RuntimeMetrics,
    egress_metrics: Arc<RtpMetricsRecorder>,
    route_metrics: Arc<RtcMetricsRecorder>,
    buffers: PacketLoopBuffers,
    forwards: Vec<ForwardingDestination>,
}

impl PacketSinkFanoutBenchFixture {
    #[must_use]
    pub fn recording_sink() -> Self {
        let source_session = test_transport_session_key(71, 0, 72, UserId::Integer(73));
        let mut state = PacketLoopState::default();
        state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: source_session.clone(),
            mid: Mid::from("cam-up"),
        });

        let packet_sinks = RoomPacketSinkRegistry::default();
        let sink = Arc::new(CountingPacketSink::new());
        packet_sinks.register_room(
            source_session.room_instance_id(),
            Arc::<CountingPacketSink>::clone(&sink),
            RtpForwardDestinationKind::Recording,
        );
        let mut packet_sink_cache = PacketSinkRouteCache::default();
        packet_sink_cache.refresh_from(&packet_sinks);

        let metrics = RuntimeMetrics::default();
        let egress_metrics = metrics.register_rtp_worker();
        let route_metrics = metrics.register_rtc_worker();
        let mut buffers = PacketLoopBuffers::new();
        buffers.pending_packets.push(sample_forwarded_packet(
            source_session,
            "cam-up",
            b"payload",
        ));

        let mut fixture = Self {
            state,
            packet_sinks: packet_sink_cache,
            sink,
            metrics,
            egress_metrics,
            route_metrics,
            buffers,
            forwards: Vec::with_capacity(64),
        };
        fixture.plan_and_flush_once();
        fixture.forwards.clear();
        fixture.sink.reset();
        fixture
    }

    #[must_use]
    pub fn route_sink_turns(&mut self) -> usize {
        for _ in 0..PACKET_SINK_FANOUT_TURNS {
            self.forwards.clear();
            self.plan_and_flush_once();
        }
        self.sink.packets()
    }

    fn plan_and_flush_once(&mut self) {
        for packet in &mut self.buffers.pending_packets {
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
                self.route_metrics.record_rtc_route_control(match decision {
                    PacketGateDecision::Allowed => RtcRouteControlOutcome::LayerAllowed,
                    PacketGateDecision::Dropped => RtcRouteControlOutcome::LayerDropped,
                });
            }
            flush_packet_forwards(
                &mut self.state,
                &self.metrics,
                &self.egress_metrics,
                &self.route_metrics,
                packet,
                &self.forwards,
            );
        }
    }
}
