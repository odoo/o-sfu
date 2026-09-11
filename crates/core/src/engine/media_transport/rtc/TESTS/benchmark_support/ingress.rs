use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use str0m::ice::{StunMessage, TransId};

use super::super::{
    bootstrap,
    packet_loop::{
        PacketRouteDatagram, UdpIngressBenchHarness, route_pkt_to_session_at,
        routing_miss::DemuxRecoveryState,
    },
    state::PacketLoopState,
    test_support::{
        sample_rtp_packet_with_len, serialize_stun_message, test_transport_session_key,
    },
    worker::route_queued_ingress_datagrams_for_benchmark,
};
use crate::{
    Bitrate,
    engine::{
        UserId,
        metrics::{RtcMetricsRecorder, RuntimeMetrics},
    },
};

pub const INGRESS_DEMUX_ATTEMPTS: usize = 256;
pub const INGRESS_COMPLETED_BURST_DATAGRAMS: usize = 256;

const RTP_HEADER_LEN: usize = 12;
const LARGE_RTP_PACKET_LEN: usize = 1200;
const INGRESS_COMPLETED_QUEUE_BURST: usize = 32;

enum IngressRoutingMode {
    CachedAccepted,
    UnknownSourceMiss,
}

/// fixed UDP ingress fixture for packet-loop demux benchmarks
///
/// cached mode drives a pinned source tuple through `Rtc::accepts()` and
/// `Rtc::handle_input()`
/// unknown-source mode primes one recent miss before measurement so repeated
/// defensive traffic exercises the recent-miss cache
pub struct IngressRoutingBenchFixture {
    mode: IngressRoutingMode,
    state: PacketLoopState,
    demux: DemuxRecoveryState,
    _metrics: RuntimeMetrics,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: Vec<u8>,
    now: Instant,
}

pub struct IngressBurstBenchFixture {
    routing: IngressRoutingBenchFixture,
    ingress: UdpIngressBenchHarness,
}

impl IngressRoutingBenchFixture {
    #[must_use]
    pub fn cached_accepted_route() -> Self {
        let source_addr = SocketAddr::from(([127, 0, 0, 1], 46_001));
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 46_000));
        let session_key = test_transport_session_key(61, 0, 62, UserId::Integer(63));
        let mut state = PacketLoopState::default();
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();

        let bootstrap_succeeded = bootstrap::test_support::ensure_session_rtc_state(
            &mut state.users,
            &session_key,
            candidate_addr,
            Bitrate::from_mbps(10),
        )
        .is_ok();
        let packet = if bootstrap_succeeded {
            let _ = state
                .remote_addr_demux
                .remember_remote_addr(source_addr, &session_key);
            state
                .users
                .get_mut(&session_key)
                .map(|session_state| session_state.rtc.direct_api().local_ice_credentials())
                .and_then(|local_ice_credentials| {
                    let username = format!("{}:remote-ufrag", local_ice_credentials.ufrag);
                    serialize_stun_message(
                        &StunMessage::binding_request(&username, TransId::new(), true, 1, 1, false),
                        Some(local_ice_credentials.pass.as_bytes()),
                    )
                })
                .unwrap_or_else(|| sample_rtp_packet_with_len(1, 11, RTP_HEADER_LEN))
        } else {
            sample_rtp_packet_with_len(1, 11, RTP_HEADER_LEN)
        };

        Self {
            mode: IngressRoutingMode::CachedAccepted,
            state,
            demux: DemuxRecoveryState::new(),
            _metrics: metrics,
            rtc_metrics,
            source_addr,
            candidate_addr,
            packet,
            now: fixed_now(),
        }
    }

    #[must_use]
    pub fn repeated_unknown_source_miss() -> Self {
        Self::unknown_source_miss(sample_rtp_packet_with_len(1, 11, RTP_HEADER_LEN))
    }

    #[must_use]
    pub fn repeated_large_unknown_source_miss() -> Self {
        Self::unknown_source_miss(sample_rtp_packet_with_len(1, 11, LARGE_RTP_PACKET_LEN))
    }

    #[must_use]
    fn unknown_source_miss(packet: Vec<u8>) -> Self {
        let source_addr = SocketAddr::from(([127, 0, 0, 1], 46_011));
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 46_010));
        let state = PacketLoopState::default();
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();
        let mut fixture = Self {
            mode: IngressRoutingMode::UnknownSourceMiss,
            state,
            demux: DemuxRecoveryState::new(),
            _metrics: metrics,
            rtc_metrics,
            source_addr,
            candidate_addr,
            packet,
            now: fixed_now(),
        };
        fixture.route_once();
        fixture
    }

    #[must_use]
    pub fn route_datagrams(&mut self) -> usize {
        for _ in 0..INGRESS_DEMUX_ATTEMPTS {
            self.route_once();
        }
        match self.mode {
            IngressRoutingMode::CachedAccepted => usize::from(self.state.has_dirty_sessions()),
            IngressRoutingMode::UnknownSourceMiss => INGRESS_DEMUX_ATTEMPTS,
        }
    }

    fn route_once(&mut self) {
        route_pkt_to_session_at(
            &mut self.state,
            &mut self.demux,
            &self.rtc_metrics,
            PacketRouteDatagram::new(
                self.source_addr,
                self.candidate_addr,
                &self.packet,
                self.now,
            ),
        );
    }
}

impl IngressBurstBenchFixture {
    #[must_use]
    pub fn cached_accepted_route() -> Self {
        Self::from_routing_fixture(IngressRoutingBenchFixture::cached_accepted_route())
    }

    #[must_use]
    pub fn repeated_large_unknown_source_miss() -> Self {
        Self::from_routing_fixture(IngressRoutingBenchFixture::repeated_large_unknown_source_miss())
    }

    #[must_use]
    pub fn route_completed_bursts(&mut self) -> usize {
        let mut routed = 0;
        for _ in 0..(INGRESS_COMPLETED_BURST_DATAGRAMS / INGRESS_COMPLETED_QUEUE_BURST) {
            let enqueued = self.enqueue_burst();
            routed += route_queued_ingress_datagrams_for_benchmark(
                &mut self.routing.state,
                &mut self.routing.demux,
                &self.routing.rtc_metrics,
                self.ingress.ingress_mut(),
                enqueued,
            );
        }
        routed + usize::from(self.routing.state.has_dirty_sessions())
    }

    fn from_routing_fixture(fixture: IngressRoutingBenchFixture) -> Self {
        let candidate_addr = fixture.candidate_addr;
        Self {
            routing: fixture,
            ingress: UdpIngressBenchHarness::new(candidate_addr),
        }
    }

    fn enqueue_burst(&mut self) -> usize {
        let mut enqueued = 0;
        for _ in 0..INGRESS_COMPLETED_QUEUE_BURST {
            if self.ingress.enqueue_completed_datagram(
                self.routing.source_addr,
                self.routing.candidate_addr,
                self.routing.now,
                self.routing.packet.as_slice(),
            ) {
                enqueued += 1;
            }
        }
        enqueued
    }
}

fn fixed_now() -> Instant {
    Instant::now() + Duration::from_secs(1)
}
