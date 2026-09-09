use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use super::{
    bootstrap,
    packet_loop::{PacketRouteDatagram, route_pkt_to_session_at, routing_miss::DemuxRecoveryState},
    state::PacketLoopState,
};
use crate::{
    Bitrate,
    engine::{
        ConnectionId, MediaWorkerId, RoomInstanceId, UserId,
        media_transport::TransportSessionKey,
        metrics::{RtcMetricsRecorder, RuntimeMetrics},
    },
};

const MAX_PACKET_LEN: usize = 1500;
const MAX_REPEATS: u8 = 8;

pub fn route_packet_loop_ingress_demux(
    mode: u8,
    source_port: u16,
    candidate_port: u16,
    packet: &[u8],
    repeats: u8,
) {
    let source_addr = SocketAddr::from(([127, 0, 0, 1], source_port));
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], candidate_port));
    let mut fixture = IngressDemuxFuzzFixture::new(source_addr, candidate_addr);
    match mode % 4 {
        0 => fixture.route_repeated(packet, repeats),
        1 => {
            fixture.pin_live_route();
            fixture.route_repeated(packet, repeats);
        }
        2 => {
            fixture.pin_live_route();
            fixture.remove_session();
            fixture.route_repeated(packet, repeats);
        }
        _ => {
            fixture.route_once(packet);
            fixture.route_repeated(packet, repeats);
        }
    }
}

struct IngressDemuxFuzzFixture {
    state: PacketLoopState,
    demux: DemuxRecoveryState,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    session_key: TransportSessionKey,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    now: Instant,
}

impl IngressDemuxFuzzFixture {
    fn new(source_addr: SocketAddr, candidate_addr: SocketAddr) -> Self {
        let session_key = TransportSessionKey::new(
            RoomInstanceId::from_raw(61),
            MediaWorkerId::from_raw(0),
            ConnectionId::from_raw(62),
            UserId::Integer(63),
        );
        let mut state = PacketLoopState::default();
        let _ = bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &session_key,
            candidate_addr,
            Bitrate::from_mbps(10),
        );
        let metrics = RuntimeMetrics::default();
        Self {
            state,
            demux: DemuxRecoveryState::new(),
            rtc_metrics: metrics.register_rtc_worker(),
            session_key,
            source_addr,
            candidate_addr,
            now: Instant::now() + Duration::from_secs(1),
        }
    }

    fn pin_live_route(&mut self) {
        let _ = self
            .state
            .remote_addr_demux
            .remember_remote_addr(self.source_addr, &self.session_key);
    }

    fn remove_session(&mut self) {
        let _ = self.state.users.remove(&self.session_key);
    }

    fn route_repeated(&mut self, packet: &[u8], repeats: u8) {
        let repeats = repeats.clamp(1, MAX_REPEATS);
        for _ in 0..repeats {
            self.route_once(packet);
        }
    }

    fn route_once(&mut self, packet: &[u8]) {
        let packet = packet.get(..MAX_PACKET_LEN).unwrap_or(packet);
        route_pkt_to_session_at(
            &mut self.state,
            &mut self.demux,
            &self.rtc_metrics,
            PacketRouteDatagram::new(self.source_addr, self.candidate_addr, packet, self.now),
        );
    }
}
