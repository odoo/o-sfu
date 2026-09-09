use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use str0m::{
    media::{MediaKind, Mid},
    rtp::Ssrc,
};

use super::super::{
    bootstrap,
    packet_loop::{
        forwarded_packet::ForwardedPacket,
        forwarding_destination::{ForwardingDestination, LocalRtcPacketDestination},
    },
    state::{
        PacketLoopState,
        bitrate::{BitrateRegistry, MediaBitrateCounter},
        route_control::PacketLayerGate,
        source_route::MediaRouteDestination,
    },
    test_support::{sample_forwarded_packet, test_transport_session_key},
};
use crate::{
    Bitrate,
    engine::{
        UserId,
        media_transport::{TransportMediaId, TransportSessionKey},
    },
};

const LOCAL_SEND_PACKETS: usize = 512;
const LOCAL_SEND_PAYLOAD: &[u8] = b"payload";

/// fixed successful local-send path with shared egress accounting
pub struct LocalSendBenchFixture {
    state: PacketLoopState,
    bitrate_registry: BitrateRegistry,
    egress_bitrate: Arc<MediaBitrateCounter>,
    session_keys: [TransportSessionKey; 1],
    observed_at: Instant,
    packet: ForwardedPacket,
    destination: LocalRtcPacketDestination,
    warmup_bytes: usize,
    sent_packets: usize,
    sent_bytes: u64,
}

impl LocalSendBenchFixture {
    /// Builds one routable local RTC destination.
    ///
    /// # Panics
    ///
    /// Panics when the RTC session or its warm-up send cannot be constructed.
    #[must_use]
    #[expect(
        clippy::expect_used,
        clippy::panic,
        reason = "benchmark setup must fail when the RTC send fixture cannot be built"
    )]
    pub fn successful() -> Self {
        let producer = test_transport_session_key(71, 0, 72, UserId::Integer(73));
        let consumer = test_transport_session_key(71, 0, 74, UserId::Integer(75));
        let mid = Mid::from("cam-down");
        let src_media = TransportMediaId::new(76);
        let dst_media = TransportMediaId::new(77);
        let mut state = PacketLoopState::default();
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &consumer,
            SocketAddr::from(([127, 0, 0, 1], 47_100)),
            Bitrate::from_mbps(10),
        )
        .expect("benchmark consumer session should enter RTC state");
        let session = state
            .users
            .get_mut(&consumer)
            .expect("benchmark consumer session should exist");
        let stream = session.consumer_streams.allocate(mid);
        let mut direct_api = session.rtc.direct_api();
        direct_api.declare_media(mid, MediaKind::Video);
        direct_api.declare_stream_tx(Ssrc::from(78), None, mid, None);

        let dst_idx = state.routes.add_consumer_route(
            src_media,
            MediaRouteDestination {
                dest_session: consumer.clone(),
                dest_transport_media_id: dst_media,
                dest_stream: stream,
                dest_mid: mid,
                dest_payload_type: None,
                repair_enabled: false,
                active: true,
                requires_decoder_refresh: false,
                delivery_generation: 0,
                packet_gate: PacketLayerGate::Open,
                pending_gate: None,
            },
        );
        let mut bitrate_registry = BitrateRegistry::default();
        let counter = Arc::clone(&session.egress_bitrate);
        bitrate_registry.register_session_egress(&consumer, Arc::clone(&counter));

        let packet = sample_forwarded_packet(producer, "cam-up", LOCAL_SEND_PAYLOAD);
        let observed_at = packet.received_at();
        let ForwardingDestination::LocalRtc(destination) =
            ForwardingDestination::from_local_route_destination(src_media, dst_idx)
        else {
            panic!("benchmark destination should be a local RTC route");
        };
        let Some(warmup_bytes) = destination.send(&mut state, &packet) else {
            panic!("benchmark local-send warm-up should succeed");
        };

        Self {
            state,
            bitrate_registry,
            egress_bitrate: counter,
            session_keys: [consumer],
            observed_at,
            packet,
            destination,
            warmup_bytes,
            sent_packets: 0,
            sent_bytes: 0,
        }
    }

    pub fn send_packets(&mut self) {
        for _ in 0..LOCAL_SEND_PACKETS {
            if let Some(payload_bytes) = self.destination.send(&mut self.state, &self.packet) {
                self.sent_packets = self.sent_packets.saturating_add(1);
                self.sent_bytes = self
                    .sent_bytes
                    .wrapping_add(u64::try_from(payload_bytes).unwrap_or(u64::MAX));
            }
        }
    }

    #[must_use]
    pub fn accounting_matches(self) -> bool {
        self.egress_bitrate
            .record(self.observed_at + Duration::from_millis(500), 0);
        self.egress_bitrate
            .record(self.observed_at + Duration::from_secs(1), 0);
        let measured_bytes = LOCAL_SEND_PACKETS.saturating_mul(LOCAL_SEND_PAYLOAD.len());
        let total_bytes = measured_bytes.saturating_add(self.warmup_bytes);
        self.warmup_bytes == LOCAL_SEND_PAYLOAD.len()
            && self.sent_packets == LOCAL_SEND_PACKETS
            && self.sent_bytes == u64::try_from(measured_bytes).unwrap_or(u64::MAX)
            && self.bitrate_registry.egress_bitrate_snapshot_at(
                &self.session_keys,
                self.observed_at + Duration::from_secs(1),
            ) == Bitrate::from_bps(
                u64::try_from(total_bytes)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(8),
            )
    }
}

#[test]
fn local_send_fixture_accounts_for_warmup_and_measured_packets() {
    let mut fixture = LocalSendBenchFixture::successful();
    fixture.send_packets();
    assert!(fixture.accounting_matches());
}
