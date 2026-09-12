use std::net::SocketAddr;

use str0m::{
    media::{MediaKind, Mid, Rid},
    rtp::Ssrc,
};

use super::super::{
    bootstrap,
    state::{
        PacketLoopState,
        media_registry::RegisteredMediaHandle,
        route_control::PacketLayerGate,
        slots::ConsumerStreamHandle,
        source_route::{DecoderDelivery, MediaRouteDestination},
    },
};
use crate::{
    Bitrate,
    engine::media_transport::{TransportMediaId, TransportSessionKey},
};

pub fn prepare_source_session(
    state: &mut PacketLoopState,
    src_key: &TransportSessionKey,
    src_mid: Mid,
    ssrc: u32,
) -> TransportMediaId {
    prepare_source_session_with_rid(state, src_key, src_mid, ssrc, None)
}

#[expect(
    clippy::panic,
    reason = "invalid session setup must fail route fixtures"
)]
pub fn prepare_source_session_with_rid(
    state: &mut PacketLoopState,
    src_key: &TransportSessionKey,
    src_mid: Mid,
    ssrc: u32,
    rid: Option<Rid>,
) -> TransportMediaId {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 47_000));
    assert!(
        bootstrap::test_support::ensure_session_rtc_state(
            &mut state.users,
            src_key,
            candidate_addr,
            Bitrate::from_mbps(10),
        )
        .is_ok()
    );
    let Some(session) = state.users.get_mut(src_key) else {
        panic!("source session should exist after RTC state bootstrap");
    };
    let mut direct_api = session.rtc.direct_api();
    direct_api.declare_media(src_mid, MediaKind::Video);
    direct_api.expect_stream_rx(Ssrc::from(ssrc), None, src_mid, rid);
    state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: src_key.clone(),
        mid: src_mid,
    })
}

pub struct MediaWorkerScenario<'a> {
    state: &'a mut PacketLoopState,
}

impl<'a> MediaWorkerScenario<'a> {
    pub fn new(state: &'a mut PacketLoopState) -> Self {
        Self { state }
    }

    pub fn source(&mut self, session_key: TransportSessionKey, mid: Mid) -> TransportMediaId {
        self.state
            .register_media_handle(RegisteredMediaHandle::Producer { session_key, mid })
    }

    pub fn destination(
        &mut self,
        src_media: TransportMediaId,
        session_key: TransportSessionKey,
        mid: Mid,
    ) -> TransportMediaId {
        self.destination_with_gate(src_media, session_key, mid, PacketLayerGate::Open)
    }

    pub fn destination_with_gate(
        &mut self,
        src_media: TransportMediaId,
        session_key: TransportSessionKey,
        mid: Mid,
        packet_gate: PacketLayerGate,
    ) -> TransportMediaId {
        self.install_destination(src_media, session_key, mid, packet_gate, None)
    }

    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub fn destination_with_pending_gate(
        &mut self,
        src_media: TransportMediaId,
        session_key: TransportSessionKey,
        mid: Mid,
        packet_gate: PacketLayerGate,
    ) -> TransportMediaId {
        self.install_destination(
            src_media,
            session_key,
            mid,
            PacketLayerGate::Open,
            Some(packet_gate),
        )
    }

    fn install_destination(
        &mut self,
        src_media: TransportMediaId,
        session_key: TransportSessionKey,
        mid: Mid,
        packet_gate: PacketLayerGate,
        pending_gate: Option<PacketLayerGate>,
    ) -> TransportMediaId {
        let transport_media_id =
            self.state
                .register_media_handle(RegisteredMediaHandle::Consumer {
                    session_key: session_key.clone(),
                    mid,
                    src_media,
                });
        let bind_session_key = session_key.clone();
        let dst_idx = self.state.routes.add_consumer_route(
            src_media,
            MediaRouteDestination {
                dest_session: session_key,
                dest_transport_media_id: transport_media_id,
                dest_stream: ConsumerStreamHandle::default(),
                dest_mid: mid,
                dest_payload_type: None,
                repair_enabled: false,
                active: true,
                delivery: DecoderDelivery::fixture(true, packet_gate, pending_gate),
            },
        );
        self.state.set_consumer_dst_idx(
            &bind_session_key,
            mid,
            transport_media_id,
            src_media,
            Some(dst_idx),
        );
        transport_media_id
    }
}
