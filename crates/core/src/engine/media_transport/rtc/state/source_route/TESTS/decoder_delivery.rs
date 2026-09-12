//! Decoder snapshots used by route and packet-loop fixtures.

use super::{DecoderDelivery, PacketLayerGate};

impl DecoderDelivery {
    pub(in crate::engine::media_transport::rtc) const fn fixture(
        requires_refresh: bool,
        effective_gate: PacketLayerGate,
        pending_gate: Option<PacketLayerGate>,
    ) -> Self {
        Self {
            requires_refresh,
            effective_gate,
            pending_gate,
            generation: 0,
        }
    }
}
