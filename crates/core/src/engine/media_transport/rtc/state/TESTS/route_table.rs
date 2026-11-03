use tracing::debug;

use super::{
    ActiveRelayTarget, Duration, Instant, PacketLayerGate, Rid, RouteSource, RouteTable,
    TransportAdapterError, TransportMediaId,
};

impl RouteTable {
    pub(in crate::engine::media_transport::rtc) fn set_source_active(
        &mut self,
        source_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.sources
            .get_mut(&source_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .set_source_active(active);
        if !active {
            self.keyframe_requests.forget_source(source_id);
            self.publisher_keyframes.cancel_source(source_id);
        }
        Ok(())
    }

    pub(in crate::engine::media_transport::rtc) fn producer_rid_is_ready(
        &self,
        source_id: TransportMediaId,
        rid: Rid,
        now: Instant,
        max_age: Duration,
    ) -> bool {
        self.sources
            .get(&source_id)
            .is_some_and(|source| source.producer.rid_is_ready(rid, now, max_age))
    }

    pub(in crate::engine::media_transport::rtc) fn set_local_pkt_gate(
        &mut self,
        source_id: TransportMediaId,
        packet_gate: Option<PacketLayerGate>,
    ) {
        if packet_gate.is_none() && !self.sources.contains_key(&source_id) {
            return;
        }
        self.source_mut(source_id).set_local_pkt_gate(packet_gate);
        self.prune_empty(source_id);
        debug!(
            ?source_id,
            effective_packet_gate = ?self.effective_packet_gate(source_id),
            "updated source packet gate"
        );
    }

    pub(in crate::engine::media_transport::rtc) fn relay_targets_for_source(
        &self,
        source_id: TransportMediaId,
    ) -> Option<&[ActiveRelayTarget]> {
        self.sources
            .get(&source_id)
            .and_then(RouteSource::active_relay_targets)
    }

    pub(in crate::engine::media_transport::rtc) fn relay_target_count(
        &self,
        source_id: TransportMediaId,
    ) -> usize {
        self.sources
            .get(&source_id)
            .map_or(0, RouteSource::relay_target_count)
    }

    pub(in crate::engine::media_transport::rtc) fn active_relay_target_count(
        &self,
        source_id: TransportMediaId,
    ) -> usize {
        self.sources
            .get(&source_id)
            .map_or(0, RouteSource::active_relay_target_count)
    }
}
