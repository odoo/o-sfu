use super::{Duration, Instant, ProducerSourceState, RelaySourceRegistration, Rid, RouteSource};

impl RouteSource {
    pub(in crate::engine::media_transport::rtc::state::route_table) fn relay_target_count(
        &self,
    ) -> usize {
        self.relay
            .as_ref()
            .map_or(0, RelaySourceRegistration::target_count)
    }

    pub(in crate::engine::media_transport::rtc::state::route_table) fn active_relay_target_count(
        &self,
    ) -> usize {
        self.relay
            .as_ref()
            .map_or(0, RelaySourceRegistration::active_target_count)
    }
}

impl ProducerSourceState {
    pub(in crate::engine::media_transport::rtc::state::route_table) fn rid_is_ready(
        &self,
        rid: Rid,
        now: Instant,
        max_age: Duration,
    ) -> bool {
        self.rids
            .iter()
            .any(|liveness| liveness.rid == rid && liveness.is_ready(now, max_age))
    }
}
