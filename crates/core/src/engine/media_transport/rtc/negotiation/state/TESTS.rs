//! Fixture operations for worker tests that do not perform a full exchange.
use super::SessionSdpNegotiationState;
impl SessionSdpNegotiationState {
    pub(in crate::engine::media_transport::rtc) fn mark_initial_offer_applied(&mut self) {
        self.initial_offer_applied = true;
    }
}
