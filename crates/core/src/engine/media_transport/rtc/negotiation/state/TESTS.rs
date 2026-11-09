//! Fixture operations for worker tests that do not perform a full exchange.
use str0m::change::{SdpOffer, SdpPendingOffer};

use super::{OfferPhase, PendingOffer, SessionSdpNegotiationState};
impl SessionSdpNegotiationState {
    pub(in crate::engine::media_transport::rtc) fn mark_initial_offer_applied(&mut self) {
        self.initial_offer_applied = true;
    }
    pub(in crate::engine::media_transport::rtc) fn set_delivered_offer(
        &mut self,
        offer: &SdpOffer,
        token: SdpPendingOffer,
    ) {
        self.phase = OfferPhase::Delivered(PendingOffer::new(offer, token, Vec::new()));
    }
}
