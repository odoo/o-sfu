//! receiver-side bandwidth-estimation target updates.
//!
//! Room policy owns the receiver send demand (eventual admitted video plus the
//! audio delivered to that receiver). The RTC worker only applies that demand to
//! the session-local str0m BWE controller after capping it to the configured
//! outgoing ceiling.

use str0m::bwe::Bitrate as Str0mBitrate;

use super::super::state::slots::SessionStore;
use crate::{
    Bitrate,
    engine::media_transport::{ReceiverBweTargetUpdate, TransportAdapterError, TransportResult},
};

pub(super) fn apply_receiver_bwe_target(
    sessions: &mut SessionStore,
    max_bitrate_out: Bitrate,
    update: &ReceiverBweTargetUpdate,
) -> TransportResult<()> {
    let target = update.target().min(max_bitrate_out);
    let Some(session_state) = sessions.get_mut(update.session_key()) else {
        return Err(TransportAdapterError::InvalidInput);
    };
    if session_state.receiver_bwe_target == Some(target) {
        return Ok(());
    }
    let previous = session_state.receiver_bwe_target;
    session_state.receiver_bwe_target = Some(target);
    // a first-ever zero target carries no send demand, so record it without
    // seeding the str0m BWE with a zero desired bitrate
    if target == Bitrate::zero() && previous.is_none() {
        return Ok(());
    }
    #[cfg(test)]
    {
        session_state.receiver_bwe_str0m_update_count += 1;
    }
    session_state
        .rtc
        .bwe()
        .set_desired_bitrate(Str0mBitrate::bps(target.as_bps()));
    Ok(())
}
