use std::{net::SocketAddr, sync::Arc};

use super::super::{RtpProfile, state::slots::SessionStore};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags,
    engine::media_transport::{TransportAdapterError, TransportSessionKey},
};

pub fn ensure_session_rtc_state(
    users: &mut SessionStore,
    session_key: &TransportSessionKey,
    candidate_addr: SocketAddr,
    max_bitrate_out: Bitrate,
) -> Result<bool, TransportAdapterError> {
    let profile = RtpProfile::compile(MediaCodecFlags::default(), CodecPreferences::default())?;
    super::ensure_session_rtc_state(
        users,
        Arc::from("test-room"),
        session_key,
        candidate_addr,
        max_bitrate_out,
        &profile,
        None,
    )
}
