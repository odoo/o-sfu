use std::{error::Error, net::SocketAddr, time::Instant};

use str0m::{Input, media::Mid, rtp::StreamTxQueueInfo};

use super::{LOCAL_SEND_PACKETS, LocalSendBenchFixture};
use crate::{
    CodecPreferences, MediaCodecFlags,
    engine::media_transport::rtc::{
        codec::RtpProfile, worker::session_drain_peer::connect_rtc_pair,
    },
};

#[test]
fn local_send_fixture_accounts_for_warmup_and_measured_packets() -> Result<(), Box<dyn Error>> {
    let mut fixture = LocalSendBenchFixture::successful();
    let profile = RtpProfile::compile(MediaCodecFlags::default(), CodecPreferences::default())?;
    let started_at = Instant::now();
    let mut peer = profile.session_config().build(started_at);
    let session = fixture
        .state
        .users
        .get_mut(&fixture.session_keys[0])
        .ok_or("benchmark consumer session should exist")?;
    // str0m only refreshes queue snapshots after DTLS supplies SRTP keys.
    let now = connect_rtc_pair(
        &mut session.rtc,
        &mut peer,
        SocketAddr::from(([127, 0, 0, 1], 47_100)),
        SocketAddr::from(([127, 0, 0, 1], 47_101)),
        started_at,
    )?;
    fixture.send_packets();
    let session = fixture
        .state
        .users
        .get_mut(&fixture.session_keys[0])
        .ok_or("benchmark consumer session should exist")?;
    session.rtc.handle_input(Input::Timeout(now))?;
    assert_eq!(
        session
            .rtc
            .direct_api()
            .stream_tx_by_mid(Mid::from("cam-down"), None)
            .and_then(|stream| stream.queue_info().map(StreamTxQueueInfo::packet_count)),
        Some(LOCAL_SEND_PACKETS),
    );
    assert!(fixture.accounting_matches());
    Ok(())
}
