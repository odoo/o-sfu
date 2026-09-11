use std::time::Instant;

use str0m::{Rtc, media::MediaKind};

use super::*;

#[test]
fn keep_existing_primary_replaces_the_complete_repair_pair() -> Result<(), &'static str> {
    let mut rtc = Rtc::builder().set_rtp_mode(true).build(Instant::now());
    let mid = Mid::from("cam-up");
    let primary = Ssrc::from(101);
    let repair = Ssrc::from(102);
    let next_repair = Ssrc::from(103);
    let bitrate = Bitrate::from_mbps(1);
    let mut api = rtc.direct_api();
    api.declare_media(mid, MediaKind::Video);

    apply_recv_stream(
        &mut api,
        mid,
        None,
        primary,
        RecvStreamRepair {
            ssrc: Some(repair),
            nack_enabled: true,
        },
        bitrate,
        StaleSsrcPolicy::ReplaceStale,
    );
    apply_recv_stream(
        &mut api,
        mid,
        None,
        Ssrc::from(201),
        RecvStreamRepair {
            ssrc: Some(next_repair),
            nack_enabled: true,
        },
        bitrate,
        StaleSsrcPolicy::KeepExisting,
    );

    let stream = api
        .stream_rx_by_mid(mid, None)
        .ok_or("receive stream should remain declared")?;
    assert_eq!(stream.ssrc(), primary);
    assert_eq!(stream.rtx(), Some(next_repair));

    apply_recv_stream(
        &mut api,
        mid,
        None,
        Ssrc::from(202),
        RecvStreamRepair {
            ssrc: None,
            nack_enabled: false,
        },
        bitrate,
        StaleSsrcPolicy::KeepExisting,
    );
    let stream = api
        .stream_rx_by_mid(mid, None)
        .ok_or("primary receive stream should remain after repair removal")?;
    assert_eq!(stream.ssrc(), primary);
    assert_eq!(stream.rtx(), None);
    Ok(())
}
