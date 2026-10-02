use str0m::{
    Event, Input, Output,
    media::Frequency,
    rtp::{RawPacket, RtpHeader, rtcp::Rtcp},
};

use super::{LocalWriteDrainFixture, *};
use crate::engine::media_transport::rtc::consumer_egress::{
    LocalForwardedRtp, LocalPacketDestination,
};

#[test]
fn sender_reports_follow_switches_and_delayed_packets() -> Result<(), &'static str> {
    let gap = Duration::from_millis(33);
    let mut fixture = LocalWriteDrainFixture::new(MediaKind::Video, false)?;
    let start = fixture.now;
    let initial_timestamp = u32::MAX - 3_000;
    write_packet(&mut fixture, 5_001, 1, initial_timestamp, false)?;
    fixture.now = start + gap;
    write_packet(&mut fixture, 5_002, 10, 42_000, false)?;
    let switched_timestamp = initial_timestamp.wrapping_add(ticks(gap)?.max(1));
    fixture.now += gap;
    let latest_at = fixture.now;
    let latest_timestamp = switched_timestamp.wrapping_add(2_970);
    write_packet(&mut fixture, 5_002, 14, 44_970, false)?;
    fixture.now += Duration::from_secs(1);
    let report = sender_report(&mut fixture)?;
    assert_clock(report, latest_timestamp, fixture.now - latest_at)?;
    // Delayed packets retain their media timestamp without replacing the clock
    // reference with a later arrival, including another packet of the same frame.
    for (sequence, timestamp, was_repair) in [
        (15, 44_970, false),
        (13, 43_485, false),
        (12, 42_742, true),
        (11, 46_455, true),
    ] {
        fixture.now += Duration::from_secs(2);
        write_packet(&mut fixture, 5_002, sequence, timestamp, was_repair)?;
        let report = sender_report(&mut fixture)?;
        assert_clock(report, latest_timestamp, fixture.now - latest_at)?;
    }
    Ok(())
}

fn write_packet(
    fixture: &mut LocalWriteDrainFixture,
    ssrc: u32,
    sequence: u16,
    timestamp: u32,
    was_repair: bool,
) -> Result<(), &'static str> {
    let payload = Arc::from(b"payload".as_slice());
    let session = fixture
        .state
        .users
        .get_mut(&fixture.consumer)
        .ok_or("consumer RTC should remain registered")?;
    let payload_type = session
        .rtc
        .codec_config()
        .find(|params| params.spec().codec == Codec::Vp8)
        .map(PayloadParams::pt)
        .ok_or("VP8 payload type should exist")?;
    let header = RtpHeader {
        has_extension: false,
        marker: true,
        payload_type,
        sequence_number: sequence,
        timestamp,
        ssrc: ssrc.into(),
        header_len: 12,
        ..RtpHeader::default()
    };
    let destination = LocalPacketDestination::new(
        TransportMediaId::new(11),
        fixture.stream,
        0,
        Mid::from("retired-write"),
        Some(payload_type),
        false,
    );
    let packet = LocalForwardedRtp::new(
        &header,
        u64::from(sequence).into(),
        fixture.now,
        Frequency::NINETY_KHZ,
        &payload,
        was_repair,
    );
    assert_eq!(
        destination.send(
            &mut session.consumer_streams,
            &mut session.rtc,
            &packet,
            None
        ),
        Some(7)
    );
    Ok(())
}

fn sender_report(fixture: &mut LocalWriteDrainFixture) -> Result<u32, &'static str> {
    for _ in 0..5 {
        let server = &mut fixture
            .state
            .users
            .get_mut(&fixture.consumer)
            .ok_or("consumer RTC should remain registered")?
            .rtc;
        server
            .handle_input(Input::Timeout(fixture.now))
            .map_err(|_error| "sender timeout should apply")?;
        for datagram in take_rtcp(server, fixture.now)? {
            datagram.deliver(&mut fixture.peer, fixture.now)?;
        }
        loop {
            match fixture
                .peer
                .poll_output()
                .map_err(|_error| "peer output should poll")?
            {
                Output::Event(Event::RawPacket(packet)) => {
                    if let RawPacket::RtcpRx(Rtcp::SenderReport(report)) = packet.as_ref()
                        && report.sender_info.ssrc == fixture.primary
                    {
                        return u32::try_from(report.sender_info.rtp_time.numer())
                            .map_err(|_error| "wire sender-report timestamp should fit u32");
                    }
                }
                Output::Timeout(_) => break,
                Output::Transmit(_) | Output::Event(_) => {}
            }
        }
        fixture.now += Duration::from_secs(1);
    }
    Err("receiver should observe an authenticated sender report")
}

fn ticks(elapsed: Duration) -> Result<u32, &'static str> {
    u32::try_from(elapsed.as_micros() * u128::from(Frequency::NINETY_KHZ.get()) / 1_000_000)
        .map_err(|_error| "test clock interval should fit u32")
}

fn assert_clock(actual: u32, anchor: u32, elapsed: Duration) -> Result<(), &'static str> {
    let expected = anchor.wrapping_add(ticks(elapsed)?);
    // str0m rounds the wallclock offset through microseconds before RTP ticks.
    let error = i32::from_ne_bytes(actual.wrapping_sub(expected).to_ne_bytes());
    assert!(
        error.unsigned_abs() <= 1,
        "sender report {actual} should follow clock {expected}"
    );
    Ok(())
}
