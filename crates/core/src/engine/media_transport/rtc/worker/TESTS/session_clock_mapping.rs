use str0m::{
    Event, Input, Output,
    media::Frequency,
    rtp::{
        RawPacket, RtpHeader,
        rtcp::{Rtcp, SenderInfo},
    },
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
    write_packet(&mut fixture, 5_001, 1, initial_timestamp, false, None)?;
    fixture.now = start + gap;
    write_packet(&mut fixture, 5_002, 10, 42_000, false, None)?;
    let switched_timestamp = initial_timestamp.wrapping_add(ticks(gap)?.max(1));
    fixture.now += gap;
    let latest_at = fixture.now;
    let latest_timestamp = switched_timestamp.wrapping_add(2_970);
    write_packet(&mut fixture, 5_002, 14, 44_970, false, None)?;
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
        write_packet(&mut fixture, 5_002, sequence, timestamp, was_repair, None)?;
        let report = sender_report(&mut fixture)?;
        assert_clock(report, latest_timestamp, fixture.now - latest_at)?;
    }
    Ok(())
}

#[test]
fn authenticated_reports_preserve_prepared_audio_video_sampling_time() -> Result<(), &'static str> {
    let mut audio = LocalWriteDrainFixture::new(MediaKind::Audio, false)?;
    let mut video = LocalWriteDrainFixture::new(MediaKind::Video, false)?;
    let sampled_at = audio.now.max(video.now);
    let mut sampling_ntps = [None; 2];
    // Start at prepared consumer metadata, then decode authenticated outgoing SRs.
    // Publisher report admission and mapping are outside this fixture.
    for (sampling_ntp, (fixture, rate, anchor, delay)) in sampling_ntps.iter_mut().zip([
        (
            &mut audio,
            Frequency::FORTY_EIGHT_KHZ,
            48_000,
            Duration::ZERO,
        ),
        (
            &mut video,
            Frequency::NINETY_KHZ,
            90_000,
            Duration::from_millis(400),
        ),
    ]) {
        fixture.now = sampled_at + delay;
        write_packet(fixture, 5_001, 1, anchor, false, Some(sampled_at))?;
        fixture.now = sampled_at + Duration::from_secs(2);
        let report = sender_report(fixture)?;
        let rtp_time = u32::try_from(report.rtp_time.numer())
            .map_err(|_error| "wire sender-report timestamp should fit u32")?;
        let elapsed = Duration::from_nanos(
            u64::from(rtp_time.wrapping_sub(anchor)) * 1_000_000_000 / u64::from(rate.get()),
        );
        *sampling_ntp = Some(
            report
                .ntp_time
                .checked_sub(elapsed)
                .ok_or("wire report should recover its sampling time")?,
        );
    }
    let [Some(audio_ntp), Some(video_ntp)] = sampling_ntps else {
        return Err("both media streams should produce authenticated sender reports");
    };
    let skew = audio_ntp
        .max(video_ntp)
        .duration_since(audio_ntp.min(video_ntp))
        .map_err(|_error| "sampling-time difference should be nonnegative")?;
    // Each report rounds through microseconds and its media clock's tick interval.
    assert!(
        skew <= Duration::from_micros(50),
        "wire A/V sampling skew: {skew:?}"
    );
    Ok(())
}

fn write_packet(
    fixture: &mut LocalWriteDrainFixture,
    ssrc: u32,
    sequence: u16,
    timestamp: u32,
    was_repair: bool,
    sampled_at: Option<Instant>,
) -> Result<(), &'static str> {
    let payload = Arc::from(b"payload".as_slice());
    let session = fixture
        .state
        .users
        .get_mut(&fixture.consumer)
        .ok_or("consumer RTC should remain registered")?;
    let mid = Mid::from("retired-write");
    let codec = match session
        .rtc
        .media(mid)
        .ok_or("test media should exist")?
        .kind()
    {
        MediaKind::Audio => Codec::Opus,
        MediaKind::Video => Codec::Vp8,
    };
    let (payload_type, clock_rate) = session
        .rtc
        .codec_config()
        .find(|params| params.spec().codec == codec)
        .map(|params| (params.pt(), params.spec().clock_rate))
        .ok_or("media payload type should exist")?;
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
        mid,
        Some(payload_type),
        false,
    );
    let packet = LocalForwardedRtp::new(
        &header,
        u64::from(sequence).into(),
        fixture.now,
        sampled_at,
        clock_rate,
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

fn sender_report(fixture: &mut LocalWriteDrainFixture) -> Result<SenderInfo, &'static str> {
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
                        return Ok(report.sender_info);
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

fn assert_clock(report: SenderInfo, anchor: u32, elapsed: Duration) -> Result<(), &'static str> {
    let actual = u32::try_from(report.rtp_time.numer())
        .map_err(|_error| "wire sender-report timestamp should fit u32")?;
    let expected = anchor.wrapping_add(ticks(elapsed)?);
    // str0m rounds the wallclock offset through microseconds before RTP ticks.
    let error = i32::from_ne_bytes(actual.wrapping_sub(expected).to_ne_bytes());
    assert!(
        error.unsigned_abs() <= 1,
        "sender report {actual} should follow clock {expected}"
    );
    Ok(())
}
