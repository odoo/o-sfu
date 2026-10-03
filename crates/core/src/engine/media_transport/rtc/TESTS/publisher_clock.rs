use std::{net::SocketAddr, num::NonZeroU32, time::Duration};

use o_sfu_router::rtp::StreamBinding;
use str0m::{
    media::{Frequency, MediaTime},
    rtp::rtcp::SenderInfo,
};

use super::*;
use crate::{
    Bitrate,
    engine::{
        UserId,
        media_transport::rtc::{
            bootstrap::test_support::ensure_session_rtc_state,
            test_support::test_transport_session_key,
        },
    },
};

struct Source {
    session_handle: SessionHandle,
    media: TransportMediaId,
    mid: Mid,
    ssrc: u32,
    rate: Frequency,
}

impl Source {
    fn new(
        state: &mut PacketLoopState,
        session: &TransportSessionKey,
        mid: &str,
        ssrc: u32,
        rate: Frequency,
    ) -> Self {
        ensure_session_rtc_state(
            &mut state.users,
            session,
            SocketAddr::from(([127, 0, 0, 1], 46_022)),
            Bitrate::from_mbps(10),
        )
        .expect("publisher RTC state should initialize");
        let mid = Mid::from(mid);
        let media = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session.clone(),
            mid,
        });
        let parameters =
            MediaStream::new(vec![], vec![], vec![StreamBinding::new().with_ssrc(ssrc)])
                .with_mid(mid.to_string());
        state.refresh_producer_ssrcs(session, mid, &parameters);
        Self {
            session_handle: state
                .users
                .handle_for_key(session)
                .expect("publisher session should have a handle"),
            media,
            mid,
            ssrc,
            rate,
        }
    }

    fn feedback(&self, received_at: Instant, timestamp: u64) -> SenderFeedback {
        SenderFeedback {
            mid: self.mid,
            rid: None,
            received_at,
            sender_info: SenderInfo {
                ssrc: self.ssrc.into(),
                ntp_time: SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000),
                rtp_time: MediaTime::new(timestamp, self.rate),
                sender_packet_count: 0,
                sender_octet_count: 0,
            },
        }
    }

    fn sample(
        &self,
        state: &mut PacketLoopState,
        timestamp: u32,
        arrived_at: Instant,
        was_repair: bool,
    ) -> Option<Instant> {
        let mut binding = ProducerStreamBinding {
            rid: None,
            primary: self.ssrc.into(),
            repair: None,
        };
        let admission = state
            .bind_producer_packet(
                self.session_handle,
                Some(self.media),
                Some(self.mid),
                &mut binding,
                ProducerPacketTime {
                    timestamp,
                    clock_rate: self.rate,
                    received_at: arrived_at,
                    was_repair,
                },
            )
            .expect("clock metadata must not drop an admitted publisher packet");
        assert_ne!(admission.update, ProducerSsrcUpdate::Rejected);
        admission.sampled_at
    }
}

#[test]
fn publisher_clock_preserves_sampling_with_400ms_asymmetric_arrival() {
    let mut state = PacketLoopState::default();
    let session = test_transport_session_key(1, 0, 2, UserId::Integer(3));
    let audio = Source::new(
        &mut state,
        &session,
        "audio",
        11,
        Frequency::FORTY_EIGHT_KHZ,
    );
    let video = Source::new(&mut state, &session, "video", 12, Frequency::NINETY_KHZ);
    let now = Instant::now();
    let at = |millis| now + Duration::from_millis(millis);
    // str0m uses SECONDS for an SR received before the first RTP packet.
    let mut first_report = audio.feedback(now, 1_000);
    first_report.sender_info.rtp_time = MediaTime::new(1_000, Frequency::SECONDS);
    state.record_producer_sender_feedback(&session, first_report, Arc::from("publisher"));
    state.record_producer_sender_feedback(
        &session,
        video.feedback(at(400), 2_000),
        Arc::from("publisher"),
    );
    assert_eq!(audio.sample(&mut state, 1_000, now, false), Some(now));
    assert_eq!(video.sample(&mut state, 2_000, at(400), false), Some(now));
    assert_eq!(audio.sample(&mut state, 1_960, at(20), false), Some(at(20)));
    for (ticks, age, accepted) in [
        (480_000, Duration::from_secs(10), true),
        (
            480_000,
            Duration::from_secs(10) + Duration::from_nanos(1),
            false,
        ),
    ] {
        assert_eq!(
            audio.sample(&mut state, 1_000 + ticks, now + age, true),
            accepted.then_some(now + Duration::from_secs(10))
        );
        assert_eq!(
            audio.sample(
                &mut state,
                1_000_u32.wrapping_sub(ticks),
                now.checked_sub(age).expect("test arrival fits"),
                true
            ),
            accepted.then_some(
                now.checked_sub(Duration::from_secs(10))
                    .expect("test sample fits")
            )
        );
    }
    assert_eq!(
        video.sample(&mut state, 3_800, at(420), false),
        Some(at(20))
    );
}

#[test]
fn publisher_clock_keeps_same_frame_fallback_when_first_sr_is_late() {
    let mut state = PacketLoopState::default();
    let session = test_transport_session_key(1, 0, 2, UserId::Integer(3));
    let audio = Source::new(
        &mut state,
        &session,
        "audio",
        11,
        Frequency::FORTY_EIGHT_KHZ,
    );
    let now = Instant::now();
    let at = |millis| now + Duration::from_millis(millis);
    assert_eq!(audio.sample(&mut state, 0, now, false), None);
    state.record_producer_sender_feedback(
        &session,
        audio.feedback(at(100), 4_800),
        Arc::from("publisher"),
    );
    assert_eq!(audio.sample(&mut state, 0, at(110), false), None);
    assert_eq!(audio.sample(&mut state, 960, at(120), false), Some(at(20)));
    assert_eq!(audio.sample(&mut state, 960, at(10), false), None);
    assert_eq!(audio.sample(&mut state, 0, at(130), true), Some(now));
    assert_eq!(audio.sample(&mut state, 960, at(140), false), Some(at(20)));
}

#[test]
fn publisher_clock_future_correction_preserves_the_group_and_ignores_repairs() {
    let mut state = PacketLoopState::default();
    let session = test_transport_session_key(1, 0, 2, UserId::Integer(3));
    let audio = Source::new(
        &mut state,
        &session,
        "audio",
        11,
        Frequency::FORTY_EIGHT_KHZ,
    );
    let video = Source::new(&mut state, &session, "video", 12, Frequency::NINETY_KHZ);
    let now = Instant::now();
    let at = |millis| now + Duration::from_millis(millis);
    state.record_producer_sender_feedback(&session, audio.feedback(now, 0), Arc::from("publisher"));
    state.record_producer_sender_feedback(&session, video.feedback(now, 0), Arc::from("publisher"));
    assert_eq!(audio.sample(&mut state, 9_600, at(100), true), None);
    assert_eq!(
        video.sample(&mut state, 18_000, at(300), false),
        Some(at(200))
    );
    assert_eq!(
        audio.sample(&mut state, 9_600, at(100), false),
        Some(at(100))
    );
    assert_eq!(
        video.sample(&mut state, 19_800, at(320), false),
        Some(at(120))
    );
}

#[test]
fn publisher_clock_groups_are_scoped_to_active_cnames_and_sessions() {
    let mut state = PacketLoopState::default();
    let session = test_transport_session_key(1, 0, 2, UserId::Integer(3));
    let other = test_transport_session_key(1, 0, 2, UserId::Integer(4));
    let first = Source::new(
        &mut state,
        &session,
        "first",
        11,
        Frequency::FORTY_EIGHT_KHZ,
    );
    let second = Source::new(
        &mut state,
        &session,
        "second",
        12,
        Frequency::FORTY_EIGHT_KHZ,
    );
    let third = Source::new(&mut state, &other, "third", 13, Frequency::FORTY_EIGHT_KHZ);
    let now = Instant::now();
    let at = |millis| now + Duration::from_millis(millis);
    state.record_producer_sender_feedback(&session, first.feedback(now, 0), Arc::from("same"));
    state.record_producer_sender_feedback(
        &session,
        second.feedback(at(400), 0),
        Arc::from("different"),
    );
    state.record_producer_sender_feedback(&other, third.feedback(at(800), 0), Arc::from("same"));
    assert_eq!(second.sample(&mut state, 0, at(400), false), Some(at(400)));
    assert_eq!(third.sample(&mut state, 0, at(800), false), Some(at(800)));
    state.remove_media_handle(first.media);
    let replacement = Source::new(
        &mut state,
        &session,
        "replacement",
        14,
        Frequency::FORTY_EIGHT_KHZ,
    );
    state.record_producer_sender_feedback(
        &session,
        replacement.feedback(at(1_000), 0),
        Arc::from("same"),
    );
    assert_eq!(
        replacement.sample(&mut state, 0, at(1_000), false),
        Some(at(1_000))
    );
    let mut changed = second.feedback(at(2_000), 0);
    changed.sender_info.ntp_time += Duration::from_secs(2);
    state.record_producer_sender_feedback(&session, changed, Arc::from("changed"));
    let fresh = Source::new(
        &mut state,
        &session,
        "fresh",
        15,
        Frequency::FORTY_EIGHT_KHZ,
    );
    state.record_producer_sender_feedback(
        &session,
        fresh.feedback(at(3_000), 0),
        Arc::from("different"),
    );
    assert_eq!(
        fresh.sample(&mut state, 0, at(3_000), false),
        Some(at(3_000))
    );
}

#[test]
fn publisher_clock_invalid_metadata_falls_back_without_rebasing_other_sources() {
    let mut state = PacketLoopState::default();
    let session = test_transport_session_key(1, 0, 2, UserId::Integer(3));
    let mut first = Source::new(
        &mut state,
        &session,
        "first",
        11,
        Frequency::FORTY_EIGHT_KHZ,
    );
    let second = Source::new(
        &mut state,
        &session,
        "second",
        12,
        Frequency::FORTY_EIGHT_KHZ,
    );
    let now = Instant::now();
    let at = |millis| now + Duration::from_millis(millis);
    let mut invalid = first.feedback(now, 0);
    invalid.sender_info.ntp_time = SystemTime::UNIX_EPOCH;
    state.record_producer_sender_feedback(&session, invalid, Arc::from("publisher"));
    assert_eq!(first.sample(&mut state, 0, now, false), None);
    state.record_producer_sender_feedback(&session, first.feedback(now, 0), Arc::from("publisher"));
    state.record_producer_sender_feedback(
        &session,
        second.feedback(now, 0),
        Arc::from("publisher"),
    );
    let mut wrong_primary = first.feedback(at(1), 0);
    wrong_primary.sender_info.ssrc = 99.into();
    wrong_primary.sender_info.ntp_time += Duration::from_secs(1);
    state.record_producer_sender_feedback(&session, wrong_primary, Arc::from("publisher"));
    assert_eq!(first.sample(&mut state, 960, at(20), false), Some(at(20)));
    let mut huge_future = first.feedback(at(1), 0);
    huge_future.sender_info.ntp_time += Duration::from_secs(100);
    state.record_producer_sender_feedback(&session, huge_future, Arc::from("publisher"));
    assert_eq!(second.sample(&mut state, 960, at(20), false), Some(at(20)));
    assert_eq!(second.sample(&mut state, 1_920, at(11_000), false), None);
    first.rate = Frequency::NINETY_KHZ;
    assert_eq!(first.sample(&mut state, 1_920, at(40), false), None);
    first.rate = Frequency::FORTY_EIGHT_KHZ;
    assert_eq!(
        state.bind_producer_stream(
            &ForwardedPacketSource::Relayed(session.clone()),
            first.media,
            ProducerStreamBinding {
                rid: None,
                primary: 21.into(),
                repair: None
            },
        ),
        ProducerSsrcUpdate::Replaced
    );
    state.record_producer_sender_feedback(&session, first.feedback(now, 0), Arc::from("publisher"));
    first.ssrc = 21;
    assert_eq!(first.sample(&mut state, 0, at(50), false), None);
    first.rate = Frequency::from_nonzero(NonZeroU32::MAX);
    state.record_producer_sender_feedback(
        &session,
        first.feedback(now, 1 << 32),
        Arc::from("publisher"),
    );
    assert_eq!(first.sample(&mut state, 1, now, false), Some(now));
    assert_eq!(first.sample(&mut state, 1 << 31, now, false), None);
    // A local result can fit even when the publisher SystemTime overflows.
    let mut low = 0;
    let mut high = u64::MAX;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(middle))
            .is_some()
        {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    first.rate = Frequency::FORTY_EIGHT_KHZ;
    let mut boundary = first.feedback(now, 0);
    boundary.sender_info.ntp_time = SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_secs(low))
        .expect("binary search retains a representable report time");
    state.record_producer_sender_feedback(&session, boundary, Arc::from("boundary"));
    assert_eq!(first.sample(&mut state, 0, now, false), Some(now));
    assert_eq!(first.sample(&mut state, 48_000, at(1_000), false), None);
    assert_eq!(
        first.sample(&mut state, 0_u32.wrapping_sub(48_000), now, true),
        Some(
            now.checked_sub(Duration::from_secs(1))
                .expect("test sample fits")
        )
    );
}
