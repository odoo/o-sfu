#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test assertions use unwrap and expect for clear failure messages"
)]

use std::time::Duration;

use super::{Bitrate, RtcPortRange, VideoAdaptationTuning, VideoAdaptationTuningError};

#[test]
fn video_adaptation_tuning_accepts_valid_knobs() {
    let tuning = VideoAdaptationTuning::try_new(
        4,
        3,
        Duration::from_millis(250),
        Duration::from_millis(1250),
        10,
        Bitrate::from_kbps(32),
    )
    .expect("valid tuning should build");
    assert_eq!(tuning.multiparty_scalable_video_threshold, 4);
    assert_eq!(tuning.thumbnail_budget_divisor, 3);
    assert_eq!(tuning.soft_pause_dwell, Duration::from_millis(250));
    assert_eq!(tuning.upgrade_dwell, Duration::from_millis(1250));
    assert_eq!(tuning.receiver_budget_headroom_percent, 10);
    assert_eq!(tuning.audio_reserve_per_speaker, Bitrate::from_kbps(32));
}

#[test]
fn video_adaptation_tuning_rejects_invalid_knobs() {
    let cases = [
        (
            VideoAdaptationTuning::try_new(
                0,
                2,
                Duration::from_millis(750),
                Duration::from_millis(750),
                0,
                Bitrate::zero(),
            ),
            VideoAdaptationTuningError::MultipartyScalableVideoThresholdZero,
        ),
        (
            VideoAdaptationTuning::try_new(
                3,
                0,
                Duration::from_millis(750),
                Duration::from_millis(750),
                0,
                Bitrate::zero(),
            ),
            VideoAdaptationTuningError::ThumbnailBudgetDivisorZero,
        ),
        (
            VideoAdaptationTuning::try_new(
                3,
                2,
                Duration::ZERO,
                Duration::from_millis(750),
                0,
                Bitrate::zero(),
            ),
            VideoAdaptationTuningError::SoftPauseDwellZero,
        ),
        (
            VideoAdaptationTuning::try_new(
                3,
                2,
                Duration::from_millis(750),
                Duration::ZERO,
                0,
                Bitrate::zero(),
            ),
            VideoAdaptationTuningError::UpgradeDwellZero,
        ),
        (
            VideoAdaptationTuning::try_new(
                3,
                2,
                Duration::from_millis(750),
                Duration::from_millis(750),
                101,
                Bitrate::zero(),
            ),
            VideoAdaptationTuningError::ReceiverBudgetHeadroomPercentTooHigh,
        ),
    ];
    for (result, expected) in cases {
        assert_eq!(result.err(), Some(expected));
    }
}

#[test]
fn video_adaptation_tuning_rejects_unrepresentable_deadlines() {
    for invalid in [
        Duration::MAX,
        VideoAdaptationTuning::MAX_DWELL + Duration::from_nanos(1),
    ] {
        assert_eq!(
            VideoAdaptationTuning::try_new(
                3,
                2,
                invalid,
                Duration::from_millis(750),
                0,
                Bitrate::zero()
            ),
            Err(VideoAdaptationTuningError::SoftPauseDwellTooLong)
        );
        assert_eq!(
            VideoAdaptationTuning::try_new(
                3,
                2,
                Duration::from_millis(750),
                invalid,
                0,
                Bitrate::zero()
            ),
            Err(VideoAdaptationTuningError::UpgradeDwellTooLong)
        );
    }
    assert!(
        VideoAdaptationTuning::try_new(
            3,
            2,
            VideoAdaptationTuning::MAX_DWELL,
            VideoAdaptationTuning::MAX_DWELL,
            0,
            Bitrate::zero()
        )
        .is_ok()
    );
}

#[test]
fn rtc_port_range_splits_ports_across_workers() {
    // zero workers → None
    assert_eq!(RtcPortRange::new(40_000, 40_000).split_for_workers(0), None,);
    // more workers than ports → None
    assert_eq!(RtcPortRange::new(40_000, 40_000).split_for_workers(2), None,);
    // single worker → full range
    assert_eq!(
        RtcPortRange::new(40_000, 40_003).split_for_workers(1),
        Some(vec![RtcPortRange::new(40_000, 40_003)]),
    );
    // workers == ports → one port each
    assert_eq!(
        RtcPortRange::new(40_000, 40_002).split_for_workers(3),
        Some(vec![
            RtcPortRange::new(40_000, 40_000),
            RtcPortRange::new(40_001, 40_001),
            RtcPortRange::new(40_002, 40_002),
        ]),
    );
    // uneven split → earlier workers get the extras
    assert_eq!(
        RtcPortRange::new(40_000, 40_004).split_for_workers(3),
        Some(vec![
            RtcPortRange::new(40_000, 40_001),
            RtcPortRange::new(40_002, 40_003),
            RtcPortRange::new(40_004, 40_004),
        ]),
    );
}
