#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test assertions use unwrap and expect for clear failure messages"
)]

use std::time::Duration;

use super::{Bitrate, VideoAdaptationTuning, effective_video_budget};

#[test]
fn effective_video_budget_no_reserve_returns_full_estimate() {
    // Default tuning reserves no headroom and no audio, so the whole estimate
    // is available to video.
    let tuning = VideoAdaptationTuning::default();
    assert_eq!(
        effective_video_budget(Bitrate::from_kbps(1000), tuning, Bitrate::zero()),
        Bitrate::from_kbps(1000),
    );
}

#[test]
fn effective_video_budget_applies_headroom_then_audio() {
    // 20% headroom on 1000 kbps -> 800 kbps, minus a 50 kbps audio reserve -> 750 kbps.
    let tuning = VideoAdaptationTuning::try_new(
        3,
        2,
        Duration::from_millis(750),
        Duration::from_millis(750),
        20,
        Bitrate::from_kbps(16),
    )
    .expect("valid tuning should build");
    assert_eq!(
        effective_video_budget(Bitrate::from_kbps(1000), tuning, Bitrate::from_kbps(50)),
        Bitrate::from_kbps(750),
    );
}

#[test]
fn effective_video_budget_saturates_to_zero_when_over_reserved() {
    // 50% of 100 kbps = 50 kbps, minus a 200 kbps audio reserve saturates to zero.
    let tuning = VideoAdaptationTuning::try_new(
        3,
        2,
        Duration::from_millis(750),
        Duration::from_millis(750),
        50,
        Bitrate::zero(),
    )
    .expect("valid tuning should build");
    assert_eq!(
        effective_video_budget(Bitrate::from_kbps(100), tuning, Bitrate::from_kbps(200)),
        Bitrate::zero(),
    );
}
