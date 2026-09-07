use std::{
    net::IpAddr,
    num::{NonZeroU64, NonZeroUsize},
    thread,
    time::Duration,
};

use anyhow::{Result, anyhow, ensure};
use o_sfu_core::prelude::{
    Bitrate, RoomMediaLimits, RoomWorkerPolicy, RtcPortRange, RtcUdpIoBackend,
    VideoAdaptationTuning, VideoAdaptationTuningError, VideoBitrateLimits,
};

use super::{
    TransportConfig,
    env::{Env, EnvParse, EnvValue, positive},
};

impl EnvParse for RtcUdpIoBackend {
    fn parse(value: EnvValue) -> Result<Self> {
        let key = value.key;
        match value.raw.as_str() {
            "tokio" => Ok(Self::Tokio),
            "io_uring" => {
                ensure!(
                    cfg!(target_os = "linux"),
                    "{key}=io_uring is only supported on Linux"
                );
                Ok(Self::IoUring)
            }
            other => Err(anyhow!(
                "{key} must be one of tokio or io_uring, got {other}"
            )),
        }
    }
}

impl TransportConfig {
    pub(super) fn from_env(env: &Env<'_>) -> Result<Self> {
        if env.var::<String>("TRANSPORT_BACKEND").optional()?.is_some() {
            return Err(anyhow!(
                "TRANSPORT_BACKEND is no longer supported; o-sfu always boots the RTC transport"
            ));
        }
        let announced_ip = env
            .var::<IpAddr>("ANNOUNCED_IP")
            .alias("PUBLIC_IP")
            .required()?;
        let rtc_min_port = env.var("RTC_MIN_PORT").default(40_000)?;
        let max_bitrate_in_bps = env
            .var("MAX_BITRATE_IN")
            .check(positive)
            .default(8_000_000)?;
        let max_bitrate_out_bps = env
            .var("MAX_BITRATE_OUT")
            .check(positive)
            .default(10_000_000)?;
        let max_video_bitrate_bps = env
            .var("MAX_VIDEO_BITRATE")
            .check(positive)
            .default(VideoBitrateLimits::DEFAULT_MAX_VIDEO_BITRATE.as_bps())?;
        let rtc_max_port = env.var("RTC_MAX_PORT").default(49_999)?;
        let rtc_udp_io_backend = env
            .var("RTC_UDP_IO_BACKEND")
            .default(RtcUdpIoBackend::Tokio)?;
        let rtc_media_worker_count = env
            .var("RTC_MEDIA_WORKER_COUNT")
            .check(positive)
            .default(default_rtc_media_worker_count())?;
        let room_max_local_routers =
            NonZeroUsize::new(env.var("ROOM_MAX_LOCAL_ROUTERS").default(1)?)
                .ok_or_else(|| anyhow!("ROOM_MAX_LOCAL_ROUTERS must be greater than zero"))?;
        let packet_loop_delay_threshold = NonZeroU64::new(
            env.var("ROOM_SPILLOVER_PACKET_LOOP_DELAY_MS")
                .default(RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS)?,
        )
        .ok_or_else(|| anyhow!("ROOM_SPILLOVER_PACKET_LOOP_DELAY_MS must be greater than zero"))?;
        let room_media_limits = room_media_limits_from_env(env)?;
        let video_adaptation_tuning = video_adaptation_tuning_from_env(env)?;
        let rtc_port_range = RtcPortRange::new(rtc_min_port, rtc_max_port);
        ensure!(
            rtc_port_range.min() <= rtc_port_range.max(),
            "RTC_MAX_PORT must be greater than or equal to RTC_MIN_PORT"
        );
        ensure!(
            rtc_media_worker_count <= usize::from(rtc_port_range.port_count()),
            "RTC_MEDIA_WORKER_COUNT must be less than or equal to the available RTC port count"
        );
        ensure!(
            room_max_local_routers.get() <= rtc_media_worker_count,
            "ROOM_MAX_LOCAL_ROUTERS must be less than or equal to RTC_MEDIA_WORKER_COUNT"
        );
        ensure!(
            !announced_ip.is_unspecified(),
            "ANNOUNCED_IP must be a concrete advertised address"
        );
        ensure!(
            !announced_ip.is_multicast(),
            "ANNOUNCED_IP cannot be a multicast address"
        );
        let room_worker_policy =
            RoomWorkerPolicy::new(room_max_local_routers, packet_loop_delay_threshold);
        Ok(Self {
            announced_ip,
            max_bitrate_in: Bitrate::from_bps(max_bitrate_in_bps),
            max_bitrate_out: Bitrate::from_bps(max_bitrate_out_bps),
            video_bitrate_limits: VideoBitrateLimits::new(Bitrate::from_bps(max_video_bitrate_bps)),
            rtc_port_range,
            rtc_udp_io_backend,
            rtc_media_worker_count,
            room_worker_policy,
            room_media_limits,
            video_adaptation_tuning,
        })
    }
}

fn room_media_limits_from_env(env: &Env<'_>) -> Result<RoomMediaLimits> {
    let active_audio_speakers = env
        .var("ROOM_MAX_ACTIVE_AUDIO_SPEAKERS")
        .check(positive)
        .default(RoomMediaLimits::DEFAULT_MAX_ACTIVE_AUDIO_SPEAKERS)?;
    let video_downloads_per_receiver = env
        .var("ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER")
        .check(positive)
        .default(RoomMediaLimits::DEFAULT_MAX_VIDEO_DOWNLOADS_PER_RECEIVER)?;
    Ok(RoomMediaLimits::try_new(
        active_audio_speakers,
        video_downloads_per_receiver,
    )?)
}

fn video_adaptation_tuning_from_env(env: &Env<'_>) -> Result<VideoAdaptationTuning> {
    let multiparty_scalable_video_threshold = env
        .var("ROOM_MULTIPARTY_SCALABLE_VIDEO_THRESHOLD")
        .check(positive)
        .default(VideoAdaptationTuning::DEFAULT_MULTIPARTY_SCALABLE_VIDEO_THRESHOLD)?;
    let thumbnail_budget_divisor = env
        .var("ROOM_THUMBNAIL_BUDGET_DIVISOR")
        .check(positive)
        .default(VideoAdaptationTuning::DEFAULT_THUMBNAIL_BUDGET_DIVISOR)?;
    for (legacy, replacement) in [
        (
            "ROOM_DOWNSWITCH_PRESSURE_OBSERVATIONS",
            "ROOM_SOFT_PAUSE_DWELL_MS",
        ),
        ("ROOM_UPSWITCH_STABLE_OBSERVATIONS", "ROOM_UPGRADE_DWELL_MS"),
    ] {
        ensure!(
            env.var::<String>(legacy).optional()?.is_none(),
            "{legacy} is no longer supported, use {replacement}"
        );
    }
    let soft_pause_dwell = env
        .var("ROOM_SOFT_PAUSE_DWELL_MS")
        .check(positive)
        .optional()?
        .map_or(
            VideoAdaptationTuning::DEFAULT_SOFT_PAUSE_DWELL,
            Duration::from_millis,
        );
    let upgrade_dwell = env
        .var("ROOM_UPGRADE_DWELL_MS")
        .check(positive)
        .optional()?
        .map_or(
            VideoAdaptationTuning::DEFAULT_UPGRADE_DWELL,
            Duration::from_millis,
        );
    let receiver_budget_headroom_percent = env
        .var("ROOM_RECEIVER_BUDGET_HEADROOM_PERCENT")
        .default(VideoAdaptationTuning::DEFAULT_RECEIVER_BUDGET_HEADROOM_PERCENT)?;
    let audio_reserve_per_speaker_bps = env
        .var("ROOM_AUDIO_RESERVE_PER_SPEAKER_BPS")
        .default(VideoAdaptationTuning::DEFAULT_AUDIO_RESERVE_PER_SPEAKER.as_bps())?;
    VideoAdaptationTuning::try_new(
        multiparty_scalable_video_threshold,
        thumbnail_budget_divisor,
        soft_pause_dwell,
        upgrade_dwell,
        receiver_budget_headroom_percent,
        Bitrate::from_bps(audio_reserve_per_speaker_bps),
    )
    .map_err(video_adaptation_tuning_error)
}

pub fn default_rtc_media_worker_count() -> usize {
    thread::available_parallelism().map_or(1, NonZeroUsize::get)
}

fn video_adaptation_tuning_error(error: VideoAdaptationTuningError) -> anyhow::Error {
    match error {
        VideoAdaptationTuningError::MultipartyScalableVideoThresholdZero => {
            anyhow!("ROOM_MULTIPARTY_SCALABLE_VIDEO_THRESHOLD must be greater than zero")
        }
        VideoAdaptationTuningError::ThumbnailBudgetDivisorZero => {
            anyhow!("ROOM_THUMBNAIL_BUDGET_DIVISOR must be greater than zero")
        }
        VideoAdaptationTuningError::SoftPauseDwellZero => {
            anyhow!("ROOM_SOFT_PAUSE_DWELL_MS must be greater than zero")
        }
        VideoAdaptationTuningError::UpgradeDwellZero => {
            anyhow!("ROOM_UPGRADE_DWELL_MS must be greater than zero")
        }
        VideoAdaptationTuningError::SoftPauseDwellTooLong => {
            anyhow!(
                "ROOM_SOFT_PAUSE_DWELL_MS must not exceed {}",
                VideoAdaptationTuning::MAX_DWELL.as_millis()
            )
        }
        VideoAdaptationTuningError::UpgradeDwellTooLong => {
            anyhow!(
                "ROOM_UPGRADE_DWELL_MS must not exceed {}",
                VideoAdaptationTuning::MAX_DWELL.as_millis()
            )
        }
        VideoAdaptationTuningError::ReceiverBudgetHeadroomPercentTooHigh => {
            anyhow!("ROOM_RECEIVER_BUDGET_HEADROOM_PERCENT must not exceed 100")
        }
    }
}

#[cfg(test)]
#[path = "TESTS/transport.rs"]
mod tests;
