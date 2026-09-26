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
use crate::config::env::EnvKey;

impl EnvParse for RtcUdpIoBackend {
    fn parse(value: EnvValue) -> Result<Self> {
        let key = value.key;
        match value.raw.as_str() {
            "tokio" => Ok(Self::Tokio),
            "io_uring" => Ok(Self::IoUring),
            other => Err(anyhow!(
                "{key} must be one of tokio or io_uring, got {other}"
            )),
        }
    }
}

impl TransportConfig {
    pub(super) fn from_env(env: &Env<'_>) -> Result<Self> {
        let announced_ip = env
            .var::<IpAddr>("ANNOUNCED_IP")
            .alias("PUBLIC_IP")
            .check(advertised_ip)
            .required()?;
        let rtc_min_port = env.var("RTC_MIN_PORT").default(40_000)?;
        let max_bitrate_in = env
            .var("MAX_BITRATE_IN")
            .check(positive)
            .default(Bitrate::from_mbps(8))?;
        let max_bitrate_out = env
            .var("MAX_BITRATE_OUT")
            .check(positive)
            .default(Bitrate::from_mbps(10))?;
        let max_video_bitrate = env
            .var("MAX_VIDEO_BITRATE")
            .check(positive)
            .default(VideoBitrateLimits::DEFAULT_MAX_VIDEO_BITRATE)?;
        let rtc_max_port = env
            .var("RTC_MAX_PORT")
            .check(|key, value| {
                ensure!(
                    value >= rtc_min_port,
                    "{} must be greater than or equal to {}RTC_MIN_PORT",
                    key,
                    key.prefix
                );
                Ok(value)
            })
            .default(49_999)?;
        let rtc_port_range = RtcPortRange::new(rtc_min_port, rtc_max_port);
        let rtc_udp_io_backend = env
            .var("RTC_UDP_IO_BACKEND")
            .check(supported_udp_io_backend)
            .default(RtcUdpIoBackend::Tokio)?;
        let rtc_media_worker_count = env
            .var("RTC_MEDIA_WORKER_COUNT")
            .check(positive)
            .check(|key, value| {
                ensure!(
                    value <= usize::from(rtc_port_range.port_count()),
                    "{key} must be less than or equal to the available RTC port count"
                );
                Ok(value)
            })
            .default(default_rtc_media_worker_count())?;
        let room_max_local_routers = env
            .var::<NonZeroUsize>("ROOM_MAX_LOCAL_ROUTERS")
            .check(|key, value| {
                ensure!(
                    value.get() <= rtc_media_worker_count,
                    "{} must be less than or equal to {}RTC_MEDIA_WORKER_COUNT",
                    key,
                    key.prefix
                );
                Ok(value)
            })
            .default(NonZeroUsize::MIN)?;
        let default_packet_loop_delay_threshold =
            NonZeroU64::try_from(RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS)?;
        let packet_loop_delay_threshold = env
            .var("ROOM_SPILLOVER_PACKET_LOOP_DELAY_MS")
            .default(default_packet_loop_delay_threshold)?;
        let room_media_limits = room_media_limits_from_env(env)?;
        let video_adaptation_tuning = video_adaptation_tuning_from_env(env)?;
        let room_worker_policy =
            RoomWorkerPolicy::new(room_max_local_routers, packet_loop_delay_threshold);
        Ok(Self {
            announced_ip,
            max_bitrate_in,
            max_bitrate_out,
            video_bitrate_limits: VideoBitrateLimits::new(max_video_bitrate),
            rtc_port_range,
            rtc_udp_io_backend,
            rtc_media_worker_count,
            room_worker_policy,
            room_media_limits,
            video_adaptation_tuning,
        })
    }
}

fn advertised_ip(key: EnvKey, value: IpAddr) -> Result<IpAddr> {
    ensure!(
        !value.is_unspecified(),
        "{key} must be a concrete advertised address"
    );
    ensure!(!value.is_multicast(), "{key} cannot be a multicast address");
    Ok(value)
}

fn supported_udp_io_backend(key: EnvKey, value: RtcUdpIoBackend) -> Result<RtcUdpIoBackend> {
    ensure!(
        value != RtcUdpIoBackend::IoUring || cfg!(target_os = "linux"),
        "{key}=io_uring is only supported on Linux"
    );
    Ok(value)
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
    let audio_reserve_per_speaker = env
        .var("ROOM_AUDIO_RESERVE_PER_SPEAKER_BPS")
        .default(VideoAdaptationTuning::DEFAULT_AUDIO_RESERVE_PER_SPEAKER)?;
    VideoAdaptationTuning::try_new(
        multiparty_scalable_video_threshold,
        thumbnail_budget_divisor,
        soft_pause_dwell,
        upgrade_dwell,
        receiver_budget_headroom_percent,
        audio_reserve_per_speaker,
    )
    .map_err(video_adaptation_tuning_error)
}

pub fn default_rtc_media_worker_count() -> usize {
    thread::available_parallelism().map_or(1, NonZeroUsize::get)
}

fn video_adaptation_tuning_error(error: VideoAdaptationTuningError) -> anyhow::Error {
    match error {
        VideoAdaptationTuningError::MultipartyScalableVideoThresholdZero => {
            anyhow!("OSFU_ROOM_MULTIPARTY_SCALABLE_VIDEO_THRESHOLD must be greater than zero")
        }
        VideoAdaptationTuningError::ThumbnailBudgetDivisorZero => {
            anyhow!("OSFU_ROOM_THUMBNAIL_BUDGET_DIVISOR must be greater than zero")
        }
        VideoAdaptationTuningError::SoftPauseDwellZero => {
            anyhow!("OSFU_ROOM_SOFT_PAUSE_DWELL_MS must be greater than zero")
        }
        VideoAdaptationTuningError::UpgradeDwellZero => {
            anyhow!("OSFU_ROOM_UPGRADE_DWELL_MS must be greater than zero")
        }
        VideoAdaptationTuningError::SoftPauseDwellTooLong => {
            anyhow!(
                "OSFU_ROOM_SOFT_PAUSE_DWELL_MS must not exceed {}",
                VideoAdaptationTuning::MAX_DWELL.as_millis()
            )
        }
        VideoAdaptationTuningError::UpgradeDwellTooLong => {
            anyhow!(
                "OSFU_ROOM_UPGRADE_DWELL_MS must not exceed {}",
                VideoAdaptationTuning::MAX_DWELL.as_millis()
            )
        }
        VideoAdaptationTuningError::ReceiverBudgetHeadroomPercentTooHigh => {
            anyhow!("OSFU_ROOM_RECEIVER_BUDGET_HEADROOM_PERCENT must not exceed 100")
        }
    }
}

#[cfg(test)]
#[path = "TESTS/transport.rs"]
mod tests;
