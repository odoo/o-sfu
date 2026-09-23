//! media transport construction and startup validation

use std::sync::Arc;

use thiserror::Error;

use super::{
    MediaTransport, SourcePolicySignal,
    config::{MediaTransportConfig, MediaTransportDeps},
    rtc::{RtcWorker, RtpProfile},
};
use crate::{MediaWorkerId, RtcUdpIoBackend};

/// Per-worker [`TransportMediaId`](crate::engine::media_transport::TransportMediaId)
/// allocation stride.
///
/// IDs have no worker namespace. Each `RtcWorker` may allocate at most this
/// many IDs during its lifetime within one `MediaTransport`.
const MEDIA_ID_STRIDE: u64 = 1_000_000_000;

impl MediaTransport {
    /// builds the runtime media transport from owner configuration and process services
    ///
    /// validation completes before worker startup and every worker has a bound
    /// socket when this function returns
    ///
    /// # Errors
    ///
    /// returns [`MediaTransportBuildError`] when worker topology or the
    /// code-controlled RTP profile is invalid, the selected UDP backend is
    /// unavailable, the media-quality interval exceeds 24 hours or a worker
    /// cannot start
    pub fn build(
        mut config: MediaTransportConfig,
        deps: MediaTransportDeps,
    ) -> Result<Self, MediaTransportBuildError> {
        if config.rtc_udp_io_backend == RtcUdpIoBackend::IoUring && !cfg!(target_os = "linux") {
            return Err(MediaTransportBuildError::UnsupportedUdpIoBackend {
                backend: config.rtc_udp_io_backend,
            });
        }
        if config.worker_count == 0 {
            return Err(MediaTransportBuildError::InvalidWorkerCount);
        }
        // str0m adds the stats interval to Instant, so validate before starting workers.
        if config
            .media_quality_interval
            .is_some_and(|interval| interval > MediaTransportConfig::MAX_MEDIA_QUALITY_INTERVAL)
        {
            return Err(MediaTransportBuildError::InvalidMediaQualityInterval);
        }
        config.media_quality_interval = config
            .media_quality_interval
            .filter(|value| !value.is_zero());
        let worker_ranges = config
            .rtc_port_range
            .split_for_workers(config.worker_count)
            .ok_or(MediaTransportBuildError::InvalidPortSplit {
                worker_count: config.worker_count,
                port_count: config.rtc_port_range.port_count(),
            })?;
        let profile = Arc::new(
            RtpProfile::compile(config.codec_flags, config.codec_preferences)
                .map_err(|_error| MediaTransportBuildError::InvalidRtpProfile)?,
        );
        let source_policy_signal = SourcePolicySignal::default();
        let workers: Arc<[_]> = (0_u16..u16::MAX)
            .zip(worker_ranges)
            .map(|(worker_index, range)| {
                // `start` returns only after socket binding, so the completed
                // transport cannot publish a worker whose first command races I/O setup.
                RtcWorker::start(
                    &config,
                    Arc::clone(&profile),
                    range,
                    &deps,
                    source_policy_signal.clone(),
                    u64::from(worker_index) * MEDIA_ID_STRIDE,
                    MediaWorkerId::from_raw(usize::from(worker_index)),
                )
                .map_err(|_error| MediaTransportBuildError::WorkerStartup {
                    worker_index: usize::from(worker_index),
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            workers,
            profile,
            metrics: deps.metrics,
            #[cfg(test)]
            media_control_batches: Arc::default(),
            #[cfg(any(test, feature = "testing-transport"))]
            source_diagnostics_requests: Arc::default(),
            source_policy_signal,
        })
    }
}

/// invalid construction inputs for the media transport
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum MediaTransportBuildError {
    /// a transport cannot be built without at least one RTC worker
    #[error("media transport worker count must be at least one")]
    InvalidWorkerCount,
    /// the configured UDP range cannot provide one port to every worker
    #[error(
        "media transport cannot split {port_count} UDP ports across {worker_count} media workers"
    )]
    InvalidPortSplit {
        worker_count: usize,
        port_count: u16,
    },
    /// the sampling interval exceeds the operational deadline bound
    #[error("media-quality interval must not exceed 24 hours")]
    InvalidMediaQualityInterval,
    /// the selected UDP I/O backend is not available on this build target
    #[error("rtc UDP I/O backend `{backend}` is not supported on this target")]
    UnsupportedUdpIoBackend { backend: RtcUdpIoBackend },
    /// the code-controlled RTC profile cannot be projected for router policy
    #[error("media transport RTP profile is invalid")]
    InvalidRtpProfile,
    /// one worker could not create its runtime or bind its assigned UDP range
    #[error("media transport worker {worker_index} failed to start")]
    WorkerStartup { worker_index: usize },
}
