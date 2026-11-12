//! Test hooks and fixtures for the media transport facade.

#[cfg(any(test, feature = "internal-benchmarks"))]
use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};
use std::{
    sync::{atomic::Ordering, mpsc},
    time::Instant,
};

use o_sfu_rfc::port as rfc_port;
#[cfg(any(test, feature = "internal-benchmarks"))]
use o_sfu_telemetry::metrics::RuntimeMetrics;
use str0m::media::Mid;
#[cfg(test)]
use {
    super::{MediaTransportBuildError, rtc::WorkerMediaControlBatch},
    crate::engine::sync::lock_unpoisoned,
    std::sync::Mutex,
};

pub use super::rtc::test_support::{
    DebugPacketGate, DebugProbeUnavailable, DebugRouteDestination, DebugRouteEntry,
};
use super::{
    MediaTransport, TransportMediaId, TransportQualitySample, TransportSessionHealth,
    TransportSessionKey, TransportSourceKey,
};
#[cfg(any(test, feature = "internal-benchmarks"))]
use super::{MediaTransportConfig, MediaTransportDeps};
use crate::RtcPortRange;
#[cfg(any(test, feature = "internal-benchmarks"))]
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, RtcUdpIoBackend, SessionBitrateLimits,
    VideoBitrateLimits, engine::packet_sink_registry::RoomPacketSinkRegistry,
};

#[cfg(test)]
pub(super) type MediaControlBatchLog = Arc<Mutex<Vec<(usize, &'static str, Vec<usize>)>>>;

#[derive(Debug, Clone, Copy)]
pub struct MediaTransportTestApi<'a> {
    transport: &'a MediaTransport,
}

impl MediaTransport {
    #[must_use]
    pub fn test_api(&self) -> MediaTransportTestApi<'_> {
        MediaTransportTestApi { transport: self }
    }

    #[cfg(test)]
    pub(super) fn observe_media_control_batch(
        &self,
        worker: usize,
        batch: &WorkerMediaControlBatch,
    ) {
        use WorkerMediaControlBatch::*;

        let (phase, indexes) = match batch {
            ReceiverBwe(items) => ("bwe", items.iter().map(|item| item.0).collect()),
            ProducerActivity(items) => ("producer", items.iter().map(|item| item.0).collect()),
            ConsumerGates { updates, .. } => ("gates", updates.iter().map(|item| item.0).collect()),
            ConsumerFollowUp(items) => ("consumer", items.iter().map(|item| item.0).collect()),
        };
        lock_unpoisoned(&self.media_control_batches).push((worker, phase, indexes));
    }
}

impl MediaTransportTestApi<'_> {
    /// Pauses the first RTC worker until the returned sender receives a value.
    pub async fn pause_first_worker(self) -> Option<mpsc::Sender<()>> {
        let (release, _probe) = self
            .transport
            .all_workers()
            .next()?
            .pause_for_test()
            .await?;
        Some(release)
    }

    /// Stops the selected worker and waits for its thread to exit.
    ///
    /// # Panics
    ///
    /// Panics when the fixture requests a nonexistent worker.
    #[cfg(test)]
    pub async fn stop_worker(self, worker_index: usize) {
        let worker = self
            .transport
            .worker_for_index(worker_index)
            .expect("test worker should exist");
        worker.cancel();
        worker.wait_for_shutdown().await;
    }

    /// Overrides packet-loop delay snapshots at the worker boundary.
    ///
    /// # Panics
    ///
    /// Panics when `delays_ms` does not contain one value per worker.
    pub fn set_packet_loop_delays_ms(self, delays_ms: Vec<Option<u64>>) {
        assert_eq!(
            delays_ms.len(),
            self.transport.all_workers().count(),
            "packet-loop delay overrides must cover every worker"
        );
        for (worker, delay_ms) in self.transport.all_workers().zip(delays_ms) {
            worker.debug_set_packet_loop_delay_ms(delay_ms);
        }
    }

    /// Returns the number of worker source-diagnostics commands issued.
    #[must_use]
    pub fn source_diagnostics_request_count(self) -> usize {
        self.transport
            .source_diagnostics_requests
            .load(Ordering::Relaxed)
    }

    /// Overrides a real RTC session health snapshot in test builds.
    ///
    /// This is a route-test hook for failure injection and is not a production
    /// control-plane operation.
    ///
    /// # Panics
    ///
    /// Panics when the required session worker is absent.
    #[allow(
        clippy::expect_used,
        reason = "required test fixture mutations must fail when their session worker is absent"
    )]
    pub fn set_session_transport_health(
        self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) {
        self.transport
            .worker_for_user(session_key)
            .expect("test session worker must exist")
            .debug_set_session_transport_health(&self.transport.metrics, session_key, health);
    }

    /// Overrides a real RTC session quality snapshot in test builds.
    ///
    /// # Panics
    ///
    /// Panics when the required session worker is absent.
    #[allow(
        clippy::expect_used,
        reason = "required test fixture mutations must fail when their session worker is absent"
    )]
    pub fn set_session_transport_quality(
        self,
        session_key: &TransportSessionKey,
        quality: TransportQualitySample,
    ) {
        self.transport
            .worker_for_user(session_key)
            .expect("test session worker must exist")
            .debug_set_session_transport_quality(session_key, quality);
    }

    /// Records incoming media on the source worker.
    ///
    /// # Errors
    ///
    /// Returns `DebugProbeUnavailable` when the source worker is absent or its
    /// probe cannot complete.
    pub async fn record_incoming_media(
        self,
        source: &TransportSourceKey,
        payload_bytes: usize,
        now: Instant,
    ) -> Result<(), DebugProbeUnavailable> {
        self.transport
            .worker_for_user(source.session_key())
            .ok_or(DebugProbeUnavailable)?
            .debug_record_incoming_media(source.transport_media_id(), payload_bytes, now)
            .await
    }

    /// Returns a source route entry when that route exists.
    ///
    /// # Errors
    ///
    /// Returns `DebugProbeUnavailable` when the source worker is absent or its
    /// probe cannot complete.
    pub async fn route_entry(
        self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Result<Option<DebugRouteEntry>, DebugProbeUnavailable> {
        self.transport
            .worker_for_user(source_session_key)
            .ok_or(DebugProbeUnavailable)?
            .debug_route_entry(source_session_key, source_mid)
            .await
    }

    /// Inspects a real RTC route by consumer mid in test builds.
    ///
    /// # Errors
    ///
    /// Returns `DebugProbeUnavailable` when the consumer worker is absent or
    /// its probe cannot complete.
    pub async fn route_entry_by_consumer_mid(
        self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Result<Option<DebugRouteEntry>, DebugProbeUnavailable> {
        self.transport
            .worker_for_user(consumer_session_key)
            .ok_or(DebugProbeUnavailable)?
            .debug_route_entry_by_consumer_mid(consumer_session_key, consumer_mid)
            .await
    }

    /// Searches worker routes for a media ID.
    ///
    /// # Errors
    ///
    /// Returns `DebugProbeUnavailable` when any worker probe cannot complete.
    pub async fn route_entry_by_media_id(
        self,
        source_transport_media_id: TransportMediaId,
    ) -> Result<Option<DebugRouteEntry>, DebugProbeUnavailable> {
        let mut observed_worker = false;
        for worker in self.transport.all_workers() {
            observed_worker = true;
            if let Some(entry) = worker
                .debug_route_entry_by_media_id(source_transport_media_id)
                .await?
            {
                return Ok(Some(entry));
            }
        }
        if observed_worker {
            Ok(None)
        } else {
            Err(DebugProbeUnavailable)
        }
    }

    #[cfg(test)]
    /// Returns a session's outbound SSRC pair when it exists.
    ///
    /// # Errors
    ///
    /// Returns `DebugProbeUnavailable` when the session worker is absent or
    /// its probe cannot complete.
    pub async fn session_stream_tx_pair(
        self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) -> Result<Option<(u32, Option<u32>)>, DebugProbeUnavailable> {
        self.transport
            .worker_for_user(session_key)
            .ok_or(DebugProbeUnavailable)?
            .debug_session_stream_tx_pair(session_key, mid)
            .await
    }

    #[cfg(test)]
    /// Counts the source's relay targets.
    ///
    /// # Errors
    ///
    /// Returns `DebugProbeUnavailable` when the source worker is absent or
    /// its probe cannot complete.
    pub async fn source_relay_target_count(
        self,
        source: &TransportSourceKey,
    ) -> Result<usize, DebugProbeUnavailable> {
        self.transport
            .worker_for_user(source.session_key())
            .ok_or(DebugProbeUnavailable)?
            .debug_relay_target_count(source.transport_media_id())
            .await
    }

    /// Returns a session's receiver BWE target when one exists.
    ///
    /// # Errors
    ///
    /// Returns `DebugProbeUnavailable` when the session worker is absent or
    /// its probe cannot complete.
    pub async fn session_receiver_bwe_target(
        self,
        session_key: &TransportSessionKey,
    ) -> Result<Option<crate::Bitrate>, DebugProbeUnavailable> {
        self.transport
            .worker_for_user(session_key)
            .ok_or(DebugProbeUnavailable)?
            .debug_session_receiver_bwe_target(session_key)
            .await
    }

    /// Injects one audio activity observation into every worker.
    ///
    /// # Errors
    ///
    /// Returns `DebugProbeUnavailable` when a worker probe cannot complete.
    pub async fn observe_audio_activity_with_level(
        self,
        transport_media_id: TransportMediaId,
        audio_level_dbov: i8,
        now: Instant,
    ) -> Result<(), DebugProbeUnavailable> {
        let mut observed_worker = false;
        for worker in self.transport.all_workers() {
            observed_worker = true;
            worker
                .debug_observe_audio_activity(
                    transport_media_id,
                    Some(true),
                    Some(audio_level_dbov),
                    now,
                )
                .await?;
        }
        if observed_worker {
            Ok(())
        } else {
            Err(DebugProbeUnavailable)
        }
    }
}

/// returns the RFC 6335 dynamic UDP port range for RTC tests and benchmarks
///
/// each RTC worker binds the first available port in its assigned subrange
///
/// # Panics
///
/// Panics if the RFC dynamic range constants stop forming a valid range.
#[allow(
    clippy::expect_used,
    reason = "the RFC dynamic range constants are a valid RTC port range"
)]
#[must_use]
pub fn test_rtc_port_range() -> RtcPortRange {
    RtcPortRange::try_new(rfc_port::DYNAMIC_RANGE_START, rfc_port::DYNAMIC_RANGE_END)
        .expect("RFC dynamic port range should be a valid RTC range")
}

#[cfg(test)]
pub(crate) fn test_media_transport(
    worker_count: usize,
    rtc_port_range: RtcPortRange,
) -> Result<MediaTransport, MediaTransportBuildError> {
    MediaTransport::build(
        test_media_transport_config(worker_count, rtc_port_range),
        test_media_transport_deps(),
    )
}

#[cfg(any(test, feature = "internal-benchmarks"))]
pub(crate) fn test_media_transport_config(
    worker_count: usize,
    rtc_port_range: RtcPortRange,
) -> MediaTransportConfig {
    MediaTransportConfig {
        worker_count,
        announced_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        bitrate_limits: SessionBitrateLimits::new(Bitrate::from_mbps(8), Bitrate::from_mbps(10)),
        video_bitrate_limits: VideoBitrateLimits::default(),
        rtc_port_range,
        rtc_udp_io_backend: RtcUdpIoBackend::Tokio,
        rtc_tcp_config: None,
        codec_flags: MediaCodecFlags::default(),
        codec_preferences: CodecPreferences::default(),
        media_quality_interval: None,
    }
}

#[cfg(any(test, feature = "internal-benchmarks"))]
pub(crate) fn test_media_transport_deps() -> MediaTransportDeps {
    MediaTransportDeps {
        packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
        metrics: Arc::new(RuntimeMetrics::default()),
    }
}

#[cfg(test)]
#[path = "TESTS/test_support.rs"]
mod tests;
