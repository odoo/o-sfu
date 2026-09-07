#[cfg(any(test, feature = "testing-transport"))]
use {
    super::{
        MediaTransport, TransportMediaId, TransportQualitySample, TransportSessionHealth,
        TransportSessionKey, TransportSourceKey,
    },
    o_sfu_rfc::port as rfc_port,
    std::{
        sync::{atomic::Ordering, mpsc},
        time::Instant,
    },
    str0m::media::Mid,
};
#[cfg(test)]
use {
    super::{MediaTransportBuildError, TransportAdapterError, rtc::WorkerMediaControlBatch},
    crate::engine::sync::lock_unpoisoned,
    o_sfu_router::rtp::MediaStream as RouterRtpParameters,
    std::sync::Mutex,
};
#[cfg(any(test, feature = "internal-benchmarks"))]
use {
    super::{MediaTransportConfig, MediaTransportDeps},
    crate::{
        Bitrate, CodecPreferences, MediaCodecFlags, RtcUdpIoBackend, SessionBitrateLimits,
        VideoBitrateLimits,
        engine::{metrics::RuntimeMetrics, packet_sink_registry::RoomPacketSinkRegistry},
    },
    std::{
        net::{IpAddr, Ipv4Addr},
        sync::Arc,
    },
};

#[cfg(any(test, feature = "testing-transport"))]
pub use super::rtc::{ForwardedPacket, test_support::*};
#[cfg(any(test, feature = "internal-benchmarks", feature = "testing-transport"))]
use crate::RtcPortRange;

#[cfg(test)]
pub(super) type MediaControlBatchLog = Arc<Mutex<Vec<(usize, &'static str, Vec<usize>)>>>;

#[derive(Debug, Clone, Copy)]
#[cfg(any(test, feature = "testing-transport"))]
pub struct MediaTransportTestApi<'a> {
    transport: &'a MediaTransport,
}

#[cfg(any(test, feature = "testing-transport"))]
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

#[cfg(any(test, feature = "testing-transport"))]
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

    #[cfg(test)]
    pub(crate) async fn negotiated_producer_parameters(
        self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        self.transport
            .worker_for_user(session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .negotiated_producer_parameters(session_key, transport_media_id)
            .await
    }

    /// Overrides a real RTC session health snapshot in test builds.
    ///
    /// This is a route-test hook for failure injection and is not a production
    /// control-plane operation.
    pub fn set_session_transport_health(
        self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) {
        if let Some(worker) = self.transport.worker_for_user(session_key) {
            worker.debug_set_session_transport_health(session_key, health);
        }
    }

    pub fn set_session_transport_quality(
        self,
        session_key: &TransportSessionKey,
        quality: TransportQualitySample,
    ) {
        if let Some(worker) = self.transport.worker_for_user(session_key) {
            worker.debug_set_session_transport_quality(session_key, quality);
        }
    }

    pub async fn record_incoming_media(
        self,
        source: &TransportSourceKey,
        payload_bytes: usize,
        now: Instant,
    ) {
        if let Some(worker) = self.transport.worker_for_user(source.session_key()) {
            worker
                .debug_record_incoming_media(source.transport_media_id(), payload_bytes, now)
                .await;
        }
    }

    pub async fn route_entry(
        self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.transport
            .worker_for_user(source_session_key)?
            .debug_route_entry(source_session_key, source_mid)
            .await
    }

    /// Inspects a real RTC route by consumer mid in test builds.
    ///
    /// This is exposed for integration assertions that need to prove routing
    /// state without exposing worker internals to production callers.
    pub async fn route_entry_by_consumer_mid(
        self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.transport
            .worker_for_user(consumer_session_key)?
            .debug_route_entry_by_consumer_mid(consumer_session_key, consumer_mid)
            .await
    }

    pub async fn route_entry_by_media_id(
        self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<DebugRouteEntry> {
        for worker in self.transport.all_workers() {
            if let Some(entry) = worker
                .debug_route_entry_by_media_id(source_transport_media_id)
                .await
            {
                return Some(entry);
            }
        }
        None
    }

    #[cfg(test)]
    pub async fn session_stream_tx_pair(
        self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) -> Option<(u32, Option<u32>)> {
        self.transport
            .worker_for_user(session_key)?
            .debug_session_stream_tx_pair(session_key, mid)
            .await
    }

    #[cfg(test)]
    pub async fn source_relay_target_count(self, source: &TransportSourceKey) -> usize {
        let Some(worker) = self.transport.worker_for_user(source.session_key()) else {
            return 0;
        };
        worker
            .debug_relay_target_count(source.transport_media_id())
            .await
    }

    pub async fn session_receiver_bwe_target(
        self,
        session_key: &TransportSessionKey,
    ) -> Option<crate::Bitrate> {
        self.transport
            .worker_for_user(session_key)?
            .debug_session_receiver_bwe_target(session_key)
            .await
    }

    pub async fn observe_audio_activity_with_level(
        self,
        transport_media_id: TransportMediaId,
        audio_level_dbov: i8,
        now: Instant,
    ) {
        for worker in self.transport.all_workers() {
            worker
                .debug_observe_audio_activity(
                    transport_media_id,
                    Some(true),
                    Some(audio_level_dbov),
                    now,
                )
                .await;
        }
    }
}

/// returns the RFC 6335 dynamic UDP port range for RTC tests and benchmarks
///
/// each RTC worker binds the first available port in its assigned subrange
#[cfg(any(test, feature = "testing-transport"))]
#[must_use]
pub const fn test_rtc_port_range() -> RtcPortRange {
    RtcPortRange::new(rfc_port::DYNAMIC_RANGE_START, rfc_port::DYNAMIC_RANGE_END)
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
