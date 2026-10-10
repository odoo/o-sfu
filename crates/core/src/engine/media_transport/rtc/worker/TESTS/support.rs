use std::time::Instant;

use str0m::media::Mid;
#[cfg(test)]
use {
    super::super::{
        RtpProfile, WorkerAssignment,
        test_support::{
            RememberRemoteAddrProbe, SessionStreamRxSsrcProbe, SessionStreamTxSsrcProbe,
        },
    },
    crate::{
        MediaWorkerId,
        engine::media_transport::{
            MediaTransportConfig, SourcePolicySignal,
            test_support::{
                test_media_transport_config, test_media_transport_deps, test_rtc_port_range,
            },
        },
    },
    std::{net::SocketAddr, sync::Arc},
};
#[cfg(any(test, feature = "testing-transport"))]
use {
    super::super::{control::WorkerCommandContext, state::PacketLoopState},
    std::sync::mpsc,
    tokio::{sync::oneshot, task::JoinHandle},
};

use super::{
    super::{
        state::TransportSessionHealth,
        test_support::{
            DebugProbe, DebugProbeUnavailable, DebugRouteEntry, ObserveAudioActivityProbe,
            ReceiverBweTargetProbe, RecordIncomingMediaProbe, RouteEntryByConsumerMidProbe,
            RouteEntryByMediaIdProbe, RouteEntryProbe,
        },
    },
    RtcWorker,
};
#[cfg(test)]
use crate::engine::media_transport::rtc::UfragWorkerMap;
#[cfg(any(test, feature = "testing-transport"))]
use crate::engine::media_transport::{TransportMediaId, TransportQualitySample};
use crate::{
    Bitrate,
    engine::{media_transport::TransportSessionKey, metrics},
};

impl RtcWorker {
    #[cfg(test)]
    pub(in crate::engine::media_transport::rtc) fn test_handle(&self) -> &super::RtcWorkerHandle {
        &self.handle
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::engine::media_transport) async fn pause_for_test(
        &self,
    ) -> Option<(mpsc::Sender<()>, JoinHandle<Option<()>>)> {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let debug_handle = self.handle.debug_handle.clone();
        let probe = tokio::spawn(async move {
            debug_handle
                .probe(move |_: &PacketLoopState, _: &WorkerCommandContext<'_>| {
                    let _ = entered_tx.send(());
                    let _result = release_rx.recv();
                })
                .await
                .ok()
        });
        entered_rx.await.ok()?;
        Some((release_tx, probe))
    }

    pub(in crate::engine::media_transport) fn debug_set_packet_loop_delay_ms(
        &self,
        delay_ms: Option<u64>,
    ) {
        self.handle.packet_loop_delay.set_for_test(delay_ms);
    }

    #[allow(
        clippy::expect_used,
        reason = "test fixture mutation must fail when its snapshot lock is poisoned"
    )]
    pub fn debug_set_session_transport_health(
        &self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) {
        let previous = self
            .handle
            .snapshot_state
            .lock()
            .expect("test snapshot state must be available")
            .set_transport_health(session_key, health);
        self.metrics.record_transport_health_transition(
            previous.map(metrics::transport_health_state),
            Some(metrics::transport_health_state(health)),
        );
    }

    #[allow(
        clippy::expect_used,
        reason = "test fixture mutation must fail when its snapshot lock is poisoned"
    )]
    pub fn debug_set_session_transport_quality(
        &self,
        session_key: &TransportSessionKey,
        quality: TransportQualitySample,
    ) {
        let mut snapshot_state = self
            .handle
            .snapshot_state
            .lock()
            .expect("test snapshot state must be available");
        snapshot_state.update_transport_quality(session_key, |sample| *sample = quality);
    }

    async fn probe_debug_worker<P>(&self, probe: P) -> Result<P::Output, DebugProbeUnavailable>
    where
        P: DebugProbe,
    {
        self.handle.debug_handle.probe(probe).await
    }

    #[cfg(test)]
    async fn read_debug_worker<F, Output>(&self, read: F) -> Result<Output, DebugProbeUnavailable>
    where
        F: FnOnce(&PacketLoopState, &WorkerCommandContext<'_>) -> Output + Send + 'static,
        Output: Send + 'static,
    {
        self.probe_debug_worker(read).await
    }

    #[cfg(test)]
    pub async fn debug_resolve_mid(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Result<Option<Mid>, DebugProbeUnavailable> {
        self.read_debug_worker(move |state, _context| state.resolve_mid(transport_media_id))
            .await
    }

    #[cfg(test)]
    pub async fn debug_remote_addr_owner(
        &self,
        source_addr: SocketAddr,
    ) -> Result<Option<TransportSessionKey>, DebugProbeUnavailable> {
        self.read_debug_worker(move |state, _context| {
            state
                .remote_addr_demux
                .session_key_for_remote_addr(source_addr)
                .cloned()
        })
        .await
    }

    #[cfg(test)]
    pub async fn debug_has_any_remote_addr_session(&self) -> Result<bool, DebugProbeUnavailable> {
        self.read_debug_worker(|state, _context| {
            !state.remote_addr_demux.is_empty() || !state.ufrag_registry.is_empty()
        })
        .await
    }

    #[cfg(test)]
    pub async fn debug_session_ufrag(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<Option<String>, DebugProbeUnavailable> {
        let owned_session_key = session_key.clone();
        self.read_debug_worker(move |state, _context| {
            state
                .users
                .get(&owned_session_key)
                .map(|session_state| session_state.local_ice_ufrag.clone())
        })
        .await
    }

    #[cfg(test)]
    pub async fn debug_ufrag_worker(
        &self,
        ufrag: String,
    ) -> Result<Option<MediaWorkerId>, DebugProbeUnavailable> {
        self.read_debug_worker(move |state, _context| state.ufrag_worker_map.get(&ufrag))
            .await
    }

    #[cfg(test)]
    pub async fn debug_remember_remote_addr(
        &self,
        source_addr: SocketAddr,
        session_key: &TransportSessionKey,
    ) -> Result<(), DebugProbeUnavailable> {
        self.probe_debug_worker(RememberRemoteAddrProbe {
            source_addr,
            session_key: session_key.clone(),
        })
        .await
    }

    #[cfg(test)]
    pub async fn debug_session_stream_rx_ssrc(
        &self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) -> Result<Option<u32>, DebugProbeUnavailable> {
        self.probe_debug_worker(SessionStreamRxSsrcProbe {
            session_key: session_key.clone(),
            mid,
        })
        .await
    }

    #[cfg(test)]
    pub async fn debug_session_stream_tx_pair(
        &self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) -> Result<Option<(u32, Option<u32>)>, DebugProbeUnavailable> {
        self.probe_debug_worker(SessionStreamTxSsrcProbe {
            session_key: session_key.clone(),
            mid,
        })
        .await
    }

    #[cfg(test)]
    pub async fn debug_session_max_bitrate_in(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<Option<Bitrate>, DebugProbeUnavailable> {
        let session_key = session_key.clone();
        self.read_debug_worker(move |state, _context| {
            state
                .users
                .get(&session_key)
                .and_then(|session_state| session_state.max_bitrate_in)
        })
        .await
    }

    #[cfg(test)]
    pub async fn debug_session_max_bitrate_out(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<Option<Bitrate>, DebugProbeUnavailable> {
        let session_key = session_key.clone();
        self.read_debug_worker(move |state, _context| {
            state
                .users
                .get(&session_key)
                .and_then(|session_state| session_state.max_bitrate_out)
        })
        .await
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn debug_session_receiver_bwe_target(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<Option<Bitrate>, DebugProbeUnavailable> {
        self.probe_debug_worker(ReceiverBweTargetProbe {
            session_key: session_key.clone(),
        })
        .await
    }

    #[cfg(test)]
    pub async fn debug_session_receiver_bwe_str0m_update_count(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<Option<u64>, DebugProbeUnavailable> {
        let session_key = session_key.clone();
        self.read_debug_worker(move |state, _context| {
            state
                .users
                .get(&session_key)
                .map(|session_state| session_state.receiver_bwe_str0m_update_count)
        })
        .await
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn debug_route_entry(
        &self,
        src_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Result<Option<DebugRouteEntry>, DebugProbeUnavailable> {
        self.probe_debug_worker(RouteEntryProbe {
            src_key: src_key.clone(),
            source_mid,
        })
        .await
    }

    pub async fn debug_route_entry_by_consumer_mid(
        &self,
        consumer_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Result<Option<DebugRouteEntry>, DebugProbeUnavailable> {
        self.probe_debug_worker(RouteEntryByConsumerMidProbe {
            consumer_key: consumer_key.clone(),
            consumer_mid,
        })
        .await
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn debug_route_entry_by_media_id(
        &self,
        src_media: TransportMediaId,
    ) -> Result<Option<DebugRouteEntry>, DebugProbeUnavailable> {
        self.probe_debug_worker(RouteEntryByMediaIdProbe { src_media })
            .await
    }

    #[cfg(any(test, feature = "testing-transport"))]
    /// Records synthetic media against the counter installed by publication.
    ///
    /// # Panics
    ///
    /// Panics if the publication counter is absent.
    ///
    /// # Errors
    ///
    /// Returns `DebugProbeUnavailable` if the worker cannot answer the probe.
    pub async fn debug_record_incoming_media(
        &self,
        transport_media_id: TransportMediaId,
        payload_bytes: usize,
        now: Instant,
    ) -> Result<(), DebugProbeUnavailable> {
        let recorded = self
            .probe_debug_worker(RecordIncomingMediaProbe {
                transport_media_id,
                payload_bytes,
                now,
            })
            .await?;
        assert!(recorded, "incoming media counter must exist");
        Ok(())
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn debug_observe_audio_activity(
        &self,
        transport_media_id: TransportMediaId,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) -> Result<(), DebugProbeUnavailable> {
        self.probe_debug_worker(ObserveAudioActivityProbe {
            transport_media_id,
            voice_activity,
            audio_level_dbov,
            now,
        })
        .await
    }

    #[cfg(test)]
    pub async fn debug_relay_target_count(
        &self,
        src_media: TransportMediaId,
    ) -> Result<usize, DebugProbeUnavailable> {
        self.read_debug_worker(move |state, _context| state.routes.relay_target_count(src_media))
            .await
    }

    #[cfg(test)]
    pub async fn debug_active_relay_target_count(
        &self,
        src_media: TransportMediaId,
    ) -> Result<usize, DebugProbeUnavailable> {
        self.read_debug_worker(move |state, _context| {
            state.routes.active_relay_target_count(src_media)
        })
        .await
    }
}

#[cfg(test)]
impl RtcWorker {
    #[must_use]
    pub(crate) fn for_test(config: MediaTransportConfig) -> Self {
        let profile = RtpProfile::compile(config.codec_flags, config.codec_preferences)
            .expect("test RTP profile should compile");
        Self::start(
            &config,
            Arc::new(profile),
            WorkerAssignment {
                rtc_port_range: config.rtc_port_range,
                media_id_base: 0,
                media_worker_id: MediaWorkerId::from_raw(0),
            },
            &test_media_transport_deps(),
            SourcePolicySignal::default(),
            UfragWorkerMap::default(),
        )
        .expect("test RTC worker should start")
    }
}

#[cfg(test)]
impl Default for RtcWorker {
    fn default() -> Self {
        Self::for_test(test_media_transport_config(1, test_rtc_port_range()))
    }
}

#[cfg(test)]
mod worker_exit_tests {
    use std::{panic::catch_unwind, slice, time::Duration};

    use tokio::{task::yield_now, time::timeout};

    use super::*;
    use crate::{
        RtcUdpIoBackend,
        engine::{
            UserId,
            media_transport::{
                TransportAdapterError,
                rtc::{
                    packet_loop::{ForwardingDestination, flush_packet_forwards},
                    test_support::{sample_already_relayed_packet, test_transport_session_key},
                },
            },
            metrics::{MetricName, test_support::RuntimeMetricsSnapshotLookup},
            packet_sink_registry::{PacketSink, PacketSinkKind, RegisteredPacketSink},
        },
    };

    struct PanickingSink;

    impl PacketSink for PanickingSink {
        fn record_packet(
            &self,
            _session_key: &TransportSessionKey,
            _transport_media_id: TransportMediaId,
            _received_at: Instant,
            _payload: &[u8],
        ) {
            panic!("test packet sink panic");
        }
    }

    async fn assert_terminal_after_sink_panic(backend: RtcUdpIoBackend) {
        let mut config = test_media_transport_config(1, test_rtc_port_range());
        config.rtc_udp_io_backend = backend;
        let failed_worker = RtcWorker::for_test(config);
        let other_worker = RtcWorker::default();
        let session_key = test_transport_session_key(1, 0, 1, UserId::Integer(1));
        assert_eq!(failed_worker.session_transport_health(&session_key), None);
        let sink = RegisteredPacketSink::new(Arc::new(PanickingSink), PacketSinkKind::Recording);
        let metrics = Arc::clone(&failed_worker.metrics);
        let packet_recorder = metrics.register_rtp_worker();
        let control_recorder = metrics.register_rtc_worker();
        let probe_session = session_key.clone();
        let probe_result = failed_worker
            .test_handle()
            .debug_handle
            .probe(move |_: &PacketLoopState, _: &WorkerCommandContext<'_>| {
                let src_media = TransportMediaId::new(1);
                let packet =
                    sample_already_relayed_packet(probe_session, src_media, "aud-up", b"payload");
                let forwards = [ForwardingDestination::from_packet_sink(src_media, sink)];
                flush_packet_forwards(
                    &mut PacketLoopState::default(),
                    &metrics,
                    &packet_recorder,
                    &control_recorder,
                    &packet,
                    &forwards,
                );
            })
            .await;
        assert_eq!(probe_result, Err(DebugProbeUnavailable));
        timeout(Duration::from_secs(1), async {
            while failed_worker.is_usable() {
                yield_now().await;
            }
        })
        .await
        .expect("panicked worker should become terminal");
        failed_worker.wait_for_shutdown().await;
        let snapshot_state = Arc::clone(&failed_worker.test_handle().snapshot_state);
        let poison_result = catch_unwind(move || {
            let _guard = snapshot_state
                .lock()
                .expect("snapshot lock should start unpoisoned");
            panic!("test snapshot poison");
        });
        assert!(poison_result.is_err());
        assert_eq!(
            failed_worker.session_transport_health(&session_key),
            Some(TransportSessionHealth::Disconnected)
        );
        assert_eq!(
            failed_worker
                .transport_health_snapshot(slice::from_ref(&session_key))
                .get(&session_key),
            Some(&TransportSessionHealth::Disconnected)
        );
        assert_eq!(
            failed_worker.close_session(&session_key).await,
            Err(TransportAdapterError::TransportUnavailable)
        );
        assert_eq!(
            failed_worker.active_speaker_source_snapshot().await,
            Err(TransportAdapterError::TransportUnavailable)
        );
        assert_eq!(
            failed_worker
                .metrics
                .snapshot()
                .counter_value(MetricName::RtcWorkerTerminalFailuresTotal, &[]),
            1
        );
        failed_worker.wait_for_shutdown().await;
        assert_eq!(
            failed_worker
                .metrics
                .snapshot()
                .counter_value(MetricName::RtcWorkerTerminalFailuresTotal, &[]),
            1
        );
        assert!(other_worker.is_usable());
        assert!(matches!(
            other_worker.active_speaker_source_snapshot().await,
            Ok(sources) if sources.is_empty()
        ));
        other_worker.wait_for_shutdown().await;
    }

    #[tokio::test]
    async fn tokio_worker_packet_sink_panic_is_terminal() {
        assert_terminal_after_sink_panic(RtcUdpIoBackend::Tokio).await;
        #[cfg(target_os = "linux")]
        assert_terminal_after_sink_panic(RtcUdpIoBackend::TokioBatch).await;
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn io_uring_worker_packet_sink_panic_is_terminal() {
        assert_terminal_after_sink_panic(RtcUdpIoBackend::IoUring).await;
    }
}
