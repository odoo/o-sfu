use std::{sync::mpsc as std_mpsc, time::Instant};

use o_sfu_telemetry::metrics::RuntimeMetrics;
use str0m::media::Mid;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
#[cfg(test)]
use {
    super::super::{
        RtpProfile, WorkerAssignment,
        state::demux::RemoteTransportAddr,
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
    std::{net::SocketAddr, ops::Deref, sync::Arc},
};

use super::{
    super::{
        commands::RtcWorkerCommand,
        control::WorkerCommandContext,
        state::PacketLoopState,
        test_support::{
            DebugProbe, DebugProbeRequest, DebugProbeUnavailable, DebugRouteEntry,
            ObserveAudioActivityProbe, ReceiverBweTargetProbe, RecordIncomingMediaProbe,
            RouteEntryByConsumerMidProbe, RouteEntryByMediaIdProbe, RouteEntryProbe,
        },
    },
    RtcWorker,
};
#[cfg(test)]
use crate::engine::media_transport::rtc::UfragWorkerMap;
use crate::{
    Bitrate,
    engine::{
        media_transport::{
            TransportMediaId, TransportQualitySample, TransportSessionHealth, TransportSessionKey,
        },
        observability,
    },
};

/// Runs `probe` on the worker behind `command_tx` after every command queued before it.
async fn send_debug_probe<P>(
    command_tx: &mpsc::Sender<RtcWorkerCommand>,
    probe: P,
) -> Result<P::Output, DebugProbeUnavailable>
where
    P: DebugProbe,
{
    let (response_tx, response_rx) = oneshot::channel();
    command_tx
        .send(RtcWorkerCommand::DebugProbe(DebugProbeRequest::new(
            probe,
            response_tx,
        )))
        .await
        .map_err(|_send_error| DebugProbeUnavailable)?;
    response_rx
        .await
        .map_err(|_receive_error| DebugProbeUnavailable)
}

impl RtcWorker {
    #[cfg(test)]
    pub(in crate::engine::media_transport::rtc) fn test_handle(&self) -> &super::RtcWorkerHandle {
        &self.handle
    }

    pub(in crate::engine::media_transport) async fn pause_for_test(
        &self,
    ) -> Option<(std_mpsc::Sender<()>, JoinHandle<Option<()>>)> {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = std_mpsc::channel();
        let command_tx = self.handle.command_tx.clone();
        let probe = tokio::spawn(async move {
            send_debug_probe(
                &command_tx,
                move |_: &PacketLoopState, _: &WorkerCommandContext<'_>| {
                    let _ = entered_tx.send(());
                    let _result = release_rx.recv();
                },
            )
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
    /// Records the health transition in `metrics`, which must be the worker's
    /// runtime metrics for transport user gauges to stay consistent.
    pub fn debug_set_session_transport_health(
        &self,
        metrics: &RuntimeMetrics,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) {
        let previous = self
            .handle
            .snapshot_state
            .lock()
            .expect("test snapshot state must be available")
            .set_transport_health(session_key, health);
        metrics.record_transport_health_transition(
            previous.map(observability::transport_health_state),
            Some(observability::transport_health_state(health)),
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

    pub(in crate::engine::media_transport::rtc) async fn probe_debug_worker<P>(
        &self,
        probe: P,
    ) -> Result<P::Output, DebugProbeUnavailable>
    where
        P: DebugProbe,
    {
        send_debug_probe(&self.handle.command_tx, probe).await
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
                .session_key_for_remote_addr(RemoteTransportAddr::udp(source_addr))
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

    pub async fn debug_route_entry_by_media_id(
        &self,
        src_media: TransportMediaId,
    ) -> Result<Option<DebugRouteEntry>, DebugProbeUnavailable> {
        self.probe_debug_worker(RouteEntryByMediaIdProbe { src_media })
            .await
    }

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

/// Test worker with the shared services it reports into.
#[cfg(test)]
pub(crate) struct TestRtcWorker {
    pub worker: RtcWorker,
    pub metrics: Arc<RuntimeMetrics>,
    pub source_policy_signal: SourcePolicySignal,
}

#[cfg(test)]
impl TestRtcWorker {
    #[must_use]
    pub(crate) fn start(config: MediaTransportConfig) -> Self {
        let profile = RtpProfile::compile(config.codec_flags, config.codec_preferences)
            .expect("test RTP profile should compile");
        let deps = test_media_transport_deps();
        let source_policy_signal = SourcePolicySignal::default();
        let worker = RtcWorker::start(
            &config,
            Arc::new(profile),
            WorkerAssignment {
                rtc_port_range: config.rtc_port_range,
                media_id_base: 0,
                media_worker_id: MediaWorkerId::from_raw(0),
            },
            &deps,
            source_policy_signal.clone(),
            UfragWorkerMap::default(),
        )
        .expect("test RTC worker should start");
        Self {
            worker,
            metrics: deps.metrics,
            source_policy_signal,
        }
    }
}

#[cfg(test)]
impl Default for TestRtcWorker {
    fn default() -> Self {
        Self::start(test_media_transport_config(1, test_rtc_port_range()))
    }
}

#[cfg(test)]
impl Deref for TestRtcWorker {
    type Target = RtcWorker;

    fn deref(&self) -> &RtcWorker {
        &self.worker
    }
}

#[cfg(test)]
impl RtcWorker {
    #[must_use]
    pub(crate) fn for_test(config: MediaTransportConfig) -> Self {
        TestRtcWorker::start(config).worker
    }
}

#[cfg(test)]
impl Default for RtcWorker {
    fn default() -> Self {
        TestRtcWorker::default().worker
    }
}

#[cfg(test)]
#[path = "TESTS/worker_exit.rs"]
mod worker_exit_tests;
