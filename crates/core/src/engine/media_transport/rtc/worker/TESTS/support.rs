use std::time::Instant;

use str0m::media::Mid;
#[cfg(test)]
use {
    super::super::{
        RtpProfile,
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
            DebugProbe, DebugRouteEntry, ObserveAudioActivityProbe, ReceiverBweTargetProbe,
            RecordIncomingMediaProbe, RouteEntryByConsumerMidProbe, RouteEntryByMediaIdProbe,
            RouteEntryProbe,
        },
    },
    RtcWorker,
};
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

    pub fn debug_set_session_transport_health(
        &self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) {
        let Ok(mut snapshot_state) = self.handle.snapshot_state.lock() else {
            return;
        };
        let previous = snapshot_state.set_transport_health(session_key, health);
        self.metrics.record_transport_health_transition(
            previous.map(metrics::transport_health_state),
            Some(metrics::transport_health_state(health)),
        );
    }

    pub fn debug_set_session_transport_quality(
        &self,
        session_key: &TransportSessionKey,
        quality: TransportQualitySample,
    ) {
        let Ok(mut snapshot_state) = self.handle.snapshot_state.lock() else {
            return;
        };
        snapshot_state.update_transport_quality(session_key, |sample| *sample = quality);
    }

    async fn probe_debug_worker<P>(&self, probe: P) -> Option<P::Output>
    where
        P: DebugProbe,
    {
        self.handle.debug_handle.probe(probe).await
    }

    #[cfg(test)]
    async fn read_debug_worker<F, Output>(&self, read: F) -> Option<Output>
    where
        F: FnOnce(&PacketLoopState, &WorkerCommandContext<'_>) -> Output + Send + 'static,
        Output: Send + 'static,
    {
        self.probe_debug_worker(read).await
    }

    #[cfg(test)]
    pub async fn debug_resolve_mid(&self, transport_media_id: TransportMediaId) -> Option<Mid> {
        self.read_debug_worker(move |state, _context| state.resolve_mid(transport_media_id))
            .await
            .flatten()
    }

    #[cfg(test)]
    pub async fn debug_remote_addr_owner(
        &self,
        source_addr: SocketAddr,
    ) -> Option<TransportSessionKey> {
        self.read_debug_worker(move |state, _context| {
            state
                .remote_addr_demux
                .session_key_for_remote_addr(source_addr)
                .cloned()
        })
        .await
        .flatten()
    }

    #[cfg(test)]
    pub async fn debug_has_any_remote_addr_session(&self) -> bool {
        self.read_debug_worker(|state, _context| !state.remote_addr_demux.is_empty())
            .await
            .unwrap_or(false)
    }

    #[cfg(test)]
    pub async fn debug_remember_remote_addr(
        &self,
        source_addr: SocketAddr,
        session_key: &TransportSessionKey,
    ) {
        let _ = self
            .probe_debug_worker(RememberRemoteAddrProbe {
                source_addr,
                session_key: session_key.clone(),
            })
            .await;
    }

    #[cfg(test)]
    pub async fn debug_session_stream_rx_ssrc(
        &self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) -> Option<u32> {
        self.probe_debug_worker(SessionStreamRxSsrcProbe {
            session_key: session_key.clone(),
            mid,
        })
        .await
        .flatten()
    }

    #[cfg(test)]
    pub async fn debug_session_stream_tx_pair(
        &self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) -> Option<(u32, Option<u32>)> {
        self.probe_debug_worker(SessionStreamTxSsrcProbe {
            session_key: session_key.clone(),
            mid,
        })
        .await
        .flatten()
    }

    #[cfg(test)]
    pub async fn debug_session_max_bitrate_in(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<Bitrate> {
        let session_key = session_key.clone();
        self.read_debug_worker(move |state, _context| {
            state
                .users
                .get(&session_key)
                .and_then(|session_state| session_state.max_bitrate_in)
        })
        .await
        .flatten()
    }

    #[cfg(test)]
    pub async fn debug_session_max_bitrate_out(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<Bitrate> {
        let session_key = session_key.clone();
        self.read_debug_worker(move |state, _context| {
            state
                .users
                .get(&session_key)
                .and_then(|session_state| session_state.max_bitrate_out)
        })
        .await
        .flatten()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn debug_session_receiver_bwe_target(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<Bitrate> {
        self.probe_debug_worker(ReceiverBweTargetProbe {
            session_key: session_key.clone(),
        })
        .await
        .flatten()
    }

    #[cfg(test)]
    pub async fn debug_session_receiver_bwe_str0m_update_count(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<u64> {
        let session_key = session_key.clone();
        self.read_debug_worker(move |state, _context| {
            state
                .users
                .get(&session_key)
                .map(|session_state| session_state.receiver_bwe_str0m_update_count)
        })
        .await
        .flatten()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn debug_route_entry(
        &self,
        src_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.probe_debug_worker(RouteEntryProbe {
            src_key: src_key.clone(),
            source_mid,
        })
        .await
        .flatten()
    }

    pub async fn debug_route_entry_by_consumer_mid(
        &self,
        consumer_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.probe_debug_worker(RouteEntryByConsumerMidProbe {
            consumer_key: consumer_key.clone(),
            consumer_mid,
        })
        .await
        .flatten()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn debug_route_entry_by_media_id(
        &self,
        src_media: TransportMediaId,
    ) -> Option<DebugRouteEntry> {
        self.probe_debug_worker(RouteEntryByMediaIdProbe { src_media })
            .await
            .flatten()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    /// Records synthetic media against the counter installed by publication.
    ///
    /// # Panics
    ///
    /// Panics if the worker is unavailable or the publication counter is absent.
    pub async fn debug_record_incoming_media(
        &self,
        transport_media_id: TransportMediaId,
        payload_bytes: usize,
        now: Instant,
    ) {
        let recorded = self
            .probe_debug_worker(RecordIncomingMediaProbe {
                transport_media_id,
                payload_bytes,
                now,
            })
            .await;
        assert_eq!(recorded, Some(true), "incoming media counter must exist");
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn debug_observe_audio_activity(
        &self,
        transport_media_id: TransportMediaId,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) {
        let _ = self
            .probe_debug_worker(ObserveAudioActivityProbe {
                transport_media_id,
                voice_activity,
                audio_level_dbov,
                now,
            })
            .await;
    }

    #[cfg(test)]
    pub async fn debug_relay_target_count(&self, src_media: TransportMediaId) -> usize {
        self.read_debug_worker(move |state, _context| state.routes.relay_target_count(src_media))
            .await
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub async fn debug_active_relay_target_count(&self, src_media: TransportMediaId) -> usize {
        self.read_debug_worker(move |state, _context| {
            state.routes.active_relay_target_count(src_media)
        })
        .await
        .unwrap_or(0)
    }
}

#[cfg(test)]
impl RtcWorker {
    #[must_use]
    #[expect(
        clippy::expect_used,
        reason = "test setup must fail when its RTC profile or worker cannot start"
    )]
    pub(crate) fn for_test(config: MediaTransportConfig) -> Self {
        let profile = RtpProfile::compile(config.codec_flags, config.codec_preferences)
            .expect("test RTP profile should compile");
        Self::start(
            &config,
            Arc::new(profile),
            config.rtc_port_range,
            &test_media_transport_deps(),
            SourcePolicySignal::default(),
            0,
            MediaWorkerId::from_raw(0),
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
