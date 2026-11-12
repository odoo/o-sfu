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
    let failed_worker = TestRtcWorker::start(config);
    let other_worker = RtcWorker::default();
    let session_key = test_transport_session_key(1, 0, 1, UserId::Integer(1));
    assert_eq!(failed_worker.session_transport_health(&session_key), None);
    let sink = RegisteredPacketSink::new(Arc::new(PanickingSink), PacketSinkKind::Recording);
    let metrics = Arc::clone(&failed_worker.metrics);
    let packet_recorder = metrics.register_rtp_worker();
    let control_recorder = metrics.register_rtc_worker();
    let probe_session = session_key.clone();
    let probe_result = failed_worker
        .probe_debug_worker(move |_: &PacketLoopState, _: &WorkerCommandContext<'_>| {
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
