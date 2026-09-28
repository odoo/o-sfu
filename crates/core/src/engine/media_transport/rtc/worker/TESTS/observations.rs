use std::{task::Poll, time::Duration};

use futures_util::poll;
use tokio::{
    sync::oneshot,
    time::{advance, pause, resume},
};

use super::super::{super::commands::RtcWorkerCommand, RtcWorker};
use crate::engine::{
    media_transport::TransportAdapterError,
    metrics::{MetricName, test_support::RuntimeMetricsSnapshotLookup},
};

fn active_speaker_timeout_count(worker: &RtcWorker) -> u64 {
    worker.metrics.snapshot().counter_value(
        MetricName::RtcWorkerObservationTimeoutsTotal,
        &[("kind", "active_speaker_sources")],
    )
}

#[tokio::test(flavor = "current_thread")]
async fn full_mailbox_observation_times_out_before_enqueue() {
    let worker = RtcWorker::default();
    let (release, paused) = worker.pause_for_test().await.expect("worker should pause");
    let sender = &worker.test_handle().command_tx;
    for _ in 0..sender.max_capacity() {
        let (response, _receiver) = oneshot::channel();
        assert!(
            sender
                .try_send(RtcWorkerCommand::ActiveSpeakerSourceSnapshot { response })
                .is_ok()
        );
    }
    assert_eq!(sender.capacity(), 0);
    pause();
    let observation = worker.active_speaker_source_snapshot();
    tokio::pin!(observation);
    assert!(matches!(poll!(observation.as_mut()), Poll::Pending));
    advance(Duration::from_secs(1)).await;
    assert_eq!(
        observation.await,
        Err(TransportAdapterError::TransportUnavailable)
    );
    assert_eq!(
        sender.capacity(),
        0,
        "timed-out read must not enter the full mailbox"
    );
    assert_eq!(active_speaker_timeout_count(&worker), 1);
    resume();
    drop(release);
    assert_eq!(paused.await.expect("pause task should finish"), Some(()));
    worker.wait_for_shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn enqueued_observation_times_out_without_changing_worker_state() {
    let worker = RtcWorker::default();
    let (release, paused) = worker.pause_for_test().await.expect("worker should pause");
    let sender = &worker.test_handle().command_tx;
    pause();
    let observation = worker.active_speaker_source_snapshot();
    tokio::pin!(observation);
    assert!(matches!(poll!(observation.as_mut()), Poll::Pending));
    assert_eq!(sender.capacity(), sender.max_capacity() - 1);
    advance(Duration::from_secs(1)).await;
    assert_eq!(
        observation.await,
        Err(TransportAdapterError::TransportUnavailable)
    );
    assert_eq!(active_speaker_timeout_count(&worker), 1);
    resume();
    drop(release);
    assert_eq!(paused.await.expect("pause task should finish"), Some(()));
    assert_eq!(
        worker.active_speaker_source_snapshot().await,
        Ok(Vec::new())
    );
    assert_eq!(active_speaker_timeout_count(&worker), 1);
    worker.wait_for_shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn saturated_worker_observation_gate_does_not_block_another_worker() {
    let saturated = RtcWorker::default();
    let healthy = RtcWorker::default();
    let permit_count = u32::try_from(saturated.observation_permits.available_permits())
        .expect("observation gate should fit a u32 permit count");
    let held = saturated
        .observation_permits
        .acquire_many(permit_count)
        .await
        .expect("observation gate should remain open");
    let observation = saturated.active_speaker_source_snapshot();
    tokio::pin!(observation);
    assert!(matches!(poll!(observation.as_mut()), Poll::Pending));
    assert_eq!(
        healthy.active_speaker_source_snapshot().await,
        Ok(Vec::new())
    );
    pause();
    advance(Duration::from_secs(1)).await;
    assert_eq!(
        observation.await,
        Err(TransportAdapterError::TransportUnavailable)
    );
    assert_eq!(active_speaker_timeout_count(&saturated), 1);
    assert_eq!(active_speaker_timeout_count(&healthy), 0);
    resume();
    drop(held);
    assert_eq!(
        saturated.active_speaker_source_snapshot().await,
        Ok(Vec::new())
    );
    saturated.wait_for_shutdown().await;
    healthy.wait_for_shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn closed_worker_rejects_observation_without_timeout() {
    let worker = RtcWorker::default();
    worker.wait_for_shutdown().await;
    assert_eq!(
        worker.active_speaker_source_snapshot().await,
        Err(TransportAdapterError::TransportUnavailable)
    );
    assert_eq!(active_speaker_timeout_count(&worker), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn mutation_reply_remains_pending_past_observation_deadline() {
    use crate::engine::{UserId, media_transport::rtc::test_support::test_transport_session_key};

    let worker = RtcWorker::default();
    let (release, paused) = worker.pause_for_test().await.expect("worker should pause");
    let session_key = test_transport_session_key(1, 0, 1, UserId::Integer(1));
    pause();
    let mutation = worker.close_session(&session_key);
    tokio::pin!(mutation);
    assert!(matches!(poll!(mutation.as_mut()), Poll::Pending));
    advance(Duration::from_secs(1)).await;
    assert!(matches!(poll!(mutation.as_mut()), Poll::Pending));
    assert_eq!(active_speaker_timeout_count(&worker), 0);
    resume();
    drop(release);
    assert_eq!(paused.await.expect("pause task should finish"), Some(()));
    assert_eq!(mutation.await, Ok(()));
    worker.wait_for_shutdown().await;
}
