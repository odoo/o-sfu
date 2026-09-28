use super::*;

#[test]
fn receive_failure_backoff_caps_at_one_hundred_milliseconds() {
    let mut backoff = Duration::from_millis(1);
    let waits: Vec<_> = (0..9)
        .map(|_| advance_receive_failure_backoff(&mut backoff))
        .collect();
    assert_eq!(
        waits,
        [1, 2, 4, 8, 16, 32, 64, 100, 100].map(Duration::from_millis)
    );
    assert_eq!(backoff, RECEIVE_FAILURE_BACKOFF_MAX);
}

#[tokio::test]
async fn shared_receive_success_resets_backoff() {
    let (tx, mut rx) = mpsc::channel(1);
    let shutdown = CancellationToken::new();
    let mut failures = ReceiveFailureControl::new(Arc::new(RtcMetricsRecorder::default()));
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 1234));
    failures.backoff = RECEIVE_FAILURE_BACKOFF_MAX;
    let stopped = ingress_should_stop(
        Ok((1, addr)),
        vec![7],
        addr,
        Instant::now(),
        &tx,
        &shutdown,
        &mut failures,
    )
    .await;
    assert!(!stopped);
    assert_eq!(failures.backoff, Duration::from_millis(1));
    assert_eq!(
        rx.recv().await.map(|datagram| datagram.packet),
        Some(vec![7])
    );
}

fn assert_active_receive_backoff_cancels() {
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };

    use crate::engine::metrics::{
        MetricName, RuntimeMetrics, test_support::RuntimeMetricsSnapshotLookup,
    };

    let (tx, _rx) = mpsc::channel(1);
    let shutdown = CancellationToken::new();
    let metrics = RuntimeMetrics::default();
    let recorder = metrics.register_rtc_worker();
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 1234));
    let mut failures = ReceiveFailureControl::new(recorder);
    failures.backoff = RECEIVE_FAILURE_BACKOFF_MAX;
    let mut pending = Box::pin(ingress_should_stop(
        Err(io::Error::other("receive failed")),
        Vec::new(),
        addr,
        Instant::now(),
        &tx,
        &shutdown,
        &mut failures,
    ));
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(pending.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(
        metrics.snapshot().counter_value(
            MetricName::RtcTransportIoFailuresTotal,
            &[("direction", "receive"), ("category", "other")],
        ),
        1
    );
    shutdown.cancel();
    assert!(matches!(
        pending.as_mut().poll(&mut context),
        Poll::Ready(true)
    ));
}

#[tokio::test]
async fn tokio_shutdown_interrupts_active_receive_error_backoff() {
    assert_active_receive_backoff_cancels();
}

#[cfg(target_os = "linux")]
#[test]
fn io_uring_shutdown_interrupts_active_receive_error_backoff() {
    tokio_uring::start(async {
        assert_active_receive_backoff_cancels();
    });
}
