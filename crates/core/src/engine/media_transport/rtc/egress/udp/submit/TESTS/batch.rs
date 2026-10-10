use std::{array, io, net::SocketAddr};

use rustix::io::Errno;
use str0m::net::{Protocol, Transmit};

use super::{BATCH_SIZE, Completion, Progress, complete};
use crate::engine::metrics::{
    MetricName, RuntimeMetrics, test_support::RuntimeMetricsSnapshotLookup,
};

fn datagrams() -> [Transmit; 4] {
    array::from_fn(|index| Transmit {
        proto: Protocol::Udp,
        source: SocketAddr::from(([127, 0, 0, 1], 1000)),
        destination: SocketAddr::from(([127, 0, 0, 1], 2000)),
        contents: vec![42; index].into(),
    })
}

fn failures(metrics: &RuntimeMetrics, category: &'static str) -> u64 {
    metrics.snapshot().counter_value(
        MetricName::RtcTransportIoFailuresTotal,
        &[("direction", "send"), ("category", category)],
    )
}

#[test]
fn partial_completion_retires_bad_lengths_without_retrying_accepted_datagrams() {
    let metrics = RuntimeMetrics::default();
    let recorder = metrics.register_rtc_worker();
    let transmits = datagrams();
    // The kernel accepted an empty datagram, one short write and one full write.
    // Unaccepted lengths must not contribute failures or retire the final entry.
    let mut lengths = [usize::MAX; BATCH_SIZE];
    for (length, value) in lengths.iter_mut().zip([0, 0, 2]) {
        *length = value;
    }
    assert_eq!(
        complete(
            &recorder,
            &transmits,
            Ok(Completion {
                accepted: 3,
                lengths
            }),
        ),
        Progress::Retired(3),
    );
    assert_eq!(failures(&metrics, "other"), 1);
}

#[test]
fn zero_acceptance_retires_one_datagram_and_reports_an_invariant_failure() {
    let metrics = RuntimeMetrics::default();
    let recorder = metrics.register_rtc_worker();
    assert_eq!(
        complete(
            &recorder,
            &datagrams(),
            Ok(Completion {
                accepted: 0,
                lengths: [0; BATCH_SIZE],
            }),
        ),
        Progress::Retired(1),
    );
    assert_eq!(failures(&metrics, "other"), 1);
}

#[test]
fn retry_and_fallback_preserve_the_first_unsent_datagram_without_reporting() {
    let metrics = RuntimeMetrics::default();
    let recorder = metrics.register_rtc_worker();
    for (error, progress) in [
        (Errno::AGAIN, Progress::WaitWritable),
        (Errno::INTR, Progress::Retry),
        (Errno::NOSYS, Progress::Unsupported),
    ] {
        assert_eq!(
            complete(&recorder, &datagrams(), Err(error.into())),
            progress
        );
    }
    for category in [
        "other",
        "would_block",
        "permission_denied",
        "network_unavailable",
    ] {
        assert_eq!(failures(&metrics, category), 0);
    }
}

#[test]
fn ordinary_errors_retire_and_report_only_the_first_unsent_datagram() {
    for (error, category) in [
        (Errno::PERM, "permission_denied"),
        (Errno::ACCESS, "permission_denied"),
        (Errno::MSGSIZE, "other"),
        (Errno::NETUNREACH, "network_unavailable"),
    ] {
        let metrics = RuntimeMetrics::default();
        let recorder = metrics.register_rtc_worker();
        assert_eq!(
            complete(&recorder, &datagrams(), Err(io::Error::from(error))),
            Progress::Retired(1),
        );
        assert_eq!(failures(&metrics, category), 1);
    }
}
