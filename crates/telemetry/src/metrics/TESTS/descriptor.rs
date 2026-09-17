use std::cell::Cell;

use super::{MetricDescriptor, MetricDestination, MetricName, MetricOutput};
use crate::metrics::labels::ControlPlaneDurationBucket;

#[test]
fn histogram_output_remains_cumulative() {
    let mut rendered = String::new();
    let mut destination = MetricDestination::Prometheus(&mut rendered);
    let mut output = MetricOutput::new(
        MetricDescriptor {
            id: MetricName::HttpRequestDurationSeconds,
            name: "test_histogram",
        },
        "# HELP test_histogram test histogram\n# TYPE test_histogram histogram\n",
        &mut destination,
    );
    let first = Cell::new(true);
    output.histogram::<ControlPlaneDurationBucket>(
        &[],
        |_| u64::from(first.replace(false)),
        || 0,
        || 0,
    );
    for expected in [
        "test_histogram_bucket{le=\"0.01\"} 1",
        "test_histogram_bucket{le=\"0.05\"} 1",
        "test_histogram_bucket{le=\"+Inf\"} 1",
        "test_histogram_count 1",
    ] {
        assert!(rendered.lines().any(|line| line == expected));
    }
}
