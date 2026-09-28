#![allow(
    clippy::panic,
    reason = "test-support snapshot lookup must fail when a declared bucket is missing"
)]

use super::{
    catalog::RuntimeMetrics,
    descriptor::{MetricLabel, MetricLabelValue, MetricName, build_snapshot},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MetricHistogramSnapshot {
    buckets: Box<[(&'static str, u64)]>,
    pub(super) count: u64,
    pub(super) sum_micros: u64,
}

impl MetricHistogramSnapshot {
    pub(super) fn bucket(&self, upper_bound: &str) -> u64 {
        self.buckets
            .iter()
            .find(|(bound, _)| *bound == upper_bound)
            .map_or_else(
                || panic!("missing histogram bucket {upper_bound}"),
                |(_, value)| *value,
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MetricValue {
    Counter(u64),
    Gauge(i64),
    Histogram(MetricHistogramSnapshot),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetricSample {
    name: MetricName,
    labels: Box<[MetricLabel]>,
    value: MetricValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeMetricsSnapshot {
    samples: Box<[MetricSample]>,
}

impl RuntimeMetricsSnapshot {
    pub(super) fn counter(&self, name: MetricName, labels: &[(&str, &str)]) -> Option<u64> {
        let MetricValue::Counter(value) = self.sample(name, labels)?.value else {
            return None;
        };
        Some(value)
    }

    pub(super) fn gauge(&self, name: MetricName, labels: &[(&str, &str)]) -> Option<i64> {
        let MetricValue::Gauge(value) = self.sample(name, labels)?.value else {
            return None;
        };
        Some(value)
    }

    pub(super) fn histogram(
        &self,
        name: MetricName,
        labels: &[(&str, &str)],
    ) -> Option<&MetricHistogramSnapshot> {
        let MetricValue::Histogram(value) = &self.sample(name, labels)?.value else {
            return None;
        };
        Some(value)
    }

    fn sample(&self, name: MetricName, labels: &[(&str, &str)]) -> Option<&MetricSample> {
        self.samples
            .iter()
            .find(|sample| sample.name == name && labels_match(&sample.labels, labels))
    }
}

#[derive(Default)]
pub(super) struct SnapshotWriter {
    samples: Vec<MetricSample>,
}

impl SnapshotWriter {
    pub(super) fn counter(&mut self, name: MetricName, labels: Box<[MetricLabel]>, value: u64) {
        self.push(name, labels, MetricValue::Counter(value));
    }

    pub(super) fn gauge(&mut self, name: MetricName, labels: Box<[MetricLabel]>, value: i64) {
        self.push(name, labels, MetricValue::Gauge(value));
    }

    pub(super) fn histogram(
        &mut self,
        name: MetricName,
        labels: Box<[MetricLabel]>,
        buckets: Box<[(&'static str, u64)]>,
        count: u64,
        sum_micros: u64,
    ) {
        self.push(
            name,
            labels,
            MetricValue::Histogram(MetricHistogramSnapshot {
                buckets,
                count,
                sum_micros,
            }),
        );
    }

    pub(super) fn finish(self) -> RuntimeMetricsSnapshot {
        RuntimeMetricsSnapshot {
            samples: self.samples.into_boxed_slice(),
        }
    }

    fn push(&mut self, name: MetricName, labels: Box<[MetricLabel]>, value: MetricValue) {
        self.samples.push(MetricSample {
            name,
            labels,
            value,
        });
    }
}

impl RuntimeMetrics {
    #[must_use]
    pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
        build_snapshot(self)
    }
}

fn labels_match(sample_labels: &[MetricLabel], labels: &[(&str, &str)]) -> bool {
    sample_labels.len() == labels.len()
        && labels.iter().all(|(name, value)| {
            sample_labels.iter().any(|label| {
                label.name == *name
                    && match label.value {
                        MetricLabelValue::Text(sample_value) => sample_value == *value,
                        MetricLabelValue::Number(sample_value) => {
                            value.parse::<usize>() == Ok(sample_value)
                        }
                    }
            })
        })
}
