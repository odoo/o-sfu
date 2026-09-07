//! lock-free metric counters for runtime hot paths
//!
//! counters use relaxed atomics because snapshots are observational telemetry
//! and never synchronize runtime state
//! padded counters isolate high-frequency worker labels onto cache lines

use std::{
    marker::PhantomData,
    sync::atomic::{AtomicI64, AtomicU64, Ordering},
    time::Duration,
};

pub(super) trait MetricLabel: Copy + 'static {
    const VARIANTS: &'static [Self];
    const COUNT: usize;

    fn as_index(self) -> usize;
}

pub(super) trait ExportedMetricLabel: MetricLabel {
    fn label_value(self) -> &'static str;
}

pub(super) trait MetricBucketLabel: MetricLabel {
    fn upper_bound(self) -> &'static str;
}

pub(super) trait HistogramBucketLabel: MetricBucketLabel {
    fn from_duration(duration: Duration) -> Self;
}

#[repr(align(64))]
#[derive(Debug, Default)]
pub(super) struct PaddedCounter {
    value: Counter,
}

impl PaddedCounter {
    pub(super) fn increment(&self) {
        self.value.increment();
    }

    pub(super) fn add(&self, value: usize) {
        self.value.add(value);
    }

    pub(super) fn add_u64(&self, value: u64) {
        self.value.add_u64(value);
    }

    pub(super) fn load(&self) -> u64 {
        self.value.load()
    }
}

#[derive(Debug, Default)]
pub(super) struct Counter {
    value: AtomicU64,
}

impl Counter {
    pub(super) fn increment(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn add(&self, value: usize) {
        if let Ok(value) = u64::try_from(value) {
            self.value.fetch_add(value, Ordering::Relaxed);
        }
    }

    pub(super) fn add_u64(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
    }

    pub(super) fn load(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
pub(super) struct UpDownCounter {
    value: AtomicI64,
}

impl UpDownCounter {
    pub(super) fn add(&self, delta: i64) {
        self.value.fetch_add(delta, Ordering::Relaxed);
    }

    pub(super) fn load(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub(super) struct UpDownCounterFamily<L: MetricLabel> {
    counters: Box<[UpDownCounter]>,
    _label: PhantomData<L>,
}

impl<L: MetricLabel> Default for UpDownCounterFamily<L> {
    fn default() -> Self {
        let counters = (0..L::COUNT)
            .map(|_| UpDownCounter::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            counters,
            _label: PhantomData,
        }
    }
}

impl<L: MetricLabel> UpDownCounterFamily<L> {
    pub(super) fn add(&self, label: L, delta: i64) {
        if let Some(counter) = self.counters.get(label.as_index()) {
            counter.add(delta);
        }
    }

    pub(super) fn load(&self, label: L) -> i64 {
        self.counters
            .get(label.as_index())
            .map_or(0, UpDownCounter::load)
    }
}

#[derive(Debug)]
pub(super) struct PaddedCounterFamily<L: MetricLabel> {
    counters: Box<[PaddedCounter]>,
    _label: PhantomData<L>,
}

impl<L: MetricLabel> Default for PaddedCounterFamily<L> {
    fn default() -> Self {
        let counters = (0..L::COUNT)
            .map(|_| PaddedCounter::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            counters,
            _label: PhantomData,
        }
    }
}

impl<L: MetricLabel> PaddedCounterFamily<L> {
    pub(super) fn accumulate_into(&self, totals: &mut [u64]) {
        for label in L::VARIANTS {
            let count = self.load(*label);
            if let Some(total) = totals.get_mut(label.as_index()) {
                *total = total.saturating_add(count);
            }
        }
    }

    pub(super) fn increment(&self, label: L) {
        if let Some(counter) = self.counters.get(label.as_index()) {
            counter.increment();
        }
    }

    pub(super) fn add(&self, label: L, value: usize) {
        if let Some(counter) = self.counters.get(label.as_index()) {
            counter.add(value);
        }
    }

    pub(super) fn add_u64(&self, label: L, value: u64) {
        if let Some(counter) = self.counters.get(label.as_index()) {
            counter.add_u64(value);
        }
    }

    pub(super) fn load(&self, label: L) -> u64 {
        self.counters
            .get(label.as_index())
            .map_or(0, PaddedCounter::load)
    }
}

#[derive(Debug)]
pub(super) struct CounterFamily<L: MetricLabel> {
    counters: Box<[Counter]>,
    _label: PhantomData<L>,
}

impl<L: MetricLabel> Default for CounterFamily<L> {
    fn default() -> Self {
        let counters = (0..L::COUNT)
            .map(|_| Counter::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            counters,
            _label: PhantomData,
        }
    }
}

impl<L: MetricLabel> CounterFamily<L> {
    pub(super) fn increment(&self, label: L) {
        if let Some(counter) = self.counters.get(label.as_index()) {
            counter.increment();
        }
    }

    pub(super) fn add(&self, label: L, value: usize) {
        if let Some(counter) = self.counters.get(label.as_index()) {
            counter.add(value);
        }
    }

    pub(super) fn add_u64(&self, label: L, value: u64) {
        if let Some(counter) = self.counters.get(label.as_index()) {
            counter.add_u64(value);
        }
    }

    pub(super) fn load(&self, label: L) -> u64 {
        self.counters.get(label.as_index()).map_or(0, Counter::load)
    }
}

#[derive(Debug)]
pub(super) struct Histogram<B: HistogramBucketLabel> {
    buckets: Box<[Counter]>,
    count: Counter,
    sum_micros: Counter,
    _bucket: PhantomData<B>,
}

impl<B: HistogramBucketLabel> Default for Histogram<B> {
    fn default() -> Self {
        let buckets = (0..B::COUNT)
            .map(|_| Counter::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            buckets,
            count: Counter::default(),
            sum_micros: Counter::default(),
            _bucket: PhantomData,
        }
    }
}

impl<B: HistogramBucketLabel> Histogram<B> {
    /// records one cumulative Prometheus histogram observation
    ///
    /// every bucket at or above the selected bound is incremented so snapshots can
    /// be rendered directly as `_bucket{le=...}` samples
    pub(super) fn observe(&self, duration: Duration) {
        self.count.increment();
        self.sum_micros
            .add_u64(u64::try_from(duration.as_micros()).unwrap_or(u64::MAX));
        let bucket_index = B::from_duration(duration).as_index();
        for counter in self.buckets.iter().skip(bucket_index) {
            counter.increment();
        }
    }

    pub(super) fn load_bucket(&self, bucket: B) -> u64 {
        self.buckets.get(bucket.as_index()).map_or(0, Counter::load)
    }

    pub(super) fn load_count(&self) -> u64 {
        self.count.load()
    }

    pub(super) fn load_sum_micros(&self) -> u64 {
        self.sum_micros.load()
    }
}

#[derive(Debug)]
pub(super) struct HistogramFamily<L: MetricLabel, B: HistogramBucketLabel> {
    histograms: Box<[Histogram<B>]>,
    _label: PhantomData<L>,
}

impl<L: MetricLabel, B: HistogramBucketLabel> Default for HistogramFamily<L, B> {
    fn default() -> Self {
        let histograms = (0..L::COUNT)
            .map(|_| Histogram::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            histograms,
            _label: PhantomData,
        }
    }
}

impl<L: MetricLabel, B: HistogramBucketLabel> HistogramFamily<L, B> {
    pub(super) fn observe(&self, label: L, duration: Duration) {
        if let Some(histogram) = self.histograms.get(label.as_index()) {
            histogram.observe(duration);
        }
    }

    pub(super) fn load_bucket(&self, label: L, bucket: B) -> u64 {
        self.histograms
            .get(label.as_index())
            .map_or(0, |histogram| histogram.load_bucket(bucket))
    }

    pub(super) fn load_count(&self, label: L) -> u64 {
        self.histograms
            .get(label.as_index())
            .map_or(0, Histogram::load_count)
    }

    pub(super) fn load_sum_micros(&self, label: L) -> u64 {
        self.histograms
            .get(label.as_index())
            .map_or(0, Histogram::load_sum_micros)
    }
}
