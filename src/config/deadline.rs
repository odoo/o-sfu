use std::time::Duration;

use anyhow::{Result, ensure};

/// Positive runtime timeout or interval of at most 24 hours.
///
/// The operational limit rejects unit mistakes and unbounded durations before
/// they reach runtime deadline arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeadlineDuration(Duration);

impl DeadlineDuration {
    /// Constructs a duration from integer milliseconds.
    ///
    /// # Errors
    /// Returns [`anyhow::Error`] unless `milliseconds` is in `1..=86_400_000`.
    pub fn from_millis(milliseconds: u64) -> Result<Self> {
        ensure!(
            (1..=86_400_000).contains(&milliseconds),
            "must be between 1 and 86400000 milliseconds"
        );
        Ok(Self(Duration::from_millis(milliseconds)))
    }

    /// Returns the validated duration for runtime timers.
    #[must_use]
    pub const fn as_duration(self) -> Duration {
        self.0
    }
}
