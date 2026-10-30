//! Completion obligations on top of Rust's affine ownership.
//!
//! Linear type systems require each resource to be used exactly once. They forbid
//! implicit duplication (contraction) and discarding (weakening). Affine type
//! systems allow discarding, so a resource may be used at most once. Rust's move
//! semantics for non-`Copy` values are affine in this sense: moving transfers
//! ownership, but leaving scope without calling `commit` or `close` is legal.
//!
//! [`Obligation`] represents the additional protocol requirement that lifecycle
//! work must reach a defined terminal outcome. The guard cannot be cloned.
//! Moving it transfers the unfinished responsibility. The domain operation calls
//! [`Obligation::complete`] only after reaching that outcome, consuming the guard.
//! For example, rollback moves a publication's guard into transport teardown.
//! Merely queuing cleanup does not complete it. The guard remains armed until
//! teardown and any session-close fallback return, including terminal failure.
//!
//! This approximates a linear completion discipline with runtime diagnostics.
//! [`Drop`] reports an unfinished obligation, including when cancellation drops
//! a cleanup future, but performs no cleanup. `#[must_use]` provides a lint, not
//! a proof that completion is reached. Rust permits bypassing destructors with
//! [`std::mem::forget`], and process termination can skip them too.
//!
//! See [Asynchronous clean-up] for the connection between linear types,
//! explicit cleanup and cancellation.
//!
//! [Asynchronous clean-up]: https://without.boats/blog/asynchronous-clean-up/

#[cfg(any(test, debug_assertions))]
use std::thread;

use tracing::error;

/// Tracks a caller's responsibility to finish lifecycle work.
///
/// Keep the guard with the pending work, including across awaits. Moving the
/// work to another owner must also move the guard. The owner calls
/// [`complete`](Self::complete) after reaching its defined terminal outcome.
/// [`merge`](Self::merge) combines responsibilities when their work is combined.
///
/// Dropping an unfinished guard emits an error event, including when cancellation
/// drops the future that holds it. The guard performs no cleanup.
///
/// # Panics
///
/// Dropping an unfinished guard panics in tests or when debug assertions are
/// enabled, unless the thread is already unwinding. This includes release-mode
/// unit tests. Other release builds emit the error event without panicking.
///
/// # Examples
///
/// These examples use crate-private APIs and are excluded from external doctests.
///
/// A batch retains its guard until every teardown returns. Combining batches
/// moves the work before merging its obligations:
///
/// ```rust,ignore
/// use crate::engine::{
///     media_transport::{MediaTransport, TransportTeardown},
///     obligation::Obligation,
/// };
///
/// struct CleanupBatch {
///     work: Vec<TransportTeardown>,
///     obligation: Obligation,
/// }
///
/// impl CleanupBatch {
///     fn new(teardown: TransportTeardown) -> Self {
///         Self {
///             work: vec![teardown],
///             obligation: Obligation::new("cleanup batch"),
///         }
///     }
///
///     fn merge(mut self, other: Self) -> Self {
///         self.work.extend(other.work);
///         self.obligation = self.obligation.merge(other.obligation);
///         self
///     }
///
///     async fn execute(self, transport: &MediaTransport) {
///         transport.teardown(self.work).await;
///         self.obligation.complete();
///     }
/// }
/// ```
///
/// Publication rollback transfers its existing reservation into a teardown.
/// Returning the command leaves the obligation pending until
/// [`MediaTransport::teardown`](crate::engine::media_transport::MediaTransport::teardown)
/// finishes executing it:
///
/// ```rust,ignore
/// use crate::engine::{
///     media_transport::{TransportMediaId, TransportSessionKey, TransportTeardown},
///     obligation::Obligation,
/// };
///
/// fn rollback(
///     session: TransportSessionKey,
///     media: TransportMediaId,
///     reservation: Obligation,
/// ) -> TransportTeardown {
///     TransportTeardown::remove_reserved_media(session, media, reservation)
/// }
/// ```
#[derive(Debug)]
#[must_use = "lifecycle work must complete or transfer its obligation"]
pub(crate) struct Obligation {
    label: Option<&'static str>,
}

impl Obligation {
    /// Starts a pending obligation without starting or registering any work.
    ///
    /// `label` identifies the responsibility in abandonment diagnostics. Use a
    /// static operation name, such as `"staged publication"`. Create the guard
    /// when work begins requiring explicit completion and keep it with that work.
    pub(crate) fn new(label: &'static str) -> Self {
        Self { label: Some(label) }
    }

    /// Acknowledges the caller's terminal outcome and consumes the guard.
    ///
    /// Call only after the work and its required failure handling have finished.
    /// A terminal outcome can include failure after all required fallback
    /// attempts. This method neither performs work nor verifies its outcome.
    /// Queuing or transferring pending work requires moving the guard with it.
    pub(crate) fn complete(mut self) {
        self.label = None;
    }

    /// Transfers both responsibilities into the returned guard.
    ///
    /// Combine the pending work before merging its guards, then retain the
    /// returned guard until all that work reaches its terminal outcome. Neither
    /// responsibility completes here and no work is moved or executed here.
    /// Subsequent abandonment reports `self`'s label. `other`'s label is discarded.
    pub(crate) fn merge(self, mut other: Self) -> Self {
        other.label = None;
        self
    }
}

impl Drop for Obligation {
    fn drop(&mut self) {
        let Some(label) = self.label else {
            return;
        };
        error!(obligation = label, "unfinished lifecycle obligation");
        #[cfg(any(test, debug_assertions))]
        if !thread::panicking() {
            assert!(
                self.label.is_none(),
                "unfinished lifecycle obligation: {label}"
            );
        }
    }
}
