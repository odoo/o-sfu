//! Completion obligations on top of Rust's affine ownership.
//!
//! Linear type systems require each resource to be used exactly once. They forbid
//! implicit duplication (contraction) and discarding (weakening). Affine type
//! systems allow discarding, so a resource may be used at most once. Rust's move
//! semantics for non-`Copy` values are affine in this sense: moving transfers
//! ownership, but leaving scope without calling `commit` or `close` is legal.
//!
//! [`Obligation`] owns work that must finish before it is dropped. Calling
//! `complete(context).await` runs that work's [`Execute`] implementation. There
//! is no separate call to mark it complete, no mutable access to replace the work
//! and no way to substitute an unrelated future.
//! Work types implement these traits beside their own code. This module only
//! manages the guard. Implementations decide what counts as finished and must
//! handle failures before returning. The compiler ties completion to those
//! implementations but cannot verify that they do the work correctly.
//!
//! A synchronous commit can use [`TryComplete`]. Rejection returns the still-armed
//! obligation. [`Transfer`] converts work to its next form without completing it.
//! For example, rollback can turn a reservation into cleanup work, keeping the
//! same guard until cleanup and any required fallback finish, even if they fail.
//! Queuing cleanup is not enough.
//! Merging batches moves the work before disarming the donor's guard.
//!
//! The guard reports unfinished work when it is dropped, including when
//! cancellation drops an execution future before its first poll or during an
//! await. It does not run cleanup. This is a runtime check: `#[must_use]` cannot
//! prove that work finishes, [`std::mem::forget`] can bypass the destructor and
//! process termination can skip it too.
//!
//! See [Asynchronous clean-up] for the connection between linear types,
//! explicit cleanup and cancellation.
//!
//! [Asynchronous clean-up]: https://without.boats/blog/asynchronous-clean-up/

#[cfg(any(test, debug_assertions))]
use std::thread;
use std::{future::Future, iter};

use tracing::error;

/// Work that must be completed, with a guard that reports an unfinished drop.
///
/// The work and its guard move together. [`complete`](Self::complete) runs the
/// work's asynchronous operation. [`try_complete`](Self::try_complete) attempts
/// a synchronous state change and returns the obligation if it is rejected.
/// [`transfer`](Self::transfer) converts the work, for example from a reservation
/// to cleanup, without disarming the guard. Callers cannot mark work
/// complete separately or replace it through a mutable reference.
///
/// Types using this wrapper should keep it in a private field and implement the
/// operations their callers need. The traits are open to new implementations.
/// Batches that report no pending work can be dropped without completing them.
///
/// # Panics
///
/// Dropping unfinished work emits an error event and panics in tests or when
/// debug assertions are enabled, unless the thread is already unwinding. This
/// includes release-mode unit tests. Other release builds log without panicking.
/// Dropping the guard never runs cleanup.
///
/// # Examples
///
/// These examples use crate-private APIs and are excluded from external doctests.
///
/// A buffered writer can require a flush before it is dropped. Here a flush
/// attempt completes the obligation and returns any I/O error to the caller:
///
/// ```rust,ignore
/// use std::io;
/// use tokio::io::{AsyncWrite, AsyncWriteExt};
/// use crate::engine::obligation::{Execute, Obligation, Work};
///
/// /// A writer with a pending flush.
/// struct Flush<W>(W);
///
/// impl<W> Work for Flush<W> {
///     fn label(&self) -> &'static str {
///         "flush buffered output"
///     }
/// }
///
/// impl<W: AsyncWrite + Unpin + Send> Execute<()> for Flush<W> {
///     type Output = io::Result<()>;
///
///     async fn execute(mut self, (): ()) -> Self::Output {
///         self.0.flush().await
///     }
/// }
///
/// async fn flush_output<W: AsyncWrite + Unpin + Send>(writer: W) -> io::Result<()> {
///     let pending = Obligation::new(Flush(writer));
///     pending.complete(()).await
/// }
/// ```
///
/// A work type can define a next step through `Transfer`. Conversion preserves
/// any existing diagnostic label. The returned work still needs to run:
///
/// ```rust,ignore
/// use crate::engine::obligation::{Obligation, Transfer};
///
/// fn next_step<W: Transfer>(pending: Obligation<W>) -> Obligation<W::Target> {
///     pending.transfer()
/// }
/// ```
#[derive(Debug)]
#[must_use = "lifecycle work must complete or transfer its obligation"]
pub(crate) struct Obligation<W: Work> {
    work: W,
    guard: Guard,
}

impl<W: Work> Obligation<W> {
    /// Guards work without starting it.
    ///
    /// The diagnostic label is a static string and needs no allocation. Work that
    /// reports nothing pending starts with an unarmed guard.
    pub(crate) fn new(work: W) -> Self {
        let label = work.is_pending().then(|| work.label());
        Self {
            work,
            guard: Guard { label },
        }
    }

    /// Borrows the work without allowing it to be removed or replaced.
    pub(crate) const fn work(&self) -> &W {
        &self.work
    }

    /// Runs the work and disarms the guard after execution returns.
    ///
    /// [`Execute::execute`] must attempt any required fallback before returning,
    /// even if that fallback also fails. The guard moves into the future when it
    /// is created and stays armed while execution is pending. Dropping the future
    /// reports unfinished work, including before its first poll.
    pub(crate) async fn complete<C>(self, context: C) -> W::Output
    where
        W: Execute<C>,
    {
        let Self { work, guard } = self;
        let outcome = work.execute(context).await;
        guard.disarm();
        outcome
    }

    /// Attempts a synchronous state change and keeps rejected work guarded.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self)` when [`TryComplete::try_complete`] rejects the change.
    /// The returned obligation is still armed with the same label. The caller
    /// can retry or transfer the work to cleanup.
    pub(crate) fn try_complete<C>(self, context: C) -> Result<W::Output, Self>
    where
        W: TryComplete<C>,
    {
        let Self { work, guard } = self;
        match work.try_complete(context) {
            Ok(outcome) => {
                guard.disarm();
                Ok(outcome)
            }
            Err(work) => Err(Self { work, guard }),
        }
    }

    /// Converts work through [`Transfer`] and preserves an armed guard's label.
    ///
    /// A pending target arms an unarmed guard with its own label. Transferring
    /// work never completes an existing obligation.
    pub(crate) fn transfer(self) -> Obligation<W::Target>
    where
        W: Transfer,
    {
        let Self { work, mut guard } = self;
        let work = work.transfer();
        if guard.label.is_none() && work.is_pending() {
            guard.label = Some(work.label());
        }
        Obligation { work, guard }
    }

    /// Adds work using the batch's `Extend` implementation.
    ///
    /// Adding work to an empty batch arms its guard. Extending an armed batch
    /// leaves it armed, even when the iterator is empty.
    pub(crate) fn extend<T>(&mut self, items: impl IntoIterator<Item = T>)
    where
        W: Extend<T>,
    {
        let was_pending = self.guard.label.is_some();
        // Arm first: the iterator may add work and then panic.
        self.guard.label = Some(self.guard.label.unwrap_or_else(|| self.work.label()));
        self.work.extend(items);
        if !was_pending && !self.work.is_pending() {
            self.guard.label = None;
        }
    }

    /// Combines both batches under one guard without running either batch.
    ///
    /// `Extend<Self>` must move the donor's work before its guard is disarmed.
    /// If both batches are armed, the combined batch keeps the destination's label.
    pub(crate) fn merge(&mut self, other: Self)
    where
        W: Extend<W>,
    {
        let Self { work, guard } = other;
        self.guard.label = self.guard.label.or(guard.label);
        self.work.extend(iter::once(work));
        guard.disarm();
    }

    /// Discards work deliberately skipped by a test.
    ///
    /// The work's [`Simulate`] implementation must handle any nested obligations
    /// before this guard is disarmed. This does not run [`Execute::execute`].
    #[cfg(any(test, feature = "testing-transport"))]
    pub(crate) fn simulate(self)
    where
        W: Simulate,
    {
        let Self { work, guard } = self;
        work.simulate();
        guard.disarm();
    }
}

impl<W: Work + Default> Default for Obligation<W> {
    fn default() -> Self {
        Self::new(W::default())
    }
}

/// Work whose completion rules are defined by its type.
///
/// Implement this beside the work type. Its constructors, execution and `Extend`
/// implementations must preserve unfinished work. These implementations need
/// review: the wrapper cannot verify them. Keep the obligation private in the
/// type that controls its execution. Do not implement `Work` for arbitrary futures
/// or callbacks.
pub(crate) trait Work: Sized {
    /// The operation name to report if the work is dropped unfinished.
    fn label(&self) -> &'static str;

    /// Whether the work needs to run. Batch types can return `false` when empty.
    fn is_pending(&self) -> bool {
        true
    }
}

/// The asynchronous operation that completes the work.
pub(crate) trait Execute<C>: Work {
    type Output;

    /// Must finish the work or handle its failure, including fallback, before returning.
    fn execute(self, context: C) -> impl Future<Output = Self::Output> + Send;
}

/// A synchronous state change that returns the work if it is rejected.
pub(crate) trait TryComplete<C>: Work {
    type Output;

    /// Returns `Err(Self)` if the state change is rejected before accepting the work.
    fn try_complete(self, context: C) -> Result<Self::Output, Self>;
}

/// Converts unfinished work to its next form, such as a reservation to cleanup.
pub(crate) trait Transfer: Work {
    type Target: Work;

    fn transfer(self) -> Self::Target;
}

/// Discards work that a test deliberately does not execute.
///
/// Implementations must also discard any nested obligations. This does not
/// replace tests of the real execution path.
#[cfg(any(test, feature = "testing-transport"))]
pub(crate) trait Simulate: Work {
    fn simulate(self);
}

/// Reports unfinished work on drop while its label is armed.
///
/// Keeping `Drop` here lets [`Obligation`] move its work into execution, transfer
/// or merging while the guard stays armed.
#[derive(Debug)]
struct Guard {
    label: Option<&'static str>,
}

impl Guard {
    /// Consumes the guard after work finishes or another guard takes it over.
    fn disarm(mut self) {
        self.label = None;
    }
}

impl Drop for Guard {
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

#[cfg(test)]
#[path = "TESTS/obligation.rs"]
mod tests;
