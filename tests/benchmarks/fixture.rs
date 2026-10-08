//! Stack boundaries for stateful benchmark fixtures.

/// Builds a fixture outside the harness stack frame.
///
/// Constructor temporaries must leave the stack before measurement starts so
/// fixture size cannot change the measured call stack through setup inlining.
#[must_use]
#[inline(never)]
pub fn boxed<T>(setup: impl FnOnce() -> T) -> Box<T> {
    Box::new(setup())
}

/// Keeps destructor temporaries outside the harness stack frame.
#[inline(never)]
pub fn release<T>(fixture: Box<T>) {
    drop(fixture);
}
