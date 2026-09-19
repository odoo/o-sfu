# E4. Rely only on documented dependency behavior

Rely on a dependency's iteration order, timing, private state or error text
only when its public documentation guarantees that behavior. Where an
undocumented assumption is unavoidable, isolate it and verify it against the
pinned version.

**Example:** `drain_single_session` succeeds once `str0m::Rtc` returns an
`Output::Timeout` with a future deadline. If the output budget runs out first,
the caller rolls back staged output and closes the session. Stopping after a
fixed number of polls without either policy could leave `Rtc` partly drained.

**Avoid**

```rust
// Four polls may stop before str0m reaches its documented drain boundary.
for _ in 0..4 {
    handle(rtc.poll_output()?)?;
}
```

**Prefer**

```rust
// A future `Output::Timeout` is str0m's documented end-of-drain signal.
let deadline = loop {
    match rtc.poll_output()? {
        Output::Timeout(timeout_at) if timeout_at <= now => {
            // Feed an already-due deadline back before continuing the drain.
            rtc.handle_input(Input::Timeout(now))?;
        }
        Output::Timeout(timeout_at) => break timeout_at,
        output => handle(output)?,
    }
};
```

Budget exhaustion ends the session after discarding its staged output:

```rust
SessionDrainOutcome::Exhausted(session_key, limit) => {
    // Discard partial output before closing the session that exceeded its budget.
    buffers.rollback_session_drain(&checkpoint);
    context
        .rtc_metrics
        .record_rtc_output_budget_exhaustion(limit);
    worker_close_session(
        state,
        context.bitrate_registry,
        context.snapshot_state,
        &session_key,
        SessionCloseDisposition::OutputBudgetExhausted,
        context.metrics,
    );
}
```

**Rationale:** Dependencies may change undocumented behavior without an API
break, turning hidden assumptions into upgrade failures.
