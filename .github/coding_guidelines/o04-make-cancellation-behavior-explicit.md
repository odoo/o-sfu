# O4. Make cancellation behavior explicit

Treat each `.await` as a point where the future may be dropped. Before racing
futures, identify the changes made before each suspension and the consequences
of dropping a losing future. If a loop recreates that future, cancellation must
allow it to restart safely or its progress must survive outside the future.

A one-shot shutdown or timeout race may lose work only when the call site
explicitly permits it. Otherwise, finish the operation before honoring
cancellation or move it into a tracked task whose result is handled. See
[R6](r06-bound-externally-driven-work.md) for polling order and fairness.

> [!NOTE]
> Further reading: **[cancellation safety in Tokio](https://tokio.rs/tokio/tutorial/select#cancellation-safety)** and **[future execution in the Asynchronous Programming in Rust Book](https://rust-lang.github.io/async-book/02_execution/01_chapter.html)**.

**Example:** Once `RtcWorker::request_worker` enqueues a command, its caller
waits for the result before honoring later shutdown.

The one-shot shutdown policy in `ingress_should_stop` permits a different
choice: it deliberately drops one received datagram when shutdown wins.

**Avoid**

```rust
tokio::select! {
    () = shutdown.cancelled() => return,
    // Cancellation can win after enqueue, leaving the result unobserved.
    result = worker.request_worker(build_command) => handle(result),
}
```

**Prefer**

```rust
if shutdown.is_cancelled() {
    return;
}

// A caller that stays alive observes the accepted command before shutdown.
let result = worker.request_worker(build_command).await;
handle(result);
```

The call site makes the accepted datagram loss explicit:

```rust
let should_stop = tokio::select! {
    biased;
    // Shutdown intentionally wins over a backpressured datagram send.
    () = shutdown.cancelled() => true,
    send_result = tx.send(datagram) => send_result.is_err(),
};
```

**Rationale:** Dropping an unfinished future stops its local work, but effects
already handed to another task can continue without a caller to observe the
result.
