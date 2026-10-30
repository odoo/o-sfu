# O3. Own spawned work through shutdown

Give every spawned task an explicit completion policy. Use `CancellationToken`
to signal cooperative shutdown and `TaskTracker` to observe when the task set
has finished. When output or panics must be observed, retain and await a
`JoinHandle` or `AbortOnDropHandle`, or collect results from a `JoinSet`. Use
`AbortOnDropHandle` or `JoinSet` when dropping the owner must also request task
cancellation.

Stop admitting work before calling `TaskTracker::close`, which allows `wait` to
complete once the tracker is empty but still permits new tasks. Dropping a
`JoinHandle` detaches the task and forfeits its result. Detach only when result
and panic observation are unnecessary. The owner must enforce cooperative
shutdown unless the work is bounded, needs no shutdown cleanup and rejects
stale effects.

See Tokio's [`TaskTracker`
contract](https://docs.rs/tokio-util/latest/tokio_util/task/struct.TaskTracker.html).

> [!NOTE]
> Further reading: **[graceful shutdown in Tokio](https://tokio.rs/tokio/topics/shutdown)** and **[the `JoinHandle` lifecycle](https://docs.rs/tokio/latest/tokio/task/struct.JoinHandle.html)**.
>
> Related lints: [suspicious::let_underscore_future](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#let_underscore_future).

**Example:** Track background tasks with a `TaskTracker`. Once the owner stops
admitting work, close the tracker, signal cancellation and wait for completion.

**Avoid**

```rust
// Detached task ignores shutdown signals and can outlive its owner.
tokio::spawn(async move {
    worker_loop().await;
});
```

**Prefer**

```rust
// The tracker observes completion while the child token carries shutdown.
let worker_shutdown = shutdown.child_token();
tracker.spawn(async move {
    worker_loop(worker_shutdown).await;
});

// The owner has stopped admitting new tasks.
tracker.close();
shutdown.cancel();
tracker.wait().await;
```

**Rationale:** Task ownership makes completion, failure and resource lifetime
observable at shutdown.
