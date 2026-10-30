# O1. Never block an async executor

Keep blocking I/O, potentially blocking OS-thread joins and sustained CPU work
off async executors. Use async APIs where available and reserve
[`spawn_blocking`](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html)
for bounded blocking work that finishes on its own, since a started job cannot
be aborted. Long-running blocking work needs a dedicated OS thread with a
lifecycle owner and cooperative shutdown.

Await completion asynchronously. Do not use a loop of `JoinHandle::is_finished`
and `yield_now` as a general completion mechanism, because Tokio may immediately
poll the same task again.

> [!NOTE]
> Further reading: **[Tokio's task documentation](https://docs.rs/tokio/latest/tokio/task/index.html#blocking-and-yielding)**, **[cooperative multitasking](https://en.wikipedia.org/wiki/Cooperative_multitasking)** and **[thread pool starvation](https://en.wikipedia.org/wiki/Starvation_\(computer_science\))**.

**Example:** Once cooperative shutdown bounds the time until a dedicated worker
exits, move its blocking join off the async executor.

**Avoid**

```rust
async fn wait_for_shutdown(thread: thread::JoinHandle<()>) {
    // `thread.join()` blocks an executor worker until the OS thread exits.
    let _ = thread.join();
}

async fn poll_for_shutdown(thread: &thread::JoinHandle<()>) {
    // `yield_now` may repoll this task without advancing thread completion.
    while !thread.is_finished() {
        yield_now().await;
    }
}
```

**Prefer**

```rust
async fn join_worker(
    shutdown: &CancellationToken,
    thread: thread::JoinHandle<()>,
) -> Result<thread::Result<()>, tokio::task::JoinError> {
    shutdown.cancel();
    // Cooperative shutdown bounds the OS-thread join moved to the blocking pool.
    spawn_blocking(move || thread.join()).await
}
```

**Rationale:** Blocking an executor thread delays unrelated futures.
