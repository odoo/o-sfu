# S2. Choose the right synchronzation primitives

Prefer one lock for fields that must remain consistent. Atomics suit
independent scalars, but dependent values require an explicit publication
protocol. Atomicity of individual operations does not guarantee a coherent
snapshot.

Use a generation, retry or release/acquire protocol only when hot-path
measurements justify it. Keep the protocol within one owner that exposes only
values satisfying the invariant. A publication protocol involving multiple
atomics must document writer and reader order, memory ordering and valid
combinations of values.

> [!NOTE]
> Further reading: **[linearizability](https://en.wikipedia.org/wiki/Linearizability)**, **[atomicity and concurrency in Rust](https://doc.rust-lang.org/nomicon/concurrency.html)** and **[atomic memory orderings in Rust](https://doc.rust-lang.org/std/sync/atomic/enum.Ordering.html)**.
>
> Related lints: [correctness::let_underscore_lock](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#let_underscore_lock),
> [perf::readonly_write_lock](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#readonly_write_lock),
> [style::mut_mutex_lock](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#mut_mutex_lock)
> and [suspicious::await_holding_lock](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#await_holding_lock).

**Example:** `PacketLoopDelaySnapshot` stores `delay_ms` before a `Release` store
to `next_deadline_elapsed_ms`. Readers load the deadline with `Acquire` before
loading the published delay. This guarantees that a new deadline is not paired
with an older delay. Use one lock when every field must come from one update.

```rust
// The Release store publishes the preceding Relaxed delay store.
self.delay_ms
    .store(delay_ms.unwrap_or(NO_HEARTBEAT), Ordering::Relaxed);
self.next_deadline_elapsed_ms
    .store(self.elapsed_ms(next_deadline), Ordering::Release);

// When Acquire sees a published deadline, the next load sees its delay or newer.
let next_deadline_elapsed_ms = self
    .next_deadline_elapsed_ms
    .load(Ordering::Acquire);
let delay_ms = self.delay_ms.load(Ordering::Relaxed);
```

**Avoid**

```rust
struct SessionState {
    // Readers can observe a new revision before the snapshot updates.
    revision: AtomicU64,
    snapshot: Mutex<TrackSnapshot>,
}
```

**Prefer**

```rust
struct SessionState {
    // Revision and snapshot update together as one coherent value.
    snapshot: Mutex<VersionedSnapshot>,
}
```

**Rationale:** A lock can protect a complete state transition. An atomic
protocol provides only its documented ordering or validation guarantee, which
may permit readers to observe values from different updates.
