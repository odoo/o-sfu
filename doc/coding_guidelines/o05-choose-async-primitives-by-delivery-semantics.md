# O5. Match async primitives to delivery semantics

Choose an async primitive by the producer and receiver count, required ordering,
retained values and overload behavior. Use bounded `mpsc` for an ordered queue
with one consumer, `watch` for replaceable state and `oneshot` for a single
result. Use `broadcast` only with a policy for receivers that fall behind.

Because `Notify` carries no data, store the condition or work separately and
check it in a loop before waiting. `notify_one` retains at most one permit while
`notify_waiters` leaves none for future waiters. When concurrent consumers pair
shared work with `notify_one`, create, pin and enable each `Notified` before
checking the work, as described in Tokio's [`Notify`
contract](https://docs.rs/tokio/latest/tokio/sync/struct.Notify.html).

Coalesce updates only when they are replaceable and share every semantic key.
Preserve each ordered transition, acknowledgement and effect whose occurrence
matters.

> [!NOTE]
> Further reading: **[the `tokio::sync` overview](https://docs.rs/tokio/latest/tokio/sync/index.html#message-passing)**.

**Example:** `UserOutboundSender` uses bounded `mpsc` for ordered output and
`watch` for the latest terminal overflow state.

**Avoid**

```rust
// `watch` would overwrite ordered output before the receiver observes it.
messages: watch::Sender<Option<QueuedUserOutbound>>,
// Repeated terminal overflow snapshots do not need an ordered queue.
overflow: mpsc::Sender<UserOutboundOverflow>,
```

**Prefer**

```rust
// Every accepted output stays ordered until received or discarded by policy.
messages: mpsc::Sender<QueuedUserOutbound>,
// Only the latest terminal overflow state matters.
overflow: watch::Sender<Option<UserOutboundOverflow>>,
```

**Rationale:** A primitive's delivery guarantees become part of the application's
behavior. Choosing the wrong guarantees can lose, duplicate or accumulate work.
