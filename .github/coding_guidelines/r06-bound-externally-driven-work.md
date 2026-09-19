# R6. Limit work triggered by external input

Give externally driven work explicit limits on count, bytes, time and distinct
identities, including work performed inside dependencies. Every queue, batch,
retry, loop and retained collection also needs a policy for exhaustion:
rejection, disconnection, dropping, coalescing or backpressure.

Bound retained state by capacity and lifetime unless another limit proves its
maximum. Reserve capacity before accepting work and keep the reservation for
as long as that capacity is in use. Release or expire reservations, permits,
per-origin buckets and cache entries when their work or lifetime ends.

Cap each loop turn so shutdown and control work are reconsidered regularly,
since `.await` and `yield_now()` do not guarantee fairness. In a biased
`select!`, place shutdown and control branches before high-volume input.
Replace recursion whose depth depends on external input with bounded work
lists.

> [!NOTE]
> Further reading: **[handling overload in Google's SRE Book](https://sre.google/sre-book/handling-overload/)** and **[fairness in `tokio::select!`](https://docs.rs/tokio/latest/tokio/macro.select.html#fairness)**.

**Example:** The user outbound queue limits both message count and queued bytes.

**Avoid**

```rust
// A peer can grow this queue without limit.
let (outbound_tx, outbound_rx) = tokio::sync::mpsc::unbounded_channel();
```

**Prefer**

```rust
// `UserOutboundSender::send` rejects output before either limit is exceeded.
let limits = UserOutboundQueueLimits::new(
    outbound_queue_capacity,
    outbound_queue_byte_capacity,
);
let (outbound_tx, outbound_rx) =
    UserOutboundSender::channel_with_limits(limits, metrics);
```

**Rationale:** Without limits, one source can consume memory or CPU needed by
everyone else.
