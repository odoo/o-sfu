# A2. Hide ordered steps behind one operation

Present validation, mutation, effects and cleanup as one operation whenever
their order affects behavior. Keep the individual steps inside the module that
enforces the sequence and define what commits on success, survives failure or
becomes externally visible.

The operation owner must verify every ordering requirement without promising
incidental internal order. Rely only on documented iteration, scheduling or
completion guarantees and preserve the operation's contract if cancellation
occurs at an `.await`. See [O4](o04-make-cancellation-behavior-explicit.md).

> [!NOTE]
> Further reading: **[caller assumptions in Google's Building Secure and Reliable Systems](https://google.github.io/building-secure-and-reliable-systems/raw/ch06.html#system_architecture)**.

**Example:** `UserOutboundSender::send` owns the full enqueue operation:
reserving byte capacity, sending the message, releasing the reservation on
failure and signaling overflow when a capacity limit is exceeded.

**Avoid**

```rust
// Manual sequencing can leak byte capacity and bypass overflow signaling.
let bytes = outbound.queued_bytes();
sender.reserve_bytes(bytes)?;
sender.messages.try_send(QueuedUserOutbound { outbound, bytes })?;
```

**Prefer**

```rust
// `send` owns capacity accounting, enqueue cleanup and overflow signaling.
sender.send(outbound)?;
```

**Rationale:** Correct sequencing belongs to the operation owner, where every
caller benefits from the same guarantees.
