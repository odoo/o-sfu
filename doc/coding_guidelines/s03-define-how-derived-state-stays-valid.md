# S3. Define how derived state stays valid

Derived state includes counters, indexes, snapshots, caches and decisions
computed from authoritative state. Recompute it when the cost is low.
If it must be stored, define its source and derivation, what updates or
invalidates it, when stale values may be used and how to rebuild or revalidate
it.

Use [M3](m03-choose-the-simplest-clear-design.md) to justify storage,
[C4](c04-put-each-invariant-in-its-lowest-owner.md) to assign ownership and
[R6](r06-bound-externally-driven-work.md) for externally driven retention.

Treat lossy keys, digests and summaries as candidate filters, verifying the
underlying value before a false positive can change behavior.

> [!NOTE]
> Further reading: **[cache invalidation](https://en.wikipedia.org/wiki/Cache_invalidation)** and **[hash collisions](https://en.wikipedia.org/wiki/Hash_collision)**.

**Example:** `PacketLoopRoutingMissCache` retains negative routing decisions for
the current topology, so callers clear it when routing inputs change. A matching
fingerprint must still be confirmed by comparing the exact packet bytes.

**Avoid**

```rust
// A fingerprint collision would suppress a different packet.
cache.iter().any(|entry| entry.key == key)
```

**Prefer**

```rust
// Exact bytes make collisions cost one comparison rather than correctness.
cache
    .iter()
    .any(|entry| entry.key == key && entry.packet.as_slice() == packet)
```

**Rationale:** Storing derived state creates an obligation to keep it consistent
with its source. Stale values and false matches can otherwise contradict the
authoritative state.
