# P4. Keep decision logic pure

Keep parsing, calculations and policy decisions pure: equal inputs should
yield equal results without changing externally visible state. Local mutation
is compatible with purity when it cannot affect state outside the computation.
Pass time and external facts as inputs so the result does not depend on hidden
observations.

Let orchestration apply the returned decisions, values or effect plans through
I/O, shared-state mutation, task spawning and telemetry.
[P2](p02-make-dependencies-and-side-effects-explicit.md) covers explicit
dependencies and [O2](o02-make-asynchronous-commit-boundaries-explicit.md)
covers the boundary for applying effects.

> [!NOTE]
> Further reading: **[pure functions](https://en.wikipedia.org/wiki/Pure_function)** and **[referential transparency](https://en.wikipedia.org/wiki/Referential_transparency)**.

**Example:** `SourcePolicyTransaction::plan` derives room state updates and
transport effects from snapshots. `SourcePolicyTransaction::commit` applies
those effects after planning.

**Avoid**

```rust
async fn decide_source_policy(
    room: &Room,
    media_transport: &MediaTransport,
    sessions: &[TransportSessionKey],
    active_speakers: &[ActiveSpeakerSource],
) -> bool {
    // Decision code performs external observations and applies its own effects.
    let receiver_bandwidth = media_transport.receiver_bandwidth_snapshot(sessions);
    let source_bitrate = media_transport.transport_bitrate_snapshot(sessions);
    let state = room.state.read().await;
    let Some(transaction) = SourcePolicyTransaction::plan(
        &state,
        active_speakers,
        &receiver_bandwidth,
        &source_bitrate,
    ) else {
        return false;
    };
    transaction.commit(room, media_transport).await;
    true
}
```

**Prefer**

```rust
let transaction = {
    // Build the transaction from snapshots while the state guard is scoped.
    let state = room.state.read().await;
    SourcePolicyTransaction::plan(
        &state,
        active_speakers,
        &receiver_bandwidth,
        &source_bitrate,
    )
};
let Some(transaction) = transaction else {
    return false;
};
// Apply effects only after planning and the state read guard are complete.
transaction.commit(room, media_transport).await;
```

**Rationale:** Pure functions are easier to test, reason with and prove because their
behavior depends only on their arguments.
