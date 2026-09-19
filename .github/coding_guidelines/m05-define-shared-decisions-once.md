# M5. Define shared decisions once

Define a shared default, mapping, limit or policy once and derive every use
from that decision, so a change cannot leave competing versions behind.
Similarity alone does not establish a shared decision. Keep cases separate when
they may evolve independently instead of introducing a helper or macro merely
to combine snippets that look alike.

**Example:** The [protocol core](../../crates/protocol/src/core.rs) gives
presence updates and broadcasts the same sending policy. Duplicated checks can
drift so one succeeds while the other is silently dropped.

**Avoid**

```rust
// In update_info, authenticated clients may send before becoming connected.
if !matches!(
    self.phase,
    ProtocolPhase::Authenticated(_) | ProtocolPhase::Connected(_)
) {
    return Vec::new();
}
// In broadcast, the duplicated rule accidentally requires a connected client.
if !matches!(self.phase, ProtocolPhase::Connected(_)) {
    return Vec::new();
}
```

**Prefer**

```rust
impl ProtocolPhase {
    const fn can_send_client_messages(&self) -> bool {
        matches!(self, Self::Authenticated(_) | Self::Connected(_))
    }
}
// Both update_info and broadcast use the same decision.
if !self.phase.can_send_client_messages() {
    return Vec::new();
}
```

**Rationale:** A shared definition makes a policy change complete in one place.
Keeping independent cases separate avoids accidental coupling.
