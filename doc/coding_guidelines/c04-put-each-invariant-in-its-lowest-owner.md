# C4. Enforce invariants where the state lives

Enforce each invariant in the lowest layer that controls every update. This
places decoding, bounds and credential checks at ingress, authorization and
transitions with state owners and translation of network or storage failures
in adapters. Callers rely on these guarantees without repeating checks, while
one owner updates related records, indexes and derived views together.

Keep identities in the layers that define them. Room source IDs
(`PublishedSourceId`, `SourceEncodingId`), negotiated publisher values (`Mid`,
`Rid`, `Ssrc`), transport IDs and receiver-local handles, sequence numbers and
timestamps have distinct scopes. Translate only at a boundary that owns both
layers and never substitute a negotiated, worker-local or receiver-local value
for a room source ID.

> [!NOTE]
> Further reading: **[private struct fields in the Rust API Guidelines](https://rust-lang.github.io/api-guidelines/future-proofing.html#structs-have-private-fields-c-struct-private)**.

**Example:** `PublishedSourceDescriptor::new` rejects empty, duplicate or
cross-source encodings. `PublishedSources` updates its map and indexes together.

**Avoid**

```rust
// This repeats the empty check but misses duplicate and cross-source encodings.
if parts.encodings.is_empty() {
    return Err(SourceModelError::SourceWithoutEncodings {
        source_id: parts.source_id,
    });
}
let descriptor = PublishedSourceDescriptor::new(parts)?;
```

**Prefer**

```rust
// Successful construction proves every descriptor invariant.
let descriptor = PublishedSourceDescriptor::new(parts)?;
```

**Rationale:** Keeping each invariant with its state prevents callers from
enforcing different versions of the same contract.
