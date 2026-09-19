# E3. Validate external input before use

Check external input against clear requirements before relying on
it. Even well-formed data may contain an out-of-range value or request an
operation that is not allowed. Distinguish invalid input, unsupported requests
and internal failures so callers can respond appropriately.

Validate each logical input before mutating state, returning per-item outcomes
when partial success is intentional. External input must never be able to
trigger a panic through `panic!`, `.unwrap()`, `.expect()` or unchecked
indexing. Any scoped `#[expect(...)]` exception requires a locally proven
invariant documented in `reason`.

> [!NOTE]
> Further reading: **[argument validation in the Rust API Guidelines](https://rust-lang.github.io/api-guidelines/dependability.html#functions-validate-their-arguments-c-validate)** and **[fallible conversion with `TryFrom`](https://doc.rust-lang.org/std/convert/trait.TryFrom.html)**.
>
> Related lints: [expect_used](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#expect_used),
> [indexing_slicing](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#indexing_slicing),
> [panic](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#panic)
> and [unwrap_used](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unwrap_used).

**Example:** `decode_envelope_batch` validates JSON and batch size before
constructing an `EnvelopeBatch`.

**Avoid**

```rust
// Malformed peer input panics the task.
let batch = serde_json::from_str::<Vec<WireEnvelope>>(payload).unwrap();
```

**Prefer**

```rust
// Reject malformed or oversized input before it enters domain state.
let wire_batch = serde_json::from_str::<Vec<WireEnvelope>>(payload)
    .map_err(|_error| EnvelopeBatchDecodeError::InvalidJson)?;
if wire_batch.len() > limit {
    return Err(EnvelopeBatchDecodeError::BatchTooLarge {
        actual: wire_batch.len(),
        limit,
    });
}
```

**Rationale:** Validation turns assumptions about external data into explicit
guarantees before that data reaches domain state.
