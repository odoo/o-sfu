# P1. Keep function inputs and outputs minimal

Choose inputs that express how a function uses its data: a slice for indexed
or repeated borrowed access, `impl IntoIterator` for a single pass and a
concrete collection when its storage, ownership or ordering is part of the
contract. Apply the same principle to outputs, returning an iterator for
traversal or a slice for repeated borrowed access. Return an owned collection
only when the caller needs more than traversal.

> [!NOTE]
> Further reading: **[generic parameters in the Rust API Guidelines](https://rust-lang.github.io/api-guidelines/flexibility.html#functions-minimize-assumptions-about-parameters-by-using-generics-c-generic)**.
>
> Related lints: [needless_pass_by_ref_mut](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#needless_pass_by_ref_mut),
> [pedantic::large_types_passed_by_value](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#large_types_passed_by_value),
> [pedantic::needless_pass_by_value](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#needless_pass_by_value),
> [pedantic::trivially_copy_pass_by_ref](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#trivially_copy_pass_by_ref),
> [pedantic::unnecessary_box_returns](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unnecessary_box_returns),
> [pedantic::unnecessary_wraps](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unnecessary_wraps)
> and [style::ptr_arg](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#ptr_arg).

**Example:** `SourcePolicySignal::mark_dirty_rooms` accepts
`impl IntoIterator<Item = RoomInstanceId>`, so callers can pass an array or an
iterator without allocating a `Vec`.

**Avoid**

```rust
// A Vec adds a heap allocation even though the callee only iterates.
self.mark_dirty_rooms(vec![room_instance_id]);
```

**Prefer**

```rust
// An array satisfies IntoIterator without a heap allocation.
self.mark_dirty_rooms([room_instance_id]);
```

Create a parameter type only for a domain concept. Do not hide a clear
signature in a broad context or configuration type.

Return only what the caller needs. Keep state, guards and adapters behind the
function boundary when a value or snapshot is enough.

**Rationale:** Minimal input requirements improve reuse and avoid costs or
coupling that the function does not need.
