# A1. Use booleans only for clear facts

A boolean suits an independent fact whose meaning is clear from the function
and parameter names. When an argument selects a mode, policy or behavior, use
separate operations or a semantic enum so the choice remains explicit at the
call site.

> [!NOTE]
> Further reading: **[custom argument types in the Rust API Guidelines](https://rust-lang.github.io/api-guidelines/type-safety.html#c-custom-type)**.
>
> Related lints: [pedantic::fn_params_excessive_bools](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#fn_params_excessive_bools)
> and [pedantic::struct_excessive_bools](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#struct_excessive_bools).

**Example:** `FlushMode` makes callers of `OutboundBatcher::enqueue`
state whether an envelope must be sent immediately or may join a batch.

**Avoid**

```rust
// `true` does not say whether the response is sent now or may be batched.
batcher.enqueue(envelope, true)
```

**Prefer**

```rust
// The response must be sent now instead of joining a later batch.
batcher.enqueue(envelope, FlushMode::Immediate)
```

**Rationale:** Named choices let a reader understand a call without looking up
what `true` or `false` means.
