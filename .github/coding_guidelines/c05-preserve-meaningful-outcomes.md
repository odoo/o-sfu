# C5. Do not discard meaningful outcomes

Use `#[must_use]` on a function or type when ignoring a value would skip a
state change, cleanup or decision. Handle or propagate `Result` and `Option`,
preserving error distinctions that callers need. Use `.ok()` only when the
contract deliberately treats failure as absence and no caller needs the error.
Make the outcome clear in its type: use enums for distinct alternatives and
named types for grouped values or ambiguous booleans, reserving tuples for
small groups with obvious meanings.

When the contract allows ignoring a fresh result, `let _ = expression`
discards it without retaining the value. An `_`-prefixed binding instead keeps
a value such as a lock guard until the end of its scope. Use `drop(value)` for
early destruction of an existing named value and explain non-obvious lifetime
choices.

> [!NOTE]
> Further reading: **[the `must_use` attribute](https://doc.rust-lang.org/stable/core/attribute.must_use.html)** and **[ignored values in Rust patterns](https://doc.rust-lang.org/book/ch19-03-pattern-syntax.html#ignoring-values-in-a-pattern)**.
>
> Related lints: [unused_result_ok](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unused_result_ok),
> [correctness::let_underscore_lock](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#let_underscore_lock),
> [pedantic::must_use_candidate](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#must_use_candidate)
> and [suspicious::let_underscore_future](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#let_underscore_future).

**Example:** Discarding `RoomEffects` skips the batch's transport, output and
source-policy effects, so the type is marked `#[must_use]`.

```rust
// Dropping `RoomEffects` would skip execution of the ordered effect plans below.
#[must_use = "room effect batches must be executed after the state transition commits"]
pub struct RoomEffects {
    policy_before_transport: bool,
    transport: RoomTransportPlan,
    output: RoomOutputPlan,
    source_policy: SourcePolicyTurn,
}
```

**Rationale:** Explicit handling makes unfinished work and required decisions
visible at the call site.
