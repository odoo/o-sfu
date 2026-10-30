# C5. Do not discard meaningful outcomes

Use `#[must_use]` on a function or type when ignoring a value would skip a
state change, cleanup or decision. Handle or propagate `Result` and `Option`,
preserving error distinctions that callers need. Use `.ok()` only when the
contract deliberately treats failure as absence and no caller needs the error.
Make the outcome clear in its type: use enums for distinct alternatives and
named types for grouped values or ambiguous booleans, reserving tuples for
small groups with obvious meanings.

In core lifecycle types that require explicit completion, pair `#[must_use]`
with an [`Obligation`](https://odoo.github.io/o-sfu/o_sfu_core/engine/obligation/index.html).
Rust permits dropping a named value and the lint cannot detect cancellation
after work starts. The guard diagnoses abandoned cleanup, staged publication
and effect batches.

Keep the guard with pending work across awaits and ownership transfers. Call
`complete()` only after the defined terminal outcome, including required
failure handling. Creating or enqueueing a cleanup command does not complete
the work. The guard reports unfinished drops but performs no cleanup and does
not make cancellation safe. Its rustdoc explains the completion discipline and
provides usage examples.

When the contract allows ignoring a fresh result, `let _ = expression`
discards it without retaining the value. An `_`-prefixed binding instead keeps
a value such as a lock guard until the end of its scope. Use `drop(value)` for
early destruction of an existing named value and explain non-obvious lifetime
choices.

> [!NOTE]
> Further reading: **[the `must_use` attribute](https://doc.rust-lang.org/stable/core/attribute.must_use.html)**,  **[ignored values in Rust patterns](https://doc.rust-lang.org/book/ch19-03-pattern-syntax.html#ignoring-values-in-a-pattern)**, **[Asynchronous Cleanup by Without Boats](https://without.boats/blog/asynchronous-clean-up/)**.
>
> Related lints: [unused_result_ok](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unused_result_ok),
> [correctness::let_underscore_lock](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#let_underscore_lock),
> [pedantic::must_use_candidate](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#must_use_candidate)
> and [suspicious::let_underscore_future](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#let_underscore_future).

**Example:** Discarding `RoomEffects` skips the batch's transport, output and
source-policy effects. `#[must_use]` warns when the batch is ignored and an
obligation detects dropping a nonempty batch before those effects finish.
`RoomEffects::default()` carries no obligation.

```rust
#[must_use = "room effect batches must be executed after the state transition commits"]
pub struct RoomEffects {
    transport: RoomTransportPlan,
    output: RoomOutputPlan,
    source_policy: SourcePolicyTurn,
    obligation: Option<Obligation>,
}
```

**Rationale:** Explicit handling makes unfinished work and required decisions
visible at the call site.
