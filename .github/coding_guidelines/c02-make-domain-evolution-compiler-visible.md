# C2. Handle behavior-changing variants and fields explicitly

Match every behavior-changing variant explicitly so additions prompt a review
of the affected behavior. Use `_` only for irrelevant omitted cases, including
future variants. When an external `#[non_exhaustive]` enum requires a wildcard,
reject or report unsupported variants explicitly.

Reserve `#[non_exhaustive]` for public types intended to grow without breaking
downstream crates. The attribute has no effect within the defining crate.

> [!NOTE]
> Further reading: **[the non_exhaustive attribute in the Rust Reference](https://doc.rust-lang.org/reference/attributes/type_system.html#the-non_exhaustive-attribute)**.
>
> Related lints: [pedantic::match_wildcard_for_single_variants](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#match_wildcard_for_single_variants)
> and [style::manual_non_exhaustive](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_non_exhaustive).

**Example:** An exhaustive `StreamType` match requires a decision for every
variant.

**Avoid**

```rust
// A future variant silently inherits camera behavior.
let (active, layout) = match stream_type {
    StreamType::Audio => (states.audio, None),
    _ => (states.camera, states.camera_layout),
};
```

**Prefer**

```rust
// A future variant cannot compile until its behavior is chosen.
let (active, layout) = match stream_type {
    StreamType::Audio => (states.audio, None),
    StreamType::Camera => (states.camera, states.camera_layout),
    StreamType::Screen => (states.screen, states.screen_layout),
};
```

Destructure a struct without `..` when adding a field should prompt review of
its users, following Canonical's [pattern matching discipline](https://canonical.github.io/rust-best-practices/pattern-matching-discipline.html#exhaustively-match-to-draw-attention).

**Rationale:** Exhaustive handling makes the compiler identify decisions that
need attention when the model changes.
