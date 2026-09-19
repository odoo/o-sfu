# R9. Prefer `expect` for lint exceptions

Fix genuine lint problems before considering an exception. When a finding is
intentional, prefer `#[expect(...)]` so `unfulfilled_lint_expectations` can
report an exception that is no longer needed.

Use `#[allow(...)]` where the lint need not appear in every build, such as
configuration-dependent code or a deliberate crate or module policy. Keep
either attribute as narrow as possible and name the exact lints it covers
instead of a group.

Every exception needs a `reason = "..."` that explains why the code is correct
and why following the lint would make it worse. The reason should justify the
decision without merely restating the lint's name.

> [!NOTE]
> Further reading: **[lint attributes in the Rust Reference](https://doc.rust-lang.org/reference/attributes/diagnostics.html#lint-check-attributes)** and **[`allow_attributes` in Clippy](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#allow_attributes)**.
>
> Related lints: [allow_attributes_without_reason](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#allow_attributes_without_reason).

**Example:** `AvpStaticPayloadType` uses `#[repr(u8)]`, which makes its
conversion to `u8` lossless. The lint exception is local to `as_u8`:

**Avoid**

```rust
// This remains silent if the cast stops triggering the lint.
#[allow(clippy::as_conversions)]
pub const fn as_u8(self) -> u8 {
    self as u8
}
```

**Prefer**

```rust
// A removed cast leaves an unfulfilled expectation, so this exception expires.
#[expect(
    clippy::as_conversions,
    reason = "repr(u8) makes this enum-to-u8 cast lossless"
)]
pub const fn as_u8(self) -> u8 {
    self as u8
}
```

The [core-room integration tests](../../tests/core-room/tests/core_room.rs)
allow panic-based assertions as a test policy. No particular panic site must
remain:

```rust
// Any test may panic on failure, but no specific panic is expected to remain.
#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]
```

**Rationale:** Lint exceptions are easy to forget. `expect` exposes obsolete
exceptions and a specific `reason` lets reviewers judge the remaining tradeoff.
