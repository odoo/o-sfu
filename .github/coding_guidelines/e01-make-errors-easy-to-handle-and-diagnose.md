# E1. Preserve error categories and context

Give callers concrete errors defined with
[`thiserror`](https://docs.rs/thiserror/latest/thiserror/) when they need to
match failure categories. Map dependency errors at boundaries so their types
stay out of caller-facing domain APIs. Use automatic `#[from]` conversion
only when every error of the source type belongs in the same caller-visible
category. Otherwise, construct the variant explicitly and retain its cause
with `#[source]`.

Where failures are reported rather than matched, such as startup and
configuration, use [`anyhow`](https://docs.rs/anyhow/latest/anyhow/). Add
`Context` when it identifies an operation or resource the existing error does
not name, keeping chains short and stating each fact once.

> [!NOTE]
> Further reading: **[error types in the Rust API Guidelines](https://rust-lang.github.io/api-guidelines/interoperability.html#error-types-are-meaningful-and-well-behaved-c-good-err)**, **[`thiserror` attributes](https://docs.rs/thiserror/latest/thiserror/#details)**, **[`anyhow::Context`](https://docs.rs/anyhow/latest/anyhow/trait.Context.html)** and **[`Error::source` in the standard library](https://doc.rust-lang.org/std/error/trait.Error.html#error-source)**.
>
> Related lints: [map_err_ignore](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#map_err_ignore)
> and [style::result_unit_err](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#result_unit_err).

**Example:** `RoomManagerJoinError` gives callers variants to match, while
`Env::var(...).required()` uses `anyhow::Context` to identify a missing variable.

**Typed domain error**

```rust
// Callers match variants instead of parsing display strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RoomManagerJoinError {
    #[error("room not found")]
    MissingRoom,
    #[error("room is full")]
    RoomFull,
    #[error("router state error")]
    RouterState,
}
```

**Boundary context**

```rust
// The report names the exact setting that blocked startup.
let value = self
    .load()
    .with_context(|| format!("{} env variable is required", self.key))?;
```

**Rationale:** Typed errors let callers respond correctly while contextual
reports make failures diagnosable.
