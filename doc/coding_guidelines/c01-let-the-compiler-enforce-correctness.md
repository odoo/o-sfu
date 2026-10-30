# C1. Let the compiler enforce correctness

Make invalid values, states and operations unrepresentable.
Give distinct concepts their own newtypes even when they
share a representation and validate construction through private fields.
Represent exclusive states and mode-dependent fields with enums, oly use
`bool` for isolated properties and `bitflags` for orthogonal, freely composable
options.

The same principle applies to permissions and ownership. Express capabilities
through types such as a cloneable sender and a single-consumer receiver, read
and write handles or generational keys for recycled slots.

> [!NOTE]
> Further reading: **[the newtype pattern in The Rust Book](https://doc.rust-lang.org/book/ch20-03-advanced-types.html#type-safety-and-abstraction-with-the-newtype-pattern)** and **[typestate programming in The Embedded Rust Book](https://docs.rust-embedded.org/book/static-guarantees/typestate-programming.html)**.
>
> Related lints: [pedantic::struct_excessive_bools](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#struct_excessive_bools).

**Example:** `PublishedSources::source` and
`PublishedSourceDescriptor::encoding` accept distinct ID types, preventing
source and encoding IDs from being interchanged. `SourceSelector` gives each
selection mode its own variant:

**Avoid**

```rust
// These fields allow contradictory states such as `true` with `Some(_)`.
struct SourceSelector {
    open: bool,
    encoding_id: Option<u64>,
}
```

**Prefer**

```rust
// Every variant represents one legal selection mode.
pub enum SourceSelector {
    Open,
    Encoding(SourceEncodingId),
}
```

Use typestate only when it simplifies validation at call sites. See the [Rust API
Guidelines](https://rust-lang.github.io/api-guidelines/dependability.html#functions-validate-their-arguments-c-validate).

**Rationale:** Strong types carry guarantees that callers would otherwise have
to reconstruct and check themselves. Or worse: guessing.
