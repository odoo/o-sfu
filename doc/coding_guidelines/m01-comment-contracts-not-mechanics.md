# M1. Comment contracts, not mechanics

Use rustdoc to explain meaning and contracts that names, types and signatures
cannot convey. Choose `//!` for subsystems, `///` for items and `//` for local
reasoning beside the governed code. Name the relevant identifiers and explain
why an invariant rules out a plausible alternative, without narrating the code.

Failure behavior belongs in that contract. Rust public and boundary APIs need
`# Errors` for every caller-visible error condition and its concrete error type,
plus `# Panics` for every reachable panic. TypeScript public APIs use `@throws`
for exceptions and describe rejection conditions when returning a promise.
Omit empty sections and prose that merely repeats the return type.

> [!NOTE]
> Further reading: **[failure documentation in the Rust API Guidelines](https://rust-lang.github.io/api-guidelines/documentation.html#function-docs-include-error-panic-and-safety-considerations-c-failure)**, **[the rustdoc writing guide](https://doc.rust-lang.org/rustdoc/how-to-write-documentation.html)**, **[TypeDoc's `@throws` tag](https://typedoc.org/documents/Tags._throws.html)** and **[Google's code review guidance on comments](https://google.github.io/eng-practices/review/reviewer/looking-for.html#comments)**.
>
> Related lints: [pedantic::missing_errors_doc](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#missing_errors_doc),
> [pedantic::missing_panics_doc](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#missing_panics_doc)
> and [style::missing_safety_doc](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#missing_safety_doc).

**Example:** The comment on `next_generation` explains the reserved value that
requires `.max(1)` after wraparound.

**Avoid**

```rust
fn next_generation(generation: u64) -> u64 {
    // Increment the generation and keep it non-zero.
    generation.wrapping_add(1).max(1)
}
```

**Prefer**

```rust
fn next_generation(generation: u64) -> u64 {
    // Keep generation 0 reserved for invalid handles after wraparound.
    generation.wrapping_add(1).max(1)
}
```

Add `# Safety` when callers must uphold unsafe preconditions and `# Examples`
when an example prevents likely misuse. Document ordering, cancellation,
protocol, compatibility, safety and performance constraints at the boundary
they govern, explaining performance choices only when their reason is not
obvious from the code.

**Rationale:** A useful comment preserves the reasoning a future change must
respect. Repeating the code creates another account that can become stale.
