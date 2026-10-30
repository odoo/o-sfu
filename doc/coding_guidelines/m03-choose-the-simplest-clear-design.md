# M3. Choose the simplest clear design

Prefer straightforward code with few concepts and special cases, using clear
names to make non-obvious decisions easy to follow.

Before adding complexity for performance, profile the affected path and confirm
a repeatable gain with realistic benchmarks. Even a measured speedup may not
justify code that is harder to understand, verify or maintain. Keep the simpler
design when the benefit does not outweigh that cost. See
[R5](r05-keep-media-hot-paths-cheap.md).

> [!NOTE]
> Further reading: **[complexity in Google's engineering practices](https://google.github.io/eng-practices/review/reviewer/looking-for.html#complexity)**.
>
> Related lints: [cognitive_complexity](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#cognitive_complexity),
> [complexity::excessive_nesting](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#excessive_nesting),
> [complexity::type_complexity](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#type_complexity)
> and [pedantic::too_many_lines](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#too_many_lines).

**Example:** For `u32` values, a bit trick can select the smaller value, but
[`min`](https://doc.rust-lang.org/std/cmp/trait.Ord.html#method.min) expresses
the same operation directly.

**Avoid**

```rust
let mask = 0_u32.wrapping_sub(u32::from(requested < available));
let granted = available ^ ((requested ^ available) & mask);
```

**Prefer**

```rust
let granted = requested.min(available);
```

**Rationale:** Complexity has a lasting maintenance cost. Performance work must
earn that cost through a benefit that matters in practice.
