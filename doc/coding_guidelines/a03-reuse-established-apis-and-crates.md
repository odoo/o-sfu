# A3. Prefer existing APIs

Use established APIs to make a contract recognizable, preferring the standard
library when it is equally clear. Add a helper or dependency only when existing
APIs leave a concern uncovered.

In this repository, use `Config::from_env` and `Env::var` for configuration,
`thiserror` and `anyhow` for errors and owner APIs such as
`UserOutboundSender::channel_with_limits` for their domain operations. Use
[`itertools`](https://docs.rs/itertools/0.14/itertools/trait.Itertools.html) when
its adapters express an iterator rule more clearly.

Use standard traits for common contracts. In particular,
[`From`](https://doc.rust-lang.org/std/convert/trait.From.html#when-to-implement-from)
is appropriate only for infallible, lossless, value-preserving and obvious
conversions, while `TryFrom` expresses conversions that can fail.

> [!NOTE]
> Related lints: [complexity::manual_find](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_find),
> [perf::map_entry](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#map_entry),
> [style::from_over_into](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#from_over_into),
> [style::should_implement_trait](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#should_implement_trait)
> and [style::unnecessary_fallible_conversions](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unnecessary_fallible_conversions).

**Examples:** The RID parser uses `exactly_one` to enforce the single
restriction it supports when a restriction list is present.

**Avoid**

```rust
// The cardinality rule is spread across two cursor reads.
let mut parts = restrictions.split(sdp::rid_restriction::PARAMETER_SEPARATOR);
let restriction = parts.next().ok_or(SimulcastAnswerError)?;
if parts.next().is_some() {
    return Err(SimulcastAnswerError);
}
let restriction = restriction.trim();
```

**Prefer**

```rust
// `exactly_one` names and enforces the cardinality rule in one operation.
let restriction = restrictions
    .split(sdp::rid_restriction::PARAMETER_SEPARATOR)
    .exactly_one()
    .map_err(|_error| SimulcastAnswerError)?
    .trim();
```

[`BTreeMap::entry`](https://doc.rust-lang.org/std/collections/struct.BTreeMap.html#method.entry)
expresses insert-or-update directly.

**Avoid**

```rust
// The caller reimplements the map's occupied and vacant cases.
match batches.get_mut(&key) {
    Some(batch) => batch.push(item),
    None => {
        batches.insert(key, vec![item]);
    }
}
```

**Prefer**

```rust
// `entry` expresses insert-or-update without duplicating the map cases.
batches.entry(key).or_default().push(item);
```

**Rationale:** Familiar APIs reduce the amount of local behavior contributors
must learn and give them documentation they can already rely on.
