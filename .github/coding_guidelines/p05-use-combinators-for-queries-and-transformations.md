# P5. Use combinators for queries and transformations

Prefer `Iterator`, `Option` and `Result` combinators when they express a query
or transformation more clearly than manual loops, accumulators or indexing. A
short chain of methods such as `find`, `filter_map` and `collect` lets the
reader follow the data without tracking (/allocating) temporary state.

Use a `for` loop when ordering, mutation of related state, `.await` or
branching carries meaning that a chain would hide. Reserve `map` for
transformations, never solely for effects. For `Option` and `Result`, choose
the intended behavior: `map` and `and_then` preserve `None` or `Err`, while
`or_else` and `unwrap_or_else` handle them with a fallback.

> [!NOTE]
> Further reading: **[`Iterator`](https://doc.rust-lang.org/std/iter/trait.Iterator.html)**,
> **[`Option`](https://doc.rust-lang.org/std/option/index.html)** and
> **[`Result`](https://doc.rust-lang.org/std/result/index.html)**.
>
> Related lints: [option_if_let_else](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#option_if_let_else),
> [single_option_map](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#single_option_map),
> [useless_let_if_seq](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#useless_let_if_seq),
> [complexity::bind_instead_of_map](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#bind_instead_of_map),
> [complexity::manual_filter](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_filter),
> [complexity::manual_filter_map](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_filter_map),
> [complexity::manual_find](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_find),
> [complexity::manual_find_map](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_find_map),
> [complexity::option_map_unit_fn](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#option_map_unit_fn),
> [complexity::result_map_unit_fn](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#result_map_unit_fn),
> [pedantic::needless_for_each](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#needless_for_each),
> [style::manual_map](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_map)
> and [style::manual_ok_or](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_ok_or).

**Example 1 (Iterator query):** `MediaCodecCapability::rtx_associated_payload_type_id`
returns the first RTX association.

**Avoid**

```rust
let mut association = None;
for setting in &self.settings {
    if let CodecSetting::RtxAssociation(payload_type) = setting {
        // Mutable state and `break` only encode a first-match query.
        association = Some(*payload_type);
        break;
    }
}
association
```

**Prefer**

```rust
// `find_map` states the first matching RTX association directly.
self.settings.iter().find_map(|setting| match setting {
    CodecSetting::RtxAssociation(payload_type) => Some(*payload_type),
    _ => None,
})
```

**Example 2 (Optional values):** `and_then` connects steps that may produce no
value. Here missing and malformed claims intentionally have the same outcome:
`.ok()` discards the parse error. Preserve the `Result` when callers need to
distinguish those cases, as required by [C5](c05-preserve-meaningful-outcomes.md).

**Avoid**

```rust
// Nested matching pyramids obscure linear data flow.
let room_id = match get_token(header) {
    Some(token) => match parse_claims(token) {
        Ok(claims) => claims.room_id,
        Err(_) => None,
    },
    None => None,
};
```

**Prefer**

```rust
// Monadic chaining models the linear transformation directly.
let room_id = get_token(header)
    .and_then(|token| parse_claims(token).ok())
    .and_then(|claims| claims.room_id);
```

`parse_codec_list` correctly keeps a loop because each iteration validates
against previously accepted codecs and may return a distinct error.

**Rationale:** The control structure should make the operation's purpose and
failure behavior easy to follow.
