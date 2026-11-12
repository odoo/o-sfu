# R3. Keep tests and proofs out of production files

Keep verification bodies in separate files: tests and their helpers belong in
the nearest `TESTS/`, private-state Kani proofs in the nearest `PROOFS/` behind
`#[cfg(kani)]` and public-API proofs in
[`tests/proofs/`](../../tests/proofs/). Rustdoc examples remain beside their
APIs, but production crates must not depend on `o-sfu-proofs`.

`TESTS/` holds code compiled only under `cfg(test)`. Support that another
crate compiles through a feature or `cfg(fuzzing)` lives in a support module
named `test_support`, `benchmark_support` or `fuzz_support`. It is a child of
the production module whose private state it reaches and is declared once,
gated on what its callers enable.

Keep test helpers with the tests and use the production API wherever possible.
When a test needs access to internal state, expose only what it needs without
changing production behavior.

**Layout:**

```text
o-sfu/
|-- crates/core/src/engine/media_transport/rtc/
|   |-- codec/
|   |   |-- vp8.rs             (production)
|   |   |-- TESTS/
|   |   |   `-- vp8.rs         (unit tests)
|   |   `-- PROOFS/
|   |       `-- vp8.rs         (private-state Kani proofs)
|   `-- worker/
|       |-- mod.rs             (production)
|       `-- test_support.rs    (hooks for test crates, behind a feature)
`-- tests/
    `-- proofs/                (proofs over public APIs)
```

**Example:** `vp8.rs` links its test and proof modules without containing their
bodies. `worker/mod.rs` declares the hooks that test crates reach through the
`testing-transport` feature.

**Avoid**

```rust
// The cfg gate excludes verification from runtime builds but leaves it in this file.
#[cfg(test)]
mod tests {
    #[test]
    fn inline_test_body() {}
}

#[cfg(kani)]
mod proofs {
    #[kani::proof]
    fn inline_proof_body() {}
}
```

**Prefer**

```rust
// Path modules retain private-item access without inline verification bodies.
#[cfg(kani)]
#[path = "PROOFS/vp8.rs"]
mod proofs;

#[cfg(test)]
#[path = "TESTS/vp8.rs"]
mod tests;

// Unit tests share the support that the feature exposes to other crates.
#[cfg(any(test, feature = "testing-transport"))]
mod test_support;
```

**Rationale:** Separate files keep production modules focused and keep
verification scaffolding out of runtime code. This also allows easy exclusion
of tests when grepping files. A file under `TESTS/` never enters the build of
another crate.
