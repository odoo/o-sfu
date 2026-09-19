# R3. Keep tests and proofs out of production files

Keep verification bodies in separate files: tests and support belong in the
nearest `TESTS/`, private-state Kani proofs in the nearest `PROOFS/` behind
`#[cfg(kani)]` and public-API proofs in
[`tests/proofs/`](../../tests/proofs/). Rustdoc examples remain beside their
APIs, but production crates must not depend on `o-sfu-proofs`.

Keep test helpers with the tests and use the production API wherever possible.
When a test needs access to internal state, expose only what it needs without
changing production behavior.

**Layout:**

```text
o-sfu/
|-- crates/core/src/engine/media_transport/rtc/codec/
|   |-- vp8.rs                 (production)
|   |-- TESTS/
|   |   `-- vp8.rs             (unit tests)
|   `-- PROOFS/
|       `-- vp8.rs             (private-state Kani proofs)
`-- tests/
    `-- proofs/                (proofs over public APIs)
```

**Example:** `vp8.rs` links its test and proof modules without containing their
bodies.

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
```

**Rationale:** Separate files keep production modules focused and keep
verification scaffolding out of runtime code. This also allows easy exclusion
of tests when grepping files.
