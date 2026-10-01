# Tests

[Integration](integration/), [room scenarios](core-room/) and shared
[harnesses](src/support/) cover server behavior. Specialized targets live in
[benchmarks](benchmarks/), [Miri](miri/), [fuzz](fuzz/) and [proofs](proofs/).

## Local checks

Run from the repository root:

```bash
cargo +nightly fmt --all --check
cargo check --locked
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --release
npm --prefix crates/client run verify
```

## Callgrind benchmarks

Requires Valgrind and a `gungraun-runner` version matching `gungraun` in
[`Cargo.lock`](../Cargo.lock). These measure instruction and simulated cycle
costs, not throughput. [CI setup](../.github/workflows/performance.yml).

Use `packet_loop_callgrind` below or another comparison target from
[`Cargo.toml`](Cargo.toml):

```bash
# On the baseline revision
cargo bench --locked -p o-sfu-tests --bench packet_loop_callgrind -- --save-baseline=local --save-summary=json

# After applying changes in the same checkout
cargo bench --locked -p o-sfu-tests --bench packet_loop_callgrind -- --baseline=local --save-summary=json
```

When changing a scenario, verify that removing its required work fails its self-test:

```bash
cargo test --locked -p o-sfu-tests --test benchmark_scenarios
```

The worker target is manual investigation only. Thread scheduling makes its
instruction counts unsuitable for the PR comparison gate:

```bash
cargo bench --locked -p o-sfu-tests --features worker-benchmarks --bench packet_loop_worker_callgrind -- --save-summary=json
```

## Specialized checks

Use the [Miri](../.github/workflows/ub-tests.yml),
[fuzzing](../.github/workflows/fuzzing.yml) and
[Kani](../.github/workflows/formal-verification.yml) workflows for toolchain setup.
Run Kani in CI unless developing a proof locally.

Local fuzz builds must use `cargo-fuzz` to supply `cfg(fuzzing)`:

```bash
cargo +nightly-2026-04-01 fuzz check --fuzz-dir tests/fuzz --features fuzz-targets
```
