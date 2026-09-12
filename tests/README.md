# Tests

## dir

  - `tests/benchmarks/`: The Gungraun (Valgrind/Callgrind) perf benchmarks
  - `tests/integration/`: integration tests
  - `tests/core-room/`: broad room lifecycle, spillover and subscription
    coverage through the public `testing-transport` harness
  - `tests/src/support/`: cross-crate integration harnesses for real server
    entry points, fake peers, websocket clients, and polling predicates
  - `tests/miri/`: UB tests
  - `tests/fuzz/`: opt-in cargo-fuzz targets
  - `tests/proofs/`: formal verification

## Default

when testing locally, run the following baseline. GitHub Actions covers the
remaining checks.

```bash
cargo +nightly fmt
cargo check --locked
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --release
npm --prefix crates/client run verify
```

## Fuzzing

the fuzz package shares the root `Cargo.lock` but is outside
`workspace.default-members`. its dependencies and binaries require the
`fuzz-targets` feature.

use cargo-fuzz for every fuzz target build because it supplies `cfg(fuzzing)`
to the core dependency graph.

type-check every fuzz target from the repository root:

```bash
cargo +nightly-2026-04-01 fuzz check --fuzz-dir tests/fuzz --features fuzz-targets
```

run one target explicitly:

```bash
cargo +nightly-2026-04-01 fuzz run --fuzz-dir tests/fuzz --features fuzz-targets protocol_decode
```

## Formal verification

`o-sfu-proofs` is a dependency leaf for proofs over public production APIs.
Proofs that require private state belong in a `PROOFS/` directory beside the
owning module behind `cfg(kani)`. Add a module-local proof to CI only when the
pinned Kani build can compile that crate.

Production crates never depend on `o-sfu-proofs`. CI selects Cargo packages and
harnesses without including production source or naming source files.

```bash
cargo kani --package o-sfu-proofs --lib \
  --harness h264_profile_level_id_parse_matches_rfc_patterns

cargo kani --package o-sfu-proofs --lib \
  --harness vp8_keyframe_prefix_matches_rfc_model

cargo kani --package o-sfu-core --lib \
  --harness vp8_identity_projection_matches_modular_model

cargo kani --package o-sfu-core --lib \
  --harness rtp_projection_matches_affine_model
```

The RTP harness checks the production projection against widened sequence and
modular timestamp arithmetic. It proves output agreement, state preservation on
rejection or observation and preservation of the active sequence invariant.
Exhausted states remain in scope: advancing packets are rejected while eligible
observations and repairs retain their mapping. Repairs cannot request reanchoring.
The theorem excludes generation admission, codec commits, SRTP validation and RTC
writes. Both Kani workflows require all 24 transition cover properties to pass.

CI builds its Rust 1.95-compatible Kani toolchain from an exact upstream
revision until that compiler support is available in a Kani release.

Run Kani in CI unless a proof is under local development. Ordinary Cargo
commands do not execute solver harnesses.

## Callgrind benchmarks

There are four comparison targets. Each scenario is documented once, on its
fixture; the bench target only picks the measured window.

  - `packet_loop_callgrind`: narrow slices, one packet-loop helper each. use it to
    attribute a regression to a single function
  - `meeting_flow_callgrind`: a whole twelve-person room's media path
    (`MeetingFlowBenchFixture`)
  - `source_policy_callgrind`: the room deciding what every receiver should get
    (`tests/benchmarks/source_policy/mod.rs`)
  - `general_call_callgrind`: the room control-plane flow (joins, publications,
    subscriptions). it stays until the two scenario targets have enough baseline
    history to replace it

Build

```bash
cargo bench --locked -p o-sfu-tests --bench packet_loop_callgrind --no-run
cargo bench --locked -p o-sfu-tests --bench meeting_flow_callgrind --no-run
cargo bench --locked -p o-sfu-tests --bench source_policy_callgrind --no-run
cargo bench --locked -p o-sfu-tests --bench general_call_callgrind --no-run
```

Save a local baseline on a Valgrind-supported host:

```bash
cargo bench --locked -p o-sfu-tests --bench packet_loop_callgrind -- --save-baseline=local --save-summary=json
cargo bench --locked -p o-sfu-tests --bench meeting_flow_callgrind -- --save-baseline=local --save-summary=json
cargo bench --locked -p o-sfu-tests --bench source_policy_callgrind -- --save-baseline=local --save-summary=json
cargo bench --locked -p o-sfu-tests --bench general_call_callgrind -- --save-baseline=local --save-summary=json
```

Compare local changes against that baseline:

```bash
cargo bench --locked -p o-sfu-tests --bench packet_loop_callgrind -- --baseline=local --save-summary=json
cargo bench --locked -p o-sfu-tests --bench meeting_flow_callgrind -- --baseline=local --save-summary=json
cargo bench --locked -p o-sfu-tests --bench source_policy_callgrind -- --baseline=local --save-summary=json
cargo bench --locked -p o-sfu-tests --bench general_call_callgrind -- --baseline=local --save-summary=json
```

Both scenarios own self-tests that fail when the flow stops reaching the code it
was built to measure, so a silently degraded scenario cannot keep reporting stable
instruction counts:

```bash
cargo test --locked -p o-sfu-tests --test benchmark_scenarios
```

Both self-tests assert on state read back out of the engine, never on counters the
fixture computed itself; each assertion carries the reason it is written that way.

When changing either scenario, break it on purpose first and confirm the self-test
fails. An assertion that cannot fail is worse than no assertion, because the gate
then reports "no regression" for a scenario that stopped measuring anything.

it requires `gungraun-runner` plus Valgrind. on
hosts without Valgrind support.
