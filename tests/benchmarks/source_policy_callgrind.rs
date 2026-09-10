//! Callgrind driver for the room's video budget solver
//!
//! the `source_policy` module documents what the scenario models, why it injects
//! a bandwidth trace and what it does not measure. this file only runs the turns
//!
//! `meeting_flow_callgrind` measures the packet-loop half of a call. this target
//! measures the recomputation that wakes it, which lives in the room layer and
//! needs a `RoomState` the packet loop never sees

#![allow(
    clippy::needless_pass_by_value,
    reason = "Gungraun's generated harness owns setup values"
)]
#![expect(
    clippy::exit,
    clippy::must_use_candidate,
    reason = "Gungraun's generated harness returns measured outputs and exits with the runner status"
)]

mod source_policy;

#[path = "callgrind_config.rs"]
mod callgrind_config;

use std::hint::black_box;

use callgrind_config::callgrind_config;
use gungraun::{library_benchmark, library_benchmark_group, main};
use source_policy::SourcePolicyFixture;

// teardown runs the differential budget-pressure check outside the measured
// window, so the benchmark cannot keep reporting stable counts after the
// scenario stops constraining the plan
fn validate_budget_pressure(mut fixture: SourcePolicyFixture) {
    fixture.assert_every_turn_planned();
    fixture.assert_budget_pressure_observed();
}

#[library_benchmark(config = callgrind_config(2.0), teardown = validate_budget_pressure)]
#[bench::budget_pressure(SourcePolicyFixture::new())]
fn policy_turns(mut fixture: SourcePolicyFixture) -> SourcePolicyFixture {
    black_box(fixture.run_policy_turns());
    black_box(fixture)
}

fn validate_mixed_speakers(fixture: SourcePolicyFixture) {
    fixture.assert_every_turn_planned();
    fixture.assert_speaker_selection();
}

#[library_benchmark(config = callgrind_config(2.0), teardown = validate_mixed_speakers)]
#[bench::mixed_speakers(SourcePolicyFixture::mixed_speakers())]
fn speaker_policy_turns(mut fixture: SourcePolicyFixture) -> SourcePolicyFixture {
    black_box(fixture.run_policy_turns());
    black_box(fixture)
}

library_benchmark_group!(
    name = source_policy_callgrind;
    benchmarks = policy_turns, speaker_policy_turns
);

main!(library_benchmark_groups = source_policy_callgrind);
