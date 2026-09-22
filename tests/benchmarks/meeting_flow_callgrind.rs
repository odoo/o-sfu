//! Callgrind driver for the twelve-person meeting packet-loop scenario
//!
//! `MeetingFlowBenchFixture` documents what the scenario models, what it cannot
//! reach and why each branch is there. this file only chooses the measured
//! windows: `meeting_2s` is the cheap base-versus-head case for a pull request,
//! `meeting_12s` is twelve seconds of a live meeting
//!
//! `packet_loop_callgrind` measures individual packet-loop helpers, so a
//! regression there can be attributed to one function. this target measures how
//! the phases interact. it is additive: `general_call_callgrind` still owns the
//! room control-plane flow until this scenario has enough baseline history to
//! replace it

#![allow(
    clippy::needless_pass_by_value,
    reason = "Gungraun's generated harness owns setup values"
)]
#![expect(
    clippy::exit,
    clippy::must_use_candidate,
    reason = "Gungraun's generated harness returns measured outputs and exits with the runner status"
)]

mod allocator;

#[path = "callgrind_config.rs"]
mod callgrind_config;

use std::hint::black_box;

use callgrind_config::callgrind_config;
use gungraun::{library_benchmark, library_benchmark_group, main};
use o_sfu_core::server::transport::benchmark_support::MeetingFlowBenchFixture;

// teardown re-checks coverage outside the measured window, so the benchmark
// cannot keep reporting stable counts after the scenario stops reaching the
// paths it exists to measure
fn validate_packet_loop_coverage(mut fixture: MeetingFlowBenchFixture) {
    fixture.assert_packet_loop_coverage();
}

#[library_benchmark(config = callgrind_config(1.0), teardown = validate_packet_loop_coverage)]
#[bench::meeting_2s(MeetingFlowBenchFixture::short_meeting())]
#[bench::meeting_12s(MeetingFlowBenchFixture::long_meeting())]
fn meeting_flow(mut fixture: MeetingFlowBenchFixture) -> MeetingFlowBenchFixture {
    black_box(fixture.run_meeting());
    black_box(fixture)
}

library_benchmark_group!(
    name = meeting;
    benchmarks = meeting_flow
);

main!(library_benchmark_groups = meeting);
