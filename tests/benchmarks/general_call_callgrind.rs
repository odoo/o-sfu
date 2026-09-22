//! deterministic Callgrind coverage for one realistic room-level call flow
//!
//! the setup builds the RTC transport and an empty room outside the measured
//! function. the measured flow then drives joins, readiness, publication,
//! subscription, VAD observations, source-policy refreshes and route inspection
//! through the same core room and media transport boundaries used by runtime code

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
mod general_call;

#[path = "callgrind_config.rs"]
mod callgrind_config;

use std::hint::black_box;

use callgrind_config::callgrind_config;
use general_call::GeneralCallFixture;
use gungraun::{library_benchmark, library_benchmark_group, main};

#[library_benchmark(config = callgrind_config(2.0))]
#[bench::mix_10s(GeneralCallFixture::new())]
fn room_flow(fixture: GeneralCallFixture) -> usize {
    black_box(fixture.run_total_work())
}

library_benchmark_group!(
    name = room_control;
    benchmarks = room_flow
);

main!(library_benchmark_groups = room_control);
