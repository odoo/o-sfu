//! manual Callgrind coverage for worker commands
//!
//! this target complements the deterministic packet-loop slice gate
//! it runs a current-thread caller runtime and a separate worker thread
//! after fixture setup and warmup
//!
//! the target is manual-only in CI
//! it can produce DHAT, cache simulation, branch simulation and flamegraph
//! artifacts without making pull requests depend on whole-worker noise
//!
//! this target owns the whole-worker investigation path
//! `packet_loop_callgrind` stays the PR comparison target because its slices are
//! small enough to compare across base and head
//! this file is for manual profiling when those slices point at a regression or
//! when scheduler or mailbox cost needs a full worker context
//!
//! the measured window is explicit
//! `Callgrind` instrumentation starts after fixture setup and warmup
//! instrumentation stops before control returns to the generated harness

#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "fixed benchmark fixtures must fail on invalid setup or missing coverage"
)]
#![expect(
    clippy::exit,
    clippy::must_use_candidate,
    reason = "Gungraun's generated harness returns measured outputs and exits with the runner status"
)]

mod allocator;
mod fixture;

use std::{env, hint::black_box};

use fixture::{boxed, release};
// github actions runs this manual target on x86_64 linux
// other targets compile no-op hooks so local checks do not require supported
// valgrind client requests
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
use gungraun::client_requests::callgrind::{start_instrumentation, stop_instrumentation};
use gungraun::{
    Callgrind, EntryPoint, FlamegraphConfig, LibraryBenchmarkConfig, ValgrindTool,
    library_benchmark, library_benchmark_group, main,
};
use o_sfu_core::server::transport::benchmark_support::WorkerLoopBenchFixture;

#[cfg(not(all(target_arch = "x86_64", target_os = "linux")))]
fn start_instrumentation() {}

#[cfg(not(all(target_arch = "x86_64", target_os = "linux")))]
fn stop_instrumentation() {}

fn callgrind_worker_config() -> LibraryBenchmarkConfig {
    let mut callgrind = Callgrind::with_args(["--instr-atstart=no"]);
    callgrind.entry_point(EntryPoint::None);
    if env::var_os("O_SFU_CALLGRIND_FLAMEGRAPHS").is_some() {
        callgrind.flamegraph(FlamegraphConfig::default());
    }

    let mut config = LibraryBenchmarkConfig::default();
    if cfg!(feature = "dhat") {
        config.default_tool(ValgrindTool::DHAT);
    } else {
        config.tool(callgrind);
    }
    config
}

#[library_benchmark(config = callgrind_worker_config(), teardown = release)]
#[bench::active_speaker_snapshot(boxed(WorkerLoopBenchFixture::command_driven_current_thread))]
fn worker_command_roundtrips(fixture: Box<WorkerLoopBenchFixture>) -> Box<WorkerLoopBenchFixture> {
    start_instrumentation();
    let result = fixture.run_command_roundtrips();
    stop_instrumentation();
    black_box(result);
    black_box(fixture)
}

library_benchmark_group!(
    name = worker;
    benchmarks = worker_command_roundtrips
);

main!(library_benchmark_groups = worker);
