//! shared Callgrind tool configuration for the comparison benchmark targets
//!
//! Instruction limits vary by scenario. Simulated cycles use a wider limit
//! because code placement can change cache misses without adding instructions.

use gungraun::{Callgrind, EventKind, LibraryBenchmarkConfig, ValgrindTool};

const CALLGRIND_CACHE_SIM: &str = "--cache-sim=yes";
const MIN_CYCLE_LIMIT: f64 = 5.0;

/// builds the Callgrind config used by every comparison target
///
/// `instruction_limit` is the allowed percentage increase in instructions.
/// Cycles allow at least 5%. Regressions fail the run after all cases finish.
pub fn callgrind_config(instruction_limit: f64) -> LibraryBenchmarkConfig {
    let mut callgrind = Callgrind::with_args([CALLGRIND_CACHE_SIM]);
    callgrind.soft_limits([
        (EventKind::Ir, instruction_limit),
        (
            EventKind::EstimatedCycles,
            instruction_limit.max(MIN_CYCLE_LIMIT),
        ),
    ]);
    callgrind.fail_fast(false);
    let mut config = LibraryBenchmarkConfig::default();
    // Timed jemalloc cache collection can add an unrelated sweep to one revision.
    // Allocation-driven collection keeps the same workload comparable.
    config.env(
        "_RJEM_MALLOC_CONF",
        "abort_conf:true,experimental_tcache_gc:false",
    );
    // A configured Callgrind tool would also run when DHAT is the default tool.
    if cfg!(feature = "dhat") {
        config.default_tool(ValgrindTool::DHAT);
    } else {
        config.tool(callgrind);
    }
    config
}
