//! shared Callgrind tool configuration for the comparison benchmark targets
//!
//! every target wants the same measurement setup and differs only in how much
//! drift it tolerates, so the soft limit is the one parameter left to the caller

use gungraun::{Callgrind, EventKind, LibraryBenchmarkConfig, ValgrindTool};

const CALLGRIND_CACHE_SIM: &str = "--cache-sim=yes";

/// builds the Callgrind config used by every comparison target
///
/// `soft_limit` is the fractional base-versus-head drift a benchmark may show
/// before the gate reports it. failures are not fatal, so one noisy case does
/// not hide the rest of the run
pub fn callgrind_config(soft_limit: f64) -> LibraryBenchmarkConfig {
    let mut callgrind = Callgrind::with_args([CALLGRIND_CACHE_SIM]);
    callgrind.soft_limits([
        (EventKind::Ir, soft_limit),
        (EventKind::EstimatedCycles, soft_limit),
    ]);
    callgrind.fail_fast(false);

    let mut config = LibraryBenchmarkConfig::default();
    // A configured Callgrind tool would also run when DHAT is the default tool.
    if cfg!(feature = "dhat") {
        config.default_tool(ValgrindTool::DHAT);
    } else {
        config.tool(callgrind);
    }
    config
}
