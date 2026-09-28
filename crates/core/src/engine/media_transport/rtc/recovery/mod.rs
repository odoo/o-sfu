//! Decoder readiness effects and keyframe dispatch across RTC workers.
//!
//! [`RouteTable`](super::state::route_table::RouteTable) owns decoder gates and
//! source/RID retry state. [`apply_src_decoder_ready`] dispatches refresh feedback
//! after packet-loop state commits gates and consumer RTX invalidation. Packet
//! liveness must already be recorded before applying decoder readiness.
//!
//! The coordinator resolves source ownership before producer or remote dispatch.
//! For remote sources the consumer worker keeps retry ownership:
//!
//! ```text
//! consumer worker: Track / Retry
//!                       |
//!                RemoteSourceControl
//!                       |
//! producer worker: Forward -> publisher interval -> producer StreamRx
//! ```
//!
//! [`worker_request_remote_kf`] revalidates relay activity and producer ownership
//! without arming another retry loop. The producer worker merges requests
//! during its dispatch interval. Due retries recheck source demand and the
//! current feedback path. A full remote queue retains pending retry state while
//! a closed channel removes it.

mod decoder_readiness;
mod feedback;
mod keyframe;
#[cfg(test)]
#[path = "TESTS/support.rs"]
mod test_support;

pub use decoder_readiness::apply_src_decoder_ready;
pub use feedback::{PendingKeyframeRequest, drain_due_kf_retries, flush_pending_kf_reqs_at};
#[cfg(test)]
pub use keyframe::request_recovery_kf;
pub use keyframe::{
    drain_due_publisher_kf, request_consumer_feedback_kf, request_source_recovery_kf,
    retry_source_kf, worker_request_consumer_kf, worker_request_remote_kf,
    worker_request_resumed_video_kf,
};
#[cfg(test)]
pub use test_support::observe_src_rid_ready;
