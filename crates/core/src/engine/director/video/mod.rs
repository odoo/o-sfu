mod input;
mod projection;
mod solver;

pub(in crate::engine) use input::{diagnostics_video_layout_role, initial_video_admission_rank};
pub(super) use solver::append_receiver_video_policy;
