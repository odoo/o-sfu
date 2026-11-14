//! Receiver media policy: decides which audio and which video encoding each
//! receiver gets within its bandwidth and when to decide again.
//!
//! Director reads committed room facts, worker observations (active speakers,
//! receiver bandwidth and source bitrate) and the time. It decides audio
//! admission, the encoding each video route forwards, route pauses, receiver
//! BWE targets, the room's featured user and the next decision deadline. It
//! never changes membership or publications, never creates or removes routes
//! and never handles packets.
//!
//! [`SourcePolicyTurn`] serializes policy refreshes with publication activity so
//! their transport effects cannot overtake each other. Each turn plans from a
//! [`SourcePolicySnapshot`](input::SourcePolicySnapshot) while it holds a
//! room-state read guard. Planned transport controls run only after that guard
//! is released.
//!
//! Audio policy admits observed speakers up to the room limit and blocks audio
//! for deaf receivers. Video policy maps receiver layout roles, source caps and
//! bandwidth estimates to encoding selectors, route activity and BWE targets.
//!
//! Gate and activity controls must be accepted before the matching
//! [`ConsumerPacketSelectionUpdate`] commits to room state. Each turn replaces
//! or cancels its earliest receiver-pressure or exact-target upgrade deadline.

mod action;
mod audio;
mod input;
mod turn;
mod video;

#[cfg(test)]
pub(super) use self::turn::SourcePolicyTransaction;
#[cfg(feature = "internal-benchmarks")]
pub use self::turn::run_source_policy_turn_for_benchmark;
pub(super) use self::{
    action::ConsumerPacketSelectionUpdate,
    turn::{SourcePolicyTurn, policy_worker_indices},
    video::{diagnostics_video_layout_role, initial_video_admission_rank},
};
