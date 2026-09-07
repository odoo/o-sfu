//! Per-room audio admission and receiver-video allocation.
//!
//! [`SourcePolicyTurn`] serializes policy refreshes with publication activity so
//! their transport effects cannot overtake each other. Each turn combines
//! committed routes, active-speaker observations, receiver bandwidth and source
//! bitrate in a [`SourcePolicySnapshot`](input::SourcePolicySnapshot). Planning
//! holds a room-state read guard. Planned transport controls run only after that
//! guard is released.
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

pub(in crate::engine::room) use self::action::ConsumerPacketSelectionUpdate;
#[cfg(test)]
pub(super) use self::turn::SourcePolicyTransaction;
pub(super) use self::turn::SourcePolicyTurn;
#[cfg(feature = "internal-benchmarks")]
pub use self::turn::run_source_policy_turn_for_benchmark;
pub use self::video::VideoAdmissionRank;
