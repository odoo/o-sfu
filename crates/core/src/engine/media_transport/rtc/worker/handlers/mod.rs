//! Worker-local RTC command mutation boundary.
//!
//! The packet loop owns the mutable [`PacketLoopState`](super::super::state::PacketLoopState)
//! for one worker. Mailbox commands and packet-driven helpers enter the same
//! focused mutation modules so user, negotiation, media and teardown state keep
//! one serialized owner.

mod bwe;
mod dispatcher;
mod media;
mod negotiation;
mod publication;
mod recv_stream;
mod session;

pub use dispatcher::{WorkerCommandContext, handle_worker_command};
#[cfg(feature = "internal-benchmarks")]
pub use media::apply_media_control_batch;
#[cfg(feature = "internal-benchmarks")]
pub(in crate::engine::media_transport::rtc) use media::guarded_pkt_gate;
pub use media::{
    KeyframeRequestMode, KeyframeRequestTarget, apply_src_decoder_ready, request_kf_for_target,
};
pub(in crate::engine::media_transport::rtc) use session::{
    SessionCloseDisposition, worker_close_session,
};
