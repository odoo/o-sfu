//! RTC command workflows for one worker.
//!
//! [`dispatcher`] applies mailbox commands during the [`worker`](super::worker)
//! turn. Handlers coordinate str0m changes with
//! [`PacketLoopState`](super::state::PacketLoopState) operations that keep
//! session, media and route indexes consistent.
//!
//! [`negotiation`] owns offer/answer exchange and uses [`publication`] to
//! reconcile producer bindings. [`media`] handles media declaration, removal
//! and controls for existing routes.
//!
//! Decoder recovery is shared with [`packet_loop`](super::packet_loop) through
//! [`recovery`](super::recovery).

mod bwe;
mod dispatcher;
mod media;
mod negotiation;
mod publication;
mod recv_stream;
mod session;

pub use self::dispatcher::{WorkerCommandContext, handle_worker_command};
#[cfg(feature = "internal-benchmarks")]
pub use self::media::apply_media_control_batch;
pub(in crate::engine::media_transport::rtc) use self::session::{
    SessionCloseDisposition, worker_close_session,
};
