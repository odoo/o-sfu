//! Media declaration and controls for producer and consumer routes.
//!
//! [`lifecycle`] combines str0m media changes with complete registration or
//! removal in
//! [`PacketLoopState`](crate::engine::media_transport::rtc::state::PacketLoopState).
//! Media changes stage renegotiation offers. [`negotiation`](super::negotiation)
//! returns those offers and applies answers.
//!
//! [`responses`] handles media batches and relay commands. [`routes`] adjusts
//! activity and packet gates, coupling delivery changes to RTX invalidation.
//! Receiver bandwidth targets use [`super::bwe`]. Keyframe feedback uses shared
//! [`recovery`](crate::engine::media_transport::rtc::recovery) dispatch.

#[cfg(test)]
#[expect(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
mod lifecycle;
mod responses;
mod routes;

use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use str0m::media::MediaKind;

use super::super::commands::RemoteSourceControl;
use crate::engine::media_transport::{TransportSessionKey, TransportSourceKey};

pub(super) struct AddSendMediaRequest<'a> {
    pub consumer_key: &'a TransportSessionKey,
    pub media_kind: MediaKind,
    pub source: &'a TransportSourceKey,
    pub remote_source_control: Option<RemoteSourceControl>,
    pub consumer_rtp_parameters: &'a RouterRtpParameters,
    pub active: bool,
}

pub(super) use self::lifecycle::{
    RecvMediaPolicy, worker_add_recv_media, worker_add_send_media, worker_remove_media,
};
pub use self::responses::apply_media_control_batch;
pub(in crate::engine::media_transport::rtc) use self::responses::apply_route_control_request;
