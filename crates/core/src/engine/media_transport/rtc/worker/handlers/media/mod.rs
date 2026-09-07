//! Worker-local media mutation boundary

#[cfg(test)]
#[expect(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
mod control;
mod keyframe;
mod lifecycle;

use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use str0m::media::MediaKind;

use super::super::super::commands::RemoteSourceControl;
use crate::engine::media_transport::{TransportSessionKey, TransportSourceKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RouteSourceKind {
    Local,
    Remote,
}

pub(super) struct AddSendMediaRequest<'a> {
    pub consumer_key: &'a TransportSessionKey,
    pub media_kind: MediaKind,
    pub source: &'a TransportSourceKey,
    pub remote_source_control: Option<RemoteSourceControl>,
    pub consumer_rtp_parameters: &'a RouterRtpParameters,
    pub active: bool,
}

impl RouteSourceKind {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

pub(in crate::engine::media_transport::rtc) use control::apply_route_control_request;
#[cfg(feature = "internal-benchmarks")]
pub(in crate::engine::media_transport::rtc) use control::guarded_pkt_gate;
#[cfg(test)]
use control::observe_src_rid_ready;
pub use control::{apply_media_control_batch, apply_src_decoder_ready};
pub(super) use control::{remove_consumer_route, remove_source_route};
pub use keyframe::{KeyframeRequestMode, KeyframeRequestTarget, request_kf_for_target};
pub(super) use lifecycle::{
    RecvMediaPolicy, worker_add_recv_media, worker_add_send_media, worker_remove_media,
};
