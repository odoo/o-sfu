use str0m::rtp::Ssrc;

pub use super::streams::{RTX_CACHE_LIFETIME, SourceRtpIdentity};
use super::{
    super::{codec, state::slots::ConsumerStreamHandle},
    streams::{ConsumerStreamStore, ProjectedIdentity},
};

pub fn project_identity(
    streams: &mut ConsumerStreamStore,
    stream_handle: ConsumerStreamHandle,
    source: SourceRtpIdentity,
    codec_identity: codec::PacketIdentity,
) -> Option<ProjectedIdentity> {
    streams.project_identity(stream_handle, source, codec_identity)
}

pub fn queue_repairable_write(
    streams: &mut ConsumerStreamStore,
    stream_handle: ConsumerStreamHandle,
    ssrc: Ssrc,
) {
    streams.queue_repairable_write(stream_handle, ssrc);
}
