//! Codec-neutral packet inspection, projection and rewrite facade.

use o_sfu_router::rtp::MediaStream;
use str0m::{media::Pt, rtp::RtpWrite};

use super::vp8;

#[derive(Debug, Clone, Copy, Default)]
pub(in crate::engine::media_transport::rtc) struct PacketInspector {
    vp8_payload_types: (u64, u64),
}

impl PacketInspector {
    pub(in crate::engine::media_transport::rtc) fn from_parameters(
        parameters: &MediaStream,
    ) -> Self {
        Self {
            vp8_payload_types: vp8::payload_type_mask(parameters),
        }
    }

    pub(in crate::engine::media_transport::rtc) fn inspect(
        &self,
        payload_type: Pt,
        payload: &[u8],
        has_rid: bool,
    ) -> Packet {
        if !vp8::payload_type_matches(self.vp8_payload_types, payload_type) {
            return Packet::default();
        }
        Packet {
            vp8: vp8::Packet::inspect(payload, has_rid),
        }
    }

    pub(in crate::engine::media_transport::rtc) fn is_empty(&self) -> bool {
        self.vp8_payload_types == (0, 0)
    }

    pub(in crate::engine::media_transport::rtc) fn decoder_refresh_is_observable(&self) -> bool {
        self.vp8_payload_types != (0, 0)
    }
}

/// Returns whether a destination may wait for an observable refresh.
///
/// Only negotiated VP8 is gated. Opaque codecs must forward immediately and use
/// keyframe requests as bounded hints because the packet path cannot prove their
/// completion. VP8 refresh detection follows
/// [RFC 7741 section 4.3](https://www.rfc-editor.org/rfc/rfc7741.html#section-4.3).
pub(in crate::engine::media_transport::rtc) fn requires_decoder_refresh(
    parameters: &MediaStream,
    payload_type: Option<Pt>,
) -> bool {
    payload_type.is_some_and(|payload_type| {
        vp8::payload_types(parameters).any(|candidate| candidate == payload_type)
    })
}

#[derive(Debug, Clone, Copy, Default)]
pub(in crate::engine::media_transport::rtc) struct Packet {
    vp8: vp8::Packet,
}

impl Packet {
    pub(in crate::engine::media_transport::rtc) const fn decoder_refresh(&self) -> bool {
        self.vp8.decoder_refresh()
    }

    pub(in crate::engine::media_transport::rtc) const fn identity(&self) -> PacketIdentity {
        PacketIdentity {
            vp8: self.vp8.identity(),
        }
    }

    pub(in crate::engine::media_transport::rtc) fn rewrite(
        &self,
        projected: ProjectedPacket,
    ) -> Option<Rewrite> {
        self.vp8.rewrite(projected.vp8).map(Rewrite::Vp8)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(in crate::engine::media_transport::rtc) struct PacketIdentity {
    vp8: vp8::Identity,
}

pub(in crate::engine::media_transport::rtc) enum Rewrite {
    Vp8(vp8::Rewrite),
}

impl Rewrite {
    pub(in crate::engine::media_transport::rtc) fn apply(self, write: RtpWrite) -> RtpWrite {
        match self {
            Self::Vp8(rewrite) => rewrite.apply(write),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(in crate::engine::media_transport::rtc) struct Projection {
    vp8: vp8::Projection,
}

impl Projection {
    // Keep the caller's constant reanchor choice visible to VP8 projection.
    #[inline]
    pub(in crate::engine::media_transport::rtc) fn project(
        &mut self,
        identity: PacketIdentity,
        reanchor: bool,
    ) -> ProjectedPacket {
        ProjectedPacket {
            vp8: if reanchor {
                self.vp8.reanchor(identity.vp8)
            } else {
                self.vp8.project(identity.vp8)
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::engine::media_transport::rtc) struct ProjectedPacket {
    vp8: vp8::Identity,
}
