//! VP8 simulcast policy, packet inspection and receiver identity projection.

use o_sfu_rfc::rtp::{CodecName, vp8, vp8::LONG_PICTURE_ID_MODULUS};
use o_sfu_router::rtp::MediaStream;
use str0m::{
    media::Pt,
    rtp::{RtpWrite, Vp8Descriptor, Vp8Patch, Vp8PatchError},
};

pub(super) fn payload_types(parameters: &MediaStream) -> impl Iterator<Item = Pt> + '_ {
    parameters
        .formats()
        .filter(|format| *format.codec() == CodecName::Vp8)
        .map(|format| Pt::from(format.payload_type()))
}

pub(super) fn payload_type_mask(parameters: &MediaStream) -> (u64, u64) {
    let mut lower = 0;
    let mut upper = 0;
    for payload_type in payload_types(parameters) {
        let payload_type = usize::from(*payload_type);
        let half = if payload_type < 64 {
            &mut lower
        } else {
            &mut upper
        };
        *half |= 1 << (payload_type % 64);
    }
    (lower, upper)
}

pub(super) fn payload_type_matches((lower, upper): (u64, u64), payload_type: Pt) -> bool {
    let payload_type = usize::from(*payload_type);
    let half = if payload_type < 64 { lower } else { upper };
    half & (1 << (payload_type % 64)) != 0
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Packet {
    descriptor: Option<Vp8Descriptor>,
    identity: Identity,
    decoder_refresh: bool,
}

impl Packet {
    pub(super) fn inspect(payload: &[u8], parse_descriptor: bool) -> Self {
        let descriptor = parse_descriptor
            .then(|| Vp8Descriptor::parse(payload).ok())
            .flatten();
        Self {
            identity: descriptor.map_or_else(Identity::default, Identity::from),
            descriptor,
            decoder_refresh: vp8::payload_starts_keyframe(payload),
        }
    }

    pub(super) const fn decoder_refresh(&self) -> bool {
        self.decoder_refresh
    }

    pub(super) const fn identity(&self) -> Identity {
        self.identity
    }

    pub(super) fn rewrite(&self, identity: Identity) -> Option<Rewrite> {
        self.patch(identity).map(Rewrite)
    }

    pub(super) fn patch(&self, identity: Identity) -> Option<Vp8Patch> {
        let descriptor = self.descriptor?;
        match Self::build_patch(descriptor, identity) {
            Ok(patch) => Some(patch),
            // str0m preserves the source descriptor width. RFC 7741 section
            // 4.2 permits 7-bit PictureID wrap, so reduce a projected identity
            // that the short descriptor cannot encode.
            // https://www.rfc-editor.org/rfc/rfc7741.html#section-4.2
            Err(Vp8PatchError::PictureIdTooLarge) => Self::build_patch(
                descriptor,
                Identity {
                    picture_id: identity
                        .picture_id
                        .map(|picture_id| picture_id & vp8::SHORT_PICTURE_ID_MASK),
                    tl0_pic_idx: identity.tl0_pic_idx,
                },
            )
            .ok(),
            Err(_) => None,
        }
    }

    fn build_patch(
        descriptor: Vp8Descriptor,
        identity: Identity,
    ) -> Result<Vp8Patch, Vp8PatchError> {
        let mut patch = descriptor.patch();
        if let Some(picture_id) = identity.picture_id {
            patch = patch.picture_id(picture_id);
        }
        if let Some(tl0_pic_idx) = identity.tl0_pic_idx {
            patch = patch.tl0_pic_idx(tl0_pic_idx);
        }
        patch.build()
    }
}

pub(in crate::engine::media_transport::rtc) struct Rewrite(Vp8Patch);

impl Rewrite {
    pub(super) fn apply(self, write: RtpWrite) -> RtpWrite {
        write.vp8_patch(self.0)
    }
}

/// Projects source-local VP8 counters into the receiver's single stream.
///
/// A source switch must not expose another RID's unrelated `PictureID` or
/// `TL0PICIDX`. `reanchor` emits the next receiver value then preserves that
/// source's modulo deltas. The moduli follow
/// [RFC 7741 section 4.2](https://www.rfc-editor.org/rfc/rfc7741.html#section-4.2).
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Projection {
    picture_id: CounterProjection<u16>,
    tl0_pic_idx: CounterProjection<u8>,
}

impl Projection {
    pub(super) fn project(&mut self, identity: Identity) -> Identity {
        self.project_inner::<false>(identity)
    }

    pub(super) fn reanchor(&mut self, identity: Identity) -> Identity {
        self.project_inner::<true>(identity)
    }

    fn project_inner<const REANCHOR: bool>(&mut self, identity: Identity) -> Identity {
        Identity {
            picture_id: self.picture_id.project::<REANCHOR>(
                identity.picture_id,
                |last| last.wrapping_add(1) % LONG_PICTURE_ID_MODULUS,
                |src, src_anchor, dst_anchor| {
                    let delta = src.wrapping_sub(src_anchor) % LONG_PICTURE_ID_MODULUS;
                    dst_anchor.wrapping_add(delta) % LONG_PICTURE_ID_MODULUS
                },
            ),
            tl0_pic_idx: self.tl0_pic_idx.project::<REANCHOR>(
                identity.tl0_pic_idx,
                |last| last.wrapping_add(1),
                |src, src_anchor, dst_anchor| dst_anchor.wrapping_add(src.wrapping_sub(src_anchor)),
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
enum CounterProjection<T> {
    #[default]
    Empty,
    LastOnly(T),
    Anchored {
        src_anchor: T,
        dst_anchor: T,
        last: T,
    },
}

impl<T: Copy> CounterProjection<T> {
    fn project<const REANCHOR: bool>(
        &mut self,
        src: Option<T>,
        next: impl Fn(T) -> T,
        from_anchors: impl Fn(T, T, T) -> T,
    ) -> Option<T> {
        let Some(src) = src else {
            if REANCHOR && let Self::Anchored { last, .. } = *self {
                *self = Self::LastOnly(last);
            }
            return None;
        };
        let dst = match *self {
            Self::Anchored { last, .. } if REANCHOR => next(last),
            Self::Anchored {
                src_anchor,
                dst_anchor,
                ..
            } => from_anchors(src, src_anchor, dst_anchor),
            Self::LastOnly(last) => next(last),
            Self::Empty => src,
        };
        *self = Self::Anchored {
            src_anchor: src,
            dst_anchor: dst,
            last: dst,
        };
        Some(dst)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Identity {
    picture_id: Option<u16>,
    tl0_pic_idx: Option<u8>,
}

impl From<Vp8Descriptor> for Identity {
    fn from(descriptor: Vp8Descriptor) -> Self {
        Self {
            picture_id: descriptor.picture_id(),
            tl0_pic_idx: descriptor.tl0_pic_idx(),
        }
    }
}

#[cfg(kani)]
#[path = "PROOFS/vp8.rs"]
mod proofs;

#[cfg(test)]
#[path = "TESTS/vp8.rs"]
mod tests;
