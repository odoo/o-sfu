//! routing-miss packet fingerprinting
//!
//! the fingerprint is only a cheap prefilter before exact packet-byte
//! comparison
//! it must stay deterministic across scalar and SIMD paths because
//! `PacketLoopRoutingMissKey` includes the value in equality and hashing.

const U64_BYTES: usize = 8;

/// computes a small fingerprint for routing-miss prefiltering
///
/// it samples length, prefix and suffix so common RTP or STUN variations
/// usually differ before the exact byte comparison
/// empty and short packets are still handled deterministically
#[must_use]
pub(super) fn packet_fingerprint(packet: &[u8]) -> u64 {
    #[cfg(all(
        any(target_arch = "aarch64", target_arch = "x86_64"),
        target_endian = "little"
    ))]
    {
        if packet.len() >= simd::PACKET_BYTES {
            return simd::packet_fingerprint(packet);
        }
    }
    packet_fingerprint_scalar(packet)
}

#[must_use]
fn packet_fingerprint_scalar(packet: &[u8]) -> u64 {
    let len = u64::try_from(packet.len()).unwrap_or(u64::MAX);
    let prefix = load_u64_padded(packet);
    let suffix = load_u64_padded(
        packet
            .get(packet.len().saturating_sub(U64_BYTES)..)
            .unwrap_or(packet),
    );
    combine(len, prefix, suffix)
}

#[must_use]
fn load_u64_padded(bytes: &[u8]) -> u64 {
    let mut buffer = [0_u8; U64_BYTES];
    for (slot, byte) in buffer.iter_mut().zip(bytes.iter().copied()) {
        *slot = byte;
    }
    u64::from_le_bytes(buffer)
}

#[must_use]
fn combine(len: u64, prefix: u64, suffix: u64) -> u64 {
    len.rotate_left(17) ^ prefix.rotate_left(29) ^ suffix.rotate_left(43)
}

#[cfg(all(
    any(target_arch = "aarch64", target_arch = "x86_64"),
    target_endian = "little"
))]
mod simd {
    pub(super) const PACKET_BYTES: usize = 16;

    #[must_use]
    pub(super) fn packet_fingerprint(packet: &[u8]) -> u64 {
        let len = u64::try_from(packet.len()).unwrap_or(u64::MAX);
        let prefix = first_u64(packet);
        let suffix = last_u64(packet);
        super::combine(len, prefix, suffix)
    }

    #[cfg(target_arch = "aarch64")]
    #[must_use]
    fn first_u64(packet: &[u8]) -> u64 {
        use std::arch::aarch64::{
            uint8x16_t, uint64x2_t, vgetq_lane_u64, vld1q_u8, vreinterpretq_u64_u8,
        };

        #[expect(
            unsafe_code,
            reason = "we need it because SIMD fingerprinting requires NEON intrinsics,
                      it is safe because aarch64 has NEON enabled by default and
                      the caller guarantees the packet slice is at least 16 bytes,
                      ensuring the 16-byte unaligned load is in-bounds"
        )]
        unsafe {
            let vector: uint8x16_t = vld1q_u8(packet.as_ptr());
            let lanes: uint64x2_t = vreinterpretq_u64_u8(vector);
            vgetq_lane_u64::<0>(lanes)
        }
    }

    #[cfg(target_arch = "aarch64")]
    #[must_use]
    fn last_u64(packet: &[u8]) -> u64 {
        use std::arch::aarch64::{
            uint8x16_t, uint64x2_t, vgetq_lane_u64, vld1q_u8, vreinterpretq_u64_u8,
        };

        #[expect(
            unsafe_code,
            reason = "we need it because SIMD fingerprinting requires NEON intrinsics,
                      it is safe because aarch64 has NEON enabled by default and
                      the pointer arithmetic offsets exactly 16 bytes from the end of the guarded slice,
                      ensuring the 16-byte unaligned load is completely in-bounds"
        )]
        unsafe {
            let vector: uint8x16_t = vld1q_u8(packet.as_ptr().add(packet.len() - PACKET_BYTES));
            let lanes: uint64x2_t = vreinterpretq_u64_u8(vector);
            vgetq_lane_u64::<1>(lanes)
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[must_use]
    fn first_u64(packet: &[u8]) -> u64 {
        use std::arch::x86_64::{__m128i, _mm_cvtsi128_si64, _mm_loadu_si128};

        #[expect(
            unsafe_code,
            reason = "we need it because SIMD fingerprinting requires SSE2 intrinsics,
                      it is safe because x86_64 guarantees SSE2 availability and
                      the caller's 16-byte minimum length guard ensures the
                      16-byte unaligned load is strictly in-bounds"
        )]
        unsafe {
            #[expect(
                clippy::cast_ptr_alignment,
                reason = "_mm_loadu_si128 requires a __m128i pointer type while performing an unaligned load"
            )]
            let vector = _mm_loadu_si128(packet.as_ptr().cast::<__m128i>());
            u64::from_le_bytes(_mm_cvtsi128_si64(vector).to_le_bytes())
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[must_use]
    fn last_u64(packet: &[u8]) -> u64 {
        use std::arch::x86_64::{__m128i, _mm_cvtsi128_si64, _mm_loadu_si128, _mm_srli_si128};

        #[expect(
            unsafe_code,
            reason = "we need it because SIMD fingerprinting requires SSE2 intrinsics,
                      it is safe because x86_64 guarantees SSE2 availability and
                      the pointer arithmetic offsets exactly 16 bytes from the end of the guarded slice,
                      ensuring the 16-byte unaligned load is entirely in-bounds"
        )]
        unsafe {
            #[expect(
                clippy::cast_ptr_alignment,
                reason = "_mm_loadu_si128 requires a __m128i pointer type while performing an unaligned load"
            )]
            let vector = _mm_loadu_si128(
                packet
                    .as_ptr()
                    .add(packet.len() - PACKET_BYTES)
                    .cast::<__m128i>(),
            );
            let high_lane = _mm_srli_si128::<8>(vector);
            u64::from_le_bytes(_mm_cvtsi128_si64(high_lane).to_le_bytes())
        }
    }
}

#[cfg(test)]
#[path = "TESTS/fingerprint.rs"]
mod tests;
