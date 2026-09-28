//! routing-miss packet fingerprinting
//!
//! the fingerprint is only a cheap prefilter before exact packet-byte
//! comparison

const U64_BYTES: usize = 8;

/// computes a small fingerprint for routing-miss prefiltering
///
/// it samples length, prefix and suffix so common RTP or STUN variations
/// usually differ before the exact byte comparison
/// empty and short packets are still handled deterministically
#[must_use]
pub(super) fn packet_fingerprint(packet: &[u8]) -> u64 {
    let len = u64::try_from(packet.len()).unwrap_or(u64::MAX);
    let (prefix, suffix) = packet.first_chunk::<U64_BYTES>().map_or_else(
        || {
            let padded = load_u64_padded(packet);
            (padded, padded)
        },
        |prefix| {
            let suffix = packet.last_chunk::<U64_BYTES>().unwrap_or(prefix);
            (u64::from_le_bytes(*prefix), u64::from_le_bytes(*suffix))
        },
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

#[cfg(test)]
#[path = "TESTS/fingerprint.rs"]
mod tests;
