use super::{packet_fingerprint, packet_fingerprint_scalar};

// Covers every 16-byte alignment offset with the suffix ending at the slice bundary.
#[test]
fn packet_fingerprint_matches_scalar_reference() {
    for len in [0_usize, 1, 7, 8, 15, 16, 17, 64, 256, 1200] {
        for seed in 0_usize..8 {
            for offset in 0_usize..16 {
                let storage = deterministic_packet(len + offset, seed);
                let (_, packet) = storage.split_at(offset);

                assert_eq!(
                    packet_fingerprint(packet),
                    packet_fingerprint_scalar(packet)
                );
            }
        }
    }
}

#[must_use]
fn deterministic_packet(len: usize, seed: usize) -> Vec<u8> {
    let mut packet = Vec::with_capacity(len);
    for byte_index in 0..len {
        packet.push(deterministic_byte(seed, byte_index));
    }
    packet
}

#[must_use]
fn deterministic_byte(seed: usize, byte_index: usize) -> u8 {
    let mixed = seed
        .wrapping_mul(37)
        .wrapping_add(byte_index.wrapping_mul(19))
        .wrapping_add(byte_index.rotate_left(3))
        .wrapping_add(11);
    u8::try_from(mixed & 0xff).unwrap_or(0)
}
