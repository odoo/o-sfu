use super::packet_fingerprint;

#[test]
fn packet_fingerprint_preserves_bytes_order_and_padding() {
    const CASES: [(usize, u64); 8] = [
        (0, 0x0000_0000_0000_0000),
        (1, 0x0000_0000_0002_0000),
        (7, 0x9068_4020_3026_e0b8),
        (8, 0x9068_4018_30d8_e0b8),
        (15, 0xc820_7850_689e_98f0),
        (16, 0xd028_0058_70a8_a0f8),
        (17, 0xd830_08a0_78b2_a8c0),
        (32, 0x50a8_80d8_f048_2078),
    ];
    for (len, expected) in CASES {
        let packet: Vec<u8> = (0..len)
            .map(|index| u8::try_from(index).unwrap_or(0))
            .collect();
        for offset in 0..16 {
            let mut storage = vec![0xa5; offset];
            storage.extend_from_slice(&packet);
            let (_, unaligned_packet) = storage.split_at(offset);
            assert_eq!(packet_fingerprint(unaligned_packet), expected);
        }
    }
}
