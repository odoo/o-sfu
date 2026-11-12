use str0m::{crypto::from_feature_flags, ice::StunMessage};

const RTP_HEADER_LEN: usize = 12;

#[cfg(test)]
#[must_use]
pub fn sample_rtp_packet(sequence_number: u16, ssrc: u32) -> Vec<u8> {
    sample_rtp_packet_with_len(sequence_number, ssrc, RTP_HEADER_LEN)
}

#[must_use]
pub fn sample_rtp_packet_with_len(sequence_number: u16, ssrc: u32, packet_len: usize) -> Vec<u8> {
    assert!(
        packet_len >= RTP_HEADER_LEN,
        "sample RTP packet length must include the fixed RTP header"
    );
    let sequence_number = sequence_number.to_be_bytes();
    let ssrc = ssrc.to_be_bytes();
    let mut packet = Vec::with_capacity(packet_len);
    packet.extend_from_slice(&[
        0x80,
        96,
        sequence_number[0],
        sequence_number[1],
        0,
        0,
        0,
        1,
        ssrc[0],
        ssrc[1],
        ssrc[2],
        ssrc[3],
    ]);
    for byte_index in packet.len()..packet_len {
        let mixed = byte_index
            .wrapping_mul(31)
            .wrapping_add(byte_index.rotate_left(5))
            .wrapping_add(17);
        let [byte, ..] = mixed.to_le_bytes();
        packet.push(byte);
    }
    packet
}

pub fn serialize_stun_message(
    message: &StunMessage<'_>,
    password: Option<&[u8]>,
) -> Option<Vec<u8>> {
    let mut buffer = [0_u8; 1024];
    let crypto_provider = from_feature_flags();
    let sha1_hmac = |key: &[u8], payloads: &[&[u8]]| {
        crypto_provider.sha1_hmac_provider.sha1_hmac(key, payloads)
    };
    let len = message.to_bytes(password, &mut buffer, sha1_hmac).ok()?;
    buffer.get(..len).map(<[u8]>::to_vec)
}
