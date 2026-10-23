//! STUN message framing from [RFC 8489 section 5](https://www.rfc-editor.org/rfc/rfc8489.html#section-5).

/// Fixed STUN header length, including the 96-bit transaction ID.
const HEADER_BYTES: usize = 20;
/// Message length follows the two-byte message type.
const LENGTH_OFFSET: usize = 2;
/// Magic cookie follows the two-byte type and two-byte length.
const COOKIE_OFFSET: usize = 4;
/// Attributes are padded to 32-bit boundaries, so message length is a multiple of four.
const ATTRIBUTE_ALIGNMENT_BYTES: u16 = 4;
/// Network-order STUN magic cookie identifying the updated message format.
const MAGIC_COOKIE: [u8; 4] = [0x21, 0x12, 0xa4, 0x42];

/// Checks framing after the WebRTC multiplexer has selected STUN.
/// Attributes, method validity, integrity and fingerprints remain unchecked.
pub(crate) fn has_datagram_shape(packet: &[u8]) -> bool {
    let Some(header) = packet.first_chunk::<HEADER_BYTES>() else {
        return false;
    };
    let length = u16::from_be_bytes([header[LENGTH_OFFSET], header[LENGTH_OFFSET + 1]]);
    header.get(COOKIE_OFFSET..COOKIE_OFFSET + MAGIC_COOKIE.len()) == Some(MAGIC_COOKIE.as_slice())
        && length.is_multiple_of(ATTRIBUTE_ALIGNMENT_BYTES)
        && usize::from(length) + HEADER_BYTES == packet.len()
}
