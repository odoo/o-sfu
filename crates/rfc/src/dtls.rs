//! Legacy DTLS record framing from [RFC 6347 section 4.1](https://www.rfc-editor.org/rfc/rfc6347.html#section-4.1).

/// Record header containing type, version, epoch, sequence number and length.
const RECORD_HEADER_BYTES: usize = 13;
/// Version follows the one-byte content type.
const VERSION_OFFSET: usize = 1;
/// Length follows type, version, epoch and the 48-bit sequence number.
const LENGTH_OFFSET: usize = 11;
/// DTLS 1.2 wire version, distinct from TLS 1.2's version bytes.
const VERSION_1_2: [u8; 2] = [0xfe, 0xfd];

/// DTLS 1.0 record-layer wire version.
/// Reference: <https://www.rfc-editor.org/rfc/rfc4347.html#section-4.1>
const VERSION_1_0: [u8; 2] = [0xfe, 0xff];

/// Checks the first record after the WebRTC multiplexer has selected DTLS.
/// Later records, record contents and cryptographic validity remain unchecked.
pub(crate) fn has_datagram_shape(packet: &[u8]) -> bool {
    let Some(header) = packet.first_chunk::<RECORD_HEADER_BYTES>() else {
        return false;
    };
    let version = header.get(VERSION_OFFSET..VERSION_OFFSET + VERSION_1_0.len());
    let length = u16::from_be_bytes([header[LENGTH_OFFSET], header[LENGTH_OFFSET + 1]]);
    (version == Some(VERSION_1_0.as_slice()) || version == Some(VERSION_1_2.as_slice()))
        && usize::from(length) + RECORD_HEADER_BYTES <= packet.len()
}
