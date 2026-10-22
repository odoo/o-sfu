//! RFC 4571 framing for RTP/RTCP over TCP.
//! - Framing RTP and RTCP packets over connection-oriented transport: <https://www.rfc-editor.org/rfc/rfc4571>

use std::io::IoSlice;

use crate::stream_decoder::{Decoded, FrameCodec, StreamDecoder};

/// The payload doesn't fit in the 2 bytes length header of a frame or exceeds [`MAX_FRAME_SIZE`].
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("frame exceeds the maximum size")]
pub struct FrameTooLargeError;

#[derive(Debug, PartialEq)]
pub struct TcpFrame {
    prefix: [u8; 2],
    payload: Vec<u8>,
}

impl TcpFrame {
    /// Frames `payload` with its RFC 4571 length prefix.
    ///
    /// # Errors
    ///
    /// Returns [`FrameTooLargeError`] when the payload doesn't fit in the 2 bytes length
    /// header.
    pub fn try_new(payload: Vec<u8>) -> Result<Self, FrameTooLargeError> {
        let length = u16::try_from(payload.len()).map_err(|_e| FrameTooLargeError)?;
        Ok(Self {
            prefix: u16::to_be_bytes(length),
            payload,
        })
    }

    /// Whether this frame is NULL according to RFC 4571 (i.e. prefix length of 0).
    #[must_use]
    pub fn is_null(&self) -> bool {
        self.payload.is_empty()
    }

    #[must_use]
    pub fn io_slices(&self) -> [IoSlice<'_>; 2] {
        [IoSlice::new(&self.prefix), IoSlice::new(&self.payload)]
    }
    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// Largest frame payload the decoder accepts, in bytes.
///
/// Stricter than RFC 4571, which allows up to 65535 bytes. The decoder buffers a whole
/// frame before returning it, so this is the most a peer can make us hold.
///
/// STUN, DTLS and RTCP should keep their messages within the path MTU (RFC 8489 §6.1, RFC
/// 9147 §4.3, RFC 3550 §6.1), which is usually 1500 bytes.
pub const MAX_FRAME_SIZE: u16 = 4096;

/// Incremental decoder of RFC 4571 frames, see [`StreamDecoder`].
pub type TcpFrameDecoder = StreamDecoder<TcpCodec>;

/// Finds RFC 4571 frames: a 2 bytes length prefix, then the payload.
#[derive(Debug)]
pub struct TcpCodec;

impl FrameCodec for TcpCodec {
    type Frame = TcpFrame;
    type Error = FrameTooLargeError;

    fn try_decode(unread: &[u8]) -> Decoded<TcpFrame, FrameTooLargeError> {
        let &[first, second] = unread.first_chunk::<2>()?;
        let length = u16::from_be_bytes([first, second]);
        if length > MAX_FRAME_SIZE {
            return Some(Err(FrameTooLargeError));
        }
        let frame_end = 2 + usize::from(length);
        let payload = unread.get(2..frame_end)?;
        Some(TcpFrame::try_new(payload.to_vec()).map(|frame| (frame, frame_end)))
    }
}

#[cfg(test)]
#[path = "TESTS/tcp_framing.rs"]
mod tests;
