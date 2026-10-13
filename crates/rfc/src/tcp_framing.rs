//! RFC 4571 framing for RTP/RTCP over TCP.
//! - Framing RTP and RTCP packets over connection-oriented transport: <https://www.rfc-editor.org/rfc/rfc4571>

use std::{
    collections::VecDeque,
    io::{IoSlice, Result as IoResult},
};

/// The payload doesn't fit in the 2 bytes length header of a frame.
#[derive(Debug, PartialEq, Eq)]
pub struct FrameTooLargeError;

#[derive(Debug, PartialEq)]
pub struct TcpFrame {
    prefix: [u8; 2],
    pub payload: Vec<u8>,
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
    #[must_use]
    pub fn io_slices(&self) -> [IoSlice<'_>; 2] {
        [IoSlice::new(&self.prefix), IoSlice::new(&self.payload)]
    }
}

pub const DECODER_CHUNK_SIZE: usize = 4096;

#[derive(Default)]
pub struct FrameDecoder {
    buffer: VecDeque<u8>,
}

impl FrameDecoder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Lends buffer space to `f` to **directly** feed bytes into the decoder. The bytes
    /// are parsed into  [`TcpFrame`]s by [`FrameDecoder::next`].
    ///
    /// Returns what `f` returned, `Ok(0)` means EOF.
    ///
    /// # Errors
    ///
    /// Returns the error of `f` unchanged, and no bytes are kept.
    pub fn feed_with(&mut self, f: impl FnOnce(&mut [u8]) -> IoResult<usize>) -> IoResult<usize> {
        let slice_start: usize = self.buffer.len();
        self.buffer.resize(slice_start + DECODER_CHUNK_SIZE, 0);
        let slice = self
            .buffer
            .make_contiguous()
            .get_mut(slice_start..)
            .unwrap_or_default();
        let result = f(slice);
        self.buffer
            .truncate(slice_start + result.as_ref().map_or(0, |&count| count));
        result
    }
}

impl Iterator for FrameDecoder {
    type Item = TcpFrame;

    fn next(&mut self) -> Option<Self::Item> {
        let unread = self.buffer.make_contiguous();
        let &[first, second] = unread.first_chunk::<2>()?;
        let length = u16::from_be_bytes([first, second]);
        let frame_end = 2 + usize::from(length);
        let payload = unread.get(2..frame_end)?.to_vec();
        self.buffer.drain(..frame_end);
        TcpFrame::try_new(payload).ok()
    }
}

#[cfg(test)]
#[path = "TESTS/tcp_framing.rs"]
mod tests;
