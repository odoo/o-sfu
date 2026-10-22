//! Incremental decoding of frames from a byte stream, shared by the stream framings: the
//! decoder buffers the bytes, a [`FrameCodec`] finds the frame boundaries.

use std::{collections::VecDeque, io::Result as IoResult, marker::PhantomData};

pub(crate) const DECODER_CHUNK_SIZE: usize = 4096;

pub type Decoded<Frame, Error> = Option<Result<(Frame, usize), Error>>;

/// How a stream framing finds the boundary of the next frame.
pub trait FrameCodec {
    type Frame;
    type Error;

    /// Decodes the first frame of `unread`.
    fn try_decode(unread: &[u8]) -> Decoded<Self::Frame, Self::Error>;
}

/// Incremental decoder of frames: bytes go in through [`StreamDecoder::feed_with`], frames
/// come out by iterating.
///
/// Drain the iterator after each feed, otherwise decoded frames accumulate in the buffer.
///
/// On error the stream is desynchronized and cannot be recovered: no byte is consumed, so
/// every further call returns the same error. Close the stream.
pub struct StreamDecoder<C> {
    bytes: VecDeque<u8>,
    codec: PhantomData<C>,
}

impl<C> Default for StreamDecoder<C> {
    fn default() -> Self {
        Self {
            bytes: VecDeque::new(),
            codec: PhantomData,
        }
    }
}

impl<C: FrameCodec> StreamDecoder<C> {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Lends buffer space to `f` to **directly** feed bytes into the decoder. The bytes
    /// are parsed into frames by [`StreamDecoder::next`].
    ///
    /// Returns what `f` returned, `Ok(0)` means EOF.
    ///
    /// # Errors
    ///
    /// Returns the error of `f` unchanged, and no bytes are kept.
    pub fn feed_with(&mut self, f: impl FnOnce(&mut [u8]) -> IoResult<usize>) -> IoResult<usize> {
        let slice_start = self.bytes.len();
        self.bytes.resize(slice_start + DECODER_CHUNK_SIZE, 0);
        let slice = self
            .bytes
            .make_contiguous()
            .get_mut(slice_start..)
            .unwrap_or_default();
        let result = f(slice);
        self.bytes
            .truncate(slice_start + result.as_ref().map_or(0, |&count| count));
        result
    }
}

impl<C: FrameCodec> Iterator for StreamDecoder<C> {
    type Item = Result<C::Frame, C::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let decoded = C::try_decode(self.bytes.make_contiguous())?;
        Some(decoded.map(|(frame, length)| {
            self.bytes.drain(..length);
            frame
        }))
    }
}
