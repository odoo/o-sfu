use std::io::{self, Read};

use super::*;
use crate::stream_decoder;

/// Feeds `chunk` to the decoder the way a socket would: through [`FrameDecoder::feed_with`].
fn feed_chunk(decoder: &mut FrameDecoder, chunk: &[u8]) {
    let mut source = chunk;
    while !source.is_empty() {
        decoder
            .feed_with(|buf| source.read(buf))
            .expect("reading from a slice cannot fail");
    }
}

#[track_caller]
fn assert_round_trips(name: &str, chunks: &[&[u8]], expected: &[&[u8]]) {
    let mut decoder = FrameDecoder::new();
    let mut frames = Vec::new();
    for chunk in chunks {
        feed_chunk(&mut decoder, chunk);
        for frame in decoder.by_ref() {
            frames.push(frame.expect("frame shouldn't be too large"));
        }
    }
    let payloads: Vec<&[u8]> = frames
        .iter()
        .map(|frame| frame.payload.as_slice())
        .collect();
    assert_eq!(payloads, expected, "case: {name} (Chunks => TcpFrame).");
    let back_to_wire: Vec<u8> = frames
        .iter()
        .flat_map(|frame| frame.io_slices())
        .flat_map(|slice| slice.to_vec())
        .collect();
    assert_eq!(
        back_to_wire,
        chunks.concat(),
        "case: {name} (TcpFrame => Chunks)"
    );
}

#[test]
fn chunked_streams_round_trip_through_frames() {
    assert_round_trips(
        "Complete frame in one chunk",
        &[&[0, 3, 1, 2, 3]],
        &[&[1, 2, 3]],
    );
    assert_round_trips("Null frame in one chunk", &[&[0, 0]], &[&[]]);
    assert_round_trips(
        "Length split in two chunks",
        &[&[0], &[2, 7, 8]],
        &[&[7, 8]],
    );
    assert_round_trips(
        "Length and payload in separate chunks",
        &[&[0, 3], &[1, 2, 3]],
        &[&[1, 2, 3]],
    );
    assert_round_trips(
        "Payload split in several chunks",
        &[&[0, 3, 1], &[2], &[3]],
        &[&[1, 2, 3]],
    );
    assert_round_trips("Two frames in one chunk", &[&[0, 1, 9, 0, 0]], &[&[9], &[]]);
    assert_round_trips(
        "Chunk ends one frame and starts the next",
        &[&[0, 1, 9, 0], &[1, 8]],
        &[&[9], &[8]],
    );
    assert_round_trips(
        "Null frame split between its two bytes",
        &[&[0], &[0]],
        &[&[]],
    );
    let payload: Vec<u8> = (0..=u8::MAX).cycle().take(258).collect();
    assert_round_trips(
        "Length spanning two bytes",
        &[&[1, 2], &payload],
        &[&payload],
    );
    let max_size_payload: Vec<u8> = (0..=u8::MAX)
        .cycle()
        .take(usize::from(MAX_FRAME_SIZE))
        .collect();
    let wire = [&MAX_FRAME_SIZE.to_be_bytes()[..], &max_size_payload].concat();
    assert!(wire.len() > stream_decoder::DECODER_CHUNK_SIZE);
    assert_round_trips(
        "Frame of the maximum size needing several reads",
        &[&wire],
        &[&max_size_payload],
    );
}

#[test]
fn incomplete_packet_produces_no_frame() {
    let mut decoder = FrameDecoder::new();
    feed_chunk(&mut decoder, &[0, 3, 1, 2]);
    assert_eq!(decoder.next(), None);
    feed_chunk(&mut decoder, &[3]);
    assert_eq!(decoder.next(), Some(TcpFrame::try_new(vec![1, 2, 3])));
    assert_eq!(decoder.next(), None);
}

#[test]
fn feed_with_errors_does_not_corrupt_decoder() {
    let mut decoder = FrameDecoder::new();
    feed_chunk(&mut decoder, &[0, 2, 7]);
    let error = decoder
        .feed_with(|_| Err(io::ErrorKind::ConnectionReset.into()))
        .expect_err("the closure failed");
    assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
    feed_chunk(&mut decoder, &[8]);
    assert_eq!(decoder.next(), Some(TcpFrame::try_new(vec![7, 8])));
    assert_eq!(decoder.next(), None);
}

#[test]
fn oversized_packet_is_rejected_by_try_new() {
    assert_eq!(
        TcpFrame::try_new(vec![0; usize::from(u16::MAX) + 1]),
        Err(FrameTooLargeError)
    );
    assert!(TcpFrame::try_new(vec![0; usize::from(u16::MAX)]).is_ok());
}

#[test]
fn packet_size_greater_than_max_frame_size_is_rejected_by_next() {
    let mut decoder = FrameDecoder::new();
    feed_chunk(&mut decoder, &(MAX_FRAME_SIZE + 1).to_be_bytes());
    assert_eq!(decoder.next(), Some(Err(FrameTooLargeError)));
}
