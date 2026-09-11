use std::hint::black_box;

use o_sfu_rfc::rtp::CodecName;
use o_sfu_router::{
    MediaKind,
    rtp::{MediaFormat, MediaStream, PayloadType},
};
use str0m::{
    media::{Mid, Pt},
    rtp::{SeqNo, Ssrc},
};

use super::{ConsumerStreamStore, SourceRtpIdentity, SourceTransition};
use crate::engine::media_transport::rtc::{codec, state::slots::ConsumerStreamHandle};

const RTP_REWRITE_PACKETS: usize = 4096;

#[derive(Clone, Copy)]
enum RewriteMode {
    Steady,
    Switching,
}

struct RewriteInput {
    source_ssrc: Ssrc,
    sequence_number: SeqNo,
    timestamp: u32,
    codec_identity: codec::PacketIdentity,
}

/// fixed RTP identity rewrite fixture for local egress benchmarks
///
/// setup allocates one destination rewrite stream. the measured path projects
/// either a steady SSRC sequence or alternating simulcast SSRC packets with VP8
/// picture identifiers
pub struct LocalRewriteBenchFixture {
    streams: ConsumerStreamStore,
    inputs: Vec<RewriteInput>,
    stream_handle: ConsumerStreamHandle,
}

impl LocalRewriteBenchFixture {
    #[must_use]
    pub fn steady_ssrc() -> Self {
        Self::new(RewriteMode::Steady)
    }

    #[must_use]
    pub fn switching_ssrc() -> Self {
        Self::new(RewriteMode::Switching)
    }

    fn new(mode: RewriteMode) -> Self {
        let mut streams = ConsumerStreamStore::default();
        let stream_handle = streams.allocate(Mid::default());
        Self {
            streams,
            inputs: rewrite_inputs(mode),
            stream_handle,
        }
    }

    #[must_use]
    pub fn project_packets(&mut self) -> u64 {
        let mut checksum = 0_u64;
        for input in &self.inputs {
            if let Some(identity) = self.streams.project_identity(
                self.stream_handle,
                SourceRtpIdentity {
                    delivery_generation: 0,
                    ssrc: input.source_ssrc,
                    seq_no: input.sequence_number,
                    timestamp: input.timestamp,
                    was_repair: false,
                },
                input.codec_identity,
            ) {
                black_box(identity.codec);
                checksum = checksum
                    .wrapping_add(u64::from(identity.rtp_timestamp))
                    .wrapping_add(u64::from(u8::from(matches!(
                        identity.transition,
                        SourceTransition::Switched { .. }
                    ))));
            }
        }
        checksum
    }
}

fn rewrite_inputs(mode: RewriteMode) -> Vec<RewriteInput> {
    let parameters = MediaStream::new(
        vec![MediaFormat::new(
            MediaKind::Video,
            CodecName::Vp8,
            PayloadType::new(96),
            90_000,
        )],
        vec![],
        vec![],
    );
    let inspector = codec::PacketInspector::from_parameters(&parameters);
    let mut inputs = Vec::with_capacity(RTP_REWRITE_PACKETS);
    for pkt_idx in 0..RTP_REWRITE_PACKETS {
        let pkt_idx_u32 = u32::try_from(pkt_idx).unwrap_or(0);
        let pkt_idx_u16 = u16::try_from(pkt_idx % 1024).unwrap_or(0);
        let pkt_idx_u8 = u8::try_from(pkt_idx % 64).unwrap_or(0);
        let [picture_id_high, picture_id_low] = pkt_idx_u16.to_be_bytes();
        let source_ssrc = match mode {
            RewriteMode::Steady => Ssrc::from(11),
            RewriteMode::Switching if pkt_idx % 2 == 0 => Ssrc::from(11),
            RewriteMode::Switching => Ssrc::from(12),
        };
        let payload = [
            0x90,
            0xe0,
            0x80 | picture_id_high,
            picture_id_low,
            pkt_idx_u8,
            0,
            0,
        ];
        inputs.push(RewriteInput {
            source_ssrc,
            sequence_number: u64::try_from(pkt_idx).unwrap_or(0).into(),
            timestamp: 90_000_u32.wrapping_add(pkt_idx_u32),
            codec_identity: inspector.inspect(Pt::from(96), &payload, true).identity(),
        });
    }
    inputs
}
