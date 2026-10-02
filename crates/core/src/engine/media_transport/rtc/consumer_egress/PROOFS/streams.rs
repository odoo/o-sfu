use str0m::rtp::SeqNo;

use super::{ProjectedRtp, RtpMapping, RtpPacket, RtpProjectionOutcome};

const SEQUENCE_LIMIT: u128 = (1_u128 << 64) - 1;
const TIMESTAMP_MODULUS: u64 = 1_u64 << 32;

#[derive(Clone, Copy)]
struct Packet {
    ssrc: u32,
    sequence: u64,
    timestamp: u32,
    reanchor: bool,
    repair: bool,
    timestamp_anchor: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Mapping {
    ssrc: u32,
    source: u128,
    destination: u128,
    highest_source: u128,
    source_timestamp: u64,
    destination_timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Model {
    next: u128,
    mapping: Option<Mapping>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Projected {
    sequence: u128,
    timestamp: u64,
    advances: bool,
    previous_ssrc: Option<u32>,
}

/// Proves `RtpMapping::start` and `RtpMapping::project` against affine RTP math.
/// Checks full-width sequence/timestamp inputs, arbitrary transition anchors,
/// the optimized packet path and overflow boundaries. Rejection and observation
/// preserve numeric mapping state.
///
/// The driver selects only empty or active storage. Production methods select
/// sources, reanchors and repairs. Rejected packets do not request switch timing.
/// Excludes generation admission, clock estimation/commits, codec commits and
/// RTC I/O. Repairs do not reanchor.
#[kani::proof]
fn rtp_projection_matches_affine_model() {
    let mut projection = arbitrary_projection();
    let before = snapshot(projection);
    kani::assume(before.valid());
    let packet = Packet {
        ssrc: kani::any(),
        sequence: kani::any(),
        timestamp: kani::any(),
        reanchor: kani::any(),
        repair: kani::any(),
        timestamp_anchor: kani::any(),
    };
    kani::assume(!packet.repair || !packet.reanchor);
    let mut expected_state = before;
    let expected = expected_state.project(packet);
    let mut requested_switch_time = false;
    let actual = projection
        .project(
            RtpPacket {
                ssrc: packet.ssrc.into(),
                seq_no: packet.sequence.into(),
                timestamp: packet.timestamp,
                was_repair: packet.repair,
            },
            packet.reanchor,
            |_| {
                requested_switch_time = true;
                packet.timestamp_anchor
            },
        )
        .map(projected_snapshot);
    let after = snapshot(projection);
    assert_eq!(actual, expected);
    assert_eq!(after, expected_state);
    assert!(after.valid());
    let replaces = before
        .mapping
        .is_some_and(|line| packet.ssrc != line.ssrc || packet.reanchor);
    assert_eq!(requested_switch_time, actual.is_some() && replaces);
    if actual.is_none_or(|output| !output.advances) {
        assert_eq!(after, before);
    }
    cover_transitions(before, after, packet, actual);
}

#[derive(Clone, Copy)]
struct Projection {
    next_seq_no: SeqNo,
    mapping: Option<RtpMapping>,
}

impl Projection {
    fn project(
        &mut self,
        source: RtpPacket,
        reanchor: bool,
        switch_timestamp: impl FnOnce(&RtpMapping) -> u32,
    ) -> Option<ProjectedRtp> {
        if let Some(mapping) = &mut self.mapping {
            return mapping.project(&mut self.next_seq_no, source, reanchor, switch_timestamp);
        }
        let (mapping, projected) =
            RtpMapping::start(&mut self.next_seq_no, source, || source.timestamp)?;
        self.mapping = Some(mapping);
        Some(projected)
    }
}

fn arbitrary_projection() -> Projection {
    let mapping = if kani::any() {
        Some(RtpMapping {
            ssrc: kani::any::<u32>().into(),
            src_seq_anchor: kani::any::<u64>().into(),
            dst_seq_anchor: kani::any::<u64>().into(),
            highest_src_seq: kani::any::<u64>().into(),
            src_timestamp_anchor: kani::any(),
            dst_timestamp_anchor: kani::any(),
        })
    } else {
        None
    };
    Projection {
        next_seq_no: kani::any::<u64>().into(),
        mapping,
    }
}

fn snapshot(projection: Projection) -> Model {
    let mapping = projection.mapping.map(|line| Mapping {
        ssrc: *line.ssrc,
        source: u128::from(*line.src_seq_anchor),
        destination: u128::from(*line.dst_seq_anchor),
        highest_source: u128::from(*line.highest_src_seq),
        source_timestamp: u64::from(line.src_timestamp_anchor),
        destination_timestamp: u64::from(line.dst_timestamp_anchor),
    });
    Model {
        next: u128::from(*projection.next_seq_no),
        mapping,
    }
}

fn projected_snapshot(projected: ProjectedRtp) -> Projected {
    let (advances, previous_ssrc) = match projected.outcome {
        RtpProjectionOutcome::Advanced => (true, None),
        RtpProjectionOutcome::Observed => (false, None),
        RtpProjectionOutcome::Switched { previous_ssrc } => (true, Some(*previous_ssrc)),
    };
    Projected {
        sequence: u128::from(*projected.seq_no),
        timestamp: u64::from(projected.rtp_timestamp),
        advances,
        previous_ssrc,
    }
}

impl Model {
    fn valid(self) -> bool {
        self.next <= SEQUENCE_LIMIT
            && self.mapping.is_none_or(|line| {
                line.source <= line.highest_source
                    && line.destination + (line.highest_source - line.source) + 1 == self.next
            })
    }

    fn project(&mut self, packet: Packet) -> Option<Projected> {
        let source = u128::from(packet.sequence);
        let source_timestamp = u64::from(packet.timestamp);
        if packet.repair
            && !self.mapping.is_some_and(|line| {
                line.ssrc == packet.ssrc && line.source <= source && source < line.highest_source
            })
        {
            return None;
        }
        let continued = self
            .mapping
            .filter(|line| line.ssrc == packet.ssrc && !packet.reanchor);
        let (sequence, timestamp, advances) = if let Some(line) = continued {
            if source < line.source {
                return None;
            }
            (
                line.destination + source - line.source,
                (line.destination_timestamp + TIMESTAMP_MODULUS + source_timestamp
                    - line.source_timestamp)
                    % TIMESTAMP_MODULUS,
                source > line.highest_source,
            )
        } else {
            (
                self.next,
                self.mapping
                    .map_or(source_timestamp, |_| u64::from(packet.timestamp_anchor)),
                true,
            )
        };
        if sequence + u128::from(advances) > SEQUENCE_LIMIT {
            return None;
        }
        let previous_ssrc = self
            .mapping
            .filter(|line| line.ssrc != packet.ssrc)
            .map(|line| line.ssrc);
        if advances {
            self.next = sequence + 1;
            self.mapping = Some(Mapping {
                highest_source: source,
                ..continued.unwrap_or(Mapping {
                    ssrc: packet.ssrc,
                    source,
                    destination: sequence,
                    highest_source: source,
                    source_timestamp,
                    destination_timestamp: timestamp,
                })
            });
        }
        Some(Projected {
            sequence,
            timestamp,
            advances,
            previous_ssrc,
        })
    }
}

fn cover_transitions(before: Model, after: Model, packet: Packet, output: Option<Projected>) {
    let accepted = output.is_some();
    let Some(line) = before.mapping else {
        kani::cover!(accepted, "first primary");
        kani::cover!(packet.repair && !accepted, "repair before first primary");
        kani::cover!(
            before.next == SEQUENCE_LIMIT && !packet.repair,
            "exhausted seed"
        );
        return;
    };
    let source = u128::from(packet.sequence);
    let continued = packet.ssrc == line.ssrc && !packet.reanchor;
    let primary = !packet.repair;
    let exhausted = before.next == SEQUENCE_LIMIT;
    kani::cover!(
        continued && primary && source == line.highest_source + 1 && accepted,
        "consecutive primary"
    );
    kani::cover!(
        continued && primary && source > line.highest_source + 1 && accepted,
        "gapped primary"
    );
    kani::cover!(
        continued && primary && source == line.highest_source && accepted,
        "duplicate primary"
    );
    kani::cover!(
        continued && primary && source < line.highest_source && accepted,
        "reordered primary"
    );
    kani::cover!(packet.repair && accepted, "admitted repair");
    kani::cover!(
        packet.repair && packet.ssrc != line.ssrc && !accepted,
        "repair from another source"
    );
    kani::cover!(
        continued && primary && source < line.source && !accepted,
        "before source anchor"
    );
    kani::cover!(
        continued
            && primary
            && source >= line.source
            && line.destination + source > SEQUENCE_LIMIT + line.source
            && !accepted,
        "receiver sequence overflow"
    );
    kani::cover!(
        continued
            && primary
            && source > line.highest_source
            && line.destination + source == SEQUENCE_LIMIT + line.source
            && !accepted,
        "receiver successor overflow"
    );
    kani::cover!(
        !exhausted && after.next == SEQUENCE_LIMIT && accepted,
        "last representable primary"
    );
    kani::cover!(
        exhausted && primary && continued && accepted,
        "primary observation after exhaustion"
    );
    kani::cover!(
        exhausted && packet.repair && accepted,
        "repair after exhaustion"
    );
    kani::cover!(
        exhausted && primary && continued && source == line.highest_source + 1 && !accepted,
        "exhausted consecutive primary"
    );
    kani::cover!(
        exhausted && primary && continued && source > line.highest_source + 1 && !accepted,
        "exhausted gapped primary"
    );
    kani::cover!(
        exhausted && packet.reanchor && packet.ssrc == line.ssrc && !accepted,
        "exhausted reanchor"
    );
    kani::cover!(
        exhausted && primary && packet.ssrc != line.ssrc && !accepted,
        "exhausted source switch"
    );
    kani::cover!(packet.ssrc != line.ssrc && accepted, "source switch");
    kani::cover!(
        packet.reanchor && packet.ssrc == line.ssrc && accepted,
        "same source reanchor"
    );
    kani::cover!(
        continued && u64::from(packet.timestamp) < line.source_timestamp && accepted,
        "source timestamp rollover"
    );
    kani::cover!(
        !continued
            && packet.timestamp_anchor == 0
            && line.destination_timestamp + 1 == TIMESTAMP_MODULUS
            && accepted,
        "receiver timestamp rollover on reanchor"
    );
    kani::cover!(
        !continued && packet.timestamp_anchor == u32::MAX && accepted,
        "maximum receiver timestamp on reanchor"
    );
    kani::cover!(
        continued && primary && line.highest_source == SEQUENCE_LIMIT && source == 0 && !accepted,
        "extended source sequence rollover"
    );
}
