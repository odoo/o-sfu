//! Shared RID and SDP mechanics for codec simulcast profiles.

use std::collections::BTreeMap;

use itertools::Itertools;
use o_sfu_rfc::webrtc::{self, sdp};
use o_sfu_router::rtp::MediaStream;
use str0m::{
    change::SdpAnswer,
    media::{Mid, Rid, Simulcast, SimulcastLayer},
};

use super::super::state::route_control::PacketLayerGate;
use crate::{Bitrate, VideoBitrateLimits, engine::media_transport::SessionUploadEncoding};

pub(super) const DEFAULT_LOW_RID: &str = "lo";
pub(super) const DEFAULT_MIDDLE_RID: &str = "mid";
pub(super) const DEFAULT_HIGH_RID: &str = "hi";
pub(super) const DEFAULT_LOW_MAX_BITRATE: Bitrate = Bitrate::from_kbps(150);
const MIDDLE_BITRATE_DIVISOR: u64 = 5;
const MAX_SEND_STREAMS: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::engine::media_transport::rtc) struct NegotiatedRid {
    pub(in crate::engine::media_transport::rtc) rid: Rid,
    pub(in crate::engine::media_transport::rtc) max_bitrate: Option<Bitrate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::media_transport::rtc) struct SimulcastAnswerError;

pub(in crate::engine::media_transport::rtc) struct ParsedAnswerRids {
    by_mid: BTreeMap<Mid, Result<Vec<AnswerRid>, SimulcastAnswerError>>,
}

impl ParsedAnswerRids {
    pub(in crate::engine::media_transport::rtc) fn parse(
        answer_sdp: &str,
        answer: &SdpAnswer,
    ) -> Self {
        let mut by_mid = BTreeMap::new();
        // str0m 0.21's public answer view drops RID restrictions and all but
        // the first RID alternative. Keep the original media section until
        // upstream exposes those declarations without loss.
        // https://github.com/algesten/str0m/blob/0.21.0/src/sdp/data.rs#L557-L593
        // https://github.com/algesten/str0m/blob/0.21.0/src/sdp/parser.rs#L682-L700
        for (media_line, section) in answer
            .media_lines
            .iter()
            .zip(answer_sdp.split("\nm=").skip(1))
        {
            match parse_section_rids(section) {
                Ok(rids) if rids.is_empty() => {}
                result => {
                    by_mid.insert(media_line.mid(), result);
                }
            }
        }
        Self { by_mid }
    }

    pub(in crate::engine::media_transport::rtc) fn negotiate(
        &self,
        mid: Mid,
        offered_encodings: &[SessionUploadEncoding],
    ) -> Result<Vec<NegotiatedRid>, SimulcastAnswerError> {
        match self.by_mid.get(&mid) {
            Some(Ok(parsed_rids)) => negotiate_answer_rids(parsed_rids, offered_encodings),
            Some(Err(error)) => Err(*error),
            None => Ok(Vec::new()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LayerSpec<'a> {
    pub(super) rid: &'a str,
    pub(super) max_bitrate: Option<Bitrate>,
}

pub(super) fn default_layers(video_bitrate_limits: VideoBitrateLimits) -> [LayerSpec<'static>; 3] {
    let high_max_bitrate = video_bitrate_limits.max_video_bitrate();
    let low_max_bitrate = DEFAULT_LOW_MAX_BITRATE.min(high_max_bitrate);
    [
        LayerSpec {
            rid: DEFAULT_LOW_RID,
            max_bitrate: Some(low_max_bitrate),
        },
        LayerSpec {
            rid: DEFAULT_MIDDLE_RID,
            max_bitrate: Some(
                high_max_bitrate
                    .divided_by(MIDDLE_BITRATE_DIVISOR)
                    .max(low_max_bitrate),
            ),
        },
        LayerSpec {
            rid: DEFAULT_HIGH_RID,
            max_bitrate: Some(high_max_bitrate),
        },
    ]
}

pub(super) fn recv_simulcast(layers: &[LayerSpec<'_>]) -> Simulcast {
    Simulcast {
        send: Vec::new(),
        recv: layers
            .iter()
            .map(|layer| SimulcastLayer {
                rid: Rid::from(layer.rid),
                attributes: layer.max_bitrate.map(|max_bitrate| {
                    vec![(
                        sdp::rid_restriction::MAX_BITRATE.to_owned(),
                        max_bitrate.as_bps().to_string(),
                    )]
                }),
            })
            .collect(),
    }
}

pub(super) fn layers_from_bindings(parameters: &MediaStream) -> Option<Vec<LayerSpec<'_>>> {
    let mut layers = Vec::new();
    for encoding in parameters.bindings() {
        let Some(rid) = encoding.rid() else {
            continue;
        };
        if !sdp::rid::is_id(rid) || layers.iter().any(|layer: &LayerSpec<'_>| layer.rid == rid) {
            continue;
        }
        layers.push(LayerSpec {
            rid,
            max_bitrate: encoding.max_bitrate().map(Bitrate::from_bps),
        });
    }
    (layers.len() >= 2).then_some(layers)
}

/// Chooses the conservative layer used until route policy selects one.
///
/// A complete bitrate ladder starts on the cheapest RID to avoid an unsolicited
/// high-rate burst. Without complete bitrate metadata the negotiated binding
/// order is the only deterministic signal. Any RID-less binding keeps the gate
/// open because an RID gate would make it unreachable.
pub(in crate::engine::media_transport::rtc) fn initial_packet_gate(
    parameters: &MediaStream,
) -> PacketLayerGate {
    let mut first_rid = None;
    let mut lowest_bitrate_rid = None;
    let mut all_encodings_have_bitrate = true;
    for encoding in parameters.bindings() {
        let Some(rid) = encoding.rid().map(Rid::from) else {
            return PacketLayerGate::Open;
        };
        if first_rid.is_none() {
            first_rid = Some(rid);
        }
        let bitrate = encoding.max_bitrate().map(Bitrate::from_bps);
        all_encodings_have_bitrate &= bitrate.is_some();
        if let Some(bitrate) = bitrate {
            match lowest_bitrate_rid.as_mut() {
                Some((selected_rid, selected_bitrate)) if bitrate < *selected_bitrate => {
                    *selected_rid = rid;
                    *selected_bitrate = bitrate;
                }
                Some(_) => {}
                None => lowest_bitrate_rid = Some((rid, bitrate)),
            }
        }
    }
    if all_encodings_have_bitrate && let Some((rid, _bitrate)) = lowest_bitrate_rid {
        return PacketLayerGate::Rid(rid);
    }
    first_rid.map_or(PacketLayerGate::Open, PacketLayerGate::Rid)
}

fn parse_section_rids(section: &str) -> Result<Vec<AnswerRid>, SimulcastAnswerError> {
    let accepted_rids = accepted_send_simulcast_rids(section)?;
    let mut rids = Vec::with_capacity(accepted_rids.len());
    for rid in accepted_rids {
        let declaration = send_rid_declaration(section, rid)?;
        rids.push(AnswerRid {
            rid: declaration.rid.to_owned(),
            max_bitrate: declaration.max_bitrate,
        });
    }
    Ok(rids)
}

fn negotiate_answer_rids(
    answer_rids: &[AnswerRid],
    offered_encodings: &[SessionUploadEncoding],
) -> Result<Vec<NegotiatedRid>, SimulcastAnswerError> {
    answer_rids
        .iter()
        .map(|answer_rid| {
            let max_bitrate = negotiated_rid_max_bitrate(answer_rid, offered_encodings)?;
            Ok(NegotiatedRid {
                rid: Rid::from(answer_rid.rid.as_str()),
                max_bitrate,
            })
        })
        .collect()
}

fn negotiated_rid_max_bitrate(
    answer_rid: &AnswerRid,
    offered_encodings: &[SessionUploadEncoding],
) -> Result<Option<Bitrate>, SimulcastAnswerError> {
    let rid = answer_rid.rid.as_str();
    let Some(offered) = offered_encodings
        .iter()
        .find(|encoding| encoding.rid == rid)
    else {
        return Err(SimulcastAnswerError);
    };
    // RFC 8851 sections 6.3 and 6.4 let an answer tighten an offered
    // restriction but never relax it or introduce a new one. Omission keeps
    // the offered ceiling instead of removing it.
    // https://www.rfc-editor.org/rfc/rfc8851.html#section-6.3
    // https://www.rfc-editor.org/rfc/rfc8851.html#section-6.4
    match (answer_rid.max_bitrate, offered.max_bitrate) {
        (RidMaxBitrate::Absent, offered) => Ok(offered),
        (RidMaxBitrate::Value(answer), Some(offer)) if answer <= offer => Ok(Some(answer)),
        (RidMaxBitrate::Value(_) | RidMaxBitrate::Valueless, _) => Err(SimulcastAnswerError),
    }
}

struct SendRidDeclaration<'a> {
    rid: &'a str,
    max_bitrate: RidMaxBitrate,
}

struct AnswerRid {
    rid: String,
    max_bitrate: RidMaxBitrate,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RidMaxBitrate {
    Absent,
    Valueless,
    Value(Bitrate),
}

fn send_rid_declaration<'a>(
    section: &'a str,
    rid: &str,
) -> Result<SendRidDeclaration<'a>, SimulcastAnswerError> {
    let mut found = None;
    for line in section.lines() {
        let Some(declaration) = parse_send_rid(line.trim_end_matches(sdp::CR))? else {
            continue;
        };
        if declaration.rid != rid {
            continue;
        }
        if found.replace(declaration).is_some() {
            return Err(SimulcastAnswerError);
        }
    }
    found.ok_or(SimulcastAnswerError)
}

fn parse_send_rid(line: &str) -> Result<Option<SendRidDeclaration<'_>>, SimulcastAnswerError> {
    let Some(rid_value) = sdp_attribute_value(line, sdp::attribute::RID) else {
        return Ok(None);
    };
    let mut parts = rid_value.splitn(3, sdp::SP);
    let Some(rid) = parts.next() else {
        return Ok(None);
    };
    if !sdp::rid::is_id(rid) {
        return Ok(None);
    }
    let Some(direction) = parts.next() else {
        return Ok(None);
    };
    if webrtc::RtpStreamDirection::parse(direction) != Some(webrtc::RtpStreamDirection::Send) {
        return Ok(None);
    }
    Ok(Some(SendRidDeclaration {
        rid,
        max_bitrate: parse_rid_restrictions(parts.next())?,
    }))
}

fn accepted_send_simulcast_rids(section: &str) -> Result<Vec<&str>, SimulcastAnswerError> {
    let mut lines = section.lines().filter_map(|line| {
        sdp_attribute_value(line.trim_end_matches(sdp::CR), sdp::attribute::SIMULCAST)
    });
    let Some(value) = lines.next() else {
        return Ok(Vec::new());
    };
    if lines.next().is_some() {
        return Err(SimulcastAnswerError);
    }
    parse_send_simulcast_value(value)
}

fn sdp_attribute_value<'a>(line: &'a str, attribute: &str) -> Option<&'a str> {
    line.strip_prefix(sdp::ATTR)?
        .strip_prefix(attribute)?
        .strip_prefix(sdp::ATTR_SEP)
}

fn parse_send_simulcast_value(value: &str) -> Result<Vec<&str>, SimulcastAnswerError> {
    let mut send = None;
    let mut seen_direction = false;
    let mut parts = value.split_whitespace();
    while let Some(direction) = parts.next() {
        seen_direction = true;
        let Some(rids) = parts.next() else {
            return Err(SimulcastAnswerError);
        };
        match direction {
            sdp::simulcast::DIRECTION_SEND => {
                if send.replace(rids).is_some() {
                    return Err(SimulcastAnswerError);
                }
            }
            sdp::simulcast::DIRECTION_RECV => {}
            _ => return Err(SimulcastAnswerError),
        }
    }
    if !seen_direction {
        return Err(SimulcastAnswerError);
    }
    send.map_or(Ok(Vec::new()), parse_simulcast_rid_list)
}

pub(super) fn send_simulcast_stream_count(value: &str) -> Result<usize, SimulcastAnswerError> {
    parse_send_simulcast_value(value).map(|rids| rids.len())
}

fn parse_simulcast_rid_list(value: &str) -> Result<Vec<&str>, SimulcastAnswerError> {
    let mut rids = Vec::new();
    for stream in value.split(sdp::simulcast::STREAM_SEPARATOR) {
        if stream.contains(sdp::simulcast::ALTERNATIVE_SEPARATOR) {
            // RFC 8853 alternatives describe formats for one simulcast position,
            // not extra layers. `SessionUploadEncoding` models one RID per
            // position, so reject the group instead of silently selecting one.
            // https://www.rfc-editor.org/rfc/rfc8853.html#section-5.2
            return Err(SimulcastAnswerError);
        }
        let rid = sdp::simulcast::strip_initial_pause_prefix(stream).unwrap_or(stream);
        if !sdp::rid::is_id(rid) || rids.contains(&rid) {
            return Err(SimulcastAnswerError);
        }
        rids.push(rid);
        if rids.len() > MAX_SEND_STREAMS {
            return Err(SimulcastAnswerError);
        }
    }
    Ok(rids)
}

fn parse_rid_restrictions(
    restrictions: Option<&str>,
) -> Result<RidMaxBitrate, SimulcastAnswerError> {
    let Some(restrictions) = restrictions else {
        return Ok(RidMaxBitrate::Absent);
    };
    let restriction = restrictions
        .split(sdp::rid_restriction::PARAMETER_SEPARATOR)
        .exactly_one()
        .map_err(|_error| SimulcastAnswerError)?
        .trim();
    if restriction.is_empty() {
        return Err(SimulcastAnswerError);
    }
    let (key, value) = restriction
        .split_once(sdp::rid_restriction::NAME_VALUE_SEPARATOR)
        .map_or((restriction, None), |(key, value)| {
            (key.trim(), Some(value.trim()))
        });
    if key != sdp::rid_restriction::MAX_BITRATE {
        return Err(SimulcastAnswerError);
    }
    match value {
        Some(value) if !value.is_empty() => value
            .parse::<u64>()
            .map(Bitrate::from_bps)
            .map(RidMaxBitrate::Value)
            .map_err(|_error| SimulcastAnswerError),
        Some(_) => Err(SimulcastAnswerError),
        None => Ok(RidMaxBitrate::Valueless),
    }
}

#[cfg(test)]
#[path = "TESTS/rid.rs"]
mod tests;
