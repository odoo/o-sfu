//! Shared RID and SDP mechanics for codec simulcast profiles.

use std::collections::BTreeMap;

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
pub const MAX_SEND_STREAMS: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::media_transport::rtc) struct SimulcastAnswerError;

pub(in crate::engine::media_transport::rtc) struct ParsedAnswerRids {
    by_mid: BTreeMap<Mid, Result<Vec<String>, SimulcastAnswerError>>,
}

impl ParsedAnswerRids {
    /// Parses the send RIDs of every media section in `answer_sdp`.
    ///
    /// # Errors
    ///
    /// Returns [`SimulcastAnswerError`] when any section declares a restricted
    /// send RID, whether or not the section or the RID is negotiated later.
    pub(in crate::engine::media_transport::rtc) fn parse(
        answer_sdp: &str,
        answer: &SdpAnswer,
    ) -> Result<Self, SimulcastAnswerError> {
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
            reject_send_rid_restrictions(section)?;
            match parse_section_rids(section) {
                Ok(rids) if rids.is_empty() => {}
                result => {
                    by_mid.insert(media_line.mid(), result);
                }
            }
        }
        Ok(Self { by_mid })
    }

    /// Returns the offered encodings the answer accepts, in answer order.
    ///
    /// # Errors
    ///
    /// Returns [`SimulcastAnswerError`] when the answer section is malformed
    /// or accepts a RID that `offered_encodings` lacks.
    pub(in crate::engine::media_transport::rtc) fn negotiate(
        &self,
        mid: Mid,
        offered_encodings: &[SessionUploadEncoding],
    ) -> Result<Vec<SessionUploadEncoding>, SimulcastAnswerError> {
        match self.by_mid.get(&mid) {
            Some(Ok(answer_rids)) => accepted_encodings(answer_rids, offered_encodings),
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

/// Builds the offered receive simulcast without RID restrictions.
///
/// `uploadSlots` is the only carrier of a layer's bitrate cap. The client
/// applies it with `setParameters`, so the SDP settles RIDs only.
pub(super) fn recv_simulcast(layers: &[LayerSpec<'_>]) -> Simulcast {
    Simulcast {
        send: Vec::new(),
        recv: layers
            .iter()
            .map(|layer| SimulcastLayer {
                rid: Rid::from(layer.rid),
                attributes: None,
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

fn reject_send_rid_restrictions(section: &str) -> Result<(), SimulcastAnswerError> {
    section
        .lines()
        .try_for_each(|line| parse_send_rid(line.trim_end_matches(sdp::CR)).map(drop))
}

fn parse_section_rids(section: &str) -> Result<Vec<String>, SimulcastAnswerError> {
    accepted_send_simulcast_rids(section)?
        .into_iter()
        .map(|rid| {
            require_one_send_rid_declaration(section, rid)?;
            Ok(rid.to_owned())
        })
        .collect()
}

fn accepted_encodings(
    answer_rids: &[String],
    offered_encodings: &[SessionUploadEncoding],
) -> Result<Vec<SessionUploadEncoding>, SimulcastAnswerError> {
    answer_rids
        .iter()
        .map(|rid| {
            offered_encodings
                .iter()
                .find(|encoding| encoding.rid == *rid)
                .cloned()
                .ok_or(SimulcastAnswerError)
        })
        .collect()
}

fn require_one_send_rid_declaration(section: &str, rid: &str) -> Result<(), SimulcastAnswerError> {
    let mut found = false;
    for line in section.lines() {
        if parse_send_rid(line.trim_end_matches(sdp::CR))? != Some(rid) {
            continue;
        }
        if found {
            return Err(SimulcastAnswerError);
        }
        found = true;
    }
    found.then_some(()).ok_or(SimulcastAnswerError)
}

fn parse_send_rid(line: &str) -> Result<Option<&str>, SimulcastAnswerError> {
    let Some(rid_value) = sdp_attribute_value(line, sdp::attribute::RID) else {
        return Ok(None);
    };
    let mut parts = rid_value.splitn(3, sdp::SP);
    let (Some(rid), Some(direction)) = (parts.next(), parts.next()) else {
        return Ok(None);
    };
    if !sdp::rid::is_id(rid)
        || webrtc::RtpStreamDirection::parse(direction) != Some(webrtc::RtpStreamDirection::Send)
    {
        return Ok(None);
    }
    // The offer writes no restriction, and RFC 8851 section 6.4 forbids an
    // answer from adding one. A layer's cap comes from `uploadSlots` alone.
    // https://www.rfc-editor.org/rfc/rfc8851.html#section-6.4
    if parts.next().is_some() {
        return Err(SimulcastAnswerError);
    }
    Ok(Some(rid))
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

#[cfg(test)]
#[path = "TESTS/rid.rs"]
mod tests;
