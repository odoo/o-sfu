//! Projects numeric loopback ICE candidates without changing other SDP lines.

use std::{
    collections::BTreeSet,
    net::{Ipv4Addr, SocketAddr},
};

use o_sfu_rfc::webrtc::{IceCandidateType, IceTransport, ice, sdp};

use crate::Result;

enum Line {
    Original(String),
    Candidate {
        before_address: String,
        after_port: String,
        target: SocketAddr,
        ending: String,
    },
}

pub(super) struct Projection {
    lines: Vec<Line>,
    targets: BTreeSet<SocketAddr>,
}

impl Projection {
    pub(super) fn parse(sdp: &str) -> Result<Self> {
        let mut lines = Vec::new();
        let mut targets = BTreeSet::new();
        let candidate_prefix = format!("{}{}", sdp::ATTR, ice::candidate_attribute::PREFIX);
        let separator = sdp::SP.to_string();
        for original in sdp.split_inclusive(sdp::LF) {
            let line = original.trim_end_matches([sdp::CR, sdp::LF]);
            if !line.starts_with(&candidate_prefix) {
                lines.push(Line::Original(original.to_owned()));
                continue;
            }
            let mut words = line.split_ascii_whitespace();
            let candidate_token = words.next().ok_or("invalid ICE candidate")?;
            let component = words.next().ok_or("invalid ICE candidate")?;
            let transport_token = words.next().ok_or("invalid ICE candidate")?;
            let transport = IceTransport::parse(transport_token);
            if transport == Some(IceTransport::Tcp) {
                continue;
            }
            let priority = words.next().ok_or("invalid ICE candidate")?;
            let address = words.next().ok_or("invalid ICE candidate")?;
            let port = words.next().ok_or("invalid ICE candidate")?;
            let type_label = words.next().ok_or("invalid ICE candidate")?;
            let candidate_type = words.next().ok_or("invalid ICE candidate")?;
            let ip = address
                .parse::<Ipv4Addr>()
                .map_err(|_error| "UDP relay requires IPv4 loopback candidates")?;
            let port = port
                .parse::<u16>()
                .map_err(|_error| "invalid ICE candidate port")?;
            if candidate_token == candidate_prefix
                || component != ice::component::RTP.to_string()
                || transport != Some(IceTransport::Udp)
                || priority.parse::<u32>().is_err()
                || ip != Ipv4Addr::LOCALHOST
                || port == 0
                || type_label != ice::candidate_attribute::TYPE_LABEL
                || IceCandidateType::parse(candidate_type) != Some(IceCandidateType::Host)
            {
                return Err(
                    "UDP relay requires component-one UDP host candidates on 127.0.0.1".into(),
                );
            }
            let target = SocketAddr::from((ip, port));
            targets.insert(target);
            let ending = if original.ends_with(sdp::CRLF) {
                sdp::CRLF.to_owned()
            } else if original.ends_with(sdp::LF) {
                sdp::LF.to_string()
            } else {
                String::new()
            };
            lines.push(Line::Candidate {
                before_address: [candidate_token, component, transport_token, priority]
                    .join(&separator),
                after_port: [type_label, candidate_type]
                    .into_iter()
                    .chain(words)
                    .collect::<Vec<_>>()
                    .join(&separator),
                target,
                ending,
            });
        }
        if targets.is_empty() {
            return Err("UDP relay requires at least one UDP candidate".into());
        }
        Ok(Self { lines, targets })
    }

    pub(super) fn targets(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.targets.iter().copied()
    }

    pub(super) fn render(
        self,
        address: impl Fn(SocketAddr) -> Option<SocketAddr>,
    ) -> Result<String> {
        let mut output = String::new();
        for line in self.lines {
            match line {
                Line::Original(original) => output.push_str(&original),
                Line::Candidate {
                    before_address,
                    after_port,
                    target,
                    ending,
                } => {
                    let relay = address(target).ok_or("missing UDP relay endpoint")?;
                    output.push_str(&before_address);
                    output.push(sdp::SP);
                    output.push_str(&relay.ip().to_string());
                    output.push(sdp::SP);
                    output.push_str(&relay.port().to_string());
                    output.push(sdp::SP);
                    output.push_str(&after_port);
                    output.push_str(&ending);
                }
            }
        }
        Ok(output)
    }
}

#[cfg(test)]
#[path = "TESTS/sdp.rs"]
mod tests;
