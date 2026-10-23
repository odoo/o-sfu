//! RFC references for this module:
//! - WebRTC RTP usage profile: <https://www.rfc-editor.org/rfc/rfc8834>
//! - ICE protocol: <https://www.rfc-editor.org/rfc/rfc8445>
//! - ICE candidate grammar (legacy, still interoperable in SDP): <https://www.rfc-editor.org/rfc/rfc5245>
//! - DTLS-SRTP protection profiles: <https://www.rfc-editor.org/rfc/rfc5764>
//! - BUNDLE and MID signaling: <https://www.rfc-editor.org/rfc/rfc9143>
//! - RTP payload restrictions and RID signaling: <https://www.rfc-editor.org/rfc/rfc8851>
//! - RTP stream ID header extensions: <https://www.rfc-editor.org/rfc/rfc8852>
//! - SDP simulcast signaling: <https://www.rfc-editor.org/rfc/rfc8853>
//! - RTCP multiplexing: <https://www.rfc-editor.org/rfc/rfc5761>
//! - WebRTC datagram multiplexing: <https://www.rfc-editor.org/rfc/rfc7983>
//! - RTCP feedback SDP signaling: <https://www.rfc-editor.org/rfc/rfc4585>
//! - RTP retransmission SDP signaling: <https://www.rfc-editor.org/rfc/rfc4588>
//! - SDP source attributes: <https://www.rfc-editor.org/rfc/rfc5576>
//! - SDP grammar: <https://www.rfc-editor.org/rfc/rfc8866>
//! - SDP offer/answer: <https://www.rfc-editor.org/rfc/rfc3264>
//! - SDP `setup` roles for connection-oriented media: <https://www.rfc-editor.org/rfc/rfc4145>
//! - DTLS-SRTP offer/answer usage of `setup`: <https://www.rfc-editor.org/rfc/rfc5763>
//! - Video frame marking RTP header extension: <https://www.rfc-editor.org/rfc/rfc9626>
//! - Layer Refresh Request feedback: <https://www.rfc-editor.org/rfc/rfc9627>

use std::fmt;

use crate::{dtls, rtp, stun};

// First-octet ranges from RFC 7983 section 7:
// <https://www.rfc-editor.org/rfc/rfc7983.html#section-7>
/// Inclusive upper bound of the STUN first-octet range, which starts at zero.
const STUN_MUX_FIRST_OCTET_END: u8 = 3;
/// Inclusive lower bound of the multiplexed DTLS content-type range.
const DTLS_MUX_FIRST_OCTET_START: u8 = 20;
/// Inclusive upper bound of the multiplexed DTLS content-type range.
const DTLS_MUX_FIRST_OCTET_END: u8 = 63;

/// Protocol selected from a multiplexed WebRTC datagram's unencrypted headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatagramKind {
    Rtp,
    Rtcp,
    Stun,
    Dtls,
}

/// Classifies STUN, DTLS 1.0/1.2, RTP/SRTP or RTCP/SRTCP by header shape.
///
/// Returns `None` for unsupported multiplexed protocols or truncated framing.
/// STUN requires the RFC 8489 cookie and exact aligned message length. DTLS
/// requires one complete initial record. RTP requires its fixed header, CSRC
/// list and declared extension block. RTCP requires an initial packet header,
/// sender SSRC and declared initial packet length, allowing compound packets
/// and SRTCP trailers.
///
/// These shallow checks do not validate STUN attributes, subsequent DTLS/RTCP
/// records, RTP payloads or padding. They do not authenticate or decrypt data
/// and cannot distinguish protected packets from plaintext packets.
///
/// References:
/// - <https://www.rfc-editor.org/rfc/rfc7983.html#section-7>
/// - <https://www.rfc-editor.org/rfc/rfc8489.html#section-5>
/// - <https://www.rfc-editor.org/rfc/rfc4347.html#section-4.1>
/// - <https://www.rfc-editor.org/rfc/rfc6347.html#section-4.1>
/// - <https://www.rfc-editor.org/rfc/rfc3550.html#section-5.1>
/// - <https://www.rfc-editor.org/rfc/rfc3550.html#section-6.4.1>
/// - <https://www.rfc-editor.org/rfc/rfc5761.html#section-4>
/// - <https://www.rfc-editor.org/rfc/rfc3711.html#section-3.4>
#[must_use]
pub fn classify_datagram(packet: &[u8]) -> Option<DatagramKind> {
    let first = *packet.first()?;
    if first <= STUN_MUX_FIRST_OCTET_END {
        return stun::has_datagram_shape(packet).then_some(DatagramKind::Stun);
    }
    if is_dtls_mux_packet(first) {
        return dtls::has_datagram_shape(packet).then_some(DatagramKind::Dtls);
    }
    match rtp::classify_datagram(packet)? {
        rtp::RtpRtcpMuxPacketKind::Rtp => Some(DatagramKind::Rtp),
        rtp::RtpRtcpMuxPacketKind::Rtcp => Some(DatagramKind::Rtcp),
    }
}

/// Returns whether a datagram first octet selects DTLS in WebRTC multiplexing.
///
/// Reference: <https://www.rfc-editor.org/rfc/rfc7983.html#section-7>
#[must_use]
pub const fn is_dtls_mux_packet(first_octet: u8) -> bool {
    first_octet >= DTLS_MUX_FIRST_OCTET_START && first_octet <= DTLS_MUX_FIRST_OCTET_END
}

/// ICE portocol registries used by WebRTC signaling.
pub mod ice {
    /// Separator between the peer and sender username fragments in a
    /// connectivity-check credential.
    ///
    /// Reference: <https://www.rfc-editor.org/rfc/rfc8445.html#section-7.2.2>
    pub const USERNAME_FRAGMENT_SEPARATOR: char = ':';

    /// ICE component IDs for RTP and RTCP.
    ///
    /// Reference: RFC 8445 section 5.1.1.
    pub mod component {
        pub const RTP: u16 = 1;
        pub const RTCP: u16 = 2;
    }

    /// ICE candidate type literals used by SDP candidate attributes.
    ///
    /// References:
    /// - RFC 5245 section 15.1 candidate grammar (`typ host|srflx|prflx|relay`)
    /// - RFC 8445 (semantic model preserved by the updated ICE specification)
    pub mod candidate_type {
        pub const HOST: &str = "host";
        pub const SERVER_REFLEXIVE: &str = "srflx";
        pub const PEER_REFLEXIVE: &str = "prflx";
        pub const RELAYED: &str = "relay";
    }

    /// ICE candidate attribute grammar tokens used by SDP candidate lines.
    ///
    /// Reference: RFC 5245 section 15.1.
    pub mod candidate_attribute {
        pub const PREFIX: &str = "candidate:";
        pub const TYPE_LABEL: &str = "typ";
    }

    /// Recommended ICE type-preference values.
    ///
    /// Reference: RFC 8445 section 5.1.2.2.
    pub mod type_preference {
        pub const HOST: u8 = 126;
        pub const PEER_REFLEXIVE: u8 = 110;
        pub const SERVER_REFLEXIVE: u8 = 100;
        pub const RELAYED: u8 = 0;
    }

    /// ICE transport token used in SDP candidate lines.
    ///
    /// Reference: RFC 8445 section 5.1.1 and candidate grammar inherited from RFC 5245.
    pub mod transport {
        pub const UDP: &str = "udp";
        pub const TCP: &str = "tcp";
    }
}

/// ICE transport tokens used in SDP candidate lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IceTransport {
    Udp,
    Tcp,
}

impl IceTransport {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Udp => ice::transport::UDP,
            Self::Tcp => ice::transport::TCP,
        }
    }

    #[must_use]
    pub fn parse(token: &str) -> Option<Self> {
        if token.eq_ignore_ascii_case(ice::transport::UDP) {
            return Some(Self::Udp);
        }
        if token.eq_ignore_ascii_case(ice::transport::TCP) {
            return Some(Self::Tcp);
        }
        None
    }
}

impl AsRef<str> for IceTransport {
    fn as_ref(&self) -> &str {
        match self {
            Self::Udp => ice::transport::UDP,
            Self::Tcp => ice::transport::TCP,
        }
    }
}

impl fmt::Display for IceTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())
    }
}

/// ICE candidate type tokens used in SDP candidate attributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IceCandidateType {
    Host,
    ServerReflexive,
    PeerReflexive,
    Relayed,
}

impl IceCandidateType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Host => ice::candidate_type::HOST,
            Self::ServerReflexive => ice::candidate_type::SERVER_REFLEXIVE,
            Self::PeerReflexive => ice::candidate_type::PEER_REFLEXIVE,
            Self::Relayed => ice::candidate_type::RELAYED,
        }
    }

    #[must_use]
    pub fn parse(token: &str) -> Option<Self> {
        match token {
            ice::candidate_type::HOST => Some(Self::Host),
            ice::candidate_type::SERVER_REFLEXIVE => Some(Self::ServerReflexive),
            ice::candidate_type::PEER_REFLEXIVE => Some(Self::PeerReflexive),
            ice::candidate_type::RELAYED => Some(Self::Relayed),
            _ => None,
        }
    }
}

impl AsRef<str> for IceCandidateType {
    fn as_ref(&self) -> &str {
        match self {
            Self::Host => ice::candidate_type::HOST,
            Self::ServerReflexive => ice::candidate_type::SERVER_REFLEXIVE,
            Self::PeerReflexive => ice::candidate_type::PEER_REFLEXIVE,
            Self::Relayed => ice::candidate_type::RELAYED,
        }
    }
}

impl fmt::Display for IceCandidateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())
    }
}

/// MIME top-level media kinds used by ORTC and SDP payloads
/// same as on web stream/tracks APIs.
pub mod media_kind {
    pub const AUDIO: &str = "audio";
    pub const VIDEO: &str = "video";
    pub const APPLICATION: &str = "application";
}

/// Technical media kind shared by RTP, SDP, and signaling metadata.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum MediaKind {
    Audio,
    Video,
}

impl MediaKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Audio => media_kind::AUDIO,
            Self::Video => media_kind::VIDEO,
        }
    }

    #[must_use]
    pub const fn is_audio(self) -> bool {
        matches!(self, Self::Audio)
    }

    #[must_use]
    pub const fn is_video(self) -> bool {
        matches!(self, Self::Video)
    }
}

impl AsRef<str> for MediaKind {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for MediaKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// RTCP feedback type and parameter tokens used by current WebRTC capability paylods.
pub mod rtcp_feedback {
    /// RTCP feedback kind tokens used in capability dictionnaries.
    pub mod kind {
        /// Generic NACK feedback type token.
        ///
        /// Reference: RFC 4585 section 6.2.1.
        pub const NACK: &str = "nack";

        /// Codec control message feedback type token.
        ///
        /// Reference: RFC 5104.
        pub const CCM: &str = "ccm";

        /// Google Receiver Estimated Maximum Bitrate token used by current browser stacks.
        pub const GOOG_REMB: &str = "goog-remb";

        /// Transport-wide congestion control feedback token.
        ///
        /// Reference:
        /// <https://www.ietf.org/archive/id/draft-holmer-rmcat-transport-wide-cc-extensions-01.txt>
        pub const TRANSPORT_CC: &str = "transport-cc";
    }

    /// RTCP feedback parameter tokens used by current WebRTC cpaability payloads.
    pub mod parameter {
        /// Picture loss indication parameter token.
        ///
        /// Reference: RFC 4585 section 6.3.1.
        pub const PLI: &str = "pli";

        /// Full intra request parameter token.
        ///
        /// Reference: RFC 5104 section 4.3.1.
        pub const FIR: &str = "fir";

        /// Layer Refresh Request parameter token.
        ///
        /// Reference: RFC 9627 section 6.
        pub const LRR: &str = "lrr";
    }
}

pub mod sdp {
    /// Carriage-return line-feed SDP line ending.
    ///
    /// Reference: <https://www.rfc-editor.org/rfc/rfc8866.html#section-5>
    pub const CRLF: &str = "\r\n";

    /// Carriage-return octet stripped by line-oriented SDP parsers.
    ///
    /// Reference: <https://www.rfc-editor.org/rfc/rfc8866.html#section-5>
    pub const CR: char = '\r';

    /// Line-feed octet used to split SDP text while preserving line endings.
    ///
    /// Reference: <https://www.rfc-editor.org/rfc/rfc8866.html#section-5>
    pub const LF: char = '\n';

    /// ASCII space separating fields within an SDP value.
    ///
    /// Reference: <https://www.rfc-editor.org/rfc/rfc8866.html#section-5>
    pub const SP: char = ' ';

    /// Attribute field prefix.
    ///
    /// Reference: <https://www.rfc-editor.org/rfc/rfc8866.html#section-5.13>
    pub const ATTR: &str = "a=";

    /// Separator between an SDP attribute name and value.
    ///
    /// Reference: <https://www.rfc-editor.org/rfc/rfc8866.html#section-5.13>
    pub const ATTR_SEP: char = ':';

    /// Session-level end-of-candidates line.
    ///
    /// Reference: <https://www.rfc-editor.org/rfc/rfc8840.html#section-8>
    pub const EOC_LINE: &str = "a=end-of-candidates\r\n";
    /// Media description field prefix.
    ///
    /// Reference: <https://www.rfc-editor.org/rfc/rfc8866.html#section-5.14>
    pub const MEDIA: &str = "m=";

    /// Media description grammar tokens.
    pub mod media {
        /// Separator between an SDP media port and port count.
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc8866.html#section-5.14>
        pub const PORT_SEP: char = '/';

        /// Port value that rejects a media description in offer/answer.
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc3264.html#section-6>
        pub const ZERO_PORT: &str = "0";
    }

    /// `a=extmap` grammar tokens.
    pub mod extmap {
        /// Separator between an extension ID and direction.
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc8285.html#section-5>
        pub const DIR_SEP: char = '/';
    }

    /// `a=rtpmap` grammar tokens.
    pub mod rtpmap {
        /// Separator between encoding name, clock rate and encoding parameters.
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc8866.html#section-6.6>
        pub const ENC_SEP: char = '/';
    }

    pub mod group_semantics {
        /// `a=group:BUNDLE ...`
        ///
        /// Reference: RFC 9143.
        pub const BUNDLE: &str = "BUNDLE";
    }

    pub mod ssrc_group_semantics {
        /// Flow Identification semantics for `a=ssrc-group`.
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc5576.html#section-4.2>
        pub const FID: &str = "FID";
    }

    pub mod attribute {
        /// `a=extmap:<id> <uri>`
        ///
        /// Reference: RFC 8285 section 5.
        pub const EXTMAP: &str = "extmap";

        /// `a=fmtp:<format> <format-specific-parameters>`
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc8866.html#section-6.15>
        pub const FMTP: &str = "fmtp";

        /// `a=rtcp-fb:<pt> <feedback-type> [<feedback-parameter>]`
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc4585.html#section-4.2>
        pub const RTCP_FB: &str = "rtcp-fb";

        /// `a=rtcp-mux`
        ///
        /// Reference: RFC 5761.
        pub const RTCP_MUX: &str = "rtcp-mux";

        /// `a=rtpmap:<payload-type> <encoding-name>/<clock-rate>`
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc8866.html#section-6.6>
        pub const RTPMAP: &str = "rtpmap";

        /// `a=rid:<rid-id> <direction> ...`
        ///
        /// Reference: RFC 8851 section 4.
        pub const RID: &str = "rid";

        /// `a=simulcast:<send-or-recv-list> ...`
        ///
        /// Reference: RFC 8853 section 5.1.
        pub const SIMULCAST: &str = "simulcast";

        /// `a=ssrc-group:<semantics> <ssrc-id> ...`
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc5576.html#section-4.2>
        pub const SSRC_GROUP: &str = "ssrc-group";

        /// `a=ssrc:<ssrc-id> <attribute>`
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc5576.html#section-4.1>
        pub const SSRC: &str = "ssrc";

        /// `a=setup:<role>`
        ///
        /// References: RFC 4145, RFC 5763.
        pub const SETUP: &str = "setup";

        /// `a=mid:<mid>`
        ///
        /// Reference: RFC 9143 section 9.
        pub const MID: &str = "mid";
    }

    /// `a=rid` directions and validation helpers.
    pub mod rid {
        pub const DIRECTION_SEND: &str = "send";
        pub const DIRECTION_RECV: &str = "recv";
        pub const MAX_ID_OCTETS: usize = 255;

        /// Returns whether `value` is a valid RTP stream identifier.
        ///
        /// RFC 8852 section 3 constrains `RtpStreamId` and
        /// `RepairedRtpStreamId` to 1-255 ASCII alphanumeric octets. RFC
        /// Editor errata 7132 applies the same bound to RFC 8851 `rid-id`.
        #[must_use]
        pub fn is_id(value: &str) -> bool {
            (1..=MAX_ID_OCTETS).contains(&value.len())
                && value.as_bytes().iter().all(|byte| is_id_byte(*byte))
        }

        #[must_use]
        pub const fn is_id_byte(value: u8) -> bool {
            matches!(value, b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z')
        }
    }

    /// `a=rid` restriction parameter names.
    ///
    /// Reference: RFC 8851 section 12.2.
    pub mod rid_restriction {
        /// Separator between RID restrictions.
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc8851.html#section-4>
        pub const PARAMETER_SEPARATOR: char = ';';

        /// Separator between a RID restriction name and value.
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc8851.html#section-4>
        pub const NAME_VALUE_SEPARATOR: char = '=';

        pub const PAYLOAD_TYPES: &str = "pt";
        pub const MAX_WIDTH: &str = "max-width";
        pub const MAX_HEIGHT: &str = "max-height";
        pub const MAX_FPS: &str = "max-fps";
        pub const MAX_FRAME_SIZE: &str = "max-fs";
        pub const MAX_BITRATE: &str = "max-br";
        pub const MAX_PIXEL_RATE: &str = "max-pps";
        pub const MAX_BITS_PER_PIXEL: &str = "max-bpp";
        pub const DEPENDS_ON: &str = "depend";
    }

    /// `a=simulcast` list delimiters and prefixes.
    ///
    /// Reference: RFC 8853 section 5.1.
    pub mod simulcast {
        pub const DIRECTION_SEND: &str = super::rid::DIRECTION_SEND;
        pub const DIRECTION_RECV: &str = super::rid::DIRECTION_RECV;
        pub const STREAM_SEPARATOR: char = ';';
        pub const ALTERNATIVE_SEPARATOR: char = ',';
        pub const INITIAL_PAUSE_PREFIX: char = '~';

        #[must_use]
        pub fn strip_initial_pause_prefix(value: &str) -> Option<&str> {
            value.strip_prefix(INITIAL_PAUSE_PREFIX)
        }
    }

    pub mod transport_protocol {
        /// `m=<media> <port> UDP/TLS/RTP/SAVPF ...`
        ///
        /// Reference: RFC 8829 section 5.8.
        pub const UDP_TLS_RTP_SAVPF: &str = "UDP/TLS/RTP/SAVPF";

        /// `m=<media> <port> UDP/TLS/RTP/SAVP ...`
        ///
        /// Reference: RFC 8829 section 5.8.
        pub const UDP_TLS_RTP_SAVP: &str = "UDP/TLS/RTP/SAVP";

        /// `m=<media> <port> RTP/SAVPF ...`
        ///
        /// Reference: RFC 8829 section 5.8.
        pub const RTP_SAVPF: &str = "RTP/SAVPF";

        /// `m=<media> <port> RTP/SAVP ...`
        ///
        /// Reference: RFC 8829 section 5.8.
        pub const RTP_SAVP: &str = "RTP/SAVP";

        /// `m=<media> <port> UDP/DTLS/SCTP ...`
        ///
        /// Reference: RFC 8841.
        pub const UDP_DTLS_SCTP: &str = "UDP/DTLS/SCTP";

        /// `m=<media> <port> TCP/DTLS/SCTP ...`
        ///
        /// Reference: RFC 8841.
        pub const TCP_DTLS_SCTP: &str = "TCP/DTLS/SCTP";
    }

    pub mod setup_role {
        pub const ACTIVE: &str = "active";
        pub const PASSIVE: &str = "passive";
        pub const ACTPASS: &str = "actpass";
        pub const HOLDCONN: &str = "holdconn";
    }

    pub mod direction {
        /// `a=inactive`
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc8866.html#section-6.7.4>
        pub const INACTIVE: &str = "inactive";

        /// `a=recvonly`
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc8866.html#section-6.7.1>
        pub const RECV_ONLY: &str = "recvonly";

        /// `a=sendrecv`
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc8866.html#section-6.7.2>
        pub const SEND_RECV: &str = "sendrecv";

        /// `a=sendonly`
        ///
        /// Reference: <https://www.rfc-editor.org/rfc/rfc8866.html#section-6.7.3>
        pub const SEND_ONLY: &str = "sendonly";
    }
}

/// Direction tokens used by RFC 8851 RID and RFC 8853 simulcast attributes.
///
/// Parsing uses the case-sensitive RFC tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RtpStreamDirection {
    Send,
    Recv,
}

impl RtpStreamDirection {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Send => sdp::rid::DIRECTION_SEND,
            Self::Recv => sdp::rid::DIRECTION_RECV,
        }
    }

    #[must_use]
    pub fn parse(token: &str) -> Option<Self> {
        match token {
            sdp::rid::DIRECTION_SEND => Some(Self::Send),
            sdp::rid::DIRECTION_RECV => Some(Self::Recv),
            _ => None,
        }
    }
}

impl AsRef<str> for RtpStreamDirection {
    fn as_ref(&self) -> &str {
        match self {
            Self::Send => sdp::rid::DIRECTION_SEND,
            Self::Recv => sdp::rid::DIRECTION_RECV,
        }
    }
}

impl fmt::Display for RtpStreamDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())
    }
}

/// DTLS fingerprint algorithms currently supported by the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DtlsFingerprintAlgorithm {
    Sha256,
}

impl DtlsFingerprintAlgorithm {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sha256 => "sha-256",
        }
    }

    #[must_use]
    pub fn parse(token: &str) -> Option<Self> {
        if token.eq_ignore_ascii_case("sha-256") {
            return Some(Self::Sha256);
        }
        None
    }
}

impl AsRef<str> for DtlsFingerprintAlgorithm {
    fn as_ref(&self) -> &str {
        match self {
            Self::Sha256 => "sha-256",
        }
    }
}

impl fmt::Display for DtlsFingerprintAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())
    }
}

/// RTP header-extension URIs commonly needed by WebRTC endpoints.
pub mod rtp_header_extension_uri {
    macro_rules! rtp_header_extension_urn {
        ($suffix:literal) => {
            concat!("urn:ietf:params:rtp-hdrext:", $suffix)
        };
    }

    macro_rules! rtp_header_extension_sdes_urn {
        ($suffix:literal) => {
            concat!("urn:ietf:params:rtp-hdrext:sdes:", $suffix)
        };
    }

    /// MID RTP header extension URI.
    ///
    /// Reference: RFC 9143 section 16.4.
    pub const MID: &str = rtp_header_extension_sdes_urn!("mid");

    /// Audio level RTP header extension URI.
    ///
    /// Reference: RFC 6464 section 3.
    pub const SSRC_AUDIO_LEVEL: &str = rtp_header_extension_urn!("ssrc-audio-level");

    /// Mixer-to-client audio level RTP header extension URI.
    ///
    /// Reference: RFC 6465 section 4.
    pub const CSRC_AUDIO_LEVEL: &str = rtp_header_extension_urn!("csrc-audio-level");

    /// RTP stream ID extension URI.
    ///
    /// Reference: RFC 8852.
    pub const RTP_STREAM_ID: &str = rtp_header_extension_sdes_urn!("rtp-stream-id");

    /// Repaired RTP stream ID extension URI.
    ///
    /// Reference: RFC 8852.
    pub const REPAIRED_RTP_STREAM_ID: &str =
        rtp_header_extension_sdes_urn!("repaired-rtp-stream-id");

    /// Video Frame Marking RTP header extension URI.
    ///
    /// Reference: RFC 9626 section 3.4.
    pub const FRAME_MARKING: &str = rtp_header_extension_urn!("framemarking");

    /// Absolute send time RTP header extension URI.
    ///
    /// The RTP header-extension framework identify extensions by URI string,
    /// and that URI is signaled verbatim in SDP `a=extmap` lines. Even though
    /// this identifier looks like an HTTP URL, it just is a protocol name.
    ///
    /// Current WebRTC stacks use this exact literal, so we preserve it for interoperability.
    ///
    /// Reference: <https://www.webrtc.org/experiments/rtp-hdrext/abs-send-time>
    pub const ABS_SEND_TIME: &str = "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time";

    /// Transport-wide sequence number RTP header extension URI.
    ///
    /// This value is likewise the negotiated wire identifier carried in SDP
    /// `a=extmap` lines. It is not dereferenced as a network resource; the
    /// exact string itself is the interoperability key used to match the
    /// extension.
    ///
    /// Browsers and other rtc ecosystems commonly advertise the
    /// historical `...-01` draft URI literal, so we keeps that deployed
    /// identifier instead of normalizing it to a different name.
    ///
    /// Reference:
    /// <https://www.ietf.org/archive/id/draft-holmer-rmcat-transport-wide-cc-extensions-01.txt>
    pub const TRANSPORT_WIDE_CC_DRAFT_01: &str =
        "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01";
}

/// RTP header-extension URIs commonly needed by WebRTC endpoints.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RtpHeaderExtensionUri {
    Mid,
    RtpStreamId,
    RepairedRtpStreamId,
    FrameMarking,
    AbsSendTime,
    TransportWideCcDraft01,
    SsrcAudioLevel,
    CsrcAudioLevel,
    Other(String),
}

impl RtpHeaderExtensionUri {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Mid => rtp_header_extension_uri::MID,
            Self::RtpStreamId => rtp_header_extension_uri::RTP_STREAM_ID,
            Self::RepairedRtpStreamId => rtp_header_extension_uri::REPAIRED_RTP_STREAM_ID,
            Self::FrameMarking => rtp_header_extension_uri::FRAME_MARKING,
            Self::AbsSendTime => rtp_header_extension_uri::ABS_SEND_TIME,
            Self::TransportWideCcDraft01 => rtp_header_extension_uri::TRANSPORT_WIDE_CC_DRAFT_01,
            Self::SsrcAudioLevel => rtp_header_extension_uri::SSRC_AUDIO_LEVEL,
            Self::CsrcAudioLevel => rtp_header_extension_uri::CSRC_AUDIO_LEVEL,
            Self::Other(uri) => uri.as_str(),
        }
    }
}

impl From<&str> for RtpHeaderExtensionUri {
    fn from(value: &str) -> Self {
        match value {
            rtp_header_extension_uri::MID => Self::Mid,
            rtp_header_extension_uri::RTP_STREAM_ID => Self::RtpStreamId,
            rtp_header_extension_uri::REPAIRED_RTP_STREAM_ID => Self::RepairedRtpStreamId,
            rtp_header_extension_uri::FRAME_MARKING => Self::FrameMarking,
            rtp_header_extension_uri::ABS_SEND_TIME => Self::AbsSendTime,
            rtp_header_extension_uri::TRANSPORT_WIDE_CC_DRAFT_01 => Self::TransportWideCcDraft01,
            rtp_header_extension_uri::SSRC_AUDIO_LEVEL => Self::SsrcAudioLevel,
            rtp_header_extension_uri::CSRC_AUDIO_LEVEL => Self::CsrcAudioLevel,
            _ => Self::Other(value.to_owned()),
        }
    }
}

impl From<String> for RtpHeaderExtensionUri {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}

impl AsRef<str> for RtpHeaderExtensionUri {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for RtpHeaderExtensionUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// RTCP SDES item type defined for MID.
///
/// Reference: RFC 9143 section 16.3.
pub const RTCP_SDES_ITEM_MID: u8 = 15;

#[cfg(test)]
#[path = "TESTS/webrtc.rs"]
mod tests;
