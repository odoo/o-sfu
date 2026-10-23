use super::{
    DatagramKind, RtpHeaderExtensionUri, RtpStreamDirection, classify_datagram, is_dtls_mux_packet,
    rtp_header_extension_uri, sdp,
};

const RTP: &[u8] = &[0x80, 96, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1];
const RTCP: &[u8] = &[0x80, 200, 0, 1, 0, 0, 0, 1];
const STUN: &[u8] = &[
    0, 1, 0, 0, 0x21, 0x12, 0xa4, 0x42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];
const DTLS: &[u8] = &[22, 0xfe, 0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

fn set_byte(packet: &mut [u8], offset: usize, value: u8) {
    let byte = packet.get_mut(offset);
    assert!(byte.is_some(), "header mutation exceeds fixture length");
    if let Some(byte) = byte {
        *byte = value;
    }
}

#[test]
fn datagram_kind_preserves_lowercase_wire_names() -> Result<(), serde_json::Error> {
    for (kind, wire) in [
        (DatagramKind::Rtp, "rtp"),
        (DatagramKind::Rtcp, "rtcp"),
        (DatagramKind::Stun, "stun"),
        (DatagramKind::Dtls, "dtls"),
    ] {
        assert_eq!(serde_json::to_value(kind)?, wire);
        assert_eq!(
            serde_json::from_value::<DatagramKind>(serde_json::json!(wire))?,
            kind
        );
    }
    for invalid in ["any", "RTP", "srtp"] {
        assert!(serde_json::from_value::<DatagramKind>(serde_json::json!(invalid)).is_err());
    }
    Ok(())
}

#[test]
fn datagram_classification_rejects_truncated_and_unsupported_protocols() {
    for (packet, kind) in [
        (RTP, DatagramKind::Rtp),
        (RTCP, DatagramKind::Rtcp),
        (STUN, DatagramKind::Stun),
        (DTLS, DatagramKind::Dtls),
    ] {
        assert_eq!(classify_datagram(packet), Some(kind));
        for length in 0..packet.len() {
            assert_eq!(packet.get(..length).and_then(classify_datagram), None);
        }
    }
    for first in [4, 15, 16, 19, 64, 79, 80, 127, 192, 255] {
        let mut packet = RTP.to_vec();
        set_byte(&mut packet, 0, first);
        assert_eq!(classify_datagram(&packet), None);
    }
}

#[test]
fn stun_classification_requires_cookie_and_exact_aligned_framing() {
    for first in 0..=3 {
        let mut packet = STUN.to_vec();
        set_byte(&mut packet, 0, first);
        assert_eq!(classify_datagram(&packet), Some(DatagramKind::Stun));
    }
    let mut packet = STUN.to_vec();
    set_byte(&mut packet, 4, 0);
    assert_eq!(classify_datagram(&packet), None);
    let mut packet = STUN.to_vec();
    packet.extend_from_slice(&[0xff; 4]);
    assert_eq!(classify_datagram(&packet), None);
    set_byte(&mut packet, 3, 4);
    // Attribute validity is intentionally outside framing classification.
    assert_eq!(classify_datagram(&packet), Some(DatagramKind::Stun));
    set_byte(&mut packet, 3, 8);
    assert_eq!(classify_datagram(&packet), None);
    set_byte(&mut packet, 3, 3);
    let _ = packet.pop();
    assert_eq!(classify_datagram(&packet), None);
}

#[test]
fn dtls_classification_sizes_only_the_initial_legacy_record() {
    for first in [20, 22, 63] {
        for version in [0xff, 0xfd] {
            let mut packet = DTLS.to_vec();
            set_byte(&mut packet, 0, first);
            set_byte(&mut packet, 2, version);
            assert_eq!(classify_datagram(&packet), Some(DatagramKind::Dtls));
            set_byte(&mut packet, 12, 1);
            assert_eq!(classify_datagram(&packet), None);
            packet.push(0);
            assert_eq!(classify_datagram(&packet), Some(DatagramKind::Dtls));
            // A later record cannot change the initial framing decision.
            packet.push(0xff);
            assert_eq!(classify_datagram(&packet), Some(DatagramKind::Dtls));
        }
    }
    for version in [[0xfe, 0xfc], [0x03, 0x03]] {
        let mut packet = DTLS.to_vec();
        for (offset, byte) in version.into_iter().enumerate() {
            set_byte(&mut packet, offset + 1, byte);
        }
        assert_eq!(classify_datagram(&packet), None);
    }
}

#[test]
fn rtp_classification_requires_csrc_and_declared_extension_bytes() {
    let mut packet = RTP.to_vec();
    set_byte(&mut packet, 0, 0x8f);
    assert_eq!(classify_datagram(&packet), None);
    packet.extend_from_slice(&[0; 60]);
    assert_eq!(classify_datagram(&packet), Some(DatagramKind::Rtp));
    set_byte(&mut packet, 0, 0x9f);
    assert_eq!(classify_datagram(&packet), None);
    packet.extend_from_slice(&[0xbe, 0xde, 0, 1]);
    assert_eq!(classify_datagram(&packet), None);
    packet.extend_from_slice(&[0; 4]);
    assert_eq!(classify_datagram(&packet), Some(DatagramKind::Rtp));
    // Padding may be encrypted in SRTP, so its contents cannot be inspected.
    set_byte(&mut packet, 0, 0xbf);
    assert_eq!(classify_datagram(&packet), Some(DatagramKind::Rtp));
}

#[test]
fn rtcp_classification_uses_full_second_octet_and_allows_srtcp_trailers() {
    for second in [63, 64, 95, 96, 191, 224, 255] {
        let mut packet = RTP.to_vec();
        set_byte(&mut packet, 1, second);
        assert_eq!(classify_datagram(&packet), Some(DatagramKind::Rtp));
    }
    for second in [192, 200, 223] {
        let mut packet = RTCP.to_vec();
        set_byte(&mut packet, 1, second);
        assert_eq!(classify_datagram(&packet), Some(DatagramKind::Rtcp));
        set_byte(&mut packet, 3, 0);
        assert_eq!(classify_datagram(&packet), None);
        set_byte(&mut packet, 3, 2);
        assert_eq!(classify_datagram(&packet), None);
        packet.extend_from_slice(&[0xff; 4]);
        assert_eq!(classify_datagram(&packet), Some(DatagramKind::Rtcp));
        // SRTCP encrypts later compound headers and appends protection data.
        packet.extend_from_slice(&[0xff; 14]);
        assert_eq!(classify_datagram(&packet), Some(DatagramKind::Rtcp));
    }
}

#[test]
fn dtls_mux_first_octet_range_follows_rfc_7983() {
    assert!(!is_dtls_mux_packet(19));
    assert!(is_dtls_mux_packet(20));
    assert!(is_dtls_mux_packet(63));
    assert!(!is_dtls_mux_packet(64));
}

#[test]
fn rtp_stream_direction_uses_case_sensitive_rfc_tokens() {
    assert_eq!(
        RtpStreamDirection::parse(sdp::rid::DIRECTION_SEND),
        Some(RtpStreamDirection::Send)
    );
    assert_eq!(
        RtpStreamDirection::parse(sdp::rid::DIRECTION_RECV),
        Some(RtpStreamDirection::Recv)
    );
    assert_eq!(RtpStreamDirection::Send.as_str(), "send");
    assert_eq!(RtpStreamDirection::parse("SEND"), None);
}

#[test]
fn rid_id_validation_follows_rfc_8852_stream_id_rules() {
    let max_length_id = "a".repeat(sdp::rid::MAX_ID_OCTETS);
    let oversized_id = "a".repeat(sdp::rid::MAX_ID_OCTETS + 1);

    assert!(sdp::rid::is_id("low1"));
    assert!(sdp::rid::is_id("HI2"));
    assert!(sdp::rid::is_id(&max_length_id));
    assert!(!sdp::rid::is_id(""));
    assert!(!sdp::rid::is_id(&oversized_id));
    assert!(!sdp::rid::is_id("low-1"));
    assert!(!sdp::rid::is_id("hi_2"));
    assert!(!sdp::rid::is_id("hi.2"));
    assert!(!sdp::rid::is_id("hi:2"));
}

#[test]
fn simulcast_prefix_and_delimiters_follow_rfc_8853() {
    assert_eq!(sdp::simulcast::STREAM_SEPARATOR, ';');
    assert_eq!(sdp::simulcast::ALTERNATIVE_SEPARATOR, ',');
    assert_eq!(
        sdp::simulcast::strip_initial_pause_prefix("~hi"),
        Some("hi")
    );
    assert_eq!(sdp::simulcast::strip_initial_pause_prefix("hi"), None);
}

#[test]
fn header_extension_uri_maps_simulcast_and_svc_values() {
    assert_eq!(
        RtpHeaderExtensionUri::from(rtp_header_extension_uri::RTP_STREAM_ID),
        RtpHeaderExtensionUri::RtpStreamId
    );
    assert_eq!(
        RtpHeaderExtensionUri::from(rtp_header_extension_uri::REPAIRED_RTP_STREAM_ID),
        RtpHeaderExtensionUri::RepairedRtpStreamId
    );
    assert_eq!(
        RtpHeaderExtensionUri::from(rtp_header_extension_uri::FRAME_MARKING),
        RtpHeaderExtensionUri::FrameMarking
    );
}
