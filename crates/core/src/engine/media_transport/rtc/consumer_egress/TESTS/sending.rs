use str0m::media::Rid;

use super::*;
use crate::engine::{
    UserId,
    media_transport::rtc::test_support::{sample_forwarded_packet, test_transport_session_key},
};

#[test]
fn local_send_contract_keeps_payload_inside_the_adapter_boundary() {
    let session_key = test_transport_session_key(45, 0, 12, UserId::Integer(9));
    let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");
    let rtp = packet.local_send_packet();

    assert_eq!(rtp.header().ext_vals.mid, Some(Mid::from("aud-up")));
    assert_eq!(rtp.payload.as_ref(), b"payload");
}

#[test]
fn outbound_extension_values_rewrite_source_identity_to_consumer_stream() {
    let session_key = test_transport_session_key(45, 0, 12, UserId::Integer(9));
    let packet = sample_forwarded_packet(session_key, "cam-up", b"payload");
    let rtp = packet.local_send_packet();
    let mut source_header = rtp.header().clone();
    source_header.ext_vals.rid = Some(Rid::from("hi"));
    source_header.ext_vals.rid_repair = Some(Rid::from("lo"));
    source_header.ext_vals.voice_activity = Some(true);

    let ext_vals = outbound_extension_values(&source_header, Mid::from("cam-down"));

    assert_eq!(ext_vals.mid, Some(Mid::from("cam-down")));
    assert_eq!(ext_vals.rid, None);
    assert_eq!(ext_vals.rid_repair, None);
    assert_eq!(ext_vals.voice_activity, Some(true));
}

#[test]
fn outbound_payload_type_prefers_consumer_negotiated_payload_type() {
    let session_key = test_transport_session_key(45, 0, 12, UserId::Integer(9));
    let packet = sample_forwarded_packet(session_key, "cam-up", b"payload");
    let rtp = packet.local_send_packet();

    assert_eq!(
        outbound_payload_type(rtp.header(), Some(Pt::from(96))),
        Pt::from(96)
    );
    assert_eq!(outbound_payload_type(rtp.header(), None), Pt::from(111));
}
