use super::{test_media_transport, test_rtc_port_range};
use crate::engine::{
    UserId,
    media_transport::{TransportSessionHealth, rtc::test_support::test_transport_session_key},
};

#[test]
#[should_panic(expected = "test session worker must exist")]
fn required_session_health_fixture_rejects_missing_worker() {
    let transport =
        test_media_transport(1, test_rtc_port_range()).expect("test media transport should start");
    let missing_session = test_transport_session_key(1, 2, 1, UserId::Integer(1));
    transport
        .test_api()
        .set_session_transport_health(&missing_session, TransportSessionHealth::Connected);
}
