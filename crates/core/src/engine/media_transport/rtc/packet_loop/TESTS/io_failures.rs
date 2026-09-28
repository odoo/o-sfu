use super::*;

#[test]
fn io_categories_are_fixed_and_preserve_fallback() {
    assert_eq!(
        io_category(&io::Error::from(io::ErrorKind::PermissionDenied), true),
        RtcTransportIoFailure::ReceivePermissionDenied
    );
    assert_eq!(
        io_category(&io::Error::from(io::ErrorKind::NetworkUnreachable), false),
        RtcTransportIoFailure::SendNetworkUnavailable
    );
    assert_eq!(
        io_category(&io::Error::from(io::ErrorKind::InvalidData), false),
        RtcTransportIoFailure::SendOther
    );
}

#[test]
fn rtc_error_categories_are_fixed_and_preserve_fallback() {
    assert_eq!(
        rtc_input_category(&RtcError::Io(io::Error::other("io"))),
        RtcInputFailure::Io
    );
    assert_eq!(
        rtc_input_category(&RtcError::NoSenderSource),
        RtcInputFailure::Other
    );
}
