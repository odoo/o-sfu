//! Bounded categories and sampled diagnostics for worker RTC failures.

use std::{io, net::SocketAddr};

use str0m::RtcError;
use tracing::warn;

use crate::engine::{
    media_transport::TransportSessionKey,
    metrics::{RtcInputFailure, RtcMetricsRecorder, RtcTransportIoFailure},
};

fn io_category(error: &io::Error, receive: bool) -> RtcTransportIoFailure {
    use RtcTransportIoFailure as Failure;
    use io::ErrorKind;
    match (receive, error.kind()) {
        (true, ErrorKind::PermissionDenied) => Failure::ReceivePermissionDenied,
        (false, ErrorKind::PermissionDenied) => Failure::SendPermissionDenied,
        (
            true,
            ErrorKind::NetworkDown | ErrorKind::NetworkUnreachable | ErrorKind::HostUnreachable,
        ) => Failure::ReceiveNetworkUnavailable,
        (
            false,
            ErrorKind::NetworkDown | ErrorKind::NetworkUnreachable | ErrorKind::HostUnreachable,
        ) => Failure::SendNetworkUnavailable,
        (true, ErrorKind::WouldBlock) => Failure::ReceiveWouldBlock,
        (false, ErrorKind::WouldBlock) => Failure::SendWouldBlock,
        (true, _) => Failure::ReceiveOther,
        (false, _) => Failure::SendOther,
    }
}

pub(in crate::engine::media_transport::rtc) fn report_udp_receive_failure(
    metrics: &RtcMetricsRecorder,
    error: &io::Error,
) {
    let category = io_category(error, true);
    if let Some(suppressed) = metrics.record_rtc_transport_io_failure(category) {
        warn!(
            ?category,
            ?error,
            raw_os_error = error.raw_os_error(),
            suppressed,
            "rtc packet loop failed to receive datagram"
        );
    }
}

pub(in crate::engine::media_transport::rtc) fn report_udp_send_failure(
    metrics: &RtcMetricsRecorder,
    destination: SocketAddr,
    error: &io::Error,
) {
    let category = io_category(error, false);
    if let Some(suppressed) = metrics.record_rtc_transport_io_failure(category) {
        warn!(
            ?category,
            ?error,
            raw_os_error = error.raw_os_error(),
            suppressed,
            %destination,
            "rtc packet loop failed to send datagram"
        );
    }
}

fn rtc_input_category(error: &RtcError) -> RtcInputFailure {
    match error {
        RtcError::Io(_) => RtcInputFailure::Io,
        RtcError::Dtls(_) => RtcInputFailure::Dtls,
        RtcError::Net(_) => RtcInputFailure::Net,
        RtcError::Ice(_) => RtcInputFailure::Ice,
        RtcError::Sctp(_) => RtcInputFailure::Sctp,
        _ => RtcInputFailure::Other,
    }
}

pub(in crate::engine::media_transport::rtc) fn report_rtc_input_failure(
    metrics: &RtcMetricsRecorder,
    session_key: &TransportSessionKey,
    error: &RtcError,
) {
    let category = rtc_input_category(error);
    if let Some(suppressed) = metrics.record_rtc_input_failure(category) {
        let raw_os_error = match error {
            RtcError::Io(error) => error.raw_os_error(),
            _ => None,
        };
        warn!(
            ?category,
            ?error,
            raw_os_error,
            suppressed,
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id().as_usize(),
            "rtc packet loop failed to feed datagram into session"
        );
    }
}

#[cfg(test)]
#[path = "TESTS/io_failures.rs"]
mod tests;
