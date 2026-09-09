//! cold-path rtc socket and session bootstrap
//!
//! this module creates worker sockets during construction and session-local
//! str0m state during negotiation
//!
//! bootstrap stops at transport setup
//! room policy, media registration, SDP
//! staging and packet routing stay in the worker modules responsible for those
//! contracts

use std::{
    io::ErrorKind,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket},
    sync::Arc,
    time::{Duration, Instant},
};

use o_sfu_rfc::webrtc;
use str0m::{Candidate, bwe::Bitrate as Str0mBitrate};
use tracing::{info, warn};

use super::{
    RtpProfile,
    consumer_egress::{ConsumerStreamStore, RTX_CACHE_MAX_PACKETS},
    packet_loop::{RtcUdpSocket, UdpIngress},
    state::{
        RtcSessionState, SessionSdpNegotiationState, SharedRtcSocket, bitrate::MediaBitrateCounter,
        slots::SessionStore,
    },
};
use crate::{
    Bitrate, RtcPortRange, RtcUdpIoBackend,
    engine::media_transport::{TransportAdapterError, TransportSessionKey},
};
#[cfg(any(test, feature = "internal-benchmarks", fuzzing))]
use crate::{CodecPreferences, MediaCodecFlags};

/// bind the shared worker UDP socket and return the advertised candidate tuple
///
/// the bind address uses the unspecified address for the configured public IP
/// family so one socket can receive traffic for every session assigned to the
/// worker
/// the advertised candidate keeps the configured public IP with the
/// bound port because that is what browsers must see in SDP
///
/// tries configured ports in order and skips only [`ErrorKind::AddrInUse`]
/// any other bind, nonblocking or backend initialization error aborts startup
///
/// # errors
///
/// returns [`TransportAdapterError::TransportUnavailable`] when every port is
/// occupied or another socket operation fails
pub(super) fn bind_shared_rtc_socket(
    announced_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    rtc_udp_io_backend: RtcUdpIoBackend,
) -> Result<SharedRtcSocket, TransportAdapterError> {
    let bind_ip = bind_ip_for_announced_ip(announced_ip);
    for port in rtc_port_range.ports() {
        let bind_addr = SocketAddr::new(bind_ip, port);
        let socket = match StdUdpSocket::bind(bind_addr) {
            Ok(socket) => socket,
            Err(error) if error.kind() == ErrorKind::AddrInUse => continue,
            Err(error) => {
                warn!(%bind_addr, ?error, "failed to bind shared rtc UDP socket");
                return Err(TransportAdapterError::TransportUnavailable);
            }
        };
        socket.set_nonblocking(true).map_err(|error| {
            warn!(%bind_addr, ?error, "failed to configure shared rtc UDP socket");
            TransportAdapterError::TransportUnavailable
        })?;
        let socket = RtcUdpSocket::from_std(socket, rtc_udp_io_backend).map_err(|error| {
            warn!(
                %bind_addr,
                backend = rtc_udp_io_backend.wire_name(),
                ?error,
                "failed to initialize shared rtc UDP socket"
            );
            TransportAdapterError::TransportUnavailable
        })?;
        let candidate_addr = SocketAddr::new(announced_ip, port);
        let ingress = UdpIngress::new(socket.clone(), bind_addr, candidate_addr);
        info!(
            %bind_addr,
            %candidate_addr,
            "booted shared rtc UDP socket"
        );
        return Ok(SharedRtcSocket {
            socket,
            ingress,
            candidate_addr,
        });
    }
    warn!(
        %bind_ip,
        port_min = rtc_port_range.min(),
        port_max = rtc_port_range.max(),
        "rtc UDP port range is unavailable"
    );
    Err(TransportAdapterError::TransportUnavailable)
}

/// choose the local bind address that matches the advertised IP family
///
/// workers bind to all local interfaces for that family while keeping SDP
/// candidates anchored to the configured announced IP
fn bind_ip_for_announced_ip(announced_ip: IpAddr) -> IpAddr {
    match announced_ip {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    }
}

/// ensure one worker-local [`RtcSessionState`] exists for a session
///
/// this is idempotent because negotiation may ask for readiness more than once
/// while a session is still alive
/// `Ok(true)` means a fresh [`str0m::Rtc`] was created and inserted
/// `Ok(false)` means the existing session state still matches that key
///
/// new sessions start in ICE-lite mode, with RTP mode enabled, bandwidth
/// estimation capped by `max_bitrate_out` and exactly one local host candidate
/// attached to the shared worker socket's advertised address
///
/// # errors
///
/// returns `TransportUnavailable` if the local candidate cannot be represented
/// by str0m or cannot be attached to the newly created rtc state
#[cfg(any(test, feature = "internal-benchmarks", fuzzing))]
pub(super) fn ensure_session_rtc_state(
    users: &mut SessionStore,
    session_key: &TransportSessionKey,
    candidate_addr: SocketAddr,
    max_bitrate_out: Bitrate,
) -> Result<bool, TransportAdapterError> {
    let profile = RtpProfile::compile(MediaCodecFlags::default(), CodecPreferences::default())?;
    ensure_session_rtc_state_with_stats_interval(
        users,
        Arc::from("test-room"),
        session_key,
        candidate_addr,
        max_bitrate_out,
        &profile,
        None,
    )
}

pub(super) fn ensure_session_rtc_state_with_stats_interval(
    users: &mut SessionStore,
    room_id: Arc<str>,
    session_key: &TransportSessionKey,
    candidate_addr: SocketAddr,
    max_bitrate_out: Bitrate,
    profile: &RtpProfile,
    stats_interval: Option<Duration>,
) -> Result<bool, TransportAdapterError> {
    if users.contains_key(session_key) {
        return Ok(false);
    }
    let started_at = Instant::now();
    let mut config = profile
        .session_config()
        .set_send_buffer_video(RTX_CACHE_MAX_PACKETS);
    if let Some(stats_interval) = stats_interval {
        config = config.set_stats_interval(Some(stats_interval));
    }
    let mut rtc = config
        .enable_bwe(Some(Str0mBitrate::bps(max_bitrate_out.as_bps())))
        .set_ice_lite(true)
        .build(started_at);
    let candidate = Candidate::host(candidate_addr, webrtc::IceTransport::Udp.as_str())
        .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
    if rtc.add_local_candidate(candidate).is_none() {
        return Err(TransportAdapterError::TransportUnavailable);
    }
    let local_ice_ufrag = rtc.direct_api().local_ice_credentials().ufrag;
    users.insert(
        session_key.clone(),
        RtcSessionState {
            room_id,
            rtc,
            started_at,
            rtcp_ingress_budget: super::state::RtcpIngressBudget::new(started_at),
            defer_rtx_expiry: false,
            pending_rtp_input: None,
            nack_totals: super::state::RtcNackTotals::default(),
            egress_bitrate: Arc::new(MediaBitrateCounter::new(started_at)),
            local_ice_ufrag,
            #[cfg(test)]
            max_bitrate_in: None,
            #[cfg(test)]
            max_bitrate_out: Some(max_bitrate_out),
            receiver_bwe_target: None,
            #[cfg(test)]
            receiver_bwe_str0m_update_count: 0,
            dtls_started: false,
            packet_loop_dirty: false,
            next_timeout: None,
            sdp_negotiation: SessionSdpNegotiationState::default(),
            consumer_streams: ConsumerStreamStore::default(),
        },
    );
    Ok(true)
}
