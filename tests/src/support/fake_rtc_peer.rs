#![allow(
    dead_code,
    reason = "the str0m-backed fake RTP peer is shared across protocol integration scenarios"
)]

use std::{
    collections::{BTreeMap, VecDeque},
    mem,
    net::SocketAddr,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use o_sfu_protocol::wire::SessionDescriptionPayload;
use o_sfu_rfc::{
    rtp::{self, CodecName},
    webrtc::{self, sdp},
};
use o_sfu_router::MediaKind;
use str0m::{
    Candidate, Event, IceConnectionState, Input, Output, Rtc,
    change::SdpOffer,
    format::{Codec, PayloadParams},
    media::{KeyframeRequest, Mid, Pt, Rid},
    net::{Protocol, Receive, Transmit},
    rtp::{
        RawPacket, RtpHeader, RtpPacket, RtpWrite, Ssrc,
        rtcp::{Nack, Rtcp},
    },
};
use str0m_netem::{
    Input as NetemInput, Netem, NetemConfig, Output as NetemOutput, Probability, RandomLoss,
};
use tokio::{net::UdpSocket, time::timeout};
use tokio_util::bytes::Bytes;

use super::fake_media::{FakeClock, FakeMediaFrame, FakeMediaSource};

const RECEIVE_BUFFER_LEN: usize = 2_000;
const MAX_SOCKET_WAIT: Duration = Duration::from_millis(50);
const IO_SLICE: Duration = Duration::from_millis(20);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedRtpPacket {
    pub mid: String,
    pub payload_type: u8,
    pub sequence_number: u16,
    pub timestamp: u32,
    pub marker: bool,
    pub ssrc: u32,
    pub rid: Option<String>,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtcTraceDirection {
    Tx,
    Rx,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TracedNack {
    pub direction: RtcTraceDirection,
    pub ssrc: u32,
    pub sequence_numbers: Vec<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TracedRtpPacket {
    pub direction: RtcTraceDirection,
    pub payload_type: u8,
    pub sequence_number: u16,
    pub original_sequence_number: Option<u16>,
    pub timestamp: u32,
    pub marker: bool,
    pub ssrc: u32,
    pub rid: Option<String>,
    pub repaired_rid: Option<String>,
    pub transport_sequence_number: Option<u16>,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DroppedRtpPacket {
    pub direction: RtcTraceDirection,
    pub payload_type: u8,
    pub sequence_number: u16,
    pub timestamp: u32,
    pub marker: bool,
    pub ssrc: u32,
    pub transport_sequence_number: Option<u16>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RtcPeerTrace {
    pub nacks: Vec<TracedNack>,
    pub rtp_packets: Vec<TracedRtpPacket>,
    pub inbound_rtp_headers: Vec<rtp::RtpFixedHeader>,
    pub dropped_rtp_packets: Vec<DroppedRtpPacket>,
    pub keyframe_requests: usize,
}

pub struct FakeRtcPeer {
    rtc: Rtc,
    socket: UdpSocket,
    local_addr: SocketAddr,
    media_mids: Vec<Mid>,
    transport_sequence_extension_id: Option<u8>,
    send_paths: BTreeMap<MediaKind, ProtocolSendPath>,
    ridless_video_fid: bool,
    pending_keyframe_requests: Vec<KeyframeRequest>,
    connected: bool,
    start_wallclock: Instant,
    next_synthetic_ssrc: u32,
    trace: RtcPeerTrace,
    trace_enabled: bool,
    outbound_rtp_hold: Option<RtpSelector>,
    outbound_rtp_delay: Option<OutboundRtpDelay>,
    drop_next_inbound_rtp: Option<RtpLoss>,
    held_outbound_rtp: VecDeque<(SocketAddr, Vec<u8>)>,
}

#[derive(Clone, Copy)]
struct RtpSelector {
    payload_type: u8,
    ssrc: u32,
}

struct RtpLoss {
    selector: RtpSelector,
    netem: Netem<Bytes>,
}

struct OutboundRtpDelay {
    selector: Option<RtpSelector>,
    pending: Netem<PendingOutboundDatagram>,
}

#[derive(Clone)]
struct PendingOutboundDatagram {
    destination: SocketAddr,
    contents: Bytes,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PumpTarget {
    Connected,
    RtpPacket,
    Deadline,
}

enum PumpResult {
    Connected,
    RtpPacket(ReceivedRtpPacket),
    Deadline,
}

#[derive(Clone)]
struct ProtocolSendPath {
    mid: Mid,
    rids: Vec<ProtocolRid>,
}

#[derive(Clone)]
struct ProtocolRid {
    rid: Rid,
    max_bitrate: Option<u64>,
}

impl FakeRtcPeer {
    pub async fn bind(port: u16) -> Option<Self> {
        let socket = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], port)))
            .await
            .ok()?;
        let local_addr = socket.local_addr().ok()?;
        let mut rtc = Rtc::builder()
            .set_rtp_mode(true)
            .enable_raw_packets(true)
            .build(Instant::now());
        rtc.add_local_candidate(Candidate::host(local_addr, webrtc::ice::transport::UDP).ok()?)?;
        Some(Self {
            rtc,
            socket,
            local_addr,
            media_mids: Vec::new(),
            transport_sequence_extension_id: None,
            send_paths: BTreeMap::new(),
            ridless_video_fid: false,
            pending_keyframe_requests: Vec::new(),
            connected: false,
            start_wallclock: Instant::now(),
            next_synthetic_ssrc: 0x0f00_0001,
            trace: RtcPeerTrace::default(),
            trace_enabled: false,
            outbound_rtp_hold: None,
            outbound_rtp_delay: None,
            drop_next_inbound_rtp: None,
            held_outbound_rtp: VecDeque::new(),
        })
    }

    pub fn answer_offer(&mut self, offer_sdp: &str) -> Option<SessionDescriptionPayload> {
        let ridless_offer = self
            .ridless_video_fid
            .then(|| offer_without_rid_simulcast(offer_sdp));
        let offer_sdp = ridless_offer.as_deref().unwrap_or(offer_sdp);
        let offer = SdpOffer::from_sdp_string(offer_sdp).ok()?;
        self.media_mids = collect_protocol_media_mids(offer_sdp);
        self.transport_sequence_extension_id = transport_sequence_extension_id(offer_sdp);
        self.send_paths = collect_protocol_send_paths(offer_sdp);
        let answer = self.rtc.sdp_api().accept_offer(offer).ok()?;
        let answer_sdp = answer_with_simulcast_send_rids(&answer.to_sdp_string(), &self.send_paths);
        self.rtc.handle_input(Input::Timeout(Instant::now())).ok()?;
        Some(SessionDescriptionPayload {
            sdp: answer_sdp,
            upload_slots: Vec::new(),
        })
    }

    pub fn answer_video_with_ridless_fid(&mut self) {
        self.ridless_video_fid = true;
    }

    pub fn answer_offer_without_candidates(
        &mut self,
        offer_sdp: &str,
    ) -> Option<SessionDescriptionPayload> {
        let mut answer = self.answer_offer(offer_sdp)?;
        answer.sdp = answer
            .sdp
            .split_inclusive(sdp::CRLF)
            .filter(|line| {
                !line.strip_prefix(sdp::ATTR).is_some_and(|attribute| {
                    attribute.starts_with(webrtc::ice::candidate_attribute::PREFIX)
                })
            })
            .collect();
        Some(answer)
    }

    pub async fn wait_until_connected(&mut self, timeout_window: Duration) -> Option<()> {
        if self.connected {
            return Some(());
        }
        match self
            .pump_until(Instant::now() + timeout_window, PumpTarget::Connected)
            .await
            .ok()?
        {
            PumpResult::Connected => Some(()),
            PumpResult::RtpPacket(_) | PumpResult::Deadline => None,
        }
    }

    pub async fn send_rtp_packets(
        &mut self,
        source: &mut FakeMediaSource,
        clock: &mut FakeClock,
        frame_count: usize,
    ) -> Option<()> {
        for _ in 0..frame_count {
            self.apply_keyframe_requests(source);
            let frame = source.next_frame(clock);
            self.write_rtp_packet(frame)?;
            self.pump_until(Instant::now() + IO_SLICE, PumpTarget::Deadline)
                .await
                .ok()?;
        }
        Some(())
    }

    pub async fn send_rtp_packet(
        &mut self,
        source: &mut FakeMediaSource,
        clock: &mut FakeClock,
    ) -> Option<Vec<u8>> {
        self.apply_keyframe_requests(source);
        let frame = source.next_frame(clock);
        let expected_payload = frame.payload.clone();
        self.write_rtp_packet(frame)?;
        self.pump_until(Instant::now() + IO_SLICE, PumpTarget::Deadline)
            .await
            .ok()?;
        Some(expected_payload)
    }

    /// Returns the next RTP packet or `Ok(None)` after a healthy receive timeout.
    ///
    /// # Errors
    ///
    /// Returns an [`anyhow::Error`] for RTC processing, UDP I/O, invalid incoming
    /// datagrams, missing received-media metadata or ICE disconnection. These
    /// failures must not satisfy assertions that media was suppressed.
    pub async fn read_rtp_packet(
        &mut self,
        timeout_window: Duration,
    ) -> Result<Option<ReceivedRtpPacket>> {
        Ok(
            match self
                .pump_until(Instant::now() + timeout_window, PumpTarget::RtpPacket)
                .await?
            {
                PumpResult::RtpPacket(packet) => Some(packet),
                PumpResult::Connected | PumpResult::Deadline => None,
            },
        )
    }

    pub async fn pump(&mut self, timeout_window: Duration) -> Option<()> {
        self.pump_until(Instant::now() + timeout_window, PumpTarget::Deadline)
            .await
            .ok()?;
        Some(())
    }

    pub fn hold_outbound_rtp(&mut self, payload_type: u8, ssrc: u32) {
        self.trace_enabled = true;
        self.outbound_rtp_hold = Some(RtpSelector { payload_type, ssrc });
    }

    pub fn clear_outbound_rtp_hold(&mut self) {
        self.outbound_rtp_hold = None;
    }

    pub fn try_delay_next_outbound_rtp(
        &mut self,
        payload_type: u8,
        ssrc: u32,
        delay: Duration,
    ) -> bool {
        if self.outbound_rtp_delay.is_some() {
            return false;
        }
        self.trace_enabled = true;
        self.outbound_rtp_delay = Some(OutboundRtpDelay::new(
            RtpSelector { payload_type, ssrc },
            delay,
        ));
        true
    }

    pub fn has_delayed_outbound_rtp(&self) -> bool {
        self.outbound_rtp_delay
            .as_ref()
            .is_some_and(|delay| !delay.pending.is_empty())
    }

    pub async fn release_delayed_outbound_rtp(&mut self) -> Option<()> {
        // Evaluate `Netem` at its own deadline so tests can release a late packet
        // deterministically without sleeping for the configured delay.
        let release_at = self.outbound_rtp_delay.as_ref()?.next_timeout()?;
        self.flush_delayed_outbound_rtp(release_at).await?;
        self.outbound_rtp_delay.is_none().then_some(())
    }

    pub fn drop_next_inbound_rtp(&mut self, payload_type: u8, ssrc: u32) {
        self.trace_enabled = true;
        self.drop_next_inbound_rtp = Some(RtpLoss::new(RtpSelector { payload_type, ssrc }));
    }

    pub fn held_outbound_rtp_count(&self) -> usize {
        self.held_outbound_rtp.len()
    }

    pub fn discard_next_held_outbound_rtp(&mut self) -> bool {
        self.held_outbound_rtp.pop_front().is_some()
    }

    pub async fn release_next_held_outbound_rtp(&mut self) -> Option<()> {
        let (destination, contents) = self.held_outbound_rtp.pop_front()?;
        self.socket.send_to(&contents, destination).await.ok()?;
        Some(())
    }

    pub fn start_trace(&mut self) {
        self.trace = RtcPeerTrace::default();
        self.trace_enabled = true;
    }

    pub fn take_trace(&mut self) -> RtcPeerTrace {
        mem::take(&mut self.trace)
    }

    pub fn repair_payload_types(&self, codec_name: &CodecName) -> Option<(u8, u8)> {
        let codec = str0m_codec(codec_name)?;
        let payload = self
            .rtc
            .codec_config()
            .find(|params| params.spec().codec == codec)
            .filter(|payload| {
                self.media_mids.iter().any(|mid| {
                    self.rtc.media(*mid).is_some_and(|media| {
                        media.remote_pts().contains(&payload.pt()) && payload.resend().is_some()
                    })
                })
            })?;
        Some((*payload.pt(), *payload.resend()?))
    }

    pub fn send_stream_ssrc_pair(
        &mut self,
        media_kind: MediaKind,
        rid: Option<&str>,
    ) -> Option<(u32, u32)> {
        let mid = self.send_paths.get(&media_kind)?.mid;
        let mut direct_api = self.rtc.direct_api();
        let stream = direct_api.stream_tx_by_mid(mid, rid.map(Rid::from))?;
        Some((*stream.ssrc(), *stream.rtx()?))
    }

    pub fn receive_repair_ssrc(&mut self, primary_ssrc: u32) -> Option<u32> {
        self.rtc
            .direct_api()
            .stream_rx(&Ssrc::from(primary_ssrc))?
            .rtx()
            .map(|ssrc| *ssrc)
    }

    pub fn reset_rtp_ssrc(&mut self, media_kind: MediaKind, rid: Option<&str>) -> Option<()> {
        let mid = self.send_paths.get(&media_kind)?.mid;
        let ssrc = self.next_synthetic_ssrc();
        let repair_ssrc = self
            .repair_enabled(mid, media_kind)
            .then(|| self.next_synthetic_ssrc());
        let current_ssrc = {
            let mut api = self.rtc.direct_api();
            *api.stream_tx_by_mid(mid, rid.map(Rid::from))?.ssrc()
        };
        let mut api = self.rtc.direct_api();
        api.remove_stream_tx(Ssrc::from(current_ssrc));
        api.declare_stream_tx(ssrc, repair_ssrc, mid, rid.map(Rid::from));
        Some(())
    }

    fn apply_keyframe_requests(&mut self, source: &mut FakeMediaSource) {
        let Some(mid) = self
            .send_paths
            .get(&source.media_kind())
            .map(|send_path| send_path.mid)
        else {
            return;
        };
        for request in mem::take(&mut self.pending_keyframe_requests) {
            if request.mid == mid {
                source.request_keyframe(request.rid.as_deref());
            } else {
                self.pending_keyframe_requests.push(request);
            }
        }
    }

    fn write_rtp_packet(&mut self, frame: FakeMediaFrame) -> Option<()> {
        let send_path = self.send_paths.get(&frame.media_kind)?.clone();
        let payload_type = payload_type_for_codec(&self.rtc, &frame.codec)?;
        let nackable = self.payload_repair_enabled(send_path.mid, payload_type);
        let stream_rid = frame.rid.as_deref().map(Rid::from);
        let extension_rid = frame.rid.as_deref().map(Rid::from);
        if !send_path.accepts_rid(stream_rid) {
            return None;
        }
        self.ensure_tx_stream(send_path.mid, stream_rid, nackable);
        let mut extension_values = frame.extension_values;
        extension_values.mid = Some(send_path.mid);
        extension_values.rid = extension_rid;
        self.rtc
            .direct_api()
            .stream_tx_by_mid(send_path.mid, stream_rid)?
            .write_rtp(
                RtpWrite::new(
                    payload_type,
                    u64::from(frame.sequence_number).into(),
                    frame.rtp_timestamp,
                    self.start_wallclock + frame.emitted_at,
                    frame.payload,
                )
                .marker(frame.marker)
                .ext_vals(extension_values)
                .nackable(nackable),
            );
        Some(())
    }

    fn ensure_tx_stream(&mut self, mid: Mid, rid: Option<Rid>, repair_enabled: bool) {
        if self.rtc.direct_api().stream_tx_by_mid(mid, rid).is_some() {
            return;
        }
        let ssrc = self.next_synthetic_ssrc();
        let repair_ssrc = repair_enabled.then(|| self.next_synthetic_ssrc());
        self.rtc
            .direct_api()
            .declare_stream_tx(ssrc, repair_ssrc, mid, rid);
    }

    fn repair_enabled(&self, mid: Mid, media_kind: MediaKind) -> bool {
        let Some(media) = self.rtc.media(mid) else {
            return false;
        };
        media_kind == MediaKind::Video
            && self.rtc.codec_config().params().iter().any(|params| {
                params.spec().codec.is_video()
                    && media.remote_pts().contains(&params.pt())
                    && params.resend().is_some()
            })
    }

    fn payload_repair_enabled(&self, mid: Mid, payload_type: Pt) -> bool {
        self.rtc.media(mid).is_some_and(|media| {
            media.remote_pts().contains(&payload_type)
                && self
                    .rtc
                    .codec_config()
                    .params()
                    .iter()
                    .any(|params| params.pt() == payload_type && params.resend().is_some())
        })
    }

    fn next_synthetic_ssrc(&mut self) -> Ssrc {
        let ssrc = self.next_synthetic_ssrc;
        self.next_synthetic_ssrc = self.next_synthetic_ssrc.wrapping_add(1);
        Ssrc::from(ssrc)
    }

    async fn pump_until(&mut self, deadline: Instant, target: PumpTarget) -> Result<PumpResult> {
        let mut receive_buffer = [0_u8; RECEIVE_BUFFER_LEN];
        while Instant::now() < deadline {
            let now = Instant::now();
            match self.rtc.poll_output()? {
                Output::Transmit(transmit) => {
                    if self
                        .intercept_selected_rtp(RtcTraceDirection::Tx, transmit.contents.as_ref())
                    {
                        self.held_outbound_rtp
                            .push_back((transmit.destination, Vec::from(transmit.contents)));
                        continue;
                    }
                    let Some(transmit) = self.schedule_delayed_outbound_rtp(now, transmit) else {
                        continue;
                    };
                    self.socket
                        .send_to(&transmit.contents, transmit.destination)
                        .await?;
                }
                Output::Event(Event::RawPacket(packet)) => {
                    if self.trace_enabled {
                        record_raw_packet(&self.rtc, &mut self.trace, packet.as_ref());
                    }
                }
                Output::Event(Event::RtpPacket(packet)) => {
                    if target == PumpTarget::RtpPacket {
                        return into_received_rtp_packet(&mut self.rtc, &packet)
                            .map(PumpResult::RtpPacket)
                            .context("received RTP packet has no negotiated media metadata");
                    }
                }
                Output::Event(Event::Connected) => {
                    self.connected = true;
                    if target == PumpTarget::Connected {
                        return Ok(PumpResult::Connected);
                    }
                }
                Output::Event(Event::IceConnectionStateChange(
                    IceConnectionState::Disconnected,
                )) => bail!("fake RTC peer disconnected"),
                Output::Event(Event::KeyframeRequest(request)) => {
                    if self.trace_enabled {
                        self.trace.keyframe_requests += 1;
                    }
                    self.pending_keyframe_requests.push(request);
                }
                Output::Event(_) => {}
                Output::Timeout(timeout_at) => {
                    if timeout_at <= now {
                        self.rtc.handle_input(Input::Timeout(now))?;
                        continue;
                    }
                    self.flush_delayed_outbound_rtp(now)
                        .await
                        .context("failed to send delayed RTP datagram")?;
                    let now = Instant::now();

                    let wait_duration = self.socket_wait_duration(timeout_at, now, deadline);
                    if wait_duration.is_zero() {
                        self.rtc.handle_input(Input::Timeout(Instant::now()))?;
                        continue;
                    }

                    match timeout(wait_duration, self.socket.recv_from(&mut receive_buffer)).await {
                        Ok(Ok((received_size, source_addr))) => {
                            if received_size == 0 {
                                continue;
                            }
                            let packet = receive_buffer
                                .get(..received_size)
                                .context("received UDP datagram exceeds buffer")?;
                            // Trace before `handle_input` because str0m rejects a late primary
                            // recovered by RTX before `Event::RawPacket` or `Event::RtpPacket`.
                            if self.trace_enabled
                                && let Some(header) = rtp::parse_muxed_rtp_fixed_header(packet)
                            {
                                self.trace.inbound_rtp_headers.push(header);
                            }
                            if self.intercept_selected_rtp(RtcTraceDirection::Rx, packet) {
                                continue;
                            }
                            let receive = Receive {
                                proto: Protocol::Udp,
                                source: source_addr,
                                destination: self.local_addr,
                                contents: packet.try_into()?,
                            };
                            self.rtc
                                .handle_input(Input::Receive(Instant::now(), receive))?;
                        }
                        Ok(Err(error)) => return Err(error.into()),
                        Err(_elapsed) => {
                            self.rtc.handle_input(Input::Timeout(Instant::now()))?;
                        }
                    }
                }
            }
        }
        Ok(PumpResult::Deadline)
    }

    fn socket_wait_duration(
        &self,
        rtc_timeout: Instant,
        now: Instant,
        deadline: Instant,
    ) -> Duration {
        // Keep polling str0m while netem holds the primary so RTX can overtake it.
        let netem_wait = self
            .outbound_rtp_delay
            .as_ref()
            .and_then(OutboundRtpDelay::next_timeout)
            .map_or(Duration::MAX, |timeout| {
                timeout.saturating_duration_since(now)
            });
        rtc_timeout
            .saturating_duration_since(now)
            .min(netem_wait)
            .min(MAX_SOCKET_WAIT)
            .min(deadline.saturating_duration_since(now))
    }

    async fn flush_delayed_outbound_rtp(&mut self, now: Instant) -> Option<()> {
        while let Some(datagram) = self
            .outbound_rtp_delay
            .as_mut()
            .and_then(|delay| delay.pop_due(now))
        {
            self.socket
                .send_to(&datagram.contents, datagram.destination)
                .await
                .ok()?;
        }
        if self
            .outbound_rtp_delay
            .as_ref()
            .is_some_and(OutboundRtpDelay::is_complete)
        {
            self.outbound_rtp_delay = None;
        }
        Some(())
    }

    fn schedule_delayed_outbound_rtp(
        &mut self,
        now: Instant,
        transmit: Transmit,
    ) -> Option<Transmit> {
        let Some(delay) = self.outbound_rtp_delay.as_mut() else {
            return Some(transmit);
        };
        let Some(selector) = delay.selector else {
            return Some(transmit);
        };
        let Some(header) = rtp::parse_muxed_rtp_fixed_header(transmit.contents.as_ref()) else {
            return Some(transmit);
        };
        if !selector.matches(header.payload_type(), header.ssrc().value()) {
            return Some(transmit);
        }
        let datagram = PendingOutboundDatagram {
            destination: transmit.destination,
            contents: Bytes::from(Vec::from(transmit.contents)),
        };
        delay.push(now, datagram);
        None
    }

    fn intercept_selected_rtp(&mut self, direction: RtcTraceDirection, raw_packet: &[u8]) -> bool {
        let Some(packet) =
            dropped_rtp_packet(direction, raw_packet, self.transport_sequence_extension_id)
        else {
            return false;
        };
        let selected = match direction {
            RtcTraceDirection::Tx => self.outbound_rtp_hold,
            RtcTraceDirection::Rx => self
                .drop_next_inbound_rtp
                .as_ref()
                .map(|loss| loss.selector),
        };
        if !selected.is_some_and(|selector| selector.matches(packet.payload_type, packet.ssrc)) {
            return false;
        }
        if direction == RtcTraceDirection::Rx {
            let Some(mut loss) = self.drop_next_inbound_rtp.take() else {
                return false;
            };
            if !loss.drops(raw_packet) {
                return false;
            }
        }
        if self.trace_enabled {
            self.trace.dropped_rtp_packets.push(packet);
        }
        true
    }
}

impl ProtocolSendPath {
    fn accepts_rid(&self, rid: Option<Rid>) -> bool {
        rid.map_or(self.rids.is_empty(), |rid| {
            !self.rids.is_empty() && self.rids.iter().any(|candidate| candidate.rid == rid)
        })
    }
}

fn into_received_rtp_packet(rtc: &mut Rtc, packet: &RtpPacket) -> Option<ReceivedRtpPacket> {
    let mid = rtc
        .direct_api()
        .stream_rx(&packet.header.ssrc)?
        .mid()
        .to_string();
    Some(ReceivedRtpPacket {
        mid,
        payload_type: *packet.header.payload_type,
        sequence_number: packet.header.sequence_number,
        timestamp: packet.header.timestamp,
        marker: packet.header.marker,
        ssrc: *packet.header.ssrc,
        rid: packet.header.ext_vals.rid.map(|rid| rid.to_string()),
        payload: Bytes::copy_from_slice(packet.payload.as_ref()),
    })
}

fn record_raw_packet(rtc: &Rtc, trace: &mut RtcPeerTrace, packet: &RawPacket) {
    match packet {
        RawPacket::RtcpTx(Rtcp::Nack(nack)) => {
            trace.nacks.push(traced_nack(RtcTraceDirection::Tx, nack));
        }
        RawPacket::RtcpRx(Rtcp::Nack(nack)) => {
            trace.nacks.push(traced_nack(RtcTraceDirection::Rx, nack));
        }
        RawPacket::RtpTx(header, packet) => {
            let Some(payload) = unpad_transmitted_payload(header, packet) else {
                return;
            };
            if payload.is_empty() {
                return;
            }
            trace.rtp_packets.push(traced_rtp_packet(
                rtc,
                RtcTraceDirection::Tx,
                header,
                payload,
            ));
        }
        RawPacket::RtpRx(header, payload) => {
            if payload.is_empty() {
                return;
            }
            trace.rtp_packets.push(traced_rtp_packet(
                rtc,
                RtcTraceDirection::Rx,
                header,
                payload,
            ));
        }
        RawPacket::RtcpTx(_) | RawPacket::RtcpRx(_) => {}
    }
}

fn traced_nack(direction: RtcTraceDirection, nack: &Nack) -> TracedNack {
    // Each Generic NACK entry carries one PID plus the following losses in BLP.
    // https://www.rfc-editor.org/rfc/rfc4585.html#section-6.2.1
    let sequence_numbers = nack
        .reports
        .iter()
        .flat_map(|report| rtp::generic_nack_sequence_numbers(report.pid, report.blp))
        .collect();
    TracedNack {
        direction,
        ssrc: *nack.ssrc,
        sequence_numbers,
    }
}

fn traced_rtp_packet(
    rtc: &Rtc,
    direction: RtcTraceDirection,
    header: &RtpHeader,
    payload: &[u8],
) -> TracedRtpPacket {
    let is_retransmission = rtc
        .codec_config()
        .params()
        .iter()
        .any(|params| params.resend() == Some(header.payload_type));
    // An RTX payload starts with the original packet sequence number.
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-4
    let original_sequence_number = if is_retransmission {
        rtp::rtx_original_sequence_number(payload)
    } else {
        None
    };
    TracedRtpPacket {
        direction,
        payload_type: *header.payload_type,
        sequence_number: header.sequence_number,
        original_sequence_number,
        timestamp: header.timestamp,
        marker: header.marker,
        ssrc: *header.ssrc,
        rid: header.ext_vals.rid.map(|rid| rid.to_string()),
        repaired_rid: header.ext_vals.rid_repair.map(|rid| rid.to_string()),
        transport_sequence_number: header.ext_vals.transport_cc,
        payload: Bytes::copy_from_slice(payload),
    }
}

fn unpad_transmitted_payload<'a>(header: &RtpHeader, packet: &'a [u8]) -> Option<&'a [u8]> {
    // str0m calls `RtpHeader::pad_packet` after cloning the `RawPacket` header,
    // so only the transmitted fixed header carries the final P bit.
    // https://www.rfc-editor.org/rfc/rfc3550.html#section-5.1
    let wire_header = rtp::parse_muxed_rtp_fixed_header(packet)?;
    let payload = packet.get(header.header_len..)?;
    unpad_rtp_payload(payload, wire_header.has_padding())
}

fn unpad_rtp_payload(payload: &[u8], has_padding: bool) -> Option<&[u8]> {
    if !has_padding {
        return Some(payload);
    }
    // The final octet counts all padding octets including itself, so zero
    // cannot describe an RTP packet with P set.
    // https://www.rfc-editor.org/rfc/rfc3550.html#section-5.1
    let padding_len = usize::from(*payload.last()?);
    if padding_len == 0 {
        return None;
    }
    payload.get(..payload.len().checked_sub(padding_len)?)
}

fn dropped_rtp_packet(
    direction: RtcTraceDirection,
    packet: &[u8],
    transport_sequence_extension_id: Option<u8>,
) -> Option<DroppedRtpPacket> {
    // RTP/RTCP mux reserves the second-octet RTCP range before the remaining
    // RTP fixed-header fields can be interpreted.
    // https://www.rfc-editor.org/rfc/rfc5761.html#section-4
    // https://www.rfc-editor.org/rfc/rfc3550.html#section-5.1
    let header = rtp::parse_muxed_rtp_fixed_header(packet)?;
    Some(DroppedRtpPacket {
        direction,
        payload_type: header.payload_type(),
        sequence_number: header.sequence_number(),
        timestamp: header.timestamp(),
        marker: header.marker(),
        ssrc: header.ssrc().value(),
        // One-byte extension padding and reserved IDs affect whether a later
        // transport sequence element is reachable.
        // https://www.rfc-editor.org/rfc/rfc8285.html#section-4.1.2
        // https://www.rfc-editor.org/rfc/rfc8285.html#section-4.2
        transport_sequence_number: transport_sequence_extension_id
            .and_then(|id| rtp::header_extension::find_one_byte_element(packet, id))
            .and_then(|value| value.try_into().ok())
            .map(u16::from_be_bytes),
    })
}

impl RtpSelector {
    fn matches(self, payload_type: u8, ssrc: u32) -> bool {
        self.payload_type == payload_type && self.ssrc == ssrc
    }
}

impl RtpLoss {
    fn new(selector: RtpSelector) -> Self {
        Self {
            selector,
            netem: Netem::new(NetemConfig::new().loss(RandomLoss::new(Probability::ONE))),
        }
    }

    fn drops(&mut self, packet: &[u8]) -> bool {
        self.netem.handle_input(NetemInput::Packet(
            Instant::now(),
            Bytes::copy_from_slice(packet),
        ));
        self.netem.is_empty()
    }
}

impl OutboundRtpDelay {
    fn new(selector: RtpSelector, delay: Duration) -> Self {
        Self {
            selector: Some(selector),
            pending: Netem::new(NetemConfig::new().latency(delay)),
        }
    }

    fn push(&mut self, now: Instant, datagram: PendingOutboundDatagram) {
        self.selector = None;
        self.pending.handle_input(NetemInput::Packet(now, datagram));
    }

    fn pop_due(&mut self, now: Instant) -> Option<PendingOutboundDatagram> {
        if self.pending.is_empty() || self.pending.poll_timeout() > now {
            return None;
        }
        self.pending.handle_input(NetemInput::Timeout(now));
        match self.pending.poll_output() {
            Some(NetemOutput::Packet(datagram)) => Some(datagram),
            Some(NetemOutput::Timeout(_)) | None => None,
        }
    }

    fn next_timeout(&self) -> Option<Instant> {
        (!self.pending.is_empty()).then(|| self.pending.poll_timeout())
    }

    fn is_complete(&self) -> bool {
        self.selector.is_none() && self.pending.is_empty()
    }
}

impl AsRef<[u8]> for PendingOutboundDatagram {
    fn as_ref(&self) -> &[u8] {
        &self.contents
    }
}

fn trim_sdp_line_ending(line: &str) -> &str {
    line.trim_end_matches([sdp::CR, sdp::LF])
}

fn sdp_attribute_value<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    // SDP value attributes use the exact `a=<name>:<value>` form.
    // https://www.rfc-editor.org/rfc/rfc8866.html#section-5.13
    let attribute = trim_sdp_line_ending(line).strip_prefix(sdp::ATTR)?;
    let (attribute_name, value) = attribute.split_once(sdp::ATTR_SEP)?;
    (attribute_name == name).then_some(value)
}

fn collect_protocol_media_mids(offer_sdp: &str) -> Vec<Mid> {
    offer_sdp
        .lines()
        .filter_map(|line| sdp_attribute_value(line, sdp::attribute::MID))
        .map(Mid::from)
        .collect()
}

fn transport_sequence_extension_id(offer_sdp: &str) -> Option<u8> {
    offer_sdp.lines().find_map(|line| {
        let mut fields =
            sdp_attribute_value(line, sdp::attribute::EXTMAP)?.split_ascii_whitespace();
        // The extmap ID may carry a direction suffix after `/`.
        // https://www.rfc-editor.org/rfc/rfc8285.html#section-5
        let id = fields
            .next()?
            .split(sdp::extmap::DIR_SEP)
            .next()?
            .parse()
            .ok()?;
        (fields.next()? == webrtc::rtp_header_extension_uri::TRANSPORT_WIDE_CC_DRAFT_01)
            .then_some(id)
    })
}

fn collect_protocol_send_paths(offer_sdp: &str) -> BTreeMap<MediaKind, ProtocolSendPath> {
    let mut send_paths = BTreeMap::new();
    // Media sections inherit a session direction. If neither level declares
    // one, SDP defaults the section to sendrecv.
    // https://www.rfc-editor.org/rfc/rfc8866.html#section-6.7
    let mut session_direction = OfferDirection::default();
    let mut in_media_section = false;
    let mut current_kind: Option<MediaKind> = None;
    let mut current_mid: Option<Mid> = None;
    let mut current_rids = Vec::new();
    let mut current_direction = session_direction;

    let mut flush_section = |kind: Option<MediaKind>,
                             mid: Option<Mid>,
                             rids: Vec<ProtocolRid>,
                             direction: OfferDirection| {
        let Some(kind) = kind else {
            return;
        };
        if !direction.allows_local_send() {
            return;
        }
        let Some(mid) = mid else {
            return;
        };
        send_paths.insert(kind, ProtocolSendPath { mid, rids });
    };

    for raw_line in offer_sdp.lines() {
        let line = trim_sdp_line_ending(raw_line);
        if let Some(kind) = parse_offer_media_kind(line) {
            flush_section(current_kind, current_mid, current_rids, current_direction);
            in_media_section = true;
            current_kind = Some(kind);
            current_mid = None;
            current_rids = Vec::new();
            current_direction = session_direction;
            continue;
        }
        if line.starts_with(sdp::MEDIA) {
            flush_section(current_kind, current_mid, current_rids, current_direction);
            in_media_section = true;
            current_kind = None;
            current_mid = None;
            current_rids = Vec::new();
            current_direction = session_direction;
            continue;
        }
        if let Some(mid) = sdp_attribute_value(line, sdp::attribute::MID) {
            current_mid = Some(Mid::from(mid));
            continue;
        }
        if let Some(rid) = parse_recv_rid(line) {
            current_rids.push(rid);
            continue;
        }
        if let Some(direction) = OfferDirection::parse(line) {
            if in_media_section {
                current_direction = direction;
            } else {
                session_direction = direction;
                current_direction = direction;
            }
        }
    }
    flush_section(current_kind, current_mid, current_rids, current_direction);
    send_paths
}

fn offer_without_rid_simulcast(offer_sdp: &str) -> String {
    offer_sdp
        .split_inclusive(sdp::LF)
        .filter(|line| {
            sdp_attribute_value(line, sdp::attribute::RID).is_none()
                && sdp_attribute_value(line, sdp::attribute::SIMULCAST).is_none()
        })
        .collect()
}

fn payload_type_for_codec(rtc: &Rtc, codec_name: &CodecName) -> Option<Pt> {
    let codec = str0m_codec(codec_name)?;
    rtc.codec_config()
        .find(|params| params.spec().codec == codec)
        .map(PayloadParams::pt)
}

fn str0m_codec(codec_name: &CodecName) -> Option<Codec> {
    match codec_name {
        CodecName::Opus => Some(Codec::Opus),
        CodecName::Vp8 => Some(Codec::Vp8),
        CodecName::H264 => Some(Codec::H264),
        CodecName::Pcmu
        | CodecName::Pcma
        | CodecName::H265
        | CodecName::Vp9
        | CodecName::Av1
        | CodecName::Rtx
        | CodecName::Other(_) => None,
    }
}

fn parse_offer_media_kind(line: &str) -> Option<MediaKind> {
    let media = line.strip_prefix(sdp::MEDIA)?;
    let kind = media.split_once(sdp::SP)?.0;
    match kind {
        webrtc::media_kind::AUDIO => Some(MediaKind::Audio),
        webrtc::media_kind::VIDEO => Some(MediaKind::Video),
        _ => None,
    }
}

fn answer_with_simulcast_send_rids(
    answer_sdp: &str,
    send_paths: &BTreeMap<MediaKind, ProtocolSendPath>,
) -> String {
    send_paths
        .values()
        .filter(|send_path| !send_path.rids.is_empty())
        .fold(answer_sdp.to_owned(), |answer_sdp, send_path| {
            answer_with_mid_send_rids(&answer_sdp, send_path)
        })
}

fn answer_with_mid_send_rids(answer_sdp: &str, send_path: &ProtocolSendPath) -> String {
    // An offered recv RID is answered as a send RID and listed in the answer's
    // send simulcast description.
    // https://www.rfc-editor.org/rfc/rfc8851.html#section-6.3
    // https://www.rfc-editor.org/rfc/rfc8853.html#section-5.3.2
    let marker = format!(
        "{}{}{}{}{}",
        sdp::ATTR,
        sdp::attribute::MID,
        sdp::ATTR_SEP,
        send_path.mid,
        sdp::CRLF,
    );
    let mut replacement = marker.clone();
    for rid in &send_path.rids {
        replacement.push_str(&sdp_rid_line(rid, sdp::rid::DIRECTION_SEND));
        replacement.push_str(sdp::CRLF);
    }
    replacement.push_str(&sdp_simulcast_line(
        sdp::simulcast::DIRECTION_SEND,
        &send_path.rids,
    ));
    replacement.push_str(sdp::CRLF);
    answer_sdp.replacen(&marker, &replacement, 1)
}

fn sdp_rid_line(rid: &ProtocolRid, direction: &str) -> String {
    let mut line = format!(
        "{}{}{}{}{}{}",
        sdp::ATTR,
        sdp::attribute::RID,
        sdp::ATTR_SEP,
        rid.rid,
        sdp::SP,
        direction
    );
    if let Some(max_bitrate) = rid.max_bitrate {
        line.push(sdp::SP);
        line.push_str(sdp::rid_restriction::MAX_BITRATE);
        line.push(sdp::rid_restriction::NAME_VALUE_SEPARATOR);
        line.push_str(&max_bitrate.to_string());
    }
    line
}

fn sdp_simulcast_line(direction: &str, rids: &[ProtocolRid]) -> String {
    let separator = sdp::simulcast::STREAM_SEPARATOR.to_string();
    let rid_values = rids
        .iter()
        .map(|rid| rid.rid.to_string())
        .collect::<Vec<_>>()
        .join(&separator);
    format!(
        "{}{}{}{}{}{}",
        sdp::ATTR,
        sdp::attribute::SIMULCAST,
        sdp::ATTR_SEP,
        direction,
        sdp::SP,
        rid_values
    )
}

fn parse_recv_rid(line: &str) -> Option<ProtocolRid> {
    // Only recv RIDs describe streams that this peer may send in its answer.
    // https://www.rfc-editor.org/rfc/rfc8851.html#section-6.3
    let rest = sdp_attribute_value(line, sdp::attribute::RID)?;
    let mut parts = rest.splitn(3, sdp::SP);
    let rid = parts.next()?;
    if !sdp::rid::is_id(rid) {
        return None;
    }
    let direction = parts.next()?;
    if webrtc::RtpStreamDirection::parse(direction) != Some(webrtc::RtpStreamDirection::Recv) {
        return None;
    }
    Some(ProtocolRid {
        rid: Rid::from(rid),
        max_bitrate: parts.next().and_then(parse_max_bitrate),
    })
}

fn parse_max_bitrate(restrictions: &str) -> Option<u64> {
    restrictions
        .split(sdp::rid_restriction::PARAMETER_SEPARATOR)
        .filter_map(|restriction| {
            restriction.split_once(sdp::rid_restriction::NAME_VALUE_SEPARATOR)
        })
        .find_map(|(key, value)| {
            (key.trim() == sdp::rid_restriction::MAX_BITRATE)
                .then(|| value.trim().parse::<u64>().ok())
                .flatten()
        })
}

#[derive(Clone, Copy, Default)]
enum OfferDirection {
    #[default]
    SendRecv,
    SendOnly,
    RecvOnly,
    Inactive,
}

impl OfferDirection {
    fn parse(line: &str) -> Option<Self> {
        let direction = line.strip_prefix(sdp::ATTR)?;
        match direction {
            sdp::direction::SEND_RECV => Some(Self::SendRecv),
            sdp::direction::SEND_ONLY => Some(Self::SendOnly),
            sdp::direction::RECV_ONLY => Some(Self::RecvOnly),
            sdp::direction::INACTIVE => Some(Self::Inactive),
            _ => None,
        }
    }

    fn allows_local_send(self) -> bool {
        matches!(self, Self::RecvOnly | Self::SendRecv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rtp_read_distinguishes_timeout_from_invalid_datagram() -> Result<()> {
        let mut peer =
            super::super::require_some(FakeRtcPeer::bind(0).await, "RTC peer should bind")?;
        assert_eq!(peer.read_rtp_packet(Duration::from_millis(10)).await?, None);

        let sender = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        sender.send_to(&[0], peer.local_addr).await?;
        assert!(peer.read_rtp_packet(Duration::from_secs(1)).await.is_err());
        Ok(())
    }

    #[test]
    fn padding_bit_rejects_zero_padding_count() {
        assert_eq!(unpad_rtp_payload(&[0xaa, 0], true), None);
        assert_eq!(unpad_rtp_payload(&[0xaa, 1], true), Some([0xaa].as_slice()));
    }

    #[test]
    fn media_without_direction_defaults_to_sendrecv() {
        let offer = concat!(
            "v=0\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:video\r\n",
        );

        assert!(collect_protocol_send_paths(offer).contains_key(&MediaKind::Video));
    }

    #[test]
    fn media_inherits_session_direction() {
        let offer = concat!(
            "v=0\r\n",
            "a=inactive\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:video\r\n",
        );

        assert!(!collect_protocol_send_paths(offer).contains_key(&MediaKind::Video));
    }
}
