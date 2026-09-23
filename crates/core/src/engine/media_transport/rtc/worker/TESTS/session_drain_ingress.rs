use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use o_sfu_router::rtp::{MediaStream, StreamBinding};
use str0m::{
    Input, Rtc,
    format::{Codec, PayloadParams},
    media::{MediaKind, Mid, Pt, Rid},
    rtp::{RtpHeader, RtpWrite, Ssrc},
};

use super::{
    PacketLoopBuffers, SessionDrainContext, drain_ready_sessions,
    peer::{
        TestDatagram, connect_rtc_pair, drain_mutation, take_rtcp, take_written_rtp_with_header,
    },
};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags,
    engine::{
        UserId,
        media_transport::{
            SourcePolicySignal, TransportMediaId, TransportSessionKey,
            rtc::{
                bootstrap::test_support::ensure_session_rtc_state,
                codec::RtpProfile,
                packet_loop::{
                    ForwardingEffects, PacketForwarder, forwarded_packet::ForwardedPacket,
                },
                state::{
                    PacketLoopState, RtcSnapshotState,
                    bitrate::BitrateRegistry,
                    media_registry::{ProducerStreamBinding, RegisteredMediaHandle},
                },
                test_support::test_transport_session_key,
                worker::TESTS::CountingSink,
            },
        },
        metrics::{
            RtcMetricsRecorder, RtpForwardDestinationKind, RtpMetricsRecorder, RuntimeMetrics,
        },
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

struct ProducerIngressFixture {
    state: PacketLoopState,
    peer: Rtc,
    session: TransportSessionKey,
    media: TransportMediaId,
    mid: Mid,
    rid: Rid,
    payload_type: Pt,
    receiver_report_interval: Duration,
    now: Instant,
    metrics: RuntimeMetrics,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    rtp_metrics: Arc<RtpMetricsRecorder>,
    packet_sinks: RoomPacketSinkRegistry,
    sink: Arc<CountingSink>,
    forwarder: PacketForwarder,
}

impl ProducerIngressFixture {
    fn new(primary: Ssrc) -> Result<Self, &'static str> {
        let session = test_transport_session_key(88, 0, 89, UserId::Integer(90));
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 46_020));
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 46_021));
        let mut state = PacketLoopState::default();
        ensure_session_rtc_state(
            &mut state.users,
            &session,
            candidate_addr,
            Bitrate::from_mbps(10),
        )
        .map_err(|_error| "producer RTC state should initialize")?;
        let profile = RtpProfile::compile(MediaCodecFlags::default(), CodecPreferences::default())
            .map_err(|_error| "producer RTP profile should compile")?;
        let started_at = Instant::now();
        let config = profile.session_config().enable_raw_packets(true);
        let receiver_report_interval = config.rtcp_report_interval_audio();
        let mut peer = config.build(started_at);
        let server = &mut state
            .users
            .get_mut(&session)
            .ok_or("producer RTC should exist")?
            .rtc;
        let now = connect_rtc_pair(
            server,
            &mut peer,
            candidate_addr,
            peer_addr,
            started_at + Duration::from_secs(1),
        )?;
        let payload_type = server
            .codec_config()
            .find(|params| params.spec().codec == Codec::Vp8)
            .map(PayloadParams::pt)
            .ok_or("producer VP8 payload type should exist")?;
        let mid = Mid::from("camera");
        let rid = Rid::from("hi");
        server
            .direct_api()
            .declare_media(mid, MediaKind::Video)
            .expect_rid_rx(rid);
        drain_mutation(server)?;
        {
            let mut api = peer.direct_api();
            api.declare_media(mid, MediaKind::Video);
            api.declare_stream_tx(primary, None, mid, Some(rid))
                .set_unpaced(true);
        }
        drain_mutation(&mut peer)?;
        let media = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session.clone(),
            mid,
        });
        state.refresh_producer_ssrcs(
            &session,
            mid,
            &MediaStream::new(vec![], vec![], vec![StreamBinding::new().with_rid("hi")]),
        );
        let metrics = RuntimeMetrics::default();
        let packet_sinks = RoomPacketSinkRegistry::default();
        let sink = Arc::new(CountingSink::new());
        packet_sinks.register_room(
            session.room_instance_id(),
            Arc::<CountingSink>::clone(&sink),
            RtpForwardDestinationKind::Recording,
        );
        Ok(Self {
            state,
            peer,
            session,
            media,
            mid,
            rid,
            payload_type,
            receiver_report_interval,
            now,
            rtc_metrics: metrics.register_rtc_worker(),
            rtp_metrics: metrics.register_rtp_worker(),
            metrics,
            packet_sinks,
            sink,
            forwarder: PacketForwarder::default(),
        })
    }

    fn write(
        &mut self,
        primary: Ssrc,
        sequence_number: u16,
        payload: &[u8],
    ) -> Result<(TestDatagram, RtpHeader), &'static str> {
        self.now += Duration::from_millis(20);
        {
            let mut api = self.peer.direct_api();
            let stream = api
                .stream_tx_by_mid(self.mid, Some(self.rid))
                .ok_or("producer transmit stream should exist")?;
            assert_eq!(stream.ssrc(), primary);
            stream.write_rtp(RtpWrite::new(
                self.payload_type,
                u64::from(sequence_number).into(),
                u32::from(sequence_number) * 3_000,
                self.now,
                payload.to_vec(),
            ));
        }
        take_written_rtp_with_header(&mut self.peer, self.now, primary, sequence_number)
    }

    fn predeclare_source_without_rid(&mut self, primary: Ssrc) -> Result<(), &'static str> {
        let mut api = self
            .state
            .users
            .get_mut(&self.session)
            .ok_or("producer RTC should retain its declared source")?
            .rtc
            .direct_api();
        api.expect_stream_rx(primary, None, self.mid, None);
        Ok(())
    }

    fn restart(&mut self, primary: Ssrc) -> Result<(), &'static str> {
        self.peer
            .direct_api()
            .reset_stream_tx(self.mid, Some(self.rid), primary, None)
            .ok_or("producer stream should restart with a new SSRC")?
            .set_unpaced(true);
        drain_mutation(&mut self.peer)
    }

    fn forward(&mut self, datagram: &TestDatagram) -> Result<Vec<ForwardedPacket>, &'static str> {
        let server = &mut self
            .state
            .users
            .get_mut(&self.session)
            .ok_or("producer RTC should remain registered")?
            .rtc;
        datagram.deliver(server, self.now)?;
        self.state.mark_session_dirty(&self.session);
        let bitrate_registry = Arc::new(Mutex::new(BitrateRegistry::default()));
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let source_policy_signal = SourcePolicySignal::default();
        let context = SessionDrainContext::new(
            &snapshot_state,
            &bitrate_registry,
            &self.metrics,
            &self.rtc_metrics,
            &source_policy_signal,
        );
        let mut buffers = PacketLoopBuffers::new();
        assert!(!drain_ready_sessions(
            &mut self.state,
            &context,
            &mut buffers,
            self.now,
        ));
        assert_eq!(buffers.pending_packets.len(), 1);
        self.forwarder.forward_batch(
            &mut self.state,
            &mut buffers.pending_packets,
            &ForwardingEffects {
                packet_sinks: &self.packet_sinks,
                source_policy_signal: &source_policy_signal,
                metrics: &self.metrics,
                rtp_metrics: &self.rtp_metrics,
                rtc_metrics: &self.rtc_metrics,
            },
        );
        Ok(buffers.pending_packets)
    }

    fn acknowledge_source(&mut self) -> Result<(), &'static str> {
        // str0m selects the audio report interval for every stream without RTX,
        // including this VP8 fixture. Advancing the configured interval keeps
        // the authenticated report deterministic without changing production.
        self.now += self.receiver_report_interval;
        let server = &mut self
            .state
            .users
            .get_mut(&self.session)
            .ok_or("producer RTC should remain registered for receiver report")?
            .rtc;
        server
            .handle_input(Input::Timeout(self.now))
            .map_err(|_error| "receiver report timeout should apply")?;
        let reports = take_rtcp(server, self.now)?;
        assert!(!reports.is_empty());
        for report in reports {
            report.deliver(&mut self.peer, self.now)?;
            drain_mutation(&mut self.peer)?;
        }
        Ok(())
    }

    fn assert_binding(&self, primary: Ssrc) {
        assert_eq!(
            self.state.src_media_for_ssrc(&self.session, primary),
            Some(self.media)
        );
        assert_eq!(
            self.state.source_rid_for_ssrc(&self.session, primary),
            Some(self.rid)
        );
        assert_eq!(
            self.state.routes.producer_ssrcs(self.media),
            Some([primary].as_slice())
        );
    }
}

#[test]
fn authenticated_producer_packets_keep_binding_after_mid_and_rid_are_omitted()
-> Result<(), &'static str> {
    let primary = Ssrc::from(4_321);
    let mut fixture = ProducerIngressFixture::new(primary)?;
    let (initial, _) = fixture.write(primary, 100, b"initial")?;
    fixture.forward(&initial)?;
    fixture.assert_binding(primary);
    // str0m omits MID/RID only after an authenticated receiver report confirms
    // that the receiver has associated the SSRC with its negotiated encoding.
    fixture.acknowledge_source()?;
    let (without_extensions, header) = fixture.write(primary, 101, b"without-extensions")?;
    assert_eq!(header.ext_vals.mid, None);
    assert_eq!(header.ext_vals.rid, None);
    let mut packets = fixture.forward(&without_extensions)?;
    let packet = packets
        .first_mut()
        .ok_or("authenticated packet should be staged")?;
    assert_eq!(
        packet
            .resolve_facts(&fixture.state)
            .and_then(|facts| facts.rid),
        Some(fixture.rid)
    );
    assert_eq!(
        packet.source_binding(),
        Some(ProducerStreamBinding {
            rid: Some(fixture.rid),
            primary,
            repair: None,
        })
    );
    let mut relay = packet
        .share_for_relay(&fixture.state, fixture.media)
        .ok_or("authenticated packet should retain its relay identity")?;
    assert!(relay.source_binding().is_none());
    assert_eq!(
        relay
            .resolve_facts(&PacketLoopState::default())
            .map(|facts| (facts.src_media, facts.rid)),
        Some((fixture.media, Some(fixture.rid))),
    );
    fixture.assert_binding(primary);
    assert_eq!(fixture.sink.packet_count(), 2);
    assert_eq!(
        fixture.sink.last_packet().1.as_slice(),
        b"without-extensions"
    );
    Ok(())
}

#[test]
fn authenticated_rid_packet_preserves_predeclared_ridless_ssrc() -> Result<(), &'static str> {
    let primary = Ssrc::from(4_321);
    let mut fixture = ProducerIngressFixture::new(primary)?;
    // A worker publication intent can contain an SSRC-only binding alongside
    // lo/hi bindings. Answer application restores that pending RID-less stream
    // before projecting the accepted RID slots, even when their SSRCs are not
    // signaled. Room publication uses empty intents instead of this lower-level
    // worker API configuration.
    fixture.predeclare_source_without_rid(primary)?;
    let (initial, header) = fixture.write(primary, 100, b"initial-rid")?;
    assert_eq!(header.ext_vals.rid, Some(fixture.rid));
    fixture.forward(&initial)?;
    fixture.assert_binding(primary);
    assert_eq!(fixture.sink.packet_count(), 1);
    fixture.acknowledge_source()?;
    let (without_extensions, header) = fixture.write(primary, 101, b"without-extensions")?;
    assert_eq!(header.ext_vals.mid, None);
    assert_eq!(header.ext_vals.rid, None);
    fixture.forward(&without_extensions)?;
    fixture.assert_binding(primary);
    assert_eq!(fixture.sink.packet_count(), 2);
    assert_eq!(
        fixture.sink.last_packet().1.as_slice(),
        b"without-extensions"
    );
    Ok(())
}

#[test]
fn authenticated_unoffered_rid_cannot_claim_predeclared_ridless_ssrc() -> Result<(), &'static str> {
    let primary = Ssrc::from(4_321);
    let mut fixture = ProducerIngressFixture::new(primary)?;
    fixture.predeclare_source_without_rid(primary)?;
    fixture.rid = Rid::from("unknown");
    {
        let mut api = fixture.peer.direct_api();
        api.remove_stream_tx(primary);
        api.declare_stream_tx(primary, None, fixture.mid, Some(fixture.rid))
            .set_unpaced(true);
    }
    let (packet, header) = fixture.write(primary, 100, b"unknown-rid")?;
    assert_eq!(header.ext_vals.rid, Some(fixture.rid));
    fixture.forward(&packet)?;
    assert_eq!(fixture.sink.packet_count(), 0);
    assert_eq!(
        fixture.state.src_media_for_ssrc(&fixture.session, primary),
        None
    );
    Ok(())
}

#[test]
fn authenticated_packets_follow_negotiated_ridless_ssrc_after_downgrade() -> Result<(), &'static str>
{
    let primary = Ssrc::from(4_321);
    let mut fixture = ProducerIngressFixture::new(primary)?;
    let (initial, _) = fixture.write(primary, 100, b"initial-rid")?;
    fixture.forward(&initial)?;
    fixture.assert_binding(primary);
    fixture.acknowledge_source()?;
    // An accepted answer without simulcast projects its sole signaled SSRC as
    // RID-less. str0m's expect_stream_rx preserves the prior exact stream RID.
    fixture.predeclare_source_without_rid(primary)?;
    fixture.state.refresh_producer_ssrcs(
        &fixture.session,
        fixture.mid,
        &MediaStream::new(
            vec![],
            vec![],
            vec![StreamBinding::new().with_ssrc(*primary)],
        ),
    );
    let (without_extensions, header) = fixture.write(primary, 101, b"ridless")?;
    assert_eq!(header.ext_vals.mid, None);
    assert_eq!(header.ext_vals.rid, None);
    let packets = fixture.forward(&without_extensions)?;
    assert_eq!(
        packets
            .first()
            .and_then(ForwardedPacket::cached_facts)
            .map(|facts| facts.rid),
        Some(None)
    );
    assert_eq!(
        packets.first().and_then(ForwardedPacket::source_binding),
        Some(ProducerStreamBinding {
            rid: None,
            primary,
            repair: None,
        })
    );
    assert_eq!(
        fixture.state.src_media_for_ssrc(&fixture.session, primary),
        Some(fixture.media)
    );
    assert_eq!(
        fixture.state.source_rid_for_ssrc(&fixture.session, primary),
        None
    );
    assert_eq!(fixture.sink.packet_count(), 2);
    assert_eq!(fixture.sink.last_packet().1.as_slice(), b"ridless");
    Ok(())
}

#[test]
fn delayed_authenticated_previous_ssrc_cannot_replace_restarted_producer()
-> Result<(), &'static str> {
    let previous = Ssrc::from(4_321);
    let current = Ssrc::from(4_322);
    let mut fixture = ProducerIngressFixture::new(previous)?;
    let (initial, _) = fixture.write(previous, 100, b"initial")?;
    fixture.forward(&initial)?;
    let (delayed, _) = fixture.write(previous, 101, b"delayed-previous")?;
    fixture.restart(current)?;
    let (restarted, _) = fixture.write(current, 200, b"restarted")?;
    fixture.forward(&restarted)?;
    fixture.assert_binding(current);
    assert_eq!(
        fixture.state.src_media_for_ssrc(&fixture.session, previous),
        None
    );
    assert_eq!(fixture.sink.packet_count(), 2);
    fixture.forward(&delayed)?;
    fixture.assert_binding(current);
    assert_eq!(fixture.sink.packet_count(), 2);
    assert_eq!(fixture.sink.last_packet().1.as_slice(), b"restarted");
    let mut api = fixture
        .state
        .users
        .get_mut(&fixture.session)
        .ok_or("producer RTC should survive the delayed packet")?
        .rtc
        .direct_api();
    assert!(api.stream_rx(&previous).is_none());
    assert!(api.stream_rx(&current).is_some());
    let (continuing, _) = fixture.write(current, 201, b"continuing")?;
    fixture.forward(&continuing)?;
    fixture.assert_binding(current);
    assert_eq!(fixture.sink.packet_count(), 3);
    assert_eq!(fixture.sink.last_packet().1.as_slice(), b"continuing");
    Ok(())
}
