use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU16, Ordering},
};

use futures_util::SinkExt;
use o_sfu_protocol::{
    host::{Command, NegotiationKind, PendingRequest, ProtocolCore, project_protocol_event},
    wire::RecordingOptions,
};
use tokio_tungstenite::{connect_async, tungstenite};

use super::{
    AvailableFeatures, BATCH_FLUSH_DELAY_MS, BTreeMap, BundleStateChange, BundleUpdate, Duration,
    ProtocolDownloadStates, ProtocolSessionId, ProtocolSessionInfo, ProtocolStreamType,
    RecordingState, RequestId, TestWebSocket, read_text_message,
    rtc::{ProtocolHarnessRtcPeer, ProtocolHarnessRtcPeerFactory, default_protocol_harness_rtc},
};

static NEXT_PROTOCOL_HARNESS_PORT: AtomicU16 = AtomicU16::new(59_000);

#[derive(Debug, Clone)]
pub(crate) struct PendingHarnessNegotiation {
    request_id: RequestId,
    kind: NegotiationKind,
    sdp: String,
}

pub(crate) struct ProtocolHarnessPeer {
    pub(crate) core: ProtocolCore,
    pub(crate) pending_request_starts: Vec<PendingRequest>,
    pub(crate) pending_request_resolutions: Vec<(RequestId, bool)>,
    pub(crate) pending_negotiations: VecDeque<PendingHarnessNegotiation>,
    pub(crate) available_features: AvailableFeatures,
    pub(crate) recording_state: RecordingState,
    rtc_peer_factory: ProtocolHarnessRtcPeerFactory,
    rtc_peer: Option<ProtocolHarnessRtcPeer>,
    pub(crate) state_changes: Vec<BundleStateChange>,
    pub(crate) timers: BTreeMap<u32, u32>,
    pub(crate) updates: Vec<BundleUpdate>,
    pub(crate) websocket: Option<TestWebSocket>,
    pub(crate) auto_answer_negotiation: bool,
}

impl Default for ProtocolHarnessPeer {
    fn default() -> Self {
        let rtc_peer_factory = ProtocolHarnessRtcPeerFactory::new(
            NEXT_PROTOCOL_HARNESS_PORT.fetch_add(1, Ordering::Relaxed),
            default_protocol_harness_rtc,
        );
        Self {
            core: ProtocolCore::default(),
            pending_request_starts: Vec::new(),
            pending_request_resolutions: Vec::new(),
            pending_negotiations: VecDeque::new(),
            available_features: AvailableFeatures {
                rtc: false,
                transcription: false,
                audio_recording: false,
                video_recording: false,
            },
            recording_state: RecordingState::default(),
            rtc_peer_factory,
            rtc_peer: rtc_peer_factory.build_peer(),
            state_changes: Vec::new(),
            timers: BTreeMap::new(),
            updates: Vec::new(),
            websocket: None,
            auto_answer_negotiation: true,
        }
    }
}

impl ProtocolHarnessPeer {
    pub(crate) fn with_real_rtc_negotiation(port: u16) -> Option<Self> {
        Self::with_custom_rtc_negotiation(port, default_protocol_harness_rtc)
    }

    pub(crate) fn with_custom_rtc_negotiation(
        port: u16,
        build_rtc: fn() -> str0m::Rtc,
    ) -> Option<Self> {
        let rtc_peer_factory = ProtocolHarnessRtcPeerFactory::new(port, build_rtc);
        Some(Self {
            rtc_peer_factory,
            rtc_peer: Some(rtc_peer_factory.build_peer()?),
            ..Self::default()
        })
    }

    pub(crate) async fn connect(
        &mut self,
        url: &str,
        jwt: &str,
        room: Option<String>,
    ) -> Option<()> {
        let commands = self.core.connect(url.to_owned(), jwt.to_owned(), room);
        self.run_commands(commands).await
    }

    pub(crate) async fn read_server_frame(&mut self) -> Option<()> {
        let payload = super::timeout(
            Duration::from_secs(1),
            read_text_message(self.websocket.as_mut()?),
        )
        .await
        .ok()??;
        let commands = self.core.on_ws_message(&payload);
        self.run_commands(commands).await
    }

    pub(crate) async fn observe_close(&mut self, code: u16) -> Option<()> {
        let commands = self.core.on_ws_close(code);
        self.run_commands(commands).await
    }

    pub(crate) async fn connect_and_finish_handshake(
        &mut self,
        url: &str,
        jwt: &str,
        room: Option<String>,
    ) -> Option<()> {
        self.connect(url, jwt, room).await?;
        self.read_server_frame().await?;
        self.read_server_frame().await?;
        Some(())
    }

    pub(crate) async fn broadcast(&mut self, message: serde_json::Value) -> Option<()> {
        let commands = self.core.broadcast(message);
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    pub(crate) async fn update_info(&mut self, info: ProtocolSessionInfo) -> Option<()> {
        let commands = self.core.update_info(info);
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    pub(crate) async fn publish(
        &mut self,
        stream_type: ProtocolStreamType,
        active: bool,
    ) -> Option<()> {
        let commands = self.core.publish(stream_type, active);
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    pub(crate) async fn subscribe(
        &mut self,
        user_id: ProtocolSessionId,
        states: ProtocolDownloadStates,
    ) -> Option<()> {
        let commands = self.core.subscribe(user_id, states);
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    pub(crate) async fn start_recording(
        &mut self,
        audio: Option<bool>,
        video: Option<bool>,
        transcription: Option<bool>,
    ) -> Option<()> {
        let commands = self.core.start_recording(RecordingOptions {
            audio,
            video,
            transcription,
        });
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    pub(crate) async fn stop_recording(&mut self) -> Option<()> {
        let commands = self.core.stop_recording();
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    pub(crate) async fn flush_timers_with_delay(&mut self, delay_ms: u32) -> Option<()> {
        let timer_ids = self
            .timers
            .iter()
            .filter_map(|(id, ms)| (*ms == delay_ms).then_some(*id))
            .collect::<Vec<_>>();
        for timer_id in timer_ids {
            let commands = self.core.on_timer(timer_id);
            self.run_commands(commands).await?;
        }
        Some(())
    }

    pub(crate) async fn answer_next_negotiation(&mut self) -> Option<()> {
        let pending = self.pending_negotiations.pop_front()?;
        let answer_sdp = match self.rtc_peer.as_mut() {
            Some(rtc_peer) => rtc_peer.answer_offer(&pending.sdp)?,
            None => String::from("v=0\r\ns=protocol-core-answer\r\n"),
        };
        let commands =
            self.core
                .submit_negotiation_answer(&pending.request_id, pending.kind, &answer_sdp);
        let mut commands = commands;
        commands.extend(self.core.on_transport_ready());
        self.run_commands(commands).await
    }

    pub(crate) async fn run_commands(&mut self, commands: Vec<Command>) -> Option<()> {
        let mut pending: VecDeque<_> = commands.into();
        while let Some(command) = pending.pop_front() {
            let follow_up = match command {
                Command::Connect { url } => {
                    let websocket = connect_async(url).await.ok()?;
                    self.websocket = Some(websocket.0);
                    self.core.on_ws_open()
                }
                Command::SendWebSocket { frame } => {
                    self.websocket
                        .as_mut()?
                        .send(tungstenite::Message::Text(frame.into()))
                        .await
                        .ok()?;
                    Vec::new()
                }
                Command::EmitStateChange { state, cause } => {
                    self.state_changes.push(BundleStateChange { state, cause });
                    Vec::new()
                }
                Command::SetAvailableFeatures { features } => {
                    self.available_features = features;
                    Vec::new()
                }
                Command::SetRecordingState { state } => {
                    self.recording_state = state;
                    Vec::new()
                }
                Command::EmitEvent { event } => {
                    self.updates.push(project_protocol_event(event));
                    Vec::new()
                }
                Command::BeginPendingRequest { request } => {
                    self.timers
                        .insert(request.timeout_timer_id, request.timeout_ms);
                    self.pending_request_starts.push(request);
                    Vec::new()
                }
                Command::ClosePeerConnection => {
                    self.rtc_peer = None;
                    Vec::new()
                }
                Command::ApplyNegotiation {
                    request_id,
                    kind,
                    sdp,
                    upload_slots: _,
                } => {
                    if kind == NegotiationKind::Offer {
                        self.rtc_peer = self.rtc_peer_factory.build_peer();
                        let _ = self.rtc_peer.as_ref()?;
                    }
                    self.handle_negotiation_command(request_id, kind, sdp)?
                }
                Command::ScheduleTimer { id, ms } => {
                    self.timers.insert(id, ms);
                    Vec::new()
                }
                Command::CancelTimer { id } => {
                    self.timers.remove(&id);
                    Vec::new()
                }
                Command::CloseWebSocket { .. } => {
                    self.websocket.as_mut()?.close(None).await.ok()?;
                    Vec::new()
                }
                Command::CompletePendingRequest {
                    request_id,
                    timeout_timer_id,
                    ok,
                } => {
                    self.timers.remove(&timeout_timer_id);
                    self.pending_request_resolutions.push((request_id, ok));
                    Vec::new()
                }
            };
            pending.extend(follow_up);
        }
        Some(())
    }

    fn handle_negotiation_command(
        &mut self,
        request_id: RequestId,
        kind: NegotiationKind,
        sdp: String,
    ) -> Option<Vec<Command>> {
        if self.auto_answer_negotiation {
            let answer_sdp = match self.rtc_peer.as_mut() {
                Some(rtc_peer) => rtc_peer.answer_offer(&sdp)?,
                None => String::from("v=0\r\ns=protocol-core-answer\r\n"),
            };
            let mut follow_up = self
                .core
                .submit_negotiation_answer(&request_id, kind, &answer_sdp);
            follow_up.extend(self.core.on_transport_ready());
            return Some(follow_up);
        }
        self.pending_negotiations
            .push_back(PendingHarnessNegotiation {
                request_id,
                kind,
                sdp,
            });
        Some(Vec::new())
    }
}
