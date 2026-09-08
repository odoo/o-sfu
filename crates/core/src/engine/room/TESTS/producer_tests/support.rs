pub(super) use std::{net::SocketAddr, time::Instant};

pub(super) use o_sfu_router::{
    MediaKind,
    test_support::rtp_samples::sample_video_rtp_parameters as router_sample_video_rtp_parameters,
};
pub(super) use o_sfu_telemetry::diagnostics::{
    DiagnosticsPolicyPauseReason, DiagnosticsRouteState, DiagnosticsSource,
    DiagnosticsSourceSelector, DiagnosticsUserView, DiagnosticsVideoLayoutRole,
    DiagnosticsVideoRoutePriority,
};
pub(super) use str0m::{Candidate, Rtc, change::SdpOffer, media::Mid};

pub(super) use super::super::{api::NegotiatedPublish, fixtures::*};
pub(super) use crate::{
    Bitrate, RoomMediaLimits,
    engine::{
        media_transport::{
            SessionOffer, TransportMediaId, TransportSessionKey,
            test_support::{
                test_media_transport_config, test_media_transport_deps, test_rtc_port_range,
            },
        },
        room::{PublishIntentOutcome, RemoteTrackSnapshot, Room},
    },
};

pub(super) async fn diagnostics_room_views(
    room: &Room,
    adapter: &MediaTransport,
) -> (Vec<DiagnosticsUserView>, Vec<DiagnosticsSource>) {
    let capture = room.diagnostics_detail_capture().await;
    let session_keys = capture.session_keys();
    let source_keys = capture.source_keys().cloned().collect::<Vec<_>>();
    let bitrate = adapter.transport_bitrate_snapshot(session_keys);
    let quality = adapter.transport_quality_snapshot(session_keys);
    let health = adapter.transport_health_snapshot(session_keys);
    let source_diagnostics = adapter.source_diagnostics_snapshot(&source_keys).await;
    let (_, users, sources) = capture.into_views(&bitrate, &quality, &health, &source_diagnostics);
    (users, sources)
}

pub(super) fn assert_remote_track_activity_snapshot(
    message: &UserOutbound,
    user_id: &UserId,
    stream_type: TestSourceKind,
    active: bool,
    requires_negotiation: bool,
) {
    let UserOutbound::RemoteTracks(snapshot) = message else {
        panic!("expected RemoteTracks, got {message:?}");
    };
    assert_eq!(snapshot.requires_negotiation, requires_negotiation);
    assert!(snapshot.tracks.iter().any(|projection| {
        &projection.user_id == user_id
            && projection.stream_id == stream_id_for_source(stream_type)
            && projection.producer_active == active
    }));
}

pub(super) fn remote_track_snapshot(message: &UserOutbound) -> Option<&RemoteTrackSnapshot> {
    match message {
        UserOutbound::RemoteTracks(snapshot) => Some(snapshot),
        UserOutbound::Message(_) | UserOutbound::Close(_) => None,
    }
}

pub(super) fn drain_remote_track_snapshots(
    receiver: &mut UserOutboundReceiver,
) -> Vec<RemoteTrackSnapshot> {
    drain_outbound(receiver)
        .into_iter()
        .filter_map(|message| match message {
            UserOutbound::RemoteTracks(snapshot) => Some(snapshot),
            UserOutbound::Message(_) | UserOutbound::Close(_) => None,
        })
        .collect()
}

pub(super) async fn assert_transport_media_mapping_is_missing(
    room: &Arc<Room>,
    transport_media_id: TransportMediaId,
) {
    assert!(
        room.test_api()
            .producer_stream_type_for_transport_media_id(transport_media_id)
            .await
            .is_none()
    );
}

pub(super) async fn assert_user_has_no_published_source(
    room: &Arc<Room>,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: TestSourceKind,
) {
    assert!(
        !room
            .test_api()
            .has_published_source(user_id, connection_id, stream_type)
            .await
    );
}

pub(super) async fn assert_subscription_layout(
    room: &Arc<Room>,
    adapter: &MediaTransport,
    consumer_user_id: &UserId,
    stream_type: TestSourceKind,
    expected_role: DiagnosticsVideoLayoutRole,
    expected_priority: DiagnosticsVideoRoutePriority,
) {
    let (diagnostics, _) = diagnostics_room_views(room, adapter).await;
    let Some(user) = diagnostics
        .iter()
        .find(|view| &view.user_id == consumer_user_id)
    else {
        panic!("diagnostics should include the consumer user");
    };
    assert!(
        user.subscriptions.iter().any(|subscription| {
            subscription.stream_id == stream_id_for_source(stream_type).to_string()
                && subscription.layout_role == Some(expected_role)
                && subscription.layout_priority == Some(expected_priority)
        }),
        "diagnostics should expose the subscription layout role and priority"
    );
}

pub(super) async fn assert_subscription_selected_rid(
    room: &Arc<Room>,
    adapter: &MediaTransport,
    consumer_user_id: &UserId,
    producer_user_id: &UserId,
    stream_type: TestSourceKind,
    expected_rid: &str,
) {
    let (diagnostics, _) = diagnostics_room_views(room, adapter).await;
    let Some(user) = diagnostics
        .iter()
        .find(|view| &view.user_id == consumer_user_id)
    else {
        panic!("diagnostics should include the consumer user");
    };
    assert!(
        user.subscriptions.iter().any(|subscription| {
            subscription.producer_user_id == *producer_user_id
                && subscription.stream_id == stream_id_for_source(stream_type).to_string()
                && subscription.selection.selector == DiagnosticsSourceSelector::Encoding
                && subscription.selection.selected_rid.as_deref() == Some(expected_rid)
                && subscription.selection.policy_allows_delivery
        }),
        "diagnostics should expose the selected RID for the subscription: {:?}",
        user.subscriptions
    );
}

pub(super) async fn receiver_selected_video_bitrate(
    room: &Arc<Room>,
    adapter: &MediaTransport,
    consumer_user_id: &UserId,
) -> Bitrate {
    let (diagnostics, _) = diagnostics_room_views(room, adapter).await;
    let Some(user) = diagnostics
        .iter()
        .find(|view| &view.user_id == consumer_user_id)
    else {
        panic!("diagnostics should include the consumer user");
    };
    let selected_video_bitrate_bps = user
        .subscriptions
        .iter()
        .map(|subscription| subscription.selection.selected_video_bitrate_bps)
        .max()
        .unwrap_or_default();
    Bitrate::from_bps(selected_video_bitrate_bps)
}

pub(super) async fn assert_subscription_selected_video_budget(
    room: &Arc<Room>,
    adapter: &MediaTransport,
    consumer_user_id: &UserId,
    producer_user_id: &UserId,
    stream_type: TestSourceKind,
    expected_budget: Bitrate,
) {
    let (diagnostics, _) = diagnostics_room_views(room, adapter).await;
    let Some(user) = diagnostics
        .iter()
        .find(|view| &view.user_id == consumer_user_id)
    else {
        panic!("diagnostics should include the consumer user");
    };
    assert!(
        user.subscriptions.iter().any(|subscription| {
            subscription.producer_user_id == *producer_user_id
                && subscription.stream_id == stream_id_for_source(stream_type).to_string()
                && subscription.selection.selected_video_budget_bps
                    == Some(expected_budget.as_bps())
        }),
        "diagnostics should expose the reserved video budget: {:?}",
        user.subscriptions
    );
}

pub(super) async fn assert_subscription_policy_pause_reason(
    room: &Arc<Room>,
    adapter: &MediaTransport,
    consumer_user_id: &UserId,
    producer_user_id: &UserId,
    stream_type: TestSourceKind,
    expected_reason: Option<DiagnosticsPolicyPauseReason>,
) {
    let (diagnostics, _) = diagnostics_room_views(room, adapter).await;
    let Some(user) = diagnostics
        .iter()
        .find(|view| &view.user_id == consumer_user_id)
    else {
        panic!("diagnostics should include the consumer user");
    };
    assert!(
        user.subscriptions.iter().any(|subscription| {
            let expected_state = if expected_reason.is_some() {
                DiagnosticsRouteState::Inactive
            } else {
                DiagnosticsRouteState::Active
            };
            subscription.producer_user_id == *producer_user_id
                && subscription.stream_id == stream_id_for_source(stream_type).to_string()
                && subscription.selection.policy_pause_reason == expected_reason
                && subscription.state == expected_state
        }),
        "diagnostics should expose the expected policy pause route state: {:?}",
        user.subscriptions
    );
}

/// Transport identity of one receiver's destination on a source route.
///
/// A policy pause keeps this pair stable, so comparing it across a pause and a
/// resume proves delivery came back on the already negotiated route instead of
/// through a fresh SDP exchange.
pub(super) async fn consumer_destination_identity(
    adapter: &MediaTransport,
    source_media_id: TransportMediaId,
    receiver_user_id: &UserId,
) -> (TransportMediaId, Mid) {
    let entry = adapter
        .test_api()
        .route_entry_by_media_id(source_media_id)
        .await
        .expect("source route should exist");
    let destination = entry
        .destinations
        .iter()
        .find(|destination| destination.dest_session.user_id() == receiver_user_id)
        .expect("receiver should keep its consumer destination");
    (destination.dest_transport_media_id, destination.dest_mid)
}

/// Every receiver that currently has an active transport destination for the
/// given sources, sorted and one entry per destination.
///
/// Assertions name the exact delivery set with this, so a route that stops or
/// starts being forwarded fails loudly instead of being proven by an absence.
pub(super) async fn active_destination_receivers(
    adapter: &MediaTransport,
    source_media_ids: impl IntoIterator<Item = TransportMediaId>,
) -> Vec<UserId> {
    let mut receivers = Vec::new();
    for source_media_id in source_media_ids {
        let Some(entry) = adapter
            .test_api()
            .route_entry_by_media_id(source_media_id)
            .await
        else {
            continue;
        };
        receivers.extend(
            entry
                .destinations
                .iter()
                .filter(|destination| destination.active)
                .map(|destination| destination.dest_session.user_id().clone()),
        );
    }
    receivers.sort();
    receivers
}

pub(super) async fn assert_receiver_bwe_target(
    room: &Arc<Room>,
    adapter: &MediaTransport,
    receiver_user_id: &UserId,
    expected_target: Bitrate,
) {
    let connection_id = user_connection_id(room, receiver_user_id).await;
    let session_key = room
        .transport_user_key(receiver_user_id, connection_id)
        .await;
    assert_eq!(
        adapter
            .test_api()
            .session_receiver_bwe_target(&session_key)
            .await,
        Some(expected_target)
    );
}

pub(super) struct RealRtcRefreshScenario {
    pub(super) room: Arc<Room>,
    pub(super) media_transport: MediaTransport,
    pub(super) metrics: Arc<RuntimeMetrics>,
    pub(super) publisher_user_id: UserId,
    pub(super) subscriber_user_id: UserId,
    pub(super) publisher_initial_offer: SessionOffer,
    pub(super) subscriber_session_key: TransportSessionKey,
    pub(super) publisher_rx: UserOutboundReceiver,
    pub(super) subscriber_rx: UserOutboundReceiver,
    pub(super) subscriber_remote: Rtc,
}

pub(super) async fn setup_real_rtc_refresh_scenario() -> RealRtcRefreshScenario {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room(
            "issuer-a",
            TEST_ROOM_KEY.into(),
            &RoomConfig::default(),
            None,
        )
        .await
        .expect("test room should be served");
    let (publisher_tx, publisher_rx) = test_sender();
    let (subscriber_tx, subscriber_rx) = test_sender();
    let publisher_user_id = UserId::Integer(1);
    let subscriber_user_id = UserId::Integer(2);
    let publisher_connection_id = room
        .test_api()
        .join_user(
            publisher_user_id.clone(),
            None,
            UserPermissions::default(),
            publisher_tx,
        )
        .await
        .expect("publisher should join");
    let subscriber_connection_id = room
        .test_api()
        .join_user(
            subscriber_user_id.clone(),
            None,
            UserPermissions::default(),
            subscriber_tx,
        )
        .await
        .expect("subscriber should join");
    let metrics = Arc::new(RuntimeMetrics::default());
    let media_transport = build_real_rtc_media_transport_with_metrics(Arc::clone(&metrics));
    let publisher_session_key = room
        .transport_user_key(&publisher_user_id, publisher_connection_id)
        .await;
    let subscriber_session_key = room
        .transport_user_key(&subscriber_user_id, subscriber_connection_id)
        .await;

    let publisher_initial_offer =
        bootstrap_real_rtc_user(&media_transport, &publisher_session_key).await;
    let subscriber_initial_offer =
        bootstrap_real_rtc_user(&media_transport, &subscriber_session_key).await;
    let mut subscriber_remote = build_remote_rtc(55_100);
    apply_offer_answer(
        &media_transport,
        &subscriber_session_key,
        &mut subscriber_remote,
        subscriber_initial_offer.into_parts().0,
    )
    .await;

    assert!(
        room.test_api()
            .mark_session_ready(
                &publisher_user_id,
                test_client_rtp_capabilities(),
                &media_transport,
            )
            .await
    );
    assert!(
        room.test_api()
            .mark_session_ready(
                &subscriber_user_id,
                test_client_rtp_capabilities(),
                &media_transport,
            )
            .await
    );

    RealRtcRefreshScenario {
        room,
        media_transport,
        metrics,
        publisher_user_id,
        subscriber_user_id,
        publisher_initial_offer,
        subscriber_session_key,
        publisher_rx,
        subscriber_rx,
        subscriber_remote,
    }
}

pub(super) async fn settle_refresh_offer(
    scenario: &mut RealRtcRefreshScenario,
    offer: SessionOffer,
) {
    apply_offer_answer(
        &scenario.media_transport,
        &scenario.subscriber_session_key,
        &mut scenario.subscriber_remote,
        offer.into_parts().0,
    )
    .await;

    assert!(
        scenario
            .room
            .test_api()
            .refresh_session(&scenario.subscriber_user_id, &scenario.media_transport)
            .await
    );
}
pub(super) fn build_real_rtc_media_transport() -> MediaTransport {
    build_real_rtc_media_transport_with_metrics(Arc::new(RuntimeMetrics::default()))
}

#[expect(
    clippy::panic,
    reason = "the RTC room test fixture uses a valid test configuration and should fail loudly if it stops being valid"
)]
fn build_real_rtc_media_transport_with_metrics(metrics: Arc<RuntimeMetrics>) -> MediaTransport {
    let mut deps = test_media_transport_deps();
    deps.metrics = metrics;
    MediaTransport::build(test_media_transport_config(1, test_rtc_port_range()), deps)
        .unwrap_or_else(|error| {
            panic!("constant RTC room test transport config should be valid: {error}")
        })
}

pub(super) async fn bootstrap_real_rtc_user(
    media_transport: &MediaTransport,
    session_key: &TransportSessionKey,
) -> SessionOffer {
    media_transport
        .create_initial_session_offer("test-room", session_key)
        .await
        .expect("rtc user should produce an initial offer")
}

pub(super) fn assert_remote_track_snapshot_for_stream(
    messages: &[UserOutbound],
    stream_type: TestSourceKind,
) {
    assert!(
        messages.iter().any(|message| {
            remote_track_snapshot(message).is_some_and(|snapshot| {
                snapshot.requires_negotiation
                    && snapshot
                        .tracks
                        .iter()
                        .any(|projection| projection.stream_id == stream_id_for_source(stream_type))
            })
        }),
        "expected a remote track snapshot for {stream_type:?}"
    );
}

pub(super) fn build_remote_rtc(port: u16) -> Rtc {
    let mut remote = Rtc::new(Instant::now());
    remote
        .add_local_candidate(
            Candidate::host(SocketAddr::from(([127, 0, 0, 1], port)), "udp")
                .expect("test host candidate should build"),
        )
        .expect("remote candidate should register");
    remote
}

pub(super) async fn apply_offer_answer(
    adapter: &MediaTransport,
    session_key: &TransportSessionKey,
    remote: &mut Rtc,
    offer_sdp: String,
) {
    let answer_sdp = remote_answer_sdp(remote, &offer_sdp);
    assert!(
        adapter
            .apply_session_answer(session_key, &answer_sdp)
            .await
            .is_ok()
    );
}

pub(super) fn remote_answer_sdp(remote: &mut Rtc, offer_sdp: &str) -> String {
    remote
        .sdp_api()
        .accept_offer(
            SdpOffer::from_sdp_string(offer_sdp)
                .expect("adapter should return parseable SDP offer"),
        )
        .expect("remote answer should build")
        .to_sdp_string()
}

pub(super) fn video_rtp_parameters_with_mid(mid: &str, ssrc: u32) -> MediaStream {
    router_sample_video_rtp_parameters(Some(mid), ssrc)
}
