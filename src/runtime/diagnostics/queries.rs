//! Endpoint-specific diagnostics queries over passive room captures and RTC observations.

use std::{collections::BTreeMap, slice};

use o_sfu_core::{
    MediaWorkerId,
    server::{
        room::{RoomOverviewCapture, RuntimeRoomDirectorySnapshot},
        transport::{MediaTransport, TransportHealthSnapshot, TransportSessionKey},
    },
};
use o_sfu_telemetry::diagnostics::{
    DiagnosticsRoomDetail, DiagnosticsRoomSummary, DiagnosticsSummaryResponse,
    DiagnosticsUserDetail, DiagnosticsUserSummary, DiagnosticsWorkerPressure,
    DiagnosticsWorkerSummary,
};

use crate::{application::stream_catalog::DiscussStream, runtime::room::RoomManager};

pub(crate) async fn summary_response(
    rooms: &RoomManager,
    transport: &MediaTransport,
) -> DiagnosticsSummaryResponse {
    let captures = overview_captures(rooms).await;
    let health = transport.transport_health_snapshot(&overview_session_keys(&captures));
    let mut response = DiagnosticsSummaryResponse {
        rooms_active: captures.len(),
        ..Default::default()
    };
    for (_, capture) in captures {
        let counts = capture.media_counts;
        let transport_counts = capture.transport_counts(&health);
        response.users_active = response
            .users_active
            .saturating_add(capture.session_keys.len());
        response.publications_active = response
            .publications_active
            .saturating_add(counts.publications);
        response.subscriptions_active = response
            .subscriptions_active
            .saturating_add(counts.subscriptions);
        response.recording_rooms_active = response
            .recording_rooms_active
            .saturating_add(usize::from(capture.recording_state.recording == Some(true)));
        response.transport.connected = response
            .transport
            .connected
            .saturating_add(transport_counts.connected);
        response.transport.disconnected = response
            .transport
            .disconnected
            .saturating_add(transport_counts.disconnected);
        response.transport.unknown = response
            .transport
            .unknown
            .saturating_add(transport_counts.unknown);
    }
    response.transport.total = response.users_active;
    response
}

pub(crate) async fn rooms_response(
    rooms: &RoomManager,
    transport: &MediaTransport,
) -> Vec<DiagnosticsRoomSummary> {
    let captures = overview_captures(rooms).await;
    let health = transport.transport_health_snapshot(&overview_session_keys(&captures));
    captures
        .into_iter()
        .map(|captured| room_summary(captured, &health))
        .collect()
}

pub(crate) async fn room_detail_response(
    rooms: &RoomManager,
    transport: &MediaTransport,
    room_id: &str,
) -> Option<DiagnosticsRoomDetail> {
    let entry = rooms.directory_snapshot(room_id).await?;
    let capture = entry.room.diagnostics_detail_capture().await;
    let session_keys = capture.session_keys();
    let source_keys = capture.source_keys().cloned().collect::<Vec<_>>();
    let bitrate = transport.transport_bitrate_snapshot(session_keys);
    let quality = transport.transport_quality_snapshot(session_keys);
    let health = transport.transport_health_snapshot(session_keys);
    let source_diagnostics = transport.source_diagnostics_snapshot(&source_keys).await;
    let (overview, users, sources) =
        capture.into_views(&bitrate, &quality, &health, &source_diagnostics);
    Some(DiagnosticsRoomDetail {
        summary: room_summary((entry, overview), &health),
        sources,
        users,
    })
}

pub(crate) async fn room_users_response(
    rooms: &RoomManager,
    transport: &MediaTransport,
    room_id: &str,
) -> Option<Vec<DiagnosticsUserSummary>> {
    let room = rooms.get_by_uuid(room_id).await?;
    let capture = room.diagnostics_users_capture().await;
    let session_keys = capture.session_keys().cloned().collect::<Vec<_>>();
    let bitrate = transport.transport_bitrate_snapshot(&session_keys);
    let health = transport.transport_health_snapshot(&session_keys);
    Some(capture.into_user_summaries(
        room_id,
        &bitrate,
        &health,
        DiscussStream::all().map(DiscussStream::label),
    ))
}

pub(crate) async fn workers_response(
    rooms: &RoomManager,
    transport: &MediaTransport,
) -> Vec<DiagnosticsWorkerSummary> {
    let mut captures = Vec::new();
    for entry in rooms.directory_snapshots().await {
        captures.push(entry.room.diagnostics_users_capture().await);
    }
    let session_keys = captures
        .iter()
        .flat_map(|capture| capture.session_keys().cloned())
        .collect::<Vec<_>>();
    let health = transport.transport_health_snapshot(&session_keys);
    let mut workers = BTreeMap::new();
    for snapshot in transport.worker_pressure_snapshots() {
        let id = snapshot.media_worker_id.as_usize();
        workers.insert(
            id,
            DiagnosticsWorkerSummary {
                media_worker_id: id,
                pressure: DiagnosticsWorkerPressure {
                    command_backlog_depth: snapshot.command_backlog_depth,
                    egress_bitrate_bps: snapshot.egress_bitrate.as_bps(),
                    packet_loop_delay_ms: snapshot.packet_loop_delay_ms,
                    relay_mailbox_depth: snapshot.relay_mailbox_depth,
                    worker_pressure_score: snapshot.worker_pressure_score,
                },
                ..Default::default()
            },
        );
    }
    for capture in captures {
        capture.add_to_worker_summaries(&health, &mut workers);
    }
    workers.into_values().collect()
}

pub(crate) async fn user_detail_response(
    rooms: &RoomManager,
    transport: &MediaTransport,
    room_id: &str,
    user_key: &str,
) -> Option<DiagnosticsUserDetail> {
    let room = rooms.get_by_uuid(room_id).await?;
    let capture = room.diagnostics_user_capture(user_key).await?;
    let session_keys = slice::from_ref(capture.session_key());
    let bitrate = transport.transport_bitrate_snapshot(session_keys);
    let quality = transport.transport_quality_snapshot(session_keys);
    let health = transport.transport_health_snapshot(session_keys);
    let (recording_state, user) = capture.into_view(&bitrate, &quality, &health);
    Some(DiagnosticsUserDetail {
        room_id: room.uuid().to_owned(),
        recording_state,
        user,
    })
}

async fn overview_captures(
    rooms: &RoomManager,
) -> Vec<(RuntimeRoomDirectorySnapshot, RoomOverviewCapture)> {
    let entries = rooms.directory_snapshots().await;
    let mut captures = Vec::with_capacity(entries.len());
    for entry in entries {
        let capture = entry.room.diagnostics_overview_capture().await;
        captures.push((entry, capture));
    }
    captures
}

fn overview_session_keys(
    captures: &[(RuntimeRoomDirectorySnapshot, RoomOverviewCapture)],
) -> Vec<TransportSessionKey> {
    captures
        .iter()
        .flat_map(|(_, capture)| capture.session_keys.iter().cloned())
        .collect()
}

fn room_summary(
    (entry, capture): (RuntimeRoomDirectorySnapshot, RoomOverviewCapture),
    health: &TransportHealthSnapshot,
) -> DiagnosticsRoomSummary {
    let counts = capture.media_counts;
    let transport = capture.transport_counts(health);
    DiagnosticsRoomSummary {
        create_date: entry.create_date,
        media_worker_id: capture
            .primary_media_worker_id
            .map_or(0, MediaWorkerId::as_usize),
        publication_count: counts.publications,
        recording_state: capture.recording_state,
        remote_address: entry.remote_address,
        source_count: counts.publications,
        user_count: capture.session_keys.len(),
        subscription_count: counts.subscriptions,
        transport,
        uuid: entry.room.uuid().to_owned(),
        web_rtc_enabled: entry.room.web_rtc_enabled(),
    }
}
