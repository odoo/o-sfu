//! Odoo-compatible room statistics for operator requests.

use std::sync::Arc;

use axum::{
    extract::{FromRef, State},
    response::IntoResponse,
};
use o_sfu_protocol::wire::StreamType;
use tracing::Instrument;

use super::super::contract::{IncomingBitRateStatsResponse, RoomStatsResponse, UsersStatsResponse};
use crate::{
    application::stream_catalog::counter_for_stream_type,
    runtime::{
        MediaTransport, RuntimeState,
        room::{RoomManager, RuntimeRoomStatsSnapshot},
        telemetry,
    },
};

#[derive(Debug, Clone)]
pub(super) struct StatsServices {
    room_manager: Arc<RoomManager>,
    media_transport: MediaTransport,
}

impl FromRef<RuntimeState> for StatsServices {
    fn from_ref(state: &RuntimeState) -> Self {
        Self {
            room_manager: Arc::clone(&state.room_manager),
            media_transport: state.media_transport.clone(),
        }
    }
}

/// Compatibility room statistics consumed by Odoo's SFU control plane.
pub(super) async fn rooms(State(services): State<StatsServices>) -> impl IntoResponse {
    async {
        axum::Json(
            services
                .room_manager
                .stats_snapshots(&services.media_transport)
                .await
                .into_iter()
                .map(http_room_stats)
                .collect::<Vec<_>>(),
        )
    }
    .instrument(telemetry::http_request_span("stats"))
    .await
}

fn http_room_stats(snapshot: RuntimeRoomStatsSnapshot) -> RoomStatsResponse {
    let incoming_bitrate = &snapshot.users_stats.incoming_bitrate;
    let active_stream_counts = &snapshot.users_stats.active_stream_counts;
    RoomStatsResponse {
        create_date: snapshot.create_date,
        uuid: snapshot.uuid,
        remote_address: snapshot.remote_address,
        users_stats: UsersStatsResponse {
            incoming_bit_rate: IncomingBitRateStatsResponse {
                total: incoming_bitrate.total,
                audio: counter_for_stream_type(&incoming_bitrate.by_stream, StreamType::Audio),
                camera: counter_for_stream_type(&incoming_bitrate.by_stream, StreamType::Camera),
                screen: counter_for_stream_type(&incoming_bitrate.by_stream, StreamType::Screen),
            },
            count: snapshot.users_stats.count,
            camera_count: counter_for_stream_type(active_stream_counts, StreamType::Camera),
            screen_count: counter_for_stream_type(active_stream_counts, StreamType::Screen),
        },
        web_rtc_enabled: snapshot.web_rtc_enabled,
    }
}
