use o_sfu_model::UserId;

use crate::diagnostics::types::{
    DiagnosticsRoomDetail, DiagnosticsRouteState, DiagnosticsSource, DiagnosticsSubscription,
    DiagnosticsTransportHealth, DiagnosticsUserView,
};

const STREAM_COLOR_PALETTE: &[&str] =
    &["blue", "orange", "purple", "green", "yellow", "red", "gray"];

pub(super) fn stream_id_label(stream_id: &str) -> &str {
    if stream_id.is_empty() {
        "source"
    } else {
        stream_id
    }
}

pub(super) fn stream_id_color(stream_id: &str) -> &'static str {
    let hash = stream_id.as_bytes().iter().fold(0_usize, |acc, byte| {
        acc.wrapping_mul(31).wrapping_add(usize::from(*byte))
    });
    STREAM_COLOR_PALETTE
        .get(hash % STREAM_COLOR_PALETTE.len())
        .copied()
        .unwrap_or("gray")
}

pub(super) fn transport_health_label(health: Option<&DiagnosticsTransportHealth>) -> &'static str {
    match health {
        Some(DiagnosticsTransportHealth::Connected) => "connected",
        Some(DiagnosticsTransportHealth::Disconnected) => "disconnected",
        None => "unknown",
    }
}

pub(super) fn route_state_label(state: &DiagnosticsRouteState) -> &'static str {
    match state {
        DiagnosticsRouteState::Active => "active",
        DiagnosticsRouteState::Inactive => "inactive",
        DiagnosticsRouteState::Pending => "pending",
    }
}

pub(super) fn route_state_color(state: &DiagnosticsRouteState) -> &'static str {
    match state {
        DiagnosticsRouteState::Active => "green",
        DiagnosticsRouteState::Inactive => "gray",
        DiagnosticsRouteState::Pending => "yellow",
    }
}

pub(super) fn download_main_stat(sub: &DiagnosticsSubscription) -> String {
    let stream_id = stream_id_label(&sub.stream_id);
    match sub.selection.selected_rid.as_deref() {
        Some(rid) if !rid.is_empty() => format!("{stream_id} {rid}"),
        _ => stream_id.to_string(),
    }
}

pub(super) fn source_by_id(
    detail: &DiagnosticsRoomDetail,
    source_id: u64,
) -> Option<&DiagnosticsSource> {
    detail
        .sources
        .iter()
        .find(|source| source.source_id == source_id)
}

pub(super) fn user_by_id<'a>(
    detail: &'a DiagnosticsRoomDetail,
    user_id: &UserId,
) -> Option<&'a DiagnosticsUserView> {
    detail.users.iter().find(|user| user.user_id == *user_id)
}
