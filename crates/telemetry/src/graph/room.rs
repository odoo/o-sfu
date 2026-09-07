use std::collections::{HashMap, HashSet};

use serde_json::{Value, json};

use super::common::{
    download_main_stat, route_state_color, route_state_label, stream_id_color, stream_id_label,
    transport_health_label,
};
use crate::diagnostics::types::{
    DiagnosticsIncomingBitrate, DiagnosticsRoomDetail, DiagnosticsRouteState, DiagnosticsSource,
    DiagnosticsSubscription, DiagnosticsUserView,
};

const ARC_RATIO_SCALE: u32 = 1_000_000;

fn room_node(detail: &DiagnosticsRoomDetail) -> Value {
    let room_uuid = detail.summary.uuid.as_str();
    let short_uuid = if room_uuid.len() > 8 {
        &room_uuid[..8]
    } else {
        room_uuid
    };

    json!({
        "id": format!("room:{}", room_uuid),
        "title": short_uuid,
        "subtitle": "room",
        "mainStat": format!("{} sessions", detail.summary.user_count),
        "secondaryStat": format!("{} pub / {} sub", detail.summary.publication_count, detail.summary.subscription_count),
        "detail__recording": format!("{:?}", detail.summary.recording_state.recording),
        "detail__worker": detail.summary.media_worker_id,
        "detail__transport": format!("{} conn / {} disc / {} unk", detail.summary.transport.connected, detail.summary.transport.disconnected, detail.summary.transport.unknown),
    })
}

fn source_ids(detail: &DiagnosticsRoomDetail) -> HashSet<u64> {
    detail
        .sources
        .iter()
        .map(|source| source.source_id)
        .collect()
}

fn download_counts(detail: &DiagnosticsRoomDetail) -> HashMap<u64, usize> {
    let mut download_counts: HashMap<u64, usize> = HashMap::new();
    for user in &detail.users {
        for sub in &user.subscriptions {
            *download_counts.entry(sub.source_id).or_insert(0) += 1;
        }
    }
    download_counts
}

fn bitrate_share(part: u64, total: u64) -> Option<f64> {
    if total == 0 {
        return None;
    }

    let scaled = (u128::from(part.min(total)) * u128::from(ARC_RATIO_SCALE)) / u128::from(total);
    let scaled = u32::try_from(scaled).map_or(ARC_RATIO_SCALE, |value| value);
    Some(f64::from(scaled) / f64::from(ARC_RATIO_SCALE))
}

fn add_bitrate_arcs(node: &mut Value, bitrate: &DiagnosticsIncomingBitrate) {
    if let Some(obj) = node.as_object_mut() {
        for (stream_id, bps) in &bitrate.by_stream_bps {
            if let Some(share) = bitrate_share(*bps, bitrate.total) {
                let field_name = stream_id
                    .chars()
                    .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
                    .collect::<String>();
                obj.insert(format!("arc__stream_{field_name}"), json!(share));
            }
        }
    }
}

fn session_node(room_uuid: &str, user: &DiagnosticsUserView) -> Value {
    let session_id = user.user_id.path_segment();
    let health = transport_health_label(user.transport.health.as_ref());
    let bitrate = &user.transport.quality_summary.current_incoming_bitrate;
    let mut node = json!({
        "id": format!("session:{}:{}", room_uuid, session_id),
        "title": session_id,
        "subtitle": health,
        "mainStat": format!("{} bps", bitrate.total),
        "secondaryStat": format!("{} pub / {} sub", user.publications.len(), user.subscriptions.len()),
        "detail__connection": user.transport.connection_id.to_string(),
        "detail__worker": user.transport.media_worker_id,
    });

    add_bitrate_arcs(&mut node, bitrate);
    node
}

fn push_session_entries(
    nodes: &mut Vec<Value>,
    edges: &mut Vec<Value>,
    detail: &DiagnosticsRoomDetail,
) {
    let room_uuid = detail.summary.uuid.as_str();
    for user in &detail.users {
        let session_id = user.user_id.path_segment();
        let health = transport_health_label(user.transport.health.as_ref());
        let edge_color = match health {
            "connected" => "green",
            "disconnected" => "red",
            _ => "gray",
        };
        nodes.push(session_node(room_uuid, user));
        edges.push(json!({
            "id": format!("member:{}:{}", room_uuid, session_id),
            "source": format!("room:{}", room_uuid),
            "target": format!("session:{}:{}", room_uuid, session_id),
            "mainStat": health,
            "color": edge_color,
        }));
    }
}

fn encoding_detail_lists(source: &DiagnosticsSource) -> (String, String, String, String, String) {
    let enc_ids: Vec<String> = source
        .encodings
        .iter()
        .map(|e| e.encoding_id.to_string())
        .collect();
    let rids: Vec<String> = source
        .encodings
        .iter()
        .filter_map(|e| e.rid.clone())
        .collect();
    let max_bitrates: Vec<String> = source
        .encodings
        .iter()
        .filter_map(|e| e.max_bitrate_bps.map(|b| b.to_string()))
        .collect();
    let primary_ssrcs: Vec<String> = source
        .encodings
        .iter()
        .filter_map(|e| e.primary_ssrc.map(|s| s.to_string()))
        .collect();
    let repair_ssrcs: Vec<String> = source
        .encodings
        .iter()
        .filter_map(|e| e.repair_ssrc.map(|s| s.to_string()))
        .collect();

    (
        enc_ids.join(", "),
        rids.join(", "),
        max_bitrates.join(", "),
        primary_ssrcs.join(", "),
        repair_ssrcs.join(", "),
    )
}

fn push_source_entries(
    nodes: &mut Vec<Value>,
    edges: &mut Vec<Value>,
    detail: &DiagnosticsRoomDetail,
    download_counts: &HashMap<u64, usize>,
) {
    let room_uuid = detail.summary.uuid.as_str();
    for source in &detail.sources {
        let stream_id = stream_id_label(&source.stream_id);
        let media_kind_str = format!("{:?}", source.media_kind).to_lowercase();
        let active_str = if source.active { "active" } else { "inactive" };
        let downloads = download_counts.get(&source.source_id).copied().unwrap_or(0);
        let (enc_ids, rids, max_bitrates, primary_ssrcs, repair_ssrcs) =
            encoding_detail_lists(source);

        nodes.push(json!({
            "id": format!("source:{}:{}", room_uuid, source.source_id),
            "title": format!("{} #{}", stream_id, source.source_id),
            "subtitle": format!("{} {}", active_str, media_kind_str),
            "mainStat": format!("{} bps", source.current_incoming_bitrate_bps),
            "secondaryStat": format!("{} encodings / {} downloads", source.encodings.len(), downloads),
            "detail__owner_session_id": source.owner_user_id.path_segment(),
            "detail__stream_id": stream_id,
            "detail__media_kind": media_kind_str,
            "detail__transport_media_id": source.transport_media_id,
            "detail__mid": source.mid,
            "detail__encoding_ids": enc_ids,
            "detail__rids": rids,
            "detail__max_bitrates_bps": max_bitrates,
            "detail__primary_ssrcs": primary_ssrcs,
            "detail__repair_ssrcs": repair_ssrcs,
        }));

        let thickness = if source.current_incoming_bitrate_bps > 0 {
            2.0
        } else {
            1.0
        };
        let color = stream_id_color(&source.stream_id);

        edges.push(json!({
            "id": format!("publish:{}:{}", room_uuid, source.source_id),
            "source": format!("session:{}:{}", room_uuid, source.owner_user_id.path_segment()),
            "target": format!("source:{}:{}", room_uuid, source.source_id),
            "mainStat": format!("{} upload", stream_id),
            "secondaryStat": format!("{} bps", source.current_incoming_bitrate_bps),
            "thickness": thickness,
            "color": color,
            "detail__source_id": source.source_id,
            "detail__encoding_ids": enc_ids,
            "detail__rids": rids,
            "detail__transport_media_id": source.transport_media_id,
        }));
    }
}

fn download_edge(
    room_uuid: &str,
    user: &DiagnosticsUserView,
    sub: &DiagnosticsSubscription,
) -> Value {
    let session_id = user.user_id.path_segment();
    let mut edge = json!({
        "id": format!("download:{}:{}:{}", room_uuid, sub.source_id, session_id),
        "source": format!("source:{}:{}", room_uuid, sub.source_id),
        "target": format!("session:{}:{}", room_uuid, session_id),
        "mainStat": download_main_stat(sub),
        "secondaryStat": route_state_label(&sub.state),
        "color": route_state_color(&sub.state),
        "detail__source_id": sub.source_id,
        "detail__producer_session_id": sub.producer_user_id.path_segment(),
        "detail__stream_id": &sub.stream_id,
        "detail__selector": format!("{:?}", sub.selection.selector),
        "detail__selection_reason": format!("{:?}", sub.selection.selection_reason),
        "detail__selection_active": sub.selection.active,
        "detail__pending_upgrade": sub.selection.pending_upgrade,
        "detail__source_transport_media_id": sub.source_transport_media_id,
        "detail__consumer_transport_media_id": sub.consumer_transport_media_id,
    });

    if let Some(obj) = edge.as_object_mut() {
        if sub.state != DiagnosticsRouteState::Active {
            obj.insert("strokeDasharray".to_string(), json!("5, 5"));
        }
        if let Some(enc_id) = sub.selection.selected_encoding_id {
            obj.insert("detail__selected_encoding_id".to_string(), json!(enc_id));
        }
        if let Some(rid) = &sub.selection.selected_rid {
            obj.insert("detail__selected_rid".to_string(), json!(rid));
        }
    }

    edge
}

fn push_download_entries(
    nodes: &mut Vec<Value>,
    edges: &mut Vec<Value>,
    detail: &DiagnosticsRoomDetail,
    source_ids: &HashSet<u64>,
) {
    let room_uuid = detail.summary.uuid.as_str();
    for user in &detail.users {
        for sub in &user.subscriptions {
            if !source_ids.contains(&sub.source_id) {
                nodes.push(json!({
                    "id": format!("source:{}:{}", room_uuid, sub.source_id),
                    "title": format!("missing #{}", sub.source_id),
                    "subtitle": "not found",
                }));
            }
            edges.push(download_edge(room_uuid, user, sub));
        }
    }
}

/// Build the Grafana node graph payload for a single room detail response.
///
/// The returned JSON has two top-level arrays:
///
/// * `nodes` contains the room, live sessions, publication sources and any
///   missing source placeholders needed by subscription edges.
/// * `edges` contains membership edges, publication upload edges and download
///   subscription edges.
///
/// This function is compatibility-shaped around Grafana's node graph field
/// names. Domain code should keep using `DiagnosticsRoomDetail`.
#[must_use]
pub fn build_graph(detail: &DiagnosticsRoomDetail) -> Value {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let source_ids = source_ids(detail);
    let download_counts = download_counts(detail);

    nodes.push(room_node(detail));
    push_session_entries(&mut nodes, &mut edges, detail);
    push_source_entries(&mut nodes, &mut edges, detail, &download_counts);
    push_download_entries(&mut nodes, &mut edges, detail, &source_ids);

    json!({
        "nodes": nodes,
        "edges": edges,
    })
}
