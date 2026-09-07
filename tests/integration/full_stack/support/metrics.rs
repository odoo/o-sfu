use std::str::FromStr;

pub(crate) use o_sfu_telemetry::metrics::RoomGaugeValues;

use super::*;

pub(crate) async fn stream_until_audio_bitrate_is_observable(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> Option<IncomingBitRateStatsResponse> {
    for _ in 0..20 {
        publisher.rtc().send_rtp_packets(source, clock, 2).await?;
        sleep(Duration::from_millis(60)).await;
        let stats = stats(server).await?;
        let room_stats = stats.into_iter().find(|entry| entry.uuid == room)?;
        if room_stats.users_stats.incoming_bit_rate.audio > 0 {
            return Some(room_stats.users_stats.incoming_bit_rate);
        }
        yield_now().await;
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct TransportUserLifetimeMetrics {
    pub(crate) le_1_second: u64,
    pub(crate) le_10_seconds: u64,
    pub(crate) le_60_seconds: u64,
    pub(crate) le_300_seconds: u64,
    pub(crate) count: u64,
    pub(crate) sum_seconds: f64,
}

pub(crate) async fn wait_for_room_gauges(server: &TestServer, expected: RoomGaugeValues) -> bool {
    timeout(super::Duration::from_secs(3), async {
        loop {
            let actual = metrics_text(server).await.and_then(|text| {
                Some(RoomGaugeValues {
                    rooms: parse_prometheus(&text, "osfu_rooms_active")?,
                    users: parse_prometheus(&text, "osfu_users_active")?,
                    publications: parse_prometheus(&text, "osfu_publications_active")?,
                    subscriptions: parse_prometheus(&text, "osfu_subscriptions_active")?,
                    recording_rooms: parse_prometheus(&text, "osfu_recording_rooms_active")?,
                })
            });
            if actual == Some(expected) {
                return;
            }
            yield_now().await;
        }
    })
    .await
    .is_ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LiveRtcMetrics {
    pub(crate) connected_transport_users: i64,
    pub(crate) disconnected_transport_users: i64,
    pub(crate) transport_health_transitions_unset_to_connected: u64,
    pub(crate) transport_health_transitions_connected_to_disconnected: u64,
    pub(crate) transport_health_transitions_connected_to_unset: u64,
    pub(crate) transport_ice_state_changes_new: u64,
    pub(crate) transport_ice_state_changes_checking: u64,
    pub(crate) transport_ice_state_changes_connected: u64,
    pub(crate) transport_ice_state_changes_completed: u64,
    pub(crate) transport_ice_state_changes_disconnected: u64,
    pub(crate) transport_dtls_connected: u64,
    pub(crate) rtp_packets_ingress: u64,
    pub(crate) rtp_packets_egress: u64,
    pub(crate) rtp_payload_bytes_ingress: u64,
    pub(crate) rtp_payload_bytes_egress: u64,
    pub(crate) rtp_forwarded_packets_local_rtc: u64,
    pub(crate) rtp_forwarded_payload_bytes_local_rtc: u64,
    pub(crate) indexed_routes: u64,
    pub(crate) scan_routes: u64,
    pub(crate) fallback_scans: u64,
    pub(crate) scan_users: u64,
}

impl LiveRtcMetrics {
    const fn early_ice_state_changes(self) -> u64 {
        self.transport_ice_state_changes_new + self.transport_ice_state_changes_checking
    }

    const fn connected_ice_state_changes(self) -> u64 {
        self.transport_ice_state_changes_connected + self.transport_ice_state_changes_completed
    }

    const fn stable_lifecycle_counts(self) -> [u64; 7] {
        [
            self.transport_health_transitions_unset_to_connected,
            self.transport_health_transitions_connected_to_disconnected,
            self.transport_health_transitions_connected_to_unset,
            self.transport_ice_state_changes_new,
            self.transport_ice_state_changes_checking,
            self.transport_ice_state_changes_connected,
            self.transport_ice_state_changes_completed,
        ]
    }

    const fn scan_counts(self) -> [u64; 3] {
        [self.scan_routes, self.fallback_scans, self.scan_users]
    }
}

pub(crate) async fn wait_for_transport_lifetime_metrics(
    server: &TestServer,
    expected_count: u64,
) -> Option<TransportUserLifetimeMetrics> {
    timeout(super::Duration::from_secs(3), async {
        loop {
            let metrics = parse_transport_lifetime_metrics(&metrics_text(server).await?)?;
            if metrics.count >= expected_count {
                return Some(metrics);
            }
            yield_now().await;
        }
    })
    .await
    .ok()
    .flatten()
}

pub(crate) async fn wait_for_live_rtc_metrics(
    server: &TestServer,
    expected_connected_users: i64,
) -> Option<LiveRtcMetrics> {
    timeout(super::Duration::from_secs(3), async {
        loop {
            let metrics = parse_live_rtc_metrics(&metrics_text(server).await?)?;
            if metrics.connected_transport_users == expected_connected_users {
                return Some(metrics);
            }
            yield_now().await;
        }
    })
    .await
    .ok()
    .flatten()
}

fn parse_transport_lifetime_metrics(metrics_text: &str) -> Option<TransportUserLifetimeMetrics> {
    Some(TransportUserLifetimeMetrics {
        le_1_second: lifetime_bucket(metrics_text, "1")?,
        le_10_seconds: lifetime_bucket(metrics_text, "10")?,
        le_60_seconds: lifetime_bucket(metrics_text, "60")?,
        le_300_seconds: lifetime_bucket(metrics_text, "300")?,
        count: parse_prometheus(metrics_text, "osfu_transport_user_lifetime_seconds_count")?,
        sum_seconds: parse_prometheus(metrics_text, "osfu_transport_user_lifetime_seconds_sum")?,
    })
}

fn parse_live_rtc_metrics(metrics_text: &str) -> Option<LiveRtcMetrics> {
    Some(LiveRtcMetrics {
        connected_transport_users: health_user(metrics_text, "connected")?,
        disconnected_transport_users: health_user(metrics_text, "disconnected")?,
        transport_health_transitions_unset_to_connected: parse_health_transition(
            metrics_text,
            "unset",
            "connected",
        )?,
        transport_health_transitions_connected_to_disconnected: parse_health_transition(
            metrics_text,
            "connected",
            "disconnected",
        )?,
        transport_health_transitions_connected_to_unset: parse_health_transition(
            metrics_text,
            "connected",
            "unset",
        )?,
        transport_ice_state_changes_new: ice_state(metrics_text, "new")?,
        transport_ice_state_changes_checking: ice_state(metrics_text, "checking")?,
        transport_ice_state_changes_connected: ice_state(metrics_text, "connected")?,
        transport_ice_state_changes_completed: ice_state(metrics_text, "completed")?,
        transport_ice_state_changes_disconnected: ice_state(metrics_text, "disconnected")?,
        transport_dtls_connected: parse_prometheus(
            metrics_text,
            "osfu_transport_dtls_connected_total",
        )?,
        rtp_packets_ingress: direction(metrics_text, "osfu_rtp_packets_total", "ingress")?,
        rtp_packets_egress: direction(metrics_text, "osfu_rtp_packets_total", "egress")?,
        rtp_payload_bytes_ingress: direction(
            metrics_text,
            "osfu_rtp_payload_bytes_total",
            "ingress",
        )?,
        rtp_payload_bytes_egress: direction(
            metrics_text,
            "osfu_rtp_payload_bytes_total",
            "egress",
        )?,
        rtp_forwarded_packets_local_rtc: destination(
            metrics_text,
            "osfu_rtp_forwarded_packets_total",
            "local_rtc",
        )?,
        rtp_forwarded_payload_bytes_local_rtc: destination(
            metrics_text,
            "osfu_rtp_forwarded_payload_bytes_total",
            "local_rtc",
        )?,
        indexed_routes: route_path(metrics_text, "indexed")?,
        scan_routes: route_path(metrics_text, "scan")?,
        fallback_scans: parse_prometheus(metrics_text, "osfu_rtc_datagram_fallback_scans_total")?,
        scan_users: parse_prometheus(metrics_text, "osfu_rtc_datagram_scan_users_total")?,
    })
}

pub(crate) fn assert_initial_live_rtc_metrics(
    metrics: &LiveRtcMetrics,
    initial_forwarded_bytes: u64,
) {
    assert_eq!(metrics.connected_transport_users, 2);
    assert_eq!(metrics.disconnected_transport_users, 0);
    assert!(
        metrics.transport_health_transitions_unset_to_connected >= 2,
        "expected both RTC users to enter a connected transport health state"
    );
    assert_eq!(
        metrics.transport_health_transitions_connected_to_disconnected,
        0
    );
    assert_eq!(metrics.transport_health_transitions_connected_to_unset, 0);
    assert!(
        metrics.early_ice_state_changes() >= 2,
        "expected both RTC users to emit early ICE lifecycle counters"
    );
    assert!(
        metrics.connected_ice_state_changes() >= 2,
        "expected both RTC users to reach a connected ICE lifecycle state"
    );
    assert_eq!(metrics.transport_ice_state_changes_disconnected, 0);
    assert_eq!(metrics.transport_dtls_connected, 2);
    assert_rtp_totals(metrics, 2, initial_forwarded_bytes);
}

pub(crate) fn assert_steady_state_live_rtc_metrics(
    before: &LiveRtcMetrics,
    during: &LiveRtcMetrics,
    additional_forwarded_bytes: u64,
) {
    assert_eq!(during.connected_transport_users, 2);
    assert_eq!(during.disconnected_transport_users, 0);
    assert_eq!(
        during.stable_lifecycle_counts(),
        before.stable_lifecycle_counts()
    );
    assert!(
        during.indexed_routes > before.indexed_routes,
        "expected steady-state media to increase indexed datagram routing"
    );
    assert_eq!(during.scan_counts(), before.scan_counts());
    assert_rtp_delta(before, during, 4, additional_forwarded_bytes);
}

fn assert_rtp_totals(metrics: &LiveRtcMetrics, packets: u64, payload_bytes: u64) {
    assert_eq!(metrics.rtp_packets_ingress, packets);
    assert_eq!(metrics.rtp_packets_egress, packets);
    assert_eq!(metrics.rtp_payload_bytes_ingress, payload_bytes);
    assert_eq!(metrics.rtp_payload_bytes_egress, payload_bytes);
    assert_eq!(metrics.rtp_forwarded_packets_local_rtc, packets);
    assert_eq!(metrics.rtp_forwarded_payload_bytes_local_rtc, payload_bytes);
}

fn assert_rtp_delta(
    before: &LiveRtcMetrics,
    during: &LiveRtcMetrics,
    packets: u64,
    payload_bytes: u64,
) {
    assert_eq!(
        during.rtp_packets_ingress - before.rtp_packets_ingress,
        packets
    );
    assert_eq!(
        during.rtp_packets_egress - before.rtp_packets_egress,
        packets
    );
    assert_eq!(
        during.rtp_payload_bytes_ingress - before.rtp_payload_bytes_ingress,
        payload_bytes
    );
    assert_eq!(
        during.rtp_payload_bytes_egress - before.rtp_payload_bytes_egress,
        payload_bytes
    );
    assert_eq!(
        during.rtp_forwarded_packets_local_rtc - before.rtp_forwarded_packets_local_rtc,
        packets
    );
    assert_eq!(
        during.rtp_forwarded_payload_bytes_local_rtc - before.rtp_forwarded_payload_bytes_local_rtc,
        payload_bytes
    );
}

fn lifetime_bucket(metrics_text: &str, le: &str) -> Option<u64> {
    parse_labeled(
        metrics_text,
        "osfu_transport_user_lifetime_seconds_bucket",
        "le",
        le,
    )
}

fn health_user(metrics_text: &str, state: &str) -> Option<i64> {
    parse_labeled(metrics_text, "osfu_transport_health_users", "state", state)
}

fn parse_health_transition(metrics_text: &str, from: &str, to: &str) -> Option<u64> {
    parse_prometheus(
        metrics_text,
        format!("osfu_transport_health_transitions_total{{from=\"{from}\",to=\"{to}\"}}"),
    )
}

fn ice_state(metrics_text: &str, state: &str) -> Option<u64> {
    parse_labeled(
        metrics_text,
        "osfu_transport_ice_state_changes_total",
        "state",
        state,
    )
}

fn direction<T: FromStr>(metrics_text: &str, metric_name: &str, direction: &str) -> Option<T> {
    parse_labeled(metrics_text, metric_name, "direction", direction)
}

fn destination<T: FromStr>(metrics_text: &str, metric_name: &str, destination: &str) -> Option<T> {
    parse_labeled(metrics_text, metric_name, "destination", destination)
}

fn route_path(metrics_text: &str, path: &str) -> Option<u64> {
    parse_labeled(metrics_text, "osfu_rtc_datagram_routes_total", "path", path)
}

fn parse_labeled<T: FromStr>(
    metrics_text: &str,
    metric_name: &str,
    label_name: &str,
    label_value: &str,
) -> Option<T> {
    parse_prometheus(
        metrics_text,
        format!("{metric_name}{{{label_name}=\"{label_value}\"}}"),
    )
}

fn parse_prometheus<T: FromStr>(metrics_text: &str, metric_name: impl AsRef<str>) -> Option<T> {
    let metric_name = metric_name.as_ref();
    metrics_text
        .lines()
        .find_map(|line| line.strip_prefix(metric_name)?.trim().parse().ok())
}
