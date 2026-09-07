use std::fmt::Write as _;

use o_sfu_model::WebSocketCloseCode;

#[cfg(any(test, feature = "test-support"))]
use super::snapshot::{RuntimeMetricsSnapshot, SnapshotWriter};
use super::{
    catalog::RuntimeMetrics,
    counter::{
        CounterFamily, ExportedMetricLabel, Histogram, HistogramBucketLabel, HistogramFamily,
        MetricBucketLabel, UpDownCounterFamily,
    },
    labels::{
        ControlPlaneDurationBucket, ExportedMetricLabelPair, HttpRoute, RtcRelayEnqueueResult,
    },
    rtc::RtcMetricsSnapshot,
    rtp::{RtpMetricsSnapshot, RtpTrafficSnapshot},
};

#[cfg(test)]
#[path = "TESTS/descriptor.rs"]
mod tests;

macro_rules! metric_catalog {
    ($($id:ident {
        name: $name:literal,
        help: $help:literal,
        kind: $kind:ident,
        samples: |$metrics:ident, $capture:ident, $output:ident| $samples:expr
    }),+ $(,)?) => {
        /// Names every exported Prometheus metric family.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum MetricName {
            $(
                #[doc = concat!("`", $name, "`\n\n", $help)]
                $id,
            )+
        }

        #[cfg(test)]
        pub(crate) const METRIC_FAMILY_COUNT: usize = [$(MetricName::$id),+].len();

        const PROMETHEUS_METADATA_CAPACITY: usize = 0 $(
            + "# HELP ".len() + $name.len() + 1 + $help.len() + 1
            + "# TYPE ".len() + $name.len() + 1 + MetricKind::$kind.name().len() + 1
        )+;

        fn export(
            metrics: &RuntimeMetrics,
            room_gauges: RoomGaugeValues,
            output: &mut MetricOutput,
        ) {
            let capture = MetricCapture {
                room_gauges,
                rtp: metrics.rtp_metrics.snapshot(),
                rtc: metrics.rtc_metrics.snapshot(),
            };
            $(
                output.begin_family(MetricDescriptor {
                    #[cfg(any(test, feature = "test-support"))]
                    id: MetricName::$id,
                    name: $name,
                    help: $help,
                    kind: MetricKind::$kind,
                });
                {
                    let $metrics = metrics;
                    let $capture = &capture;
                    let $output = &mut *output;
                    let _ = ($metrics, $capture);
                    $samples
                }
            )+
        }
    };
}

#[derive(Clone, Copy)]
enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

impl MetricKind {
    const fn name(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

#[derive(Clone, Copy)]
struct MetricDescriptor {
    #[cfg(any(test, feature = "test-support"))]
    id: MetricName,
    name: &'static str,
    help: &'static str,
    kind: MetricKind,
}

struct MetricCapture {
    room_gauges: RoomGaugeValues,
    rtp: RtpMetricsSnapshot,
    rtc: RtcMetricsSnapshot,
}

/// Room counts supplied to one export and saturated during gauge encoding.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RoomGaugeValues {
    pub rooms: usize,
    pub users: usize,
    pub publications: usize,
    pub subscriptions: usize,
    pub recording_rooms: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MetricLabelValue {
    Text(&'static str),
    Number(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MetricLabel {
    pub(super) name: &'static str,
    pub(super) value: MetricLabelValue,
}

impl MetricLabel {
    const fn text(name: &'static str, value: &'static str) -> Self {
        Self {
            name,
            value: MetricLabelValue::Text(value),
        }
    }

    const fn number(name: &'static str, value: usize) -> Self {
        Self {
            name,
            value: MetricLabelValue::Number(value),
        }
    }
}

struct MetricOutput {
    family: Option<MetricDescriptor>,
    text: Option<String>,
    #[cfg(any(test, feature = "test-support"))]
    snapshot: Option<SnapshotWriter>,
}

impl MetricOutput {
    fn prometheus() -> Self {
        Self {
            family: None,
            text: Some(String::with_capacity(PROMETHEUS_METADATA_CAPACITY)),
            #[cfg(any(test, feature = "test-support"))]
            snapshot: None,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    fn snapshot() -> Self {
        Self {
            family: None,
            text: None,
            snapshot: Some(SnapshotWriter::default()),
        }
    }

    fn begin_family(&mut self, descriptor: MetricDescriptor) {
        self.family = Some(descriptor);
        let Some(output) = &mut self.text else {
            return;
        };
        output.push_str("# HELP ");
        output.push_str(descriptor.name);
        output.push(' ');
        output.push_str(descriptor.help);
        output.push('\n');
        output.push_str("# TYPE ");
        output.push_str(descriptor.name);
        output.push(' ');
        output.push_str(descriptor.kind.name());
        output.push('\n');
    }

    fn counter(&mut self, labels: &[MetricLabel], value: u64) {
        let Some(descriptor) = self.family else {
            return;
        };
        if let Some(output) = &mut self.text {
            append_sample_name(output, descriptor.name, labels);
            let _ = writeln!(output, " {value}");
        }
        #[cfg(any(test, feature = "test-support"))]
        if let Some(snapshot) = &mut self.snapshot {
            snapshot.counter(descriptor.id, Box::from(labels), value);
        }
    }

    fn gauge(&mut self, labels: &[MetricLabel], value: i64) {
        let Some(descriptor) = self.family else {
            return;
        };
        if let Some(output) = &mut self.text {
            append_sample_name(output, descriptor.name, labels);
            let _ = writeln!(output, " {value}");
        }
        #[cfg(any(test, feature = "test-support"))]
        if let Some(snapshot) = &mut self.snapshot {
            snapshot.gauge(descriptor.id, Box::from(labels), value);
        }
    }

    fn histogram<B>(
        &mut self,
        labels: &[MetricLabel],
        load_bucket: impl Fn(B) -> u64,
        load_count: impl Fn() -> u64,
        load_sum_micros: impl Fn() -> u64,
    ) where
        B: MetricBucketLabel,
    {
        let Some(descriptor) = self.family else {
            return;
        };
        if let Some(output) = &mut self.text {
            let mut floor = 0;
            for bucket in B::VARIANTS {
                let value = floor.max(load_bucket(*bucket));
                floor = value;
                output.push_str(descriptor.name);
                output.push_str("_bucket");
                append_labels(
                    output,
                    labels,
                    Some(MetricLabel::text("le", bucket.upper_bound())),
                );
                let _ = writeln!(output, " {value}");
            }
            let count = floor.max(load_count());
            let sum_micros = load_sum_micros();
            output.push_str(descriptor.name);
            output.push_str("_bucket");
            append_labels(output, labels, Some(MetricLabel::text("le", "+Inf")));
            let _ = writeln!(output, " {count}");
            output.push_str(descriptor.name);
            output.push_str("_sum");
            append_labels(output, labels, None);
            output.push(' ');
            append_seconds_from_micros(output, sum_micros);
            output.push('\n');
            output.push_str(descriptor.name);
            output.push_str("_count");
            append_labels(output, labels, None);
            let _ = writeln!(output, " {count}");
        }
        #[cfg(any(test, feature = "test-support"))]
        if let Some(snapshot) = &mut self.snapshot {
            let mut floor = 0;
            let buckets = B::VARIANTS
                .iter()
                .map(|bucket| {
                    let value = floor.max(load_bucket(*bucket));
                    floor = value;
                    (bucket.upper_bound(), value)
                })
                .collect();
            snapshot.histogram(
                descriptor.id,
                Box::from(labels),
                buckets,
                floor.max(load_count()),
                load_sum_micros(),
            );
        }
    }

    fn finish_prometheus(self) -> String {
        self.text.unwrap_or_default()
    }

    #[cfg(any(test, feature = "test-support"))]
    fn finish_snapshot(self) -> RuntimeMetricsSnapshot {
        self.snapshot.unwrap_or_default().finish()
    }
}

pub(crate) fn render_prometheus_text(metrics: &RuntimeMetrics, gauges: RoomGaugeValues) -> String {
    let mut output = MetricOutput::prometheus();
    export(metrics, gauges, &mut output);
    output.finish_prometheus()
}

#[cfg(any(test, feature = "test-support"))]
pub(super) fn build_snapshot(metrics: &RuntimeMetrics) -> RuntimeMetricsSnapshot {
    let mut output = MetricOutput::snapshot();
    export(metrics, RoomGaugeValues::default(), &mut output);
    output.finish_snapshot()
}

fn gauge_count(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

metric_catalog! {
    HttpNoopRequestsTotal {
        name: "osfu_http_noop_requests_total",
        help: "Total HTTP requests served by /v1/noop.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], metrics.http_requests.load(HttpRoute::Noop))
    },
    HttpStatsRequestsTotal {
        name: "osfu_http_stats_requests_total",
        help: "Total HTTP requests served by /v1/stats.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], metrics.http_requests.load(HttpRoute::Stats))
    },
    HttpRoomRequestsTotal {
        name: "osfu_http_room_requests_total",
        help: "Total HTTP requests received by /v1/channel.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], metrics.http_requests.load(HttpRoute::Room))
    },
    HttpRoomResponsesTotal {
        name: "osfu_http_room_responses_total",
        help: "Total HTTP /v1/channel responses by status.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.http_room_responses, "status")
    },
    HttpDisconnectRequestsTotal {
        name: "osfu_http_disconnect_requests_total",
        help: "Total HTTP requests received by /v1/disconnect.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], metrics.http_requests.load(HttpRoute::Disconnect))
    },
    HttpDisconnectResponsesTotal {
        name: "osfu_http_disconnect_responses_total",
        help: "Total HTTP /v1/disconnect responses by status.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.http_disconnect_responses, "status")
    },
    HttpMetricsRequestsTotal {
        name: "osfu_http_metrics_requests_total",
        help: "Total HTTP requests served by /metrics.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], metrics.http_requests.load(HttpRoute::Metrics))
    },
    HttpInflightRequests {
        name: "osfu_http_inflight_requests",
        help: "Current in-flight HTTP requests by route.",
        kind: Gauge,
        samples: |metrics, capture, output| write_up_down_counter_family(output, &metrics.http_inflight_requests, "route")
    },
    HttpRequestDurationSeconds {
        name: "osfu_http_request_duration_seconds",
        help: "HTTP request duration by route.",
        kind: Histogram,
        samples: |metrics, capture, output| write_histogram_family(output, &metrics.http_request_duration, "route")
    },
    WsConnectionsTotal {
        name: "osfu_ws_connections_total",
        help: "Total websocket connections observed at each handshake stage.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.ws_connections, "stage")
    },
    WsHandshakeRejectionsTotal {
        name: "osfu_ws_handshake_rejections_total",
        help: "Total websocket handshake rejections by close code bucket.",
        kind: Counter,
        samples: |metrics, capture, output| {
            counter(output,
                [("close_code", WebSocketCloseCode::AuthTimeout.label_value())],
                metrics.ws_handshake_rejections.load(WebSocketCloseCode::AuthTimeout),
            );
            counter(output,
                [("close_code", WebSocketCloseCode::AuthFailed.label_value())],
                metrics.ws_handshake_rejections.load(WebSocketCloseCode::AuthFailed),
            );
            counter(output,
                [("close_code", WebSocketCloseCode::ProtocolError.label_value())],
                metrics.ws_handshake_rejections.load(WebSocketCloseCode::ProtocolError),
            );
            counter(output,
                [("close_code", WebSocketCloseCode::RoomFull.label_value())],
                metrics.ws_handshake_rejections.load(WebSocketCloseCode::RoomFull),
            );
            counter(output, [("close_code", "error")], metrics.ws_handshake_rejections_other.load());
        }
    },
    WsStartupFailuresTotal {
        name: "osfu_ws_startup_failures_total",
        help: "Total websocket startup failures before the steady-state user loop.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.ws_startup_failures, "kind")
    },
    WsHandshakeDurationSeconds {
        name: "osfu_ws_handshake_duration_seconds",
        help: "Websocket handshake duration from upgrade to user readiness or rejection.",
        kind: Histogram,
        samples: |metrics, capture, output| control_plane_histogram(output, &metrics.ws_handshake_duration)
    },
    WsAuthDurationSeconds {
        name: "osfu_ws_auth_duration_seconds",
        help: "Websocket authentication duration from first auth wait through token validation.",
        kind: Histogram,
        samples: |metrics, capture, output| control_plane_histogram(output, &metrics.ws_auth_duration)
    },
    WsUserInitializeDurationSeconds {
        name: "osfu_ws_user_initialize_duration_seconds",
        help: "Websocket user initialization duration after room admission.",
        kind: Histogram,
        samples: |metrics, capture, output| control_plane_histogram(output, &metrics.ws_user_initialize_duration)
    },
    WsUserLoopsStartedTotal {
        name: "osfu_ws_user_loops_started_total",
        help: "Total websocket user loops started after a successful join.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], metrics.ws_user_loops_started.load())
    },
    WsUserLoopExitsTotal {
        name: "osfu_ws_user_loop_exits_total",
        help: "Total websocket user loop exits by reason.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.ws_user_loop_exits, "reason")
    },
    WsBusBatchesTotal {
        name: "osfu_ws_bus_batches_total",
        help: "Total websocket signaling batches processed by direction.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.ws_bus_batches, "direction")
    },
    WsBusEnvelopesTotal {
        name: "osfu_ws_bus_envelopes_total",
        help: "Total websocket signaling envelopes processed by direction.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.ws_bus_envelopes, "direction")
    },
    WsBusParseFailuresTotal {
        name: "osfu_ws_bus_parse_failures_total",
        help: "Total websocket signaling parse failures.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], metrics.ws_bus_parse_failures.load())
    },
    WsBusFailuresTotal {
        name: "osfu_ws_bus_failures_total",
        help: "Total websocket signaling failures by kind.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.ws_bus_failures, "kind")
    },
    WsBusClientFramesTotal {
        name: "osfu_ws_bus_client_frames_total",
        help: "Total client websocket signaling frames by kind.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.ws_bus_client_frames, "kind")
    },
    WsOutboundQueuedMessages {
        name: "osfu_ws_outbound_queued_messages",
        help: "Current websocket outbound room messages waiting in per-user queues.",
        kind: Gauge,
        samples: |metrics, capture, output| output.gauge(&[], metrics.ws_outbound_queued_messages.load())
    },
    WsOutboundQueueOverflowsTotal {
        name: "osfu_ws_outbound_queue_overflows_total",
        help: "Total websocket users marked for slow-consumer shutdown after outbound queue overflow.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], metrics.ws_outbound_queue_overflows.load())
    },
    RoomsActive {
        name: "osfu_rooms_active",
        help: "Current number of active rooms owned by this runtime.",
        kind: Gauge,
        samples: |metrics, capture, output| output.gauge(&[], gauge_count(capture.room_gauges.rooms))
    },
    UsersActive {
        name: "osfu_users_active",
        help: "Current number of active room users owned by this runtime.",
        kind: Gauge,
        samples: |metrics, capture, output| output.gauge(&[], gauge_count(capture.room_gauges.users))
    },
    PublicationsActive {
        name: "osfu_publications_active",
        help: "Current number of committed or pending published media entries owned by this runtime.",
        kind: Gauge,
        samples: |metrics, capture, output| output.gauge(&[], gauge_count(capture.room_gauges.publications))
    },
    SubscriptionsActive {
        name: "osfu_subscriptions_active",
        help: "Current number of committed or pending consumer subscriptions owned by this runtime.",
        kind: Gauge,
        samples: |metrics, capture, output| output.gauge(&[], gauge_count(capture.room_gauges.subscriptions))
    },
    TransportUsersActive {
        name: "osfu_transport_users_active",
        help: "Current number of active RTC transport users on this runtime.",
        kind: Gauge,
        samples: |metrics, capture, output| output.gauge(&[], metrics.active_transport_users.load())
    },
    RecordingActionsTotal {
        name: "osfu_recording_actions_total",
        help: "Total recording control actions by action and outcome.",
        kind: Counter,
        samples: |metrics, capture, output| write_label_pair_counter_family(output, &metrics.recording_actions)
    },
    RecordingRoomsActive {
        name: "osfu_recording_rooms_active",
        help: "Current number of rooms with an active recording user.",
        kind: Gauge,
        samples: |metrics, capture, output| output.gauge(&[], gauge_count(capture.room_gauges.recording_rooms))
    },
    RecordingCapturedPacketsTotal {
        name: "osfu_recording_captured_packets_total",
        help: "Total packets accepted by the recording capture path.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], metrics.recording_captured_packets.load())
    },
    RecordingCapturedStreamsTotal {
        name: "osfu_recording_captured_streams_total",
        help: "Total unique media streams first seen by the recording capture path.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], metrics.recording_captured_streams.load())
    },
    TransportHealthUsers {
        name: "osfu_transport_health_users",
        help: "Current number of transport users by observed health state.",
        kind: Gauge,
        samples: |metrics, capture, output| write_up_down_counter_family(output, &metrics.transport_health_users, "state")
    },
    TransportHealthTransitionsTotal {
        name: "osfu_transport_health_transitions_total",
        help: "Total transport health-state transitions observed from the transport adapter.",
        kind: Counter,
        samples: |metrics, capture, output| write_label_pair_counter_family(output, &metrics.transport_health_transitions)
    },
    RtpPacketsTotal {
        name: "osfu_rtp_packets_total",
        help: "Total RTP packets processed by flow direction.",
        kind: Counter,
        samples: |metrics, capture, output| write_snapshot_counters(output,
            &capture.rtp.traffic,
            "direction",
            RtpTrafficSnapshot::packets
        )
    },
    RtpPayloadBytesTotal {
        name: "osfu_rtp_payload_bytes_total",
        help: "Total RTP payload bytes processed by flow direction.",
        kind: Counter,
        samples: |metrics, capture, output| write_snapshot_counters(output,
            &capture.rtp.traffic,
            "direction",
            RtpTrafficSnapshot::payload_bytes
        )
    },
    RtpForwardedPacketsTotal {
        name: "osfu_rtp_forwarded_packets_total",
        help: "Total RTP packet fan-out operations by forwarding destination.",
        kind: Counter,
        samples: |metrics, capture, output| write_snapshot_counters(output,
            &capture.rtp.traffic,
            "destination",
            RtpTrafficSnapshot::forwarded_packets
        )
    },
    RtpForwardedPayloadBytesTotal {
        name: "osfu_rtp_forwarded_payload_bytes_total",
        help: "Total RTP payload bytes fanned out by forwarding destination.",
        kind: Counter,
        samples: |metrics, capture, output| write_snapshot_counters(output,
            &capture.rtp.traffic,
            "destination",
            RtpTrafficSnapshot::forwarded_payload_bytes
        )
    },
    WorkerRtpPacketsTotal {
        name: "osfu_worker_rtp_packets_total",
        help: "Total RTP packets processed by media worker and flow direction.",
        kind: Counter,
        samples: |metrics, capture, output| write_rtp_worker_counters(output,
            &capture.rtp,
            "direction",
            RtpTrafficSnapshot::packets
        )
    },
    WorkerRtpPayloadBytesTotal {
        name: "osfu_worker_rtp_payload_bytes_total",
        help: "Total RTP payload bytes processed by media worker and flow direction.",
        kind: Counter,
        samples: |metrics, capture, output| write_rtp_worker_counters(output,
            &capture.rtp,
            "direction",
            RtpTrafficSnapshot::payload_bytes
        )
    },
    WorkerRtpForwardedPacketsTotal {
        name: "osfu_worker_rtp_forwarded_packets_total",
        help: "Total RTP packet fan-out operations by media worker and forwarding destination.",
        kind: Counter,
        samples: |metrics, capture, output| write_rtp_worker_counters(output,
            &capture.rtp,
            "destination",
            RtpTrafficSnapshot::forwarded_packets
        )
    },
    WorkerRtpForwardedPayloadBytesTotal {
        name: "osfu_worker_rtp_forwarded_payload_bytes_total",
        help: "Total RTP payload bytes fanned out by media worker and forwarding destination.",
        kind: Counter,
        samples: |metrics, capture, output| write_rtp_worker_counters(output,
            &capture.rtp,
            "destination",
            RtpTrafficSnapshot::forwarded_payload_bytes
        )
    },
    RtpRelayOverloadDropsTotal {
        name: "osfu_rtp_relay_overload_drops_total",
        help: "Total RTP relay packets dropped because the bounded relay mailbox was full.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.rtp_relay_overload_drops, "destination")
    },
    RtpDecoderRefreshesTotal {
        name: "osfu_rtp_decoder_refreshes_total",
        help: "Total decoder-refresh RTP packets observed by source scope.",
        kind: Counter,
        samples: |metrics, capture, output| write_snapshot_counters(output,
            &capture.rtp,
            "scope",
            RtpMetricsSnapshot::decoder_refreshes
        )
    },
    TransportIceStateChangesTotal {
        name: "osfu_transport_ice_state_changes_total",
        help: "Total RTC ICE state-change events observed from the transport adapter.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.transport_ice_state_changes, "state")
    },
    TransportDtlsConnectedTotal {
        name: "osfu_transport_dtls_connected_total",
        help: "Total RTC DTLS-connected events observed from the transport adapter.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], metrics.transport_dtls_connected.load())
    },
    TransportUserLifetimeSeconds {
        name: "osfu_transport_user_lifetime_seconds",
        help: "Lifetime of closed RTC transport users observed at cold-path teardown.",
        kind: Histogram,
        samples: |metrics, capture, output| output.histogram(
            &[],
            |bucket| metrics.transport_user_lifetime_buckets.load(bucket),
            || metrics.transport_user_lifetime_count.load(),
            || metrics.transport_user_lifetime_sum_micros.load(),
        )
    },
    MediaQualitySamplesTotal {
        name: "osfu_media_quality_samples_total",
        help: "Total sampled transport-quality events by str0m stats source.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.media_quality_samples, "sample")
    },
    MediaQualityRttSeconds {
        name: "osfu_media_quality_rtt_seconds",
        help: "Sampled RTC round-trip time by str0m stats source.",
        kind: Histogram,
        samples: |metrics, capture, output| write_histogram_family(output, &metrics.media_quality_rtt, "sample")
    },
    MediaQualityLossPpmObservedTotal {
        name: "osfu_media_quality_loss_ppm_observed_total",
        help: "Sum of sampled packet loss observations in parts per million by direction.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.media_quality_loss_ppm_observed, "direction")
    },
    MediaQualityLossObservationsTotal {
        name: "osfu_media_quality_loss_observations_total",
        help: "Total packet loss observations by direction.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.media_quality_loss_observations, "direction")
    },
    MediaQualityBweBpsObservedTotal {
        name: "osfu_media_quality_bwe_bps_observed_total",
        help: "Sum of sampled peer egress bandwidth estimates in bits per second.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], metrics.media_quality_bwe_bps_observed.load())
    },
    MediaQualityBweObservationsTotal {
        name: "osfu_media_quality_bwe_observations_total",
        help: "Total peer egress bandwidth estimate observations.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], metrics.media_quality_bwe_observations.load())
    },
    MediaQualityJitterRtpTimestampUnitsObservedTotal {
        name: "osfu_media_quality_jitter_rtp_timestamp_units_observed_total",
        help: "Sum of sampled remote egress jitter observations in RTP timestamp units.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(
            &[],
            metrics.media_quality_jitter_rtp_timestamp_units_observed.load(),
        )
    },
    MediaQualityJitterObservationsTotal {
        name: "osfu_media_quality_jitter_observations_total",
        help: "Total remote egress jitter observations.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], metrics.media_quality_jitter_observations.load())
    },
    TransportCleanupFailuresTotal {
        name: "osfu_transport_cleanup_failures_total",
        help: "Total terminal transport cleanup failures.",
        kind: Counter,
        samples: |metrics, capture, output| counter(output,
            [("kind", "terminal")],
            metrics.transport_cleanup_failures.load()
        )
    },
    RtcDatagramRoutesTotal {
        name: "osfu_rtc_datagram_routes_total",
        help: "Total RTC UDP datagrams accepted by routing path.",
        kind: Counter,
        samples: |metrics, capture, output| write_snapshot_counters(output,
            &capture.rtc,
            "path",
            RtcMetricsSnapshot::datagram_routes
        )
    },
    RtcDatagramDropsTotal {
        name: "osfu_rtc_datagram_drops_total",
        help: "Total RTC UDP datagrams dropped by ingress routing before session delivery.",
        kind: Counter,
        samples: |metrics, capture, output| write_snapshot_counters(output,
            &capture.rtc,
            "reason",
            RtcMetricsSnapshot::datagram_drops
        )
    },
    RtcDatagramFallbackScansTotal {
        name: "osfu_rtc_datagram_fallback_scans_total",
        help: "Total fallback scans across RTC users for UDP datagram routing.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], capture.rtc.datagram_fallback_scans())
    },
    RtcDatagramScanUsersTotal {
        name: "osfu_rtc_datagram_scan_users_total",
        help: "Total RTC users examined by UDP fallback scans.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], capture.rtc.datagram_scan_users())
    },
    RtcNacksTotal {
        name: "osfu_rtc_nacks_total",
        help: "Total Generic NACK feedback events by WebRTC direction.",
        kind: Counter,
        samples: |metrics, capture, output| write_snapshot_counters(output,
            &capture.rtc,
            "direction",
            RtcMetricsSnapshot::nacks
        )
    },
    RtcRtxPacketsTotal {
        name: "osfu_rtc_rtx_packets_total",
        help: "Total authenticated RTX packets accepted from publishers.",
        kind: Counter,
        samples: |metrics, capture, output| counter(output,
            [("direction", "received_from_publisher")],
            capture.rtc.rtx_packets_received_from_publisher()
        )
    },
    RtcRtxPayloadBytesTotal {
        name: "osfu_rtc_rtx_payload_bytes_total",
        help: "Total de-RTX media payload bytes accepted from publishers.",
        kind: Counter,
        samples: |metrics, capture, output| counter(output,
            [("direction", "received_from_publisher")],
            capture.rtc.rtx_payload_bytes_received_from_publisher()
        )
    },
    RtcRtcpIngressBudgetDropsTotal {
        name: "osfu_rtc_rtcp_ingress_budget_drops_total",
        help: "Total candidate RTCP datagrams dropped by the per-session ingress byte budget.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], capture.rtc.rtcp_ingress_budget_drops())
    },
    RtcOutputBudgetExhaustionsTotal {
        name: "osfu_rtc_output_budget_exhaustions_total",
        help: "Total RTC session drains that exhausted the output budget by limit.",
        kind: Counter,
        samples: |metrics, capture, output| write_snapshot_counters(output,
            &capture.rtc,
            "limit",
            RtcMetricsSnapshot::output_budget_exhaustions
        )
    },
    RtcOutputBudgetSessionClosesTotal {
        name: "osfu_rtc_output_budget_session_closes_total",
        help: "Total RTC sessions closed after output-budget exhaustion.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], capture.rtc.output_budget_session_closes())
    },
    RtcRouteControlTotal {
        name: "osfu_rtc_route_control_total",
        help: "Total RTC route-control decisions observed at the transport boundary.",
        kind: Counter,
        samples: |metrics, capture, output| write_snapshot_counters(output,
            &capture.rtc,
            "outcome",
            RtcMetricsSnapshot::route_control
        )
    },
    RtcKeyframeRequestsTotal {
        name: "osfu_rtc_keyframe_requests_total",
        help: "Total RTC keyframe request tracker outcomes.",
        kind: Counter,
        samples: |metrics, capture, output| write_snapshot_counters(output,
            &capture.rtc,
            "outcome",
            RtcMetricsSnapshot::keyframe_requests
        )
    },
    RtcRelayEnqueuesTotal {
        name: "osfu_rtc_relay_enqueues_total",
        help: "Total relay enqueue attempts by target kind and outcome.",
        kind: Counter,
        samples: |metrics, capture, output| write_label_pair_counters(output, |result: RtcRelayEnqueueResult| {
            capture.rtc.relay_enqueues(result)
        })
    },
    RtcRelayMailboxDepthSamplesTotal {
        name: "osfu_rtc_relay_mailbox_depth_samples_total",
        help: "Total sampled intra-node relay mailbox depth observations.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], capture.rtc.relay_mailbox_depth_samples())
    },
    RtcRelayMailboxDepthObservedTotal {
        name: "osfu_rtc_relay_mailbox_depth_observed_total",
        help: "Sum of sampled intra-node relay mailbox depths.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], capture.rtc.relay_mailbox_depth_total())
    },
    RtcRelayDrainBatchesTotal {
        name: "osfu_rtc_relay_drain_batches_total",
        help: "Total non-empty packet-loop relay drain batches.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], capture.rtc.relay_drain_batches())
    },
    RtcRelayDrainedPacketsTotal {
        name: "osfu_rtc_relay_drained_packets_total",
        help: "Total relay packets drained into packet-loop batches.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], capture.rtc.relay_drained_packets())
    },
    RtcRelayDrainCapHitsTotal {
        name: "osfu_rtc_relay_drain_cap_hits_total",
        help: "Total relay drain batches that left queued relay packets behind after hitting the per-turn cap.",
        kind: Counter,
        samples: |metrics, capture, output| output.counter(&[], capture.rtc.relay_drain_cap_hits())
    },
    RtcRemoteControlDropsTotal {
        name: "osfu_rtc_remote_control_drops_total",
        help: "Total remote-source control commands dropped before enqueue by command kind.",
        kind: Counter,
        samples: |metrics, capture, output| write_snapshot_counters(output,
            &capture.rtc,
            "kind",
            RtcMetricsSnapshot::remote_control_drops
        )
    },
    RtcRemotePacketGateConvergenceTotal {
        name: "osfu_rtc_remote_packet_gate_convergence_total",
        help: "Total remote packet-gate convergence retry attempts and successful pending flushes.",
        kind: Counter,
        samples: |metrics, capture, output| write_snapshot_counters(output,
            &capture.rtc,
            "outcome",
            RtcMetricsSnapshot::remote_packet_gate_convergence
        )
    },
    SourceSelectionUpdatesTotal {
        name: "osfu_source_selection_updates_total",
        help: "Total room-scoped source selector updates accepted by source policy.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.source_selection_updates, "selector")
    },
    BudgetSolverOutcomesTotal {
        name: "osfu_budget_solver_outcomes_total",
        help: "Total committed receiver video route degradation, pause and resume transitions.",
        kind: Counter,
        samples: |metrics, capture, output| write_counter_family(output, &metrics.budget_solver_outcomes, "outcome")
    },
}

fn write_counter_family<L>(
    output: &mut MetricOutput,
    family: &CounterFamily<L>,
    label_name: &'static str,
) where
    L: ExportedMetricLabel,
{
    for label in L::VARIANTS {
        counter(
            output,
            [(label_name, label.label_value())],
            family.load(*label),
        );
    }
}

fn write_label_pair_counter_family<L>(output: &mut MetricOutput, family: &CounterFamily<L>)
where
    L: ExportedMetricLabelPair,
{
    write_label_pair_counters(output, |label| family.load(label));
}

fn write_label_pair_counters<L>(output: &mut MetricOutput, load: impl Fn(L) -> u64)
where
    L: ExportedMetricLabelPair,
{
    for label in L::VARIANTS {
        counter(output, label.label_pair(), load(*label));
    }
}

fn write_snapshot_counters<S, L>(
    output: &mut MetricOutput,
    snapshot: &S,
    label_name: &'static str,
    read: fn(&S, L) -> u64,
) where
    L: ExportedMetricLabel,
{
    for label in L::VARIANTS {
        counter(
            output,
            [(label_name, label.label_value())],
            read(snapshot, *label),
        );
    }
}

fn write_rtp_worker_counters<L>(
    output: &mut MetricOutput,
    snapshot: &RtpMetricsSnapshot,
    label_name: &'static str,
    read: fn(&RtpTrafficSnapshot, L) -> u64,
) where
    L: ExportedMetricLabel,
{
    for worker in snapshot.worker_snapshots() {
        for label in L::VARIANTS {
            output.counter(
                &[
                    MetricLabel::number("media_worker_id", worker.media_worker_id()),
                    MetricLabel::text(label_name, label.label_value()),
                ],
                read(&worker.traffic, *label),
            );
        }
    }
}

fn write_up_down_counter_family<L>(
    output: &mut MetricOutput,
    family: &UpDownCounterFamily<L>,
    label_name: &'static str,
) where
    L: ExportedMetricLabel,
{
    for label in L::VARIANTS {
        output.gauge(
            &[MetricLabel::text(label_name, label.label_value())],
            family.load(*label),
        );
    }
}

fn write_histogram_family<L, B>(
    output: &mut MetricOutput,
    family: &HistogramFamily<L, B>,
    label_name: &'static str,
) where
    L: ExportedMetricLabel,
    B: HistogramBucketLabel,
{
    for label in L::VARIANTS {
        output.histogram(
            &[MetricLabel::text(label_name, label.label_value())],
            |bucket| family.load_bucket(*label, bucket),
            || family.load_count(*label),
            || family.load_sum_micros(*label),
        );
    }
}

fn counter<const N: usize>(
    output: &mut MetricOutput,
    labels: [(&'static str, &'static str); N],
    value: u64,
) {
    output.counter(
        &labels.map(|(name, value)| MetricLabel::text(name, value)),
        value,
    );
}

fn control_plane_histogram(
    output: &mut MetricOutput,
    histogram: &Histogram<ControlPlaneDurationBucket>,
) {
    output.histogram(
        &[],
        |bucket| histogram.load_bucket(bucket),
        || histogram.load_count(),
        || histogram.load_sum_micros(),
    );
}

fn append_sample_name(output: &mut String, name: &str, labels: &[MetricLabel]) {
    output.push_str(name);
    append_labels(output, labels, None);
}

fn append_labels(output: &mut String, labels: &[MetricLabel], extra_label: Option<MetricLabel>) {
    if labels.is_empty() && extra_label.is_none() {
        return;
    }
    output.push('{');
    for (index, label) in labels.iter().chain(extra_label.iter()).enumerate() {
        if index != 0 {
            output.push(',');
        }
        output.push_str(label.name);
        output.push_str("=\"");
        match label.value {
            MetricLabelValue::Text(value) => output.push_str(value),
            MetricLabelValue::Number(value) => {
                let _ = write!(output, "{value}");
            }
        }
        output.push('"');
    }
    output.push('}');
}

fn append_seconds_from_micros(output: &mut String, micros: u64) {
    let whole_seconds = micros / 1_000_000;
    let fractional_micros = micros % 1_000_000;
    let _ = write!(output, "{whole_seconds}");
    if fractional_micros == 0 {
        output.push_str(".0");
        return;
    }
    let _ = write!(output, ".{fractional_micros:06}");
    while output.ends_with('0') {
        output.pop();
    }
}
