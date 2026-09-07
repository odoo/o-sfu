#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]
pub(super) use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    slice,
    sync::Arc,
    time::{Duration, Instant},
};

use o_sfu_rfc::rtp::CodecName;
pub(super) use o_sfu_router::rtp::{
    MediaStream as RouterRtpParameters, StreamBinding as RouterRtpEncoding,
};
use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::{MediaFormat, PayloadType},
};
pub(super) use str0m::media::{MediaKind as Str0mMediaKind, Mid};

pub(super) use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, SessionBitrateLimits,
    engine::{
        UserId,
        media_transport::{
            ActiveSpeakerSource, ReceiverBweTargetUpdate, SessionOffer, TransportAdapterError,
            TransportConsumerRoute, TransportMediaId, TransportResult, TransportSessionKey,
            TransportSourceKey,
            rtc::{
                commands::{
                    RtcWorkerCommand, WorkerMediaControlBatch, WorkerMediaControlBatchOutcome,
                },
                test_support::{DebugPacketGate, test_transport_session_key},
                worker::RtcWorker,
            },
            test_support::{test_media_transport_config, test_rtc_port_range},
        },
        metrics::test_support::RuntimeMetricsSnapshotTestExt,
    },
};

pub(super) fn transport_key(
    room_instance_id: u64,
    connection_id: u64,
    user_id: UserId,
) -> TransportSessionKey {
    transport_key_on_worker(room_instance_id, 0, connection_id, user_id)
}

pub(super) fn transport_key_on_worker(
    room_instance_id: u64,
    media_worker_id: usize,
    connection_id: u64,
    user_id: UserId,
) -> TransportSessionKey {
    test_transport_session_key(room_instance_id, media_worker_id, connection_id, user_id)
}

pub(super) fn transport_consumer_route(
    consumer_key: &TransportSessionKey,
    consumer_media: TransportMediaId,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
) -> TransportConsumerRoute {
    TransportConsumerRoute::new(
        consumer_key.clone(),
        consumer_media,
        TransportSourceKey::new(src_key.clone(), src_media),
    )
}

pub(super) fn sample_router_rtp_parameters(mid: &str, ssrc: u32) -> RouterRtpParameters {
    RouterRtpParameters::new(
        vec![],
        vec![],
        vec![RouterRtpEncoding::new().with_ssrc(ssrc)],
    )
    .with_mid(mid.to_owned())
}

pub(super) fn sample_vp8_rtp_parameters(
    mid: &str,
    ssrc: u32,
    rid: Option<&str>,
) -> RouterRtpParameters {
    let encoding = RouterRtpEncoding::new()
        .with_ssrc(ssrc)
        .with_payload_type(PayloadType::new(96));
    let encoding = match rid {
        Some(rid) => encoding.with_rid(rid),
        None => encoding,
    };
    RouterRtpParameters::new(
        vec![MediaFormat::new(
            RouterMediaKind::Video,
            CodecName::Vp8,
            PayloadType::new(96),
            90_000,
        )],
        vec![],
        vec![encoding],
    )
    .with_mid(mid.to_owned())
}

pub(super) fn rtc_with_bitrate_limits(
    max_bitrate_in: Bitrate,
    max_bitrate_out: Bitrate,
) -> RtcWorker {
    let mut config = test_media_transport_config(1, test_rtc_port_range());
    config.bitrate_limits = SessionBitrateLimits::new(max_bitrate_in, max_bitrate_out);
    RtcWorker::for_test(config)
}

pub(super) fn rtc_with_codec_flags(codec_flags: MediaCodecFlags) -> RtcWorker {
    rtc_with_codec_policy(codec_flags, CodecPreferences::default())
}

pub(super) fn rtc_with_codec_policy(
    codec_flags: MediaCodecFlags,
    codec_preferences: CodecPreferences,
) -> RtcWorker {
    let mut config = test_media_transport_config(1, test_rtc_port_range());
    config.codec_flags = codec_flags;
    config.codec_preferences = codec_preferences;
    RtcWorker::for_test(config)
}

pub(super) async fn expect_initial_offer(
    adapter: &RtcWorker,
    session_key: &TransportSessionKey,
) -> SessionOffer {
    adapter
        .create_initial_session_offer("test-room", session_key)
        .await
        .expect("initial offer should succeed")
}

pub(super) async fn apply_worker_media_control(
    adapter: &RtcWorker,
    batch: WorkerMediaControlBatch,
) -> WorkerMediaControlBatchOutcome {
    adapter
        .request_worker(|response| RtcWorkerCommand::ApplyMediaControlBatch { batch, response })
        .await
        .expect("media control batch should reach the worker")
}

pub(super) async fn apply_receiver_bwe_batch(
    adapter: &RtcWorker,
    updates: impl IntoIterator<Item = ReceiverBweTargetUpdate>,
) -> Vec<TransportResult<()>> {
    let outcome = apply_worker_media_control(
        adapter,
        WorkerMediaControlBatch::ReceiverBwe(updates.into_iter().enumerate().collect()),
    )
    .await;
    let WorkerMediaControlBatchOutcome::Applied(results) = outcome else {
        panic!("receiver BWE batch should return applied results");
    };
    results
}
