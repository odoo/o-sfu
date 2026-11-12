use o_sfu_telemetry::metrics::RuntimeMetrics;

use super::{super::outbound::VersionedRemoteTrackSnapshot, fixtures::*};
use crate::engine::{
    room::{
        RemoteTrackProjection, RemoteTrackSnapshot, UserOutboundOverflowKind,
        UserOutboundQueueLimits, UserOutboundSendError,
    },
    source_model::UserStreamId,
};

#[test]
fn remote_track_snapshot_accounts_for_string_bytes() {
    let (sender, _receiver) = UserOutboundSender::channel_with_limits(
        UserOutboundQueueLimits::new(8, 1_536),
        Arc::new(RuntimeMetrics::default()),
    );
    let snapshot = RemoteTrackSnapshot {
        tracks: vec![RemoteTrackProjection {
            consumer_mid: "0".to_owned(),
            user_id: UserId::String("x".repeat(256)),
            stream_id: UserStreamId::new("camera"),
            producer_active: true,
        }],
        requires_negotiation: true,
    };

    let UserOutboundSendError::Full(overflow) = sender
        .send(UserOutbound::RemoteTracks(snapshot))
        .expect_err("large track snapshots should exhaust byte capacity")
    else {
        panic!("large track snapshots should fail with a queue overflow");
    };

    assert_eq!(overflow.kind(), UserOutboundOverflowKind::QueuedBytes);
    assert!(overflow.message_bytes() > overflow.byte_capacity());
}

#[test]
fn remote_track_snapshots_preserve_stale_negotiation_on_latest_revision() {
    let snapshot = |revision, requires_negotiation, mid: &str| VersionedRemoteTrackSnapshot {
        snapshot: RemoteTrackSnapshot {
            tracks: vec![RemoteTrackProjection {
                consumer_mid: mid.to_owned(),
                user_id: UserId::Integer(1),
                stream_id: UserStreamId::new("camera"),
                producer_active: true,
            }],
            requires_negotiation,
        },
        revision,
    };

    for latest_requires_negotiation in [false, true] {
        let (sender, mut receiver) =
            UserOutboundSender::channel(8, Arc::new(RuntimeMetrics::default()));
        let newer_sender = sender.clone();
        newer_sender
            .send_remote_tracks(snapshot(2, latest_requires_negotiation, "new"))
            .expect("newer snapshot should enqueue");
        sender
            .send_remote_tracks(snapshot(1, false, "stale"))
            .expect("older track state should be suppressed");
        sender
            .send_remote_tracks(snapshot(1, true, "old"))
            .expect("older negotiation edge should promote the latest snapshot");

        for requires_negotiation in [latest_requires_negotiation, true] {
            let UserOutbound::RemoteTracks(snapshot) = receiver
                .try_recv()
                .expect("latest snapshot should remain queued")
            else {
                panic!("expected a remote track snapshot");
            };
            assert_eq!(snapshot.tracks[0].consumer_mid, "new");
            assert_eq!(snapshot.requires_negotiation, requires_negotiation);
        }
        assert!(receiver.try_recv().is_err());
        drop(receiver);
        assert_eq!(
            sender.send_remote_tracks(snapshot(0, false, "closed")),
            Err(UserOutboundSendError::Closed)
        );
    }
}
