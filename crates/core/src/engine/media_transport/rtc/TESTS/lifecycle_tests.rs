use futures_util::future::join_all;
use str0m::{Event, IceConnectionState};
use tokio::{sync::oneshot, task::spawn_blocking, time::timeout};

use super::fixtures::*;
use crate::engine::media_transport::rtc::{
    commands::{RtcWorkerCommand, WorkerMediaControlBatch},
    packet_loop::{transport_health_from_event, transport_ice_state},
    state::TransportSessionHealth,
};

fn expect_first_candidate_port(offer_sdp: &str) -> u16 {
    offer_sdp
        .lines()
        .find_map(|line| line.trim().strip_prefix("a=candidate:"))
        .and_then(|candidate| candidate.split_whitespace().nth(5))
        .and_then(|port| port.parse::<u16>().ok())
        .expect("offer should expose at least one parseable candidate line")
}

#[tokio::test]
async fn rtc_initial_session_offer_contains_real_ice_and_dtls_parameters() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 13, UserId::Integer(13));

    let offer_sdp = expect_initial_offer(&adapter, &session_key)
        .await
        .into_parts()
        .0;

    assert!(offer_sdp.contains("a=ice-ufrag:"));
    assert!(offer_sdp.contains("a=ice-pwd:"));
    assert!(offer_sdp.contains("a=setup:actpass"));
    assert!(offer_sdp.contains("a=fingerprint:sha-256 "));

    let candidate_port = expect_first_candidate_port(&offer_sdp);
    assert_ne!(candidate_port, 0);
}

#[test]
fn rtc_transport_health_maps_connected_and_disconnected_events() {
    assert_eq!(
        transport_health_from_event(&Event::Connected),
        Some(TransportSessionHealth::Connected)
    );
    assert_eq!(
        transport_health_from_event(&Event::IceConnectionStateChange(
            IceConnectionState::Connected
        )),
        Some(TransportSessionHealth::Connected)
    );
    assert_eq!(
        transport_health_from_event(&Event::IceConnectionStateChange(
            IceConnectionState::Disconnected
        )),
        Some(TransportSessionHealth::Disconnected)
    );
    assert_eq!(
        transport_health_from_event(&Event::IceConnectionStateChange(IceConnectionState::New)),
        None
    );
}

#[test]
fn rtc_transport_ice_state_metric_maps_all_supported_states() {
    use crate::engine::metrics::TransportIceState;

    assert_eq!(
        transport_ice_state(IceConnectionState::New),
        TransportIceState::New
    );
    assert_eq!(
        transport_ice_state(IceConnectionState::Checking),
        TransportIceState::Checking
    );
    assert_eq!(
        transport_ice_state(IceConnectionState::Connected),
        TransportIceState::Connected
    );
    assert_eq!(
        transport_ice_state(IceConnectionState::Completed),
        TransportIceState::Completed
    );
    assert_eq!(
        transport_ice_state(IceConnectionState::Disconnected),
        TransportIceState::Disconnected
    );
}

#[tokio::test]
async fn rtc_transport_close_session_allows_recreating_the_initial_offer() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 14, UserId::Integer(14));

    let _offer = expect_initial_offer(&adapter, &session_key).await;
    assert!(adapter.close_session(&session_key).await.is_ok());
    let _offer = expect_initial_offer(&adapter, &session_key).await;
}

#[tokio::test]
async fn rtc_transport_close_session_cleans_transport_health_snapshot() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 143, UserId::Integer(143));
    let _offer = expect_initial_offer(&adapter, &session_key).await;

    adapter.debug_set_session_transport_health(&session_key, TransportSessionHealth::Disconnected);
    let metrics_snapshot = adapter.metrics.snapshot();
    assert_eq!(metrics_snapshot.connected_transport_users(), 0);
    assert_eq!(metrics_snapshot.disconnected_transport_users(), 1);
    assert_eq!(
        adapter.session_transport_health(&session_key),
        Some(TransportSessionHealth::Disconnected)
    );

    assert!(adapter.close_session(&session_key).await.is_ok());
    assert_eq!(adapter.session_transport_health(&session_key), None);
    let metrics_snapshot = adapter.metrics.snapshot();
    assert_eq!(metrics_snapshot.connected_transport_users(), 0);
    assert_eq!(metrics_snapshot.disconnected_transport_users(), 0);
}

#[tokio::test]
async fn rtc_transport_close_session_cleans_remote_addr_demux_state() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 140, UserId::Integer(140));
    let _offer = expect_initial_offer(&adapter, &session_key).await;

    let source_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 45_000);
    adapter
        .debug_remember_remote_addr(source_addr, &session_key)
        .await;
    assert_eq!(
        adapter.debug_remote_addr_owner(source_addr).await,
        Some(session_key.clone())
    );

    assert!(adapter.close_session(&session_key).await.is_ok());

    assert_eq!(adapter.debug_remote_addr_owner(source_addr).await, None);
    assert!(!adapter.debug_has_any_remote_addr_session().await);
}

#[tokio::test]
async fn rtc_transport_close_last_session_reuses_idle_packet_loop_worker() {
    let adapter = RtcWorker::default();
    let first_session_key = transport_key(1, 141, UserId::Integer(141));
    let _offer = expect_initial_offer(&adapter, &first_session_key).await;
    assert!(adapter.close_session(&first_session_key).await.is_ok());

    let second_session_key = transport_key(1, 142, UserId::Integer(142));
    let _offer = expect_initial_offer(&adapter, &second_session_key).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rtc_worker_finishes_accepted_commands_and_cancelled_drop_is_nonblocking() {
    let adapter = RtcWorker::default();
    let first_session = transport_key(1, 144, UserId::Integer(144));
    let second_session = transport_key(1, 145, UserId::Integer(145));
    let _offer = expect_initial_offer(&adapter, &first_session).await;
    let _offer = expect_initial_offer(&adapter, &second_session).await;
    let worker_handle = adapter.test_handle();
    let updates = [(first_session.clone(), 640), (second_session.clone(), 720)];
    timeout(Duration::from_secs(1), async {
        let (release_probe_tx, probe) = adapter
            .pause_for_test()
            .await
            .expect("blocking probe should enter the worker");
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::ApplyMediaControlBatch {
                batch: WorkerMediaControlBatch::ReceiverBwe(
                    updates
                        .iter()
                        .enumerate()
                        .map(|(index, (session, kbps))| {
                            (
                                index,
                                ReceiverBweTargetUpdate::new(
                                    session.clone(),
                                    Bitrate::from_kbps(*kbps),
                                ),
                            )
                        })
                        .collect(),
                ),
                response: response_tx,
            })
            .await
            .expect("worker should accept the media removal");
        drop(response_rx);
        release_probe_tx
            .send(())
            .expect("blocking probe should still be waiting");
        assert_eq!(probe.await.expect("probe task should complete"), Some(()));
        for (session, kbps) in updates {
            assert_eq!(
                adapter.debug_session_receiver_bwe_target(&session).await,
                Some(Bitrate::from_kbps(kbps))
            );
        }
    })
    .await
    .expect("accepted worker command should complete before timeout");
    let (release, probe) = adapter
        .pause_for_test()
        .await
        .expect("blocking probe should enter the worker");

    adapter.cancel();
    let dropped = timeout(
        Duration::from_secs(1),
        spawn_blocking(move || drop(adapter)),
    )
    .await;

    release
        .send(())
        .expect("blocked worker should still be waiting");
    assert!(matches!(dropped, Ok(Ok(()))));
    assert!(matches!(
        timeout(Duration::from_secs(1), probe).await,
        Ok(Ok(Some(())))
    ));
}

#[tokio::test]
async fn rtc_transport_distinguishes_same_session_id_across_channels() {
    let adapter = RtcWorker::default();
    let first_session_key = transport_key_on_worker(1, 0, 30, UserId::Integer(30));
    let second_session_key = transport_key_on_worker(2, 1, 30, UserId::Integer(30));

    let _offer = expect_initial_offer(&adapter, &first_session_key).await;
    let _offer = expect_initial_offer(&adapter, &second_session_key).await;

    adapter.debug_set_session_transport_health(
        &second_session_key,
        TransportSessionHealth::Disconnected,
    );
    assert!(adapter.close_session(&first_session_key).await.is_ok());
    assert_eq!(
        adapter.session_transport_health(&second_session_key),
        Some(TransportSessionHealth::Disconnected)
    );
}

#[tokio::test]
async fn rtc_transport_concurrent_initial_offers_deliver_all_worker_responses() {
    let adapter = Arc::new(RtcWorker::default());
    let results = timeout(
        Duration::from_secs(1),
        join_all((0_u32..8).map(|offset| {
            let session_key = transport_key(
                3,
                200_u64 + u64::from(offset),
                UserId::Integer(200_i64 + i64::from(offset)),
            );
            let adapter = Arc::clone(&adapter);
            async move {
                adapter
                    .create_initial_session_offer("test-room", &session_key)
                    .await
            }
        })),
    )
    .await;
    assert!(results.is_ok());
    let Ok(results) = results else {
        return;
    };

    for result in results {
        assert!(result.is_ok());
    }
}

#[tokio::test]
async fn rtc_transport_concurrent_last_session_close_keeps_worker_reusable() {
    let adapter = Arc::new(RtcWorker::default());
    let first_session_key = transport_key(4, 301, UserId::Integer(301));
    let second_session_key = transport_key(4, 302, UserId::Integer(302));

    let _offer = expect_initial_offer(&adapter, &first_session_key).await;
    let _offer = expect_initial_offer(&adapter, &second_session_key).await;

    let close_results = timeout(Duration::from_secs(1), async {
        tokio::join!(
            adapter.close_session(&first_session_key),
            adapter.close_session(&second_session_key),
        )
    })
    .await;
    assert!(close_results.is_ok());
    let Ok((first_close, second_close)) = close_results else {
        return;
    };
    assert!(first_close.is_ok());
    assert!(second_close.is_ok());

    let next_session_key = transport_key(4, 303, UserId::Integer(303));
    let _offer = expect_initial_offer(&adapter, &next_session_key).await;
}
