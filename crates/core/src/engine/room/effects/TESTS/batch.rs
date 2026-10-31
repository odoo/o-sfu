#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "batch regressions fail loudly when room setup or obligation diagnostics are missing"
)]

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::Duration,
};

use tokio::{task, time};

use super::{RoomEffectContext, RoomEffects};
use crate::engine::{
    ConnectionId, UserId, UserInfo, UserPermissions,
    media_transport::{
        TransportMediaId, TransportSessionHealth,
        test_support::{test_media_transport, test_rtc_port_range},
    },
    metrics::RuntimeMetrics,
    room::{
        Room, RoomConfig, RoomManager, UserCloseReason, UserOutbound, UserOutboundReceiver,
        UserOutboundSender,
        media_graph::{PublishCommit, ReceiverRouteCommit, ReceiverRouteWork},
    },
};

#[tokio::test]
async fn dropping_signaling_or_policy_only_batch_diagnoses_unfinished_room_effects() {
    let fixture = BatchFixture::new().await;
    let presence = fixture
        .room
        .state
        .write()
        .await
        .apply_presence_update(
            &fixture.user_id,
            fixture.connection_id,
            &UserInfo {
                is_deaf: Some(true),
                ..UserInfo::default()
            },
        )
        .expect("current user presence should commit");
    assert_unfinished_room_effects(RoomEffects::from_presence(presence));
    for start_execution in [false, true] {
        let effects = RoomEffects::from_receiver_intent(ReceiverRouteCommit {
            work: ReceiverRouteWork::default(),
            track_snapshots: Vec::new(),
        });
        if start_execution {
            assert_unfinished_room_effects(
                effects.execute(&fixture.room, RoomEffectContext::state_only(None)),
            );
        } else {
            assert_unfinished_room_effects(effects);
        }
    }
}

#[test]
fn dropping_publication_wrapper_preserves_room_effects_obligation() {
    assert_unfinished_room_effects(RoomEffects::from_publish(PublishCommit {
        receiver_route_work: ReceiverRouteWork::default(),
        presence: None,
    }));
}

#[test]
fn empty_room_effects_have_no_completion_obligation() {
    drop(RoomEffects::default());
}

#[tokio::test]
async fn state_only_simulation_completes_batch_and_teardown_obligations() {
    let mut fixture = BatchFixture::new().await;
    let commit = fixture
        .room
        .state
        .write()
        .await
        .close_connection(&fixture.user_id, fixture.connection_id)
        .expect("current connection should close");
    RoomEffects::from_connection_close(commit)
        .execute(&fixture.room, RoomEffectContext::state_only(None))
        .await;
    assert!(matches!(
        fixture.outbound.try_recv(),
        Ok(UserOutbound::Close(UserCloseReason::RemovedByRuntime))
    ));
    assert!(fixture.outbound.try_recv().is_err());
    assert!(!fixture.room.test_api().has_session(&fixture.user_id).await);
}

#[tokio::test]
async fn cancelling_after_transport_teardown_keeps_room_effects_obligation() {
    let mut fixture = BatchFixture::new().await;
    let media_transport = test_media_transport(4, test_rtc_port_range())
        .expect("test transport config should be valid");
    let session = fixture
        .room
        .transport_user_key(&fixture.user_id, fixture.connection_id)
        .await;
    media_transport
        .create_initial_session_offer("room-effects-obligation", &session)
        .await
        .expect("test session should create an initial offer");
    media_transport
        .test_api()
        .set_session_transport_health(&session, TransportSessionHealth::Disconnected);
    assert_eq!(
        media_transport.session_transport_health(&session),
        Some(TransportSessionHealth::Disconnected)
    );
    let commit = fixture
        .room
        .state
        .write()
        .await
        .close_connection(&fixture.user_id, fixture.connection_id)
        .expect("current connection should close");
    let _policy_guard = fixture.room.lock_source_policy().await;
    let mut execution = Box::pin(
        RoomEffects::from_connection_close(commit)
            .execute(&fixture.room, RoomEffectContext::runtime(&media_transport)),
    );
    time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            () = execution.as_mut() => panic!("the policy lock should keep room effects pending"),
            () = async {
                while media_transport.session_transport_health(&session).is_some() {
                    task::yield_now().await;
                }
                assert!(
                    media_transport
                        .transport_media_mid(&session, TransportMediaId::new(1))
                        .await
                        .expect("worker observation should complete")
                        .is_none()
                );
            } => {}
        }
    })
    .await
    .expect("session teardown should finish while policy remains locked");
    // The worker round trip follows CloseSession's acknowledgement. Poll again
    // so only the room batch remains unfinished at the source-policy lock.
    assert!(futures_util::poll!(execution.as_mut()).is_pending());
    assert!(fixture.outbound.try_recv().is_err());
    assert_unfinished_room_effects(execution);
}

fn assert_unfinished_room_effects(value: impl Sized) {
    let panic = catch_unwind(AssertUnwindSafe(|| drop(value)))
        .expect_err("dropping pending room effects should diagnose their obligation");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied());
    assert!(message.is_some_and(|message| {
        message.contains("unfinished lifecycle obligation: room effects")
    }));
}

struct BatchFixture {
    room: Arc<Room>,
    user_id: UserId,
    connection_id: ConnectionId,
    outbound: UserOutboundReceiver,
}

impl BatchFixture {
    async fn new() -> Self {
        let room = RoomManager::for_test()
            .serve_room(
                "room-effects-obligation",
                b"room".to_vec().into(),
                &RoomConfig::default(),
                None,
            )
            .await
            .expect("test room should be served");
        let (sender, outbound) =
            UserOutboundSender::channel(128, Arc::new(RuntimeMetrics::default()));
        let user_id = UserId::Integer(1);
        let connection_id = room
            .test_api()
            .join_user(user_id.clone(), None, UserPermissions::default(), sender)
            .await
            .expect("test user should join");
        Self {
            room,
            user_id,
            connection_id,
            outbound,
        }
    }
}
