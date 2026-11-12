//! Test hooks for rooms and the room manager.

use o_sfu_model::UserId;

use super::{Room, RoomManager};

mod inspect;
mod lifecycle;
mod media;

pub use self::media::NegotiatedPublish;
pub use super::{media_graph::ConsumerRouteState, placement::test_support::JoinPlacementTestGate};

#[derive(Clone, Copy)]
pub struct RoomTestApi<'a> {
    room: &'a Room,
}

#[derive(Clone, Copy)]
pub struct RoomManagerTestApi<'a> {
    manager: &'a RoomManager,
}

impl Room {
    #[must_use]
    pub const fn test_api(&self) -> RoomTestApi<'_> {
        RoomTestApi { room: self }
    }
}

impl RoomManager {
    #[must_use]
    pub const fn test_api(&self) -> RoomManagerTestApi<'_> {
        RoomManagerTestApi { manager: self }
    }
}

impl RoomManagerTestApi<'_> {
    pub async fn has_session(self, room_id: &str, user_id: &UserId) -> bool {
        let Some(room) = self.manager.get_by_uuid(room_id).await else {
            return false;
        };
        room.test_api().has_session(user_id).await
    }
}
