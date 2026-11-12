mod membership;
mod shared;
#[cfg(any(test, feature = "testing-transport"))]
mod test_support;

pub use self::{
    membership::{
        ConnectionCloseCommit, DisconnectCommit, JoinCommit, LifecycleEffects, PresenceCommit,
    },
    shared::{ActiveUser, RoomState},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UserJoinedFanout {
    Emit,
    #[cfg(any(test, feature = "testing-transport"))]
    Suppress,
}
