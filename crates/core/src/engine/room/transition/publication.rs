//! publication transitions keep unnegotiated media out of the room graph
//!
//! ```text
//! publish intent
//!   |
//!   +-- existing producer --> activity commit --> effects after lock
//!   |
//!   +-- offer in flight ----> queued intent ---> answer ---> stage next offer
//!   |
//!   +-- new producer -------> StagedPublish ---> answer-proven RTP
//!                              |                  |
//!                              |                  v
//!                              |                room graph commit
//!                              |                  |
//!                              v                  v
//!                         rollback teardown   effects after lock
//! ```
//!
//! only answer-proven RTP enters the room graph
//! teardown, worker route updates and fanout run after state mutation releases
//! the room lock

use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use tracing::warn;

use super::super::{
    Room, RoomUserOperation, SourcePolicyGuard,
    effects::batch::{RoomEffectContext, RoomEffects},
    media_graph::{ProducerActivityCommit, PublishIntentPlan, ValidatedPublish},
};
#[cfg(any(test, feature = "testing-transport"))]
use crate::engine::{ConnectionId, UserId};
use crate::engine::{
    media_transport::{AppliedSessionAnswer, MediaTransport, TransportAdapterError},
    source_model::{SourceDeactivateIntent, SourcePublishIntent, UserStreamId},
};

mod staging;
#[cfg(any(test, feature = "testing-transport"))]
#[path = "TESTS/publication_support.rs"]
mod test_support;

pub use staging::{StagedPublish, StagedPublishes};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishIntentOutcome {
    Noop,
    Queue,
    Activated,
    Staged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeactivateIntentOutcome {
    Noop,
    RolledBack,
    Deactivated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishStageOutcome {
    Staged,
    Duplicate,
    DuplicateAfterReservation,
    #[cfg(test)]
    Rejected,
}

impl RoomUserOperation<'_> {
    #[cfg(test)]
    pub async fn stage_negotiated_publish(
        self,
        intent: &SourcePublishIntent,
    ) -> Result<PublishStageOutcome, TransportAdapterError> {
        let Some(validated_descriptor) = ({
            let state = self.room.state.read().await;
            state.validate_publish(self.user_id, self.connection_id, intent)
        }) else {
            return Ok(PublishStageOutcome::Rejected);
        };
        self.stage_validated_publish(validated_descriptor).await
    }

    async fn stage_validated_publish(
        self,
        validated_descriptor: ValidatedPublish,
    ) -> Result<PublishStageOutcome, TransportAdapterError> {
        let is_duplicate = {
            let state = self.room.state.read().await;
            state.staged_publishes.contains(
                self.user_id,
                self.connection_id,
                validated_descriptor.intent.stream_id(),
            )
        };
        if is_duplicate {
            return Ok(PublishStageOutcome::Duplicate);
        }
        let rtp_parameters = RouterRtpParameters::default();
        let media = match self
            .media_transport
            .publish_media(
                &validated_descriptor.session_key,
                validated_descriptor.intent.media_kind(),
                &rtp_parameters,
            )
            .await
        {
            Ok(media) => media,
            Err(error) => {
                warn!(
                    user_id = ?self.user_id,
                    connection_id = ?self.connection_id,
                    stream_id = %validated_descriptor.intent.stream_id(),
                    media_kind = ?validated_descriptor.intent.media_kind(),
                    "failed to stage negotiated publish stream"
                );
                return Err(error);
            }
        };
        let reserved_publish = StagedPublish::new(validated_descriptor, media);
        let duplicate = {
            let mut state = self.room.state.write().await;
            // `publish_media` ran without the room lock. Revalidate connection
            // identity and stream uniqueness before room state accepts it.
            if state
                .validate_publish_commit(&reserved_publish.descriptor, reserved_publish.media)
                .is_some()
            {
                state.staged_publishes.stage(reserved_publish)
            } else {
                Some(reserved_publish)
            }
        };
        if let Some(duplicate) = duplicate {
            duplicate.release_reserved_media(self.media_transport).await;
            return Ok(PublishStageOutcome::DuplicateAfterReservation);
        }
        Ok(PublishStageOutcome::Staged)
    }

    pub(crate) async fn start_publish(
        self,
        intent: &SourcePublishIntent,
        can_stage: bool,
    ) -> Result<PublishIntentOutcome, TransportAdapterError> {
        let has_staged_publish = {
            let state = self.room.state.read().await;
            state
                .staged_publishes
                .contains(self.user_id, self.connection_id, intent.stream_id())
        };
        if has_staged_publish {
            return Ok(PublishIntentOutcome::Noop);
        }
        let source_policy_guard = self.room.lock_source_policy().await;
        let plan = {
            let mut state = source_policy_guard.room().state.write().await;
            state.apply_publish_intent(self.user_id, self.connection_id, intent, can_stage)
        };
        match plan {
            PublishIntentPlan::Activate(commit) => {
                // Prevent another source-policy turn from interleaving with this
                // activity commit and its ordered policy and transport effects.
                execute_publication_activity(&source_policy_guard, self.media_transport, commit)
                    .await;
                drop(source_policy_guard);
                Ok(PublishIntentOutcome::Activated)
            }
            PublishIntentPlan::Noop => {
                drop(source_policy_guard);
                Ok(PublishIntentOutcome::Noop)
            }
            PublishIntentPlan::Queue => {
                drop(source_policy_guard);
                Ok(PublishIntentOutcome::Queue)
            }
            PublishIntentPlan::Stage(validated) => {
                drop(source_policy_guard);
                if self.stage_validated_publish(validated).await? == PublishStageOutcome::Staged {
                    Ok(PublishIntentOutcome::Staged)
                } else {
                    Ok(PublishIntentOutcome::Noop)
                }
            }
        }
    }

    pub async fn rollback_staged_publish(self, stream_id: &UserStreamId) -> bool {
        let Some(staged) = ({
            let mut state = self.room.state.write().await;
            state
                .staged_publishes
                .take(self.user_id, self.connection_id, stream_id)
        }) else {
            return false;
        };
        staged.release_reserved_media(self.media_transport).await;
        true
    }

    pub(crate) async fn deactivate_publication(
        self,
        intent: &SourceDeactivateIntent,
    ) -> DeactivateIntentOutcome {
        if self.rollback_staged_publish(intent.stream_id()).await {
            return DeactivateIntentOutcome::RolledBack;
        }
        // Keep publication activity and its policy effects in one serialized
        // turn so policy cannot observe the state change without its transport work.
        let source_policy_guard = self.room.lock_source_policy().await;
        let commit = {
            let mut state = source_policy_guard.room().state.write().await;
            state.apply_publication_activity(
                self.user_id,
                self.connection_id,
                intent.stream_id(),
                false,
                intent.presence(),
            )
        };
        let Ok(commit) = commit else {
            return DeactivateIntentOutcome::Noop;
        };
        execute_publication_activity(&source_policy_guard, self.media_transport, commit).await;
        drop(source_policy_guard);
        DeactivateIntentOutcome::Deactivated
    }

    /// Resolves this connection's staged publishes against an accepted answer.
    pub(crate) async fn commit_staged_publishes(self, applied_answer: &AppliedSessionAnswer) {
        // Hold one source-policy turn across the answer batch. Policy must not
        // observe a committed prefix while later answer-proven publishes remain
        // outside the room graph.
        let source_policy_guard = self.room.lock_source_policy().await;
        let staged = {
            let mut state = source_policy_guard.room().state.write().await;
            state
                .staged_publishes
                .take_for_connection(self.user_id, self.connection_id)
        };
        for publish in staged {
            publish
                .commit_answer_guarded(&source_policy_guard, self.media_transport, applied_answer)
                .await;
        }
        drop(source_policy_guard);
    }
}

async fn execute_publication_activity(
    guard: &SourcePolicyGuard<'_>,
    media_transport: &MediaTransport,
    commit: ProducerActivityCommit,
) {
    RoomEffects::from_publication_activity(commit)
        .execute_with_source_policy_guard(guard, RoomEffectContext::runtime(media_transport))
        .await;
}

impl Room {
    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub async fn has_staged_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> bool {
        self.state
            .read()
            .await
            .staged_publishes
            .contains(user_id, connection_id, stream_id)
    }
}

#[cfg(test)]
#[path = "TESTS/publication.rs"]
mod tests;
