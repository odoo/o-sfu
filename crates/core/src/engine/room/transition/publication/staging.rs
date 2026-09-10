use std::{
    collections::{BTreeMap, btree_map::Entry},
    marker::PhantomData,
};

use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::{info, warn};

use crate::engine::{
    ConnectionId, UserId,
    media_transport::{
        AppliedSessionAnswer, MediaTransport, SessionUploadEncoding, TransportMediaId,
        TransportTeardown,
    },
    room::{
        SourcePolicyGuard,
        effects::batch::{RoomEffectContext, RoomEffects},
        media_graph::ValidatedPublish,
    },
    source_model::UserStreamId,
};
#[cfg(test)]
use crate::engine::{TestSourceKind, source_model::test_support::stream_id_for_source};

#[derive(Debug, Default)]
pub struct StagedPublishes {
    staged: BTreeMap<StagedPublishKey, StagedPublish>,
}

type StagedPublishKey = (UserId, ConnectionId, UserStreamId);

#[derive(Debug)]
pub struct StagedPublish {
    pub(super) descriptor: ValidatedPublish,
    pub(super) media: TransportMediaId,
    reservation: PublishReservation<Reserved>,
}

impl StagedPublishes {
    pub fn contains(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> bool {
        self.staged
            .contains_key(&staged_publish_key(user_id, connection_id, stream_id))
    }

    pub fn stage(&mut self, publish: StagedPublish) -> Option<StagedPublish> {
        let key = publish.key();
        if let Entry::Vacant(slot) = self.staged.entry(key) {
            slot.insert(publish);
            return None;
        }
        Some(publish)
    }

    pub fn take_teardowns_for_connection(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Vec<TransportTeardown> {
        self.take_for_connection(user_id, connection_id)
            .into_iter()
            .map(StagedPublish::release_into_teardown)
            .collect()
    }

    pub(in crate::engine::room) fn take(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<StagedPublish> {
        self.staged
            .remove(&staged_publish_key(user_id, connection_id, stream_id))
    }

    pub(in crate::engine::room) fn take_for_connection(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Vec<StagedPublish> {
        self.staged
            .extract_if(.., |key, _publish| {
                &key.0 == user_id && key.1 == connection_id
            })
            .map(|(_key, publish)| publish)
            .collect()
    }

    #[cfg(test)]
    pub fn staged_count(&self, user_id: &UserId, connection_id: ConnectionId) -> usize {
        self.staged
            .keys()
            .filter(|(user, connection, _stream)| user == user_id && *connection == connection_id)
            .count()
    }

    #[cfg(test)]
    pub fn staged_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: TestSourceKind,
    ) -> Option<TransportMediaId> {
        let stream_id = stream_id_for_source(stream_type);
        self.staged
            .get(&staged_publish_key(user_id, connection_id, &stream_id))
            .map(|publish| publish.media)
    }
}

fn staged_publish_key(
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_id: &UserStreamId,
) -> StagedPublishKey {
    (user_id.clone(), connection_id, stream_id.clone())
}

impl StagedPublish {
    pub fn new(descriptor: ValidatedPublish, transport_media_id: TransportMediaId) -> Self {
        Self {
            descriptor,
            media: transport_media_id,
            reservation: PublishReservation::new(),
        }
    }

    fn key(&self) -> StagedPublishKey {
        staged_publish_key(
            self.descriptor.session_key.user_id(),
            self.descriptor.session_key.connection_id(),
            self.descriptor.intent.stream_id(),
        )
    }

    pub(super) async fn commit_answer_guarded(
        self,
        guard: &SourcePolicyGuard<'_>,
        media_transport: &MediaTransport,
        applied_answer: &AppliedSessionAnswer,
    ) -> Option<UserStreamId> {
        let media = self.media;
        let Some(rtp) = applied_answer
            .negotiated_producer_parameters(media)
            .cloned()
        else {
            let user = self.descriptor.session_key.user_id().clone();
            let connection = self.descriptor.session_key.connection_id();
            let stream_id = self.descriptor.intent.stream_id().clone();
            self.release_reserved_media(media_transport).await;
            warn!(
                user_id = ?user,
                connection_id = ?connection,
                stream_id = %stream_id,
                transport_media_id = ?media,
                "answered negotiation did not include staged publish parameters during room commit"
            );
            return None;
        };
        let encodings = applied_answer.negotiated_producer_upload_encodings(media);
        self.commit_rtp_guarded(guard, media_transport, rtp, encodings)
            .await
    }

    pub(super) async fn commit_rtp_guarded(
        self,
        guard: &SourcePolicyGuard<'_>,
        media_transport: &MediaTransport,
        rtp: RouterRtpParameters,
        upload_encodings: &[SessionUploadEncoding],
    ) -> Option<UserStreamId> {
        let room = guard.room();
        let user = self.descriptor.session_key.user_id().clone();
        let connection = self.descriptor.session_key.connection_id();
        let stream_id = self.descriptor.intent.stream_id().clone();
        let media = self.media;
        let worker = self.descriptor.session_key.media_worker_id();
        let committed = {
            let mut state = room.state.write().await;
            state.commit_publish_reservation(self.descriptor.clone(), rtp, upload_encodings, media)
        };
        let Some(commit) = committed else {
            self.release_reserved_media(media_transport).await;
            warn!(
                user_id = ?user,
                connection_id = ?connection,
                stream_id = %stream_id,
                transport_media_id = ?media,
                "room rejected staged negotiated publish during commit"
            );
            return None;
        };
        // Room topology now owns the transport media. Disarm only after
        // acceptance so rejected commits still take the explicit teardown path.
        self.commit_reservation();
        RoomEffects::from_publish(commit)
            .execute_with_source_policy_guard(guard, RoomEffectContext::runtime(media_transport))
            .await;
        info!(
            event = telemetry_event::PUBLISH_COMMITTED,
            room_id = room.uuid(),
            user_id = %user.path_segment(),
            connection_id = connection.as_u64(),
            media_worker_id = worker.as_usize(),
            transport_media_id = media.as_u64(),
            "publication committed"
        );
        Some(stream_id)
    }

    pub(super) async fn release_reserved_media(self, media_transport: &MediaTransport) {
        media_transport
            .teardown([self.release_into_teardown()])
            .await;
    }

    pub(super) fn release_into_teardown(self) -> TransportTeardown {
        let Self {
            descriptor,
            media,
            reservation,
        } = self;
        let _released = reservation.release();
        TransportTeardown::RemoveMedia {
            session_key: descriptor.session_key,
            transport_media_id: media,
        }
    }

    fn commit_reservation(self) {
        let _committed = self.reservation.commit();
    }
}

#[derive(Debug)]
struct Reserved;

#[derive(Debug)]
struct Committed;

#[derive(Debug)]
struct Released;

#[derive(Debug)]
#[must_use = "publish reservations must be committed or released"]
/// Typestate guard requiring reserved transport media to be committed or released.
///
/// Tests and debug builds diagnose an armed reservation on `Drop`. Cleanup
/// paths must explicitly convert reserved media into [`TransportTeardown`].
struct PublishReservation<State> {
    guard: PublishReservationGuard,
    _state: PhantomData<fn() -> State>,
}

#[derive(Debug)]
struct PublishReservationGuard {
    armed: bool,
}

impl PublishReservation<Reserved> {
    fn new() -> Self {
        Self {
            guard: PublishReservationGuard { armed: true },
            _state: PhantomData,
        }
    }

    fn commit(mut self) -> PublishReservation<Committed> {
        self.guard.disarm();
        PublishReservation {
            guard: self.guard,
            _state: PhantomData,
        }
    }

    fn release(mut self) -> PublishReservation<Released> {
        self.guard.disarm();
        PublishReservation {
            guard: self.guard,
            _state: PhantomData,
        }
    }
}

impl PublishReservationGuard {
    fn disarm(&mut self) {
        debug_assert!(self.armed);
        self.armed = false;
    }
}

impl Drop for PublishReservationGuard {
    fn drop(&mut self) {
        #[cfg(test)]
        assert!(
            !self.armed,
            "publish reservation dropped while still reserved"
        );
        #[cfg(all(debug_assertions, not(test)))]
        debug_assert!(
            !self.armed,
            "publish reservation dropped while still reserved"
        );
    }
}
