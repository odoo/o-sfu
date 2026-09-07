use itertools::Itertools;
use o_sfu_rfc::rtp::{Mid, Rid, Ssrc};
use o_sfu_router::{
    MediaKind as RouterMediaKind, RouterError,
    rtp::{MediaFormat, MediaStream as RouterRtpParameters},
};
use tracing::{error, warn};

use super::{
    super::{
        outbound::{OutboundSender, VersionedRemoteTrackSnapshot},
        state::{PresenceCommit, RoomState},
    },
    ReceiverRouteWork,
    source_index::PublishedSources,
    subscription::ReceiverRouteScope,
};
use crate::{
    Bitrate,
    engine::{
        ConnectionId, UserId, UserInfo,
        media_transport::{
            ProducerActivity, SessionUploadEncoding, SourceActivityUpdate, TransportMediaId,
            TransportSessionKey, TransportSourceActivityEffect, TransportSourceKey,
        },
        source_model::{
            PublishedSourceDescriptor, PublishedSourceDescriptorParts, PublishedSourceId,
            PublishedSourceOwner, SourceEncodingDescriptor, SourceEncodingDescriptorParts,
            SourceModelError, SourcePolicy, SourcePublishIntent, UploadLayerPolicyRole,
            UserStreamId,
        },
    },
};

#[derive(Debug, Clone)]
pub struct ValidatedPublish {
    pub session_key: TransportSessionKey,
    pub stream_id: UserStreamId,
    pub media_kind: RouterMediaKind,
    pub policy: SourcePolicy,
    pub presence: Option<UserInfo>,
}

#[derive(Debug)]
pub struct PublishCommit {
    pub receiver_route_work: ReceiverRouteWork,
    pub presence: Option<PresenceCommit>,
}

#[derive(Debug)]
pub enum PublishIntentPlan {
    Activate(ProducerActivityCommit),
    Noop,
    Queue,
    Stage(ValidatedPublish),
}

#[derive(Debug)]
pub struct ProducerActivityCommit {
    pub source: TransportSourceKey,
    pub stream_id: UserStreamId,
    pub update: SourceActivityUpdate,
    pub remote_activity_effects: Vec<TransportSourceActivityEffect>,
    pub track_snapshots: Vec<(OutboundSender, VersionedRemoteTrackSnapshot)>,
    pub presence: Option<PresenceCommit>,
}

#[derive(Debug)]
pub enum ProducerActivityRejection {
    MissingPublication,
    StalePublication,
}

#[derive(Debug, thiserror::Error)]
pub(in crate::engine::room) enum PublicationCommitError {
    #[error(transparent)]
    Source(#[from] SourceModelError),
    #[error(transparent)]
    Router(#[from] RouterError),
}

impl RoomState {
    pub fn apply_publish_intent(
        &mut self,
        user_id: &UserId,
        publisher_connection_id: ConnectionId,
        intent: &SourcePublishIntent,
        can_stage: bool,
    ) -> PublishIntentPlan {
        let Some(user) = self.users.get(user_id) else {
            warn!(
                ?user_id,
                publisher_connection_id = ?publisher_connection_id,
                stream_id = %intent.stream_id(),
                "cannot start publish because the user is missing from room state"
            );
            return PublishIntentPlan::Noop;
        };
        if user.connection_id != publisher_connection_id {
            warn!(
                ?user_id,
                publisher_connection_id = ?publisher_connection_id,
                current_connection_id = ?user.connection_id,
                stream_id = %intent.stream_id(),
                "cannot start publish because the connection is stale"
            );
            return PublishIntentPlan::Noop;
        }
        if self
            .published_source_id(user_id, publisher_connection_id, intent.stream_id())
            .is_some()
        {
            return self
                .apply_publication_activity(
                    user_id,
                    publisher_connection_id,
                    intent.stream_id(),
                    true,
                    intent.presence(),
                )
                .map_or_else(|_| PublishIntentPlan::Noop, PublishIntentPlan::Activate);
        }
        if !can_stage {
            return PublishIntentPlan::Queue;
        }
        self.validate_publish(user_id, publisher_connection_id, intent)
            .map_or(PublishIntentPlan::Noop, PublishIntentPlan::Stage)
    }

    pub fn validate_publish(
        &self,
        user_id: &UserId,
        publisher_connection_id: ConnectionId,
        intent: &SourcePublishIntent,
    ) -> Option<ValidatedPublish> {
        let Some(user) = self.users.get(user_id) else {
            warn!(
                ?user_id,
                publisher_connection_id = ?publisher_connection_id,
                stream_id = %intent.stream_id(),
                "cannot prepare negotiated publish because the user is missing from room state"
            );
            return None;
        };
        if user.connection_id != publisher_connection_id {
            warn!(
                ?user_id,
                publisher_connection_id = ?publisher_connection_id,
                current_connection_id = ?user.connection_id,
                stream_id = %intent.stream_id(),
                "cannot prepare negotiated publish because the connection is stale"
            );
            return None;
        }
        if user.parsed_client_rtp_capabilities.is_none() {
            warn!(
                ?user_id,
                publisher_connection_id = ?publisher_connection_id,
                stream_id = %intent.stream_id(),
                "cannot prepare negotiated publish because the user is not publish-ready"
            );
            return None;
        }
        Some(ValidatedPublish {
            session_key: self.transport_user_key(user_id, publisher_connection_id),
            stream_id: intent.stream_id().clone(),
            media_kind: intent.media_kind(),
            policy: intent.policy(),
            presence: intent.presence().cloned(),
        })
    }

    pub fn commit_publish_reservation(
        &mut self,
        publish: ValidatedPublish,
        consumable_rtp_parameters: RouterRtpParameters,
        upload_encodings: &[SessionUploadEncoding],
        transport_media_id: TransportMediaId,
    ) -> Option<PublishCommit> {
        self.validate_publish_commit(&publish, transport_media_id)?;
        let owner_user_id = publish.session_key.user_id().clone();
        let owner_connection_id = publish.session_key.connection_id();
        let stream_id = publish.stream_id.clone();
        let presence = publish.presence.clone();
        let source_id = match self.topology.commit_publication(
            publish,
            consumable_rtp_parameters,
            upload_encodings,
            transport_media_id,
        ) {
            Ok(source_id) => source_id,
            Err(error) => {
                error!(
                    user_id = ?owner_user_id,
                    ?owner_connection_id,
                    stream_id = %stream_id,
                    ?transport_media_id,
                    ?error,
                    "failed to commit negotiated publication"
                );
                return None;
            }
        };
        let receiver_route_work =
            self.plan_missing_receiver_routes(ReceiverRouteScope::Source(source_id));
        let presence = presence.and_then(|info| {
            self.apply_presence_update(&owner_user_id, owner_connection_id, &info)
        });
        Some(PublishCommit {
            receiver_route_work,
            presence,
        })
    }

    pub(in crate::engine::room) fn validate_publish_commit(
        &self,
        publish: &ValidatedPublish,
        transport_media_id: TransportMediaId,
    ) -> Option<()> {
        let user_id = publish.session_key.user_id();
        let connection_id = publish.session_key.connection_id();
        let Some(user) = self.users.get(user_id) else {
            warn!(
                ?user_id,
                ?connection_id,
                stream_id = %publish.stream_id,
                ?transport_media_id,
                "cannot commit negotiated publish because the user is missing from room state"
            );
            return None;
        };
        let publish_ready = user.parsed_client_rtp_capabilities.is_some();
        if user.connection_id != connection_id || !publish_ready {
            warn!(
                ?user_id,
                ?connection_id,
                current_connection_id = ?user.connection_id,
                publish_ready,
                stream_id = %publish.stream_id,
                ?transport_media_id,
                "cannot commit negotiated publish because the user state changed before commit"
            );
            return None;
        }
        if self
            .topology
            .source_id_for_owner_stream(user_id, &publish.stream_id)
            .is_some()
        {
            warn!(
                ?user_id,
                ?connection_id,
                stream_id = %publish.stream_id,
                ?transport_media_id,
                "cannot commit negotiated publish because a source already exists for this stream"
            );
            return None;
        }
        Some(())
    }

    #[must_use]
    pub fn producer_stream_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<UserStreamId> {
        self.topology
            .source_for_transport_media(transport_media_id)
            .map(|source| source.descriptor.stream_id().clone())
    }

    #[must_use]
    pub fn published_source_id(
        &self,
        owner: &UserId,
        connection: ConnectionId,
        stream: &UserStreamId,
    ) -> Option<PublishedSourceId> {
        self.topology.published_source_id(owner, connection, stream)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn published_source_id_for_user(
        &self,
        user: &UserId,
        stream: &UserStreamId,
    ) -> Option<PublishedSourceId> {
        self.published_source_id(user, self.user_connection_id(user)?, stream)
    }

    pub fn apply_publication_activity(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
        active: bool,
        presence: Option<&UserInfo>,
    ) -> Result<ProducerActivityCommit, ProducerActivityRejection> {
        let source_id = self
            .published_source_id(user_id, connection_id, stream_id)
            .ok_or(ProducerActivityRejection::MissingPublication)?;
        let revision = self
            .topology
            .set_published_source_activity(source_id, connection_id, active)
            .ok_or(ProducerActivityRejection::StalePublication)?;
        let source_recipients = self
            .topology
            .committed_consumer_user_ids_for_source(source_id);
        let source = self
            .topology
            .published_source(source_id)
            .ok_or(ProducerActivityRejection::StalePublication)?
            .transport
            .clone();
        let update = SourceActivityUpdate::new(ProducerActivity::from_active(active), revision);
        let remote_activity_effects = self.topology.source_activity_effects(&source, update);
        let presence =
            presence.and_then(|info| self.apply_presence_update(user_id, connection_id, info));
        Ok(ProducerActivityCommit {
            source,
            stream_id: stream_id.clone(),
            update,
            remote_activity_effects,
            track_snapshots: self.remote_track_snapshots_for_users(source_recipients, false),
            presence,
        })
    }
}

pub(super) fn allocate_source_descriptor(
    sources: &mut PublishedSources,
    publish: &ValidatedPublish,
    consumable_rtp_parameters: &RouterRtpParameters,
    upload_encodings: &[SessionUploadEncoding],
) -> Result<PublishedSourceDescriptor, SourceModelError> {
    let source_id = sources.allocate_id();
    let encodings = consumable_rtp_parameters
        .bindings()
        .map(|binding| {
            let upload_profile = upload_profile_for_rid(upload_encodings, binding.rid());
            SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
                encoding_id: sources.allocate_encoding_id(),
                source_id,
                rid: binding.rid().map(Rid::new),
                primary_ssrc: binding.ssrc().map(Ssrc::new),
                repair_ssrc: None,
                max_bitrate: binding
                    .max_bitrate()
                    .map(Bitrate::from_bps)
                    .or_else(|| upload_profile.and_then(MatchedUploadEncoding::max_bitrate)),
                resolution_scale: upload_profile.and_then(MatchedUploadEncoding::resolution_scale),
                max_framerate: upload_profile.and_then(MatchedUploadEncoding::max_framerate),
                policy_role: upload_profile.map(|profile| {
                    upload_layer_policy_role_for_rank(profile.rank, upload_encodings.len())
                }),
                negotiated_format: negotiated_format_for_binding(
                    consumable_rtp_parameters,
                    binding.payload_type(),
                ),
            })
        })
        .collect::<Vec<_>>();
    PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id,
        owner: PublishedSourceOwner::new(publish.session_key.user_id().clone()),
        stream_id: publish.stream_id.clone(),
        media_kind: publish.media_kind,
        policy: publish.policy,
        mid: consumable_rtp_parameters.mid().map(Mid::new),
        encodings,
    })
}

fn upload_profile_for_rid<'a>(
    upload_encodings: &'a [SessionUploadEncoding],
    rid: Option<&str>,
) -> Option<MatchedUploadEncoding<'a>> {
    let rid = rid?;
    upload_encodings
        .iter()
        .enumerate()
        .find(|(_rank, encoding)| encoding.rid == rid)
        .map(|(rank, encoding)| MatchedUploadEncoding { rank, encoding })
}

#[derive(Debug, Clone, Copy)]
struct MatchedUploadEncoding<'a> {
    rank: usize,
    encoding: &'a SessionUploadEncoding,
}

impl MatchedUploadEncoding<'_> {
    const fn max_bitrate(self) -> Option<Bitrate> {
        self.encoding.max_bitrate
    }

    const fn resolution_scale(self) -> Option<u16> {
        self.encoding.resolution_scale
    }

    const fn max_framerate(self) -> Option<u16> {
        self.encoding.max_framerate
    }
}

const fn upload_layer_policy_role_for_rank(
    rank: usize,
    layer_count: usize,
) -> UploadLayerPolicyRole {
    if layer_count > 1 && rank + 1 == layer_count {
        UploadLayerPolicyRole::Featured
    } else if rank == 0 && layer_count > 2 {
        UploadLayerPolicyRole::DegradedThumbnail
    } else {
        UploadLayerPolicyRole::Thumbnail
    }
}

fn negotiated_format_for_binding(
    parameters: &RouterRtpParameters,
    payload_type: Option<u8>,
) -> Option<MediaFormat> {
    if let Some(payload_type) = payload_type
        && let Some(format) = parameters
            .formats()
            .find(|format| format.payload_type() == payload_type)
    {
        return Some(format.clone());
    }
    parameters
        .formats()
        .find_or_first(|format| !format.codec().is_rtx())
        .cloned()
}
