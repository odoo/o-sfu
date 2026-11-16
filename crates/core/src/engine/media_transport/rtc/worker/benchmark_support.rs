use std::sync::Arc;

#[cfg(test)]
use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
#[cfg(test)]
use str0m::media::MediaKind;
#[cfg(test)]
use {
    super::super::{commands::ParsedSessionAnswer, negotiation::QueuedChange},
    crate::engine::{
        media_transport::{AppliedSessionAnswer, TransportMediaId, TransportSourceKey},
        source_model::SourceSyncPolicy,
    },
};

use super::{
    super::commands::{RtcSessionOffer, RtcWorkerCommand},
    RtcWorker,
};
use crate::engine::media_transport::{SessionOffer, TransportAdapterError, TransportSessionKey};

impl RtcWorker {
    pub async fn create_initial_session_offer(
        &self,
        room_id: &str,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        let room_id: Arc<str> = Arc::from(room_id);
        self.request_worker(
            move |response| RtcWorkerCommand::CreateInitialSessionOffer {
                room_id,
                session_key: session_key.clone(),
                mid_reuse: true,
                response,
            },
        )
        .await
        .map(RtcSessionOffer::into_session_offer)
    }

    /// Offers every queued change. Worker tests have no room, so each queued
    /// consumer counts as committed.
    #[cfg(test)]
    pub async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        let queued_session = session_key.clone();
        let committed_consumers = self
            .read_debug_worker(move |state, _context| {
                state
                    .users
                    .get(&queued_session)
                    .map_or_else(Vec::new, |session| {
                        session
                            .sdp_negotiation
                            .queue
                            .iter()
                            .filter_map(|change| match change {
                                QueuedChange::AddSend { media, .. } => Some(*media),
                                QueuedChange::AddRecv { .. } | QueuedChange::Remove(_) => None,
                            })
                            .collect()
                    })
            })
            .await
            .map_err(|_unavailable| TransportAdapterError::TransportUnavailable)?;
        self.request_worker(
            |response| RtcWorkerCommand::CreateSessionRenegotiationOffer {
                session_key: session_key.clone(),
                committed_consumers,
                response,
            },
        )
        .await
        .map(RtcSessionOffer::into_session_offer)
    }

    #[cfg(test)]
    pub async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        let answer = ParsedSessionAnswer::parse(answer_sdp)?;
        self.request_worker(|response| RtcWorkerCommand::ApplySessionAnswer {
            session_key: session_key.clone(),
            answer,
            response,
        })
        .await
    }
    pub async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::CloseSession {
            session_key: session_key.clone(),
            response,
        })
        .await
    }
    #[cfg(test)]
    pub async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::RemoveMedia {
            session_key: session_key.clone(),
            transport_media_id,
            response,
        })
        .await
    }

    #[cfg(test)]
    pub async fn add_recv_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::AddRecvMedia {
            session_key: session_key.clone(),
            media_kind,
            rtp_parameters: rtp_parameters.clone(),
            response,
        })
        .await
    }

    #[cfg(test)]
    pub async fn add_send_media(
        &self,
        consumer_key: &TransportSessionKey,
        media_kind: MediaKind,
        source: TransportSourceKey,
        consumer_rtp_parameters: &RouterRtpParameters,
        active: bool,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::AddSendMedia {
            consumer_key: consumer_key.clone(),
            media_kind,
            source,
            sync: SourceSyncPolicy::Independent,
            remote_source_control: None,
            consumer_rtp_parameters: consumer_rtp_parameters.clone(),
            active,
            response,
        })
        .await
    }
}
