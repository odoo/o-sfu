use std::sync::Arc;

use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use str0m::media::MediaKind;
#[cfg(test)]
use {
    super::super::commands::ParsedSessionAnswer,
    crate::engine::media_transport::{AppliedSessionAnswer, TransportSourceKey},
};

use super::{
    super::commands::{RtcSessionOffer, RtcWorkerCommand},
    RtcWorker,
};
use crate::engine::media_transport::{
    SessionOffer, TransportAdapterError, TransportMediaId, TransportSessionKey,
};

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
                response,
            },
        )
        .await
        .map(RtcSessionOffer::into_session_offer)
    }

    #[cfg(test)]
    pub async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.request_worker(
            |response| RtcWorkerCommand::CreateSessionRenegotiationOffer {
                session_key: session_key.clone(),
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
    pub async fn negotiated_producer_parameters(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        self.request_worker(
            |response| RtcWorkerCommand::ResolveNegotiatedProducerParameters {
                session_key: session_key.clone(),
                transport_media_id,
                response,
            },
        )
        .await
    }

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
            remote_source_control: None,
            consumer_rtp_parameters: consumer_rtp_parameters.clone(),
            active,
            response,
        })
        .await
    }
}
