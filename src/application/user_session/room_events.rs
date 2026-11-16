use o_sfu_protocol::wire::{
    PeerInfoPayload, PeerLeftPayload, ServerBroadcastPayload, ServerEnvelope, ServerMessage,
};

use super::{User, UserError, UserOutput};
use crate::runtime::room::{RoomEventMessage, UserOutbound};

impl User {
    pub(crate) async fn apply_room_outbound(
        &mut self,
        outbound: UserOutbound,
    ) -> Result<UserOutput, UserError> {
        let mut output = UserOutput::new();
        match outbound {
            UserOutbound::Close(_) => {}
            UserOutbound::Message(message) => match message {
                RoomEventMessage::Broadcast { sender_id, message } => {
                    output.push(ServerEnvelope::Message(ServerMessage::Broadcast(
                        ServerBroadcastPayload {
                            sender_id,
                            message: message.to_json(),
                        },
                    )));
                }
                RoomEventMessage::UserJoined { user_id, info } => {
                    output.push(ServerEnvelope::Message(ServerMessage::PeerJoined(
                        PeerInfoPayload { user_id, info },
                    )));
                }
                RoomEventMessage::UserDeparted { user_id } => {
                    output.push(ServerEnvelope::Message(ServerMessage::PeerLeft(
                        PeerLeftPayload { user_id },
                    )));
                }
                RoomEventMessage::UserInfoChanged(snapshot) => {
                    output.extend(snapshot.into_iter().map(|(user_id, info)| {
                        ServerEnvelope::Message(ServerMessage::PeerInfo(PeerInfoPayload {
                            user_id,
                            info,
                        }))
                    }));
                }
                RoomEventMessage::RecordingStateChanged(state) => {
                    output.push(ServerEnvelope::Message(ServerMessage::RecordingChange(
                        state,
                    )));
                }
            },
            UserOutbound::RemoteTracks(snapshot) => {
                let requires_negotiation = snapshot.requires_negotiation;
                output.extend(self.media.remote_tracks(snapshot));
                if requires_negotiation {
                    output.extend(self.renegotiate().await?);
                }
            }
        }
        Ok(output)
    }
}
