use std::net::SocketAddr;

use o_sfu_protocol::wire::{Envelope, PeerLeftPayload};
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, connect_async};

use super::*;
use crate::support::{TestResult, require_some};

#[tokio::test]
async fn message_reads_preserve_batch_order_and_reject_malformed_batches() -> TestResult {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let address = listener.local_addr()?;
    let server = async {
        let (stream, _) = listener.accept().await?;
        let mut websocket = accept_async(stream).await?;
        for user_ids in [
            [1, 2].as_slice(),
            [3].as_slice(),
            [4, 5, 6].as_slice(),
            [7, 8].as_slice(),
        ] {
            let envelopes = user_ids
                .iter()
                .map(|user_id| {
                    if *user_id == 5 {
                        return Ok(Envelope::message("peerLeft", None));
                    }
                    ServerEnvelope::Message(ServerMessage::PeerLeft(PeerLeftPayload {
                        user_id: UserId::Integer(*user_id),
                    }))
                    .into_envelope()
                })
                .collect::<Result<Vec<_>, _>>()?;
            websocket
                .send(tungstenite::Message::Text(
                    serde_json::to_string(&envelopes)?.into(),
                ))
                .await?;
        }
        websocket.close(None).await?;
        TestResult::Ok(())
    };
    let (sent, connected) = timeout(Duration::from_secs(1), async {
        tokio::join!(server, connect_async(format!("ws://{address}")))
    })
    .await?;
    sent?;
    let (websocket, _) = connected?;
    let mut client = ProtocolWebSocketClient {
        websocket,
        rtc_peer: require_some(FakeRtcPeer::bind(0).await, "RTC peer should bind")?,
        pending_server_envelopes: VecDeque::new(),
    };

    for user_id in [Some(1), Some(2), Some(3), None, Some(7), Some(8)] {
        assert_eq!(
            client
                .read_server_message_with_timeout(Duration::from_secs(1))
                .await,
            user_id.map(|user_id| ServerMessage::PeerLeft(PeerLeftPayload {
                user_id: UserId::Integer(user_id),
            }))
        );
    }
    Ok(())
}
