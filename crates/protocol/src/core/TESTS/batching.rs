use super::*;

#[test]
fn protocol_core_batches_outbound_control_plane_messages_until_flush_timer() -> Result<(), String> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let first_commands = core.update_info(UserInfo {
        is_talking: Some(true),
        ..UserInfo::default()
    });
    let second_commands = core.broadcast(serde_json::json!({ "kind": "notice" }));
    let flush_timer_id = expect_flush_timer(first_commands.as_slice(), "first")?;
    assert!(second_commands.is_empty());
    let flush_commands = core.on_timer(flush_timer_id);
    assert_sent_client_envelopes(
        &flush_commands,
        vec![
            ClientEnvelope::Message(ClientMessage::Info(UserInfo {
                is_talking: Some(true),
                ..UserInfo::default()
            })),
            ClientEnvelope::Message(ClientMessage::Broadcast(ClientBroadcastPayload {
                message: serde_json::json!({ "kind": "notice" }),
            })),
        ],
    );
    Ok(())
}

#[test]
fn protocol_core_publish_and_unpublish_flush_only_websocket_envelopes() -> Result<(), String> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();

    let publish_timer_id =
        expect_flush_timer(core.publish(StreamType::Camera, true).as_slice(), "publish")?;

    assert_sent_client_envelopes(
        &core.on_timer(publish_timer_id),
        vec![ClientEnvelope::Message(ClientMessage::Publish(
            StreamIntentPayload {
                stream_type: StreamType::Camera,
            },
        ))],
    );

    let unpublish_timer_id = expect_flush_timer(
        core.publish(StreamType::Camera, false).as_slice(),
        "unpublish",
    )?;

    assert_sent_client_envelopes(
        &core.on_timer(unpublish_timer_id),
        vec![ClientEnvelope::Message(ClientMessage::Unpublish(
            StreamIntentPayload {
                stream_type: StreamType::Camera,
            },
        ))],
    );
    Ok(())
}

#[test]
fn protocol_core_defers_publish_until_transport_ready() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());

    assert!(core.publish(StreamType::Camera, true).is_empty());
    assert_sent_client_envelopes(
        &core.on_transport_ready(),
        vec![ClientEnvelope::Message(ClientMessage::Publish(
            StreamIntentPayload {
                stream_type: StreamType::Camera,
            },
        ))],
    );
}

fn expect_flush_timer(commands: &[Command], label: &str) -> Result<u32, String> {
    let [
        Command::ScheduleTimer {
            id: flush_timer_id,
            ms: 100,
        },
    ] = commands
    else {
        return Err(format!("expected {label} flush timer, got {commands:?}"));
    };
    Ok(*flush_timer_id)
}
