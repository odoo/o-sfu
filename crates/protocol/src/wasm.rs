use serde::{Serialize, de::DeserializeOwned};
use wasm_bindgen::{JsValue, prelude::wasm_bindgen};

use crate::{
    core::{Command, NegotiationKind, ProtocolCore},
    shared::{JsonPayload, StreamType},
};

/// wasm-bindgen facade for the browser [`ProtocolCore`] contract
///
/// protocol transitions return serialized commands as plain JS objects for
/// the TypeScript browser runtime to execute outside [`ProtocolCore`]
#[wasm_bindgen(js_name = ProtocolCoreWasm)]
pub struct WasmProtocolCore {
    inner: ProtocolCore,
}

#[wasm_bindgen(js_class = ProtocolCoreWasm)]
impl WasmProtocolCore {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: ProtocolCore::new(),
        }
    }

    /// # Errors
    ///
    /// Returns a string-valued [`JsValue`] if command serialization fails.
    pub fn connect(
        &mut self,
        url: String,
        jwt: String,
        room: Option<String>,
    ) -> Result<JsValue, JsValue> {
        commands_to_js(self.inner.connect(url, jwt, room))
    }

    /// # Errors
    ///
    /// Returns a string-valued [`JsValue`] if command serialization fails.
    #[wasm_bindgen(js_name = onWsOpen)]
    pub fn on_ws_open(&mut self) -> Result<JsValue, JsValue> {
        commands_to_js(self.inner.on_ws_open())
    }

    /// Malformed frames produce protocol-close commands.
    ///
    /// # Errors
    ///
    /// Returns a string-valued [`JsValue`] if command serialization fails.
    #[wasm_bindgen(js_name = onWsMessage)]
    pub fn on_ws_message(&mut self, frame: String) -> Result<JsValue, JsValue> {
        commands_to_js(self.inner.on_ws_message(&frame))
    }

    /// # Errors
    ///
    /// Returns a string-valued [`JsValue`] if command serialization fails.
    #[wasm_bindgen(js_name = onTransportReady)]
    pub fn on_transport_ready(&mut self) -> Result<JsValue, JsValue> {
        commands_to_js(self.inner.on_transport_ready())
    }

    /// # Errors
    ///
    /// Returns a string-valued [`JsValue`] if command serialization fails.
    #[wasm_bindgen(js_name = onWsClose)]
    pub fn on_ws_close(&mut self, code: u16) -> Result<JsValue, JsValue> {
        commands_to_js(self.inner.on_ws_close(code))
    }

    /// # Errors
    ///
    /// Returns a string-valued [`JsValue`] if command serialization fails.
    #[wasm_bindgen(js_name = onTimer)]
    pub fn on_timer(&mut self, timer_id: u32) -> Result<JsValue, JsValue> {
        commands_to_js(self.inner.on_timer(timer_id))
    }

    /// # Errors
    ///
    /// Returns a string-valued [`JsValue`] if `stream_type` is not a [`StreamType`]
    /// or command serialization fails.
    pub fn publish(&mut self, stream_type: &str, active: bool) -> Result<JsValue, JsValue> {
        let stream_type: StreamType = from_js(JsValue::from_str(stream_type))?;
        commands_to_js(self.inner.publish(stream_type, active))
    }

    /// # Errors
    ///
    /// Returns a string-valued [`JsValue`] if `user_id` cannot decode as
    /// [`crate::wire::UserId`], `states` cannot decode as [`crate::wire::DownloadStates`]
    /// or command serialization fails.
    pub fn subscribe(&mut self, user_id: JsValue, states: JsValue) -> Result<JsValue, JsValue> {
        let user_id = from_js(user_id)?;
        let states = from_js(states)?;
        commands_to_js(self.inner.subscribe(user_id, states))
    }

    /// # Errors
    ///
    /// Returns a string-valued [`JsValue`] if `info` cannot decode as
    /// [`crate::wire::UserInfo`] or command serialization fails.
    #[wasm_bindgen(js_name = updateInfo)]
    pub fn update_info(&mut self, info: JsValue) -> Result<JsValue, JsValue> {
        let info = from_js(info)?;
        commands_to_js(self.inner.update_info(info))
    }

    /// # Errors
    ///
    /// Returns a string-valued [`JsValue`] if `message_json` cannot decode as
    /// [`JsonPayload`] or command serialization fails.
    pub fn broadcast(&mut self, message_json: &str) -> Result<JsValue, JsValue> {
        let message: JsonPayload =
            serde_json::from_str(message_json).map_err(|error| js_error(error.to_string()))?;
        commands_to_js(self.inner.broadcast(message))
    }

    /// Omitted, null and undefined options use the default recording options.
    ///
    /// # Errors
    ///
    /// Returns a string-valued [`JsValue`] if provided `options` cannot decode as
    /// [`crate::wire::RecordingOptions`] or command serialization fails.
    #[wasm_bindgen(js_name = startRecording)]
    pub fn start_recording(&mut self, options: Option<JsValue>) -> Result<JsValue, JsValue> {
        let options = from_optional_js(options)?;
        commands_to_js(self.inner.start_recording(options))
    }

    /// # Errors
    ///
    /// Returns a string-valued [`JsValue`] if command serialization fails.
    #[wasm_bindgen(js_name = stopRecording)]
    pub fn stop_recording(&mut self) -> Result<JsValue, JsValue> {
        commands_to_js(self.inner.stop_recording())
    }

    /// # Errors
    ///
    /// Returns a string-valued [`JsValue`] if `negotiation_kind` is not a
    /// [`NegotiationKind`] or command serialization fails.
    #[wasm_bindgen(js_name = submitNegotiationAnswer)]
    pub fn submit_negotiation_answer(
        &mut self,
        request_id: String,
        negotiation_kind: &str,
        sdp: String,
    ) -> Result<JsValue, JsValue> {
        let kind: NegotiationKind = from_js(JsValue::from_str(negotiation_kind))?;
        commands_to_js(self.inner.submit_negotiation_answer(
            &crate::signaling::RequestId::new(request_id),
            kind,
            sdp,
        ))
    }

    /// # Errors
    ///
    /// Returns a string-valued [`JsValue`] if command serialization fails.
    pub fn disconnect(&mut self) -> Result<JsValue, JsValue> {
        commands_to_js(self.inner.disconnect())
    }
}

fn commands_to_js(commands: Vec<Command>) -> Result<JsValue, JsValue> {
    to_js(&commands)
}

fn to_js<T: Serialize + ?Sized>(value: &T) -> Result<JsValue, JsValue> {
    value
        .serialize(&serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true))
        .map_err(|error| js_error(error.to_string()))
}

fn from_js<T: DeserializeOwned>(value: JsValue) -> Result<T, JsValue> {
    serde_wasm_bindgen::from_value(value).map_err(|error| js_error(error.to_string()))
}

fn from_optional_js<T>(value: Option<JsValue>) -> Result<T, JsValue>
where
    T: Default + DeserializeOwned,
{
    let Some(value) = value else {
        return Ok(T::default());
    };
    if value.is_null() || value.is_undefined() {
        Ok(T::default())
    } else {
        from_js(value)
    }
}

fn js_error(message: impl Into<String>) -> JsValue {
    JsValue::from_str(&message.into())
}
