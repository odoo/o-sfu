# o-sfu Client API

The browser bundle exposes the Odoo-facing SFU client facade:

```js
import { CLIENT_UPDATE, SFU_CLIENT_STATE, SfuClient, __info__ } from "/bundle/odoo_sfu.js";

const sfu = new SfuClient();
```

The JavaScript module exports exactly `SfuClient`, `SFU_CLIENT_STATE`,
`CLIENT_UPDATE` and `__info__`. Browser integrations should not construct
WebSocket protocol envelopes directly from this bundle.

The compatibility bundle is built with:

```bash
npm --prefix crates/client run build:odoo
```

The build also emits `dist/odoo_sfu.d.ts`. Keep it beside `odoo_sfu.js` under
the same basename. TypeScript and compatible editors use this declaration for
the public types, method documentation and typed event payloads. Test-only
constructor dependencies and protocol bindings are excluded.

`STREAM_TYPES` and `VIDEO_LAYOUT_INTENTS` below describe the accepted type
values. They are not additional JavaScript exports.

## Bundle Metadata

The Odoo bundle exports build metadata through `__info__`.

```js
console.log(__info__.version, __info__.hash, __info__.date);
```

Shape:

```ts
interface BundleInfo {
    date: string;
    hash: string;
    url: string;
    version: string;
}
```

`version` comes from the root `o-sfu` Cargo package. `hash` is the short Git
commit hash used when `build:odoo` generated the asset.

The bundle is intentionally close to the old Node SFU client API. New code should
prefer `publish()` and `subscribe()`. The legacy `updateUpload()` and
`updateDownload()` names are still available as direct aliases for compatibility.

## Connection State

`sfu.state` is one of:

```ts
type ConnectionState =
    | "disconnected"
    | "connecting"
    | "authenticated"
    | "connected"
    | "recovering"
    | "closed";
```

The same values are exported through `SFU_CLIENT_STATE`:

```js
sfu.state === SFU_CLIENT_STATE.CONNECTED;
```

State changes are emitted through the `"stateChange"` event:

```js
sfu.addEventListener("stateChange", ({ detail }) => {
    const { state, cause } = detail;
});
```

## connect(url, jwt, options)

Connects to an o-sfu websocket endpoint.

```js
sfu.connect("https://sfu.example.com/ws", jsonWebToken, {
    channelUUID: "mail-channel-uuid",
    iceServers: [{ urls: "stun:stun.example.com:3478" }],
});
```

The client accepts `http:` and `https:` URLs and normalizes them to `ws:` or
`wss:` before opening the websocket.

The method validates its options synchronously then returns before
authentication and media negotiation finish. Observe `stateChange` and
`handledError` for the result.

Options:

```ts
interface ConnectOptions {
    channelUUID?: string;
    iceServers?: RTCIceServer[];
}
```

`channelUUID` is optional when the token already identifies the channel.
`iceServers` is passed to `RTCPeerConnection`.

## disconnect()

Ends the current connection attempt and clears `errors`.

```js
sfu.disconnect();
```

An active session emits a final disconnected `stateChange`. Calling the method
while already disconnected or closed has no protocol effect.

## publish(type, track)

Sets the desired local media track.

```js
const audioTrack = audioStream.getAudioTracks()[0];
const cameraTrack = cameraStream.getVideoTracks()[0];

sfu.publish("audio", audioTrack);
sfu.publish("camera", cameraTrack);
sfu.publish("screen", screenTrack);

sfu.publish("camera", null);
sfu.publish("screen", undefined);
```

`type` must be one of:

```ts
const STREAM_TYPES = ["audio", "camera", "screen"] as const;
type StreamType = (typeof STREAM_TYPES)[number];
```

The supplied track must match the stream type:

- `audio` requires an audio `MediaStreamTrack`
- `camera` requires a video `MediaStreamTrack`
- `screen` requires a video `MediaStreamTrack`

An unknown stream type or a mismatched track throws synchronously.

The first non-null track may require SDP negotiation. The server classifies an
inactive request when it applies it. A queued or staged first publication is
cancelled. A committed publication is paused.

The client uses `RTCRtpSender.replaceTrack(null)` for a committed publication
while retaining the negotiated sender, transceiver direction and MID. The
server keeps the same source identity and consumer routes in an inactive state.

Restoring a compatible track uses `replaceTrack(track)` on the same sender then
reactivates the committed publication without another SDP exchange. Disconnect,
recovery and peer replacement clear the old peer-generation MID. Session close
or replacement performs the complete server-side publication teardown.

### updateUpload(type, track)

Deprecated compatibility alias for `publish(type, track)`.

```js
sfu.updateUpload("camera", cameraTrack);
sfu.updateUpload("camera", undefined);
```

## subscribe(sessionId, states)

Updates what the current browser wants to receive from a remote participant.

```js
sfu.subscribe(remoteSessionId, {
    audio: true,
    camera: true,
    screen: false,
});
```

`states` is a partial object. Omitted fields keep their previous value for that
remote session.

Unknown fields, non-boolean download flags and invalid layout values throw
synchronously.

```ts
interface DownloadStates {
    audio?: boolean;
    camera?: boolean;
    screen?: boolean;
    cameraLayout?: VideoLayoutIntent;
    screenLayout?: VideoLayoutIntent;
}
```

The boolean fields control whether the receiver wants the stream:

- `audio`: receive or stop receiving remote audio
- `camera`: receive or stop receiving the remote camera
- `screen`: receive or stop receiving the remote screen share

The layout fields tell the SFU how important each video route is for this
receiver. They are additive policy hints, not new tracks and not mandatory for
basic audio/video subscription.

```ts
const VIDEO_LAYOUT_INTENTS = [
    "featured",
    "pinned",
    "visible_thumbnail",
    "hidden",
    "overflow"
] as const;
type VideoLayoutIntent = (typeof VIDEO_LAYOUT_INTENTS)[number];
```

### Layout Intent

Use `cameraLayout` for the participant camera tile and `screenLayout` for the
participant screen-share tile.

```js
sfu.subscribe(remoteSessionId, {
    camera: true,
    cameraLayout: "pinned",
});
```

The SFU uses the resolved layout role when selecting video quality for this
receiver:

- `pinned`: the user explicitly pinned this video; highest priority with
  explicit featured treatment.
- `featured`: the UI explicitly treats this video as featured without
  necessarily being user-pinned.
- `visible_thumbnail`: the video is visible as a normal thumbnail.
- `hidden`: the video is subscribed but not currently visible.
- `overflow`: the video is outside the visible grid or in an overflow list.

For camera video, if `cameraLayout` is omitted, the SFU keeps compatibility
behavior: an active speaker can receive featured-quality treatment, and other
cameras are treated as visible thumbnails.

For screen share, if `screenLayout` is omitted, the SFU treats the stream as a
screen-share readability route. A `screenLayout` value of `"visible_thumbnail"`
keeps that screen-specific policy rather than turning the screen share into a
camera thumbnail.

Layout-only updates are valid:

```js
sfu.subscribe(remoteSessionId, {
    cameraLayout: "hidden",
});
```

That call keeps the current audio/camera/screen download flags and only changes
the server-side video priority for this receiver.

### Common Subscription Patterns

Receive a remote participant normally:

```js
sfu.subscribe(remoteSessionId, {
    audio: true,
    camera: true,
    cameraLayout: "visible_thumbnail",
});
```

Pin one participant camera:

```js
sfu.subscribe(pinnedSessionId, {
    camera: true,
    cameraLayout: "pinned",
});
```

Move an offscreen camera out of the visible budget:

```js
sfu.subscribe(remoteSessionId, {
    cameraLayout: "overflow",
});
```

Keep a screen share readable:

```js
sfu.subscribe(remoteSessionId, {
    screen: true,
});
```

Hide a screen share while preserving the existing subscription state:

```js
sfu.subscribe(remoteSessionId, {
    screenLayout: "hidden",
});
```

### updateDownload(sessionId, states)

Deprecated compatibility alias for `subscribe(sessionId, states)`.

The new layout-intent fields are available through the old name as well:

```js
sfu.updateDownload(remoteSessionId, {
    camera: true,
    cameraLayout: "pinned",
});
```

New code should call `subscribe()` directly.

## updateInfo(info, options)

Updates the local participant metadata sent to other participants.

```js
sfu.updateInfo({
    isTalking: false,
    isFeatured: true,
    isCameraOn: true,
    isScreenSharingOn: false,
    isSelfMuted: false,
    isDeaf: false,
    isRaisingHand: false,
});
```

Supported fields:

```ts
interface SessionInfo {
    isTalking?: boolean;
    isFeatured?: boolean;
    isCameraOn?: boolean;
    isScreenSharingOn?: boolean;
    isSelfMuted?: boolean;
    isDeaf?: boolean;
    isRaisingHand?: boolean;
}
```

`isCameraOn` and `isScreenSharingOn` remain accepted for compatibility. Their
published values are derived from committed publication activity. Generic
`updateInfo()` calls do not change them.

`options.needRefresh` is accepted for legacy callers and is currently a
compatibility no-op.

## broadcast(message)

Sends an application-level message to the channel.

```js
sfu.broadcast({ type: "reaction", value: "raised-hand" });
```

Other clients receive it through the `"update"` event with
`CLIENT_UPDATE.BROADCAST`.

The method serializes a JSON-compatible snapshot at call time. Serialization
failures are reported through `handledError` and can end the session.

## getStats()

Returns WebRTC stats reports for the peer connection and local senders when
available.

```js
const stats = await sfu.getStats();

const {
    uploadStats,
    downloadStats,
    audio,
    camera,
    screen,
} = stats;
```

Shape:

```ts
interface SfuStats {
    uploadStats?: RTCStatsReport;
    downloadStats?: RTCStatsReport;
    audio?: RTCStatsReport;
    camera?: RTCStatsReport;
    screen?: RTCStatsReport;
}
```

`uploadStats` and `downloadStats` are compatibility names for the peer
connection stats report. The method returns an empty object before negotiation.

If peer connection or sender stats collection fails, the promise rejects with
the browser error. The rejection does not emit `handledError` or add an entry
to `sfu.errors`.

## Recording

Recording capabilities are exposed through `availableFeatures`:

```js
if (sfu.availableFeatures.videoRecording) {
    const allowed = await sfu.startRecording({ video: true });
}
```

Recording methods:

```js
const allowed = await sfu.startRecording({
    audio: true,
    video: true,
    transcription: false,
});

const stopped = await sfu.stopRecording();
```

Each promise resolves to the server acceptance result. It resolves `false` for
refusal, timeout, teardown or when no request starts. A fatal runtime failure
rejects the promise and emits `handledError`.

Options:

```ts
interface RecordingOptions {
    audio?: boolean;
    video?: boolean;
    transcription?: boolean;
}
```

Current recording state is exposed through `sfu.recordingState` and updated by
`CLIENT_UPDATE.CHANNEL_INFO_CHANGE` events.

```ts
interface SfuRecordingState {
    recording?: boolean;
    audio?: boolean;
    video?: boolean;
    transcription?: boolean;
}
```

## Events

The client emits four event types:

- `"stateChange"` for connection-state transitions
- `"update"` for SFU protocol updates
- `"handledError"` after a runtime error is captured by the client
- `"log"` for client/runtime diagnostics

### update

```js
sfu.addEventListener("update", ({ detail }) => {
    switch (detail.name) {
        case CLIENT_UPDATE.TRACK: {
            const { sessionId, type, track, active } = detail.payload;
            break;
        }
        case CLIENT_UPDATE.DISCONNECT: {
            const { sessionId } = detail.payload;
            break;
        }
        case CLIENT_UPDATE.INFO_CHANGE: {
            const sessions = detail.payload;
            break;
        }
        case CLIENT_UPDATE.BROADCAST: {
            const { senderId, message } = detail.payload;
            break;
        }
        case CLIENT_UPDATE.CHANNEL_INFO_CHANGE: {
            const { state, stopCode } = detail.payload;
            break;
        }
    }
});
```

`CLIENT_UPDATE` values:

```ts
const CLIENT_UPDATE = {
    TRACK: "track",
    DISCONNECT: "disconnect",
    INFO_CHANGE: "info_change",
    BROADCAST: "broadcast",
    CHANNEL_INFO_CHANGE: "channel_info_change",
} as const;
```

Track update payload:

```ts
interface TrackUpdateDetail {
    sessionId: SessionId;
    type: StreamType;
    track: MediaStreamTrack;
    active: boolean;
}
```

### handledError

```js
sfu.addEventListener("handledError", ({ detail }) => {
    console.error(detail.error);
});
```

The same errors are retained in `sfu.errors`.

### log

```js
sfu.addEventListener("log", ({ detail }) => {
    const { id, level, message } = detail;
});
```

`level` is one of `"debug"`, `"info"`, `"warn"`, or `"error"`.

## Public Properties

`SfuClient` exposes `state`, `errors`, `availableFeatures`, `recordingState` and
the compatibility-only `_consumers` view. `connect()` and `disconnect()` clear
`errors` before starting their transition.

`availableFeatures` has this shape:

```ts
interface AvailableFeatures {
    rtc: boolean;
    transcription: boolean;
    audioRecording: boolean;
    videoRecording: boolean;
}
```

## Compatibility Notes

- `updateUpload()` is retained and delegates to `publish()`.
- `updateDownload()` is retained and delegates to `subscribe()`.
- `updateDownload()` therefore supports `cameraLayout` and `screenLayout` exactly
  like `subscribe()`.
- `_consumers` remains available as a compatibility/debug view for Odoo Discuss
  diagnostics. New integrations should consume `"update"` events instead.
