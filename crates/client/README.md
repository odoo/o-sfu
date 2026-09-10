# o-sfu client

## Build and distribution

`npm run build` regenerates the default `ProtocolCoreWasm` runtime into
`generated/` then compiles the TypeScript part.

`npm run build:odoo` generates the Odoo module at `dist/odoo_sfu.js` and its
documented TypeScript declaration at `dist/odoo_sfu.d.ts`. Keep both files
together when distributing the client so editors and TypeScript resolve the API
documentation beside the JavaScript module.

The JavaScript module exports exactly `SfuClient`, `SFU_CLIENT_STATE`,
`CLIENT_UPDATE` and `__info__`. See the [client API reference](./API.md).

## Runtime architecture

[`ProtocolCore`](../protocol/src/core.rs) owns protocol state.
[`BrowserRuntime`](./src/internals/browser_runtime.ts) owns browser effects and
uses [`TurnQueue`](./src/internals/turn_queue.ts) to serialize turns and
invalidate stale asynchronous continuations.

```text
Queued protocol input / socket event / timer / peer callback
                              |
                              v
                 BrowserRuntime + TurnQueue
                              |
                              v
                   ProtocolCore transition
                   host-effect-free state
                              |
                     Vec<Command> in Rust
                              |
                   serde_wasm_bindgen
                              |
                    HostCommand[] in TS
                              |
     preapply public state when the batch closes the peer
                              |
                              v
                  execute in command order
                    /                    \
     synchronous browser effect      applyNegotiation
       continue immediately          await guarded WebRTC
                    |                         |
                    |              submitNegotiationAnswer
                    |              optional onTransportReady
                    |                         |
                    +-------------------------+
                              |
                     execute next command
```

Each Wasm method serializes the returned `Vec<Command>` directly into plain
JavaScript objects typed as `HostCommand[]`. Within command processing only
`applyNegotiation` awaits browser work. Its result re-enters `ProtocolCore` and
appends the returned commands to the same drain. State changes and public
updates are dispatched at their command positions. The internal `remote_media`
update is projected by `RemoteMedia` and can emit zero or more public track
updates. A batch that closes the peer connection commits public state before
host teardown can re-enter through callbacks.

The diagram shows the regular queued path. WebSocket close and `disconnect()`
without active cleanup evaluate a control transition then interrupt the queue.
A disconnect during active cleanup waits behind that control turn.

[`RemoteMedia`](./src/internals/remote_media.ts) joins browser tracks and server
binding snapshots by transceiver MID.

```text
RTCPeerConnection track event              Server Tracks snapshot
             |                                      |
   PeerSession active-peer guard            ProtocolCore TrackSnapshot
             |                                      |
   handleTrackEvent(event)              emitUpdate(remote_media) command
   store track by MID                               |
   attach mute/unmute listeners           replaceTrackBindings()
             |                            applyBinding(mid, binding)
             |                            store binding by MID
             +------------------+-------------------+
                                |
                    RemoteMediaSlot keyed by MID
                      { track?, binding? }
                                |
                    projectTrackSlot()
                  waits for track and binding
                                |
                    effectiveActive()
       states = subscriptionStates.get(binding.sessionId)
   active = binding.active && (states?.[binding.type] ?? true)
                                |
              update _consumers compatibility map
                 emit CLIENT_UPDATE.TRACK
```

Track and binding arrival order is independent. Rebinding a MID to a different
session or stream clears the previous consumer and track then waits for a fresh
track event. Local `DownloadStates` only overlays `active` for the binding's
stream type. The mute and unmute listeners force a new projection so consumers
observe the track's changed `muted` state without changing the computed
`active` value.

## Verification

`npm run test:browser` runs the Playwright suite against the built browser
bundle.

Requires `wasm-pack`:

```bash
cargo install wasm-pack
```

```bash
npm exec playwright install chromium firefox
```
