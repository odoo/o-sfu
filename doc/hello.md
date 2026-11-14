# Hello?? Can you see my screen?

It's going to be long, but it's a synthesis of what I think is important to learn
if you really want to dive deep into (/understand the internals of?) the sfu.

Suppose you want to change what happen when someone turns on their camera.
Past the browser method that handles the request, a few more questions come
up. What the other participants want to see? When does the server start
forwarding packets? Which version of the video does each of them get?

For the examples, we will follow a call with 3 participants that looks like this:

```text
Alice   publishes audio and camera video, then reconnecst later
Bob     pins Alices camera
Carol   watches on a weaker connection
```

(pinning is a Discuss feature, for example when you're watching a single
stream in full screen. The SFU uses that preference to select video.)

The reconnect happens while the server is still cleaning up Alice's old
connection.

During a call, **events happen concurrently**. We learn about browser state
through messages, which may arrive late, out of order or not at all.

It also happen very fast, on a clock nobody can pause (/in real time, with
no way to pause?). Alice's camera produces a frame every 33ms, often spread
over several packets. [ITU-T G.114](https://www.itu.int/rec/T-REC-G.114)
puts the one-way delay budget of a natural conversation around 150ms,
network included. A worker forwarding 100k packets per second has 10µs for
each one.

That changes what "slow" means. A packet that arrives after its frame has
been played is useless to Bob and queuing it only delays the packets behind
it. That's why the repair cache stores packets only briefly and why a decoder
that has fallen too far behind gets a keyframe instead.

Also (/On top of that?), there is no single "now" in a system like this.
Leslie Lamport's
[Time, Clocks, and the Ordering of Events in a Distributed System](https://lamport.azurewebsites.net/pubs/time-clocks.pdf)
shows that without a shared clock, **the only order you can trust is
causality**. Two events with no causal link can reach the server in either
order, so the code has to be correct for both.

Here is what a few seconds in a call could look like:

```text
time ───────────────────────────────────────────────────────────────────>

Alice     publish(camera) ...... SDP answer .......... Wi-Fi drops
Bob              subscribe(Alice's camera, pinned)     disconnect()
Carol                             connect() ....... Error .......... recovery
network          Carol's packets   Bob's NACK       RTCP reports
timers                     ICE check due        keyframe request due
server    reserve media ... commit? ......................... cleanup?
packet loop     packet - command - packet - packet - packet - packet
```

This is **a concurrency problem before it's a parallelism one**. Even on a single
thread, Alice can reconnect during an `.await`.

You'll meet these consequences throughout the guide:

- **A message describes the past**. Check that it still applies to the current
  connection ([identities and generations](#identity-state-and-time)).
- **Keep what people want, not only what exists right now**. Bob's request for
  Alice's camera has to wait for the camera
  ([intent and reconciliation](#bob-can-want-a-route-that-cant-exist-yet)).
- **Work you start is work you owe**, even if the participant who asked for it
  has left ([obligations](#linear-types-and-lifecycle-obligations)).
- **Don't hold a lock while you wait**. Decide, release the lock, then apply the
  decision ([decisions and effects](#separate-decisions-from-effects)).

To see how this shapes the code, we'll follow a call through o-sfu, from joining a room to forwarding media. We'll explain the systems, types and lifecycle checks as we encounter the problems they solve.

## Table of contents

1. [What an SFU does](#what-an-sfu-does)
2. [The application and the room](#the-application-and-the-room)
3. [The browser client](#the-browser-client)
4. [A call through the architecture](#a-call-through-the-architecture)
5. [The protocols in a call](#the-protocols-in-a-call)
6. [Publications, subscriptions and routing](#publications-subscriptions-and-routing)
7. [Choose media for each reciever](#choose-media-for-each-reciever)
8. [Find your way around the repository](#find-your-way-around-the-repository)
9. [Identity, state and time](#identity-state-and-time)
10. [Follow a participant into a room](#follow-a-participant-into-a-room)
11. [Separate decisions from effects](#separate-decisions-from-effects)
12. [Linear types and lifecycle obligations](#linear-types-and-lifecycle-obligations)
13. [Cancellation, channels and shutdown](#cancellation-channels-and-shutdown)
14. [Follow a media packet](#follow-a-media-packet)
15. [Workers, scheduling and network backends](#workers-scheduling-and-network-backends)
16. [A map of o-sfu-core](#a-map-of-o-sfu-core)
17. [Back in the browser client](#back-in-the-browser-client)
18. [Conclusion](#conclusion)
19. [Rust learning ressources](#rust-learning-ressources)

## What an SFU does

Picture Alice and Bob in a call. Then Carol joins. In a mesh (P2P) call,
Alice sends media to both. Adding someone means another
upload, so `n` participants means `n - 1` uploads (+encryption):

```text
Alice's uploads in a mesh call

2 people:  Alice ──> Bob                               1 upload
3 people:  Alice ──> Bob, Carol                        2 upload
6 people:  Alice ──> Bob, Carol, Didier, Eric, Frank   5 upload
```

With a SFU, Alice sends her audio and video once. The server chooses what to
forward to Bob and Carol and makes the copies itself, which is called
**fanout**. The SFU handles the extra uploads for Alice.

```text
                           ┌──> Bob: Alice's camera at higher quality
Alice: audio + camera ──> SFU
                           └──> Carol: Alice's camera at lower quality
```

Alice can also upload several video qualities, so you can receive a
version that fits your connection.

You may also have heard of an MCU (multipoint control unit), which decodes
participants' media and re-encodes it into a new output. With an SFU, you
receive the original streams and arrange your view of the call locally.
We avoid the CPU cost of transcoding on the server.

Inside the SFU, follow control and media separately:

```text
Control: join the room, turn on a camera, choose what to recieve
              ─> the server decides who should recieve what

Media:   participants send their audio and video
              ─> the server forwards the selected streams
```

Control selects streams. Media forwards packets. **Either can fail
independently**, so you might see everyone in the participant list
without receiving video.

## Tenants and rooms

(I will mention Odoo/Discuss a few times below, but o-sfu is tenant agnostic, it does not need
to communicate with an Odoo "DB" specifically)

When you join a call in Discuss, Odoo determines which call you belong to and
gives you the credentials to join it (Odoo negotiate these credentials with
the SFU, one room at a time). One SFU process can serve many tenants, such
as Odoo databases, each with many rooms:

```text
tenants, outside o-sfu               o-sfu process: one flat set of rooms
                                     ┌──────────────────────────────────┐
Odoo database A ──┬── /v1/channel ──>│ room A:channel:42                │
                  │                  │   [<Alice>, <Bob>, <Carol>]      │
                  └── /v1/channel ──>│ room A:channel:7                 │
                                     │   [another call]                 │
Odoo database B ───── /v1/channel ──>│ room B:channel:3                 │
                                     │   ...                            │
                                     └──────────────────────────────────┘
                                      each room keeps its issuer and its
                                      own signing key
```

note: o-sfu does not "spawn" odoo servers, here it just means that o-sfu's rooms can be "managed/owned" by different tenants

## The browser client

Discuss uses `SfuClient` from the browser bundle to control the call.
For example, Alice can join with her microphone and camera:

```js
import { SfuClient } from "/bundle/odoo_sfu.js";

const sfu = new SfuClient();
sfu.connect(serverUrl, jsonWebToken, { channelUUID });
sfu.publish("audio", microphoneTrack);
sfu.publish("camera", cameraTrack);
```

Bob can pin Alice's camera and listen for the track:

```js
sfu.subscribe(aliceSessionId, { audio: true, camera: true, cameraLayout: "pinned" });
// by default you are subscribed to all participants
// (but still subject to SFU limits decisions)

sfu.addEventListener("update", ({ detail: { name, playload } }) => {
    switch(name) {
        case CLIENT_UPDATE.TRACK: // CLIENT_UPDATE enum (object) can be imported from the bundle
            const {
                /** the id of the RtcSession that is associated to the user */
                sessionId,
                /** `audio` | `camera` | `screen` */
                type,
                /** MediaStreamTrack */
                track,
                /**
                 * boolean
                 * we may think that we could have derived that from `MediaStreamTrack.enabled`,
                 * but `enabled` could be true even if the stream doesn't actually have data.
                 * In fact the `enabled` state of the remote track (the other side of the transceiver)
                 * has no effect on the `enabled` state of this MediaStreamTrack.
                 * So `active` here represents the actual intention of the sender,
                 * no matter the technical state of the underlying stream.
                 */
                active
            } = payload;
    }
});
```

Carol uses `cameraLayout: "visible_thumbnail"` in her `subscribe` call to
request a smaller picture.

The main API calls are:

| Discuss wants to | `SfuClient` |
| --- | --- |
| Join or leave the call | `connect(url, jwt, options)`, `disconnect()` |
| Send a microphone, camera or screen track | `publish(type, track)`, with `null` to stop |
| Choose what to receive from someone | `subscribe(sessionId, states)` |
| Share its status (muted, hand raised...) | `updateInfo(info)` |
| Send an application message to everyone | `broadcast(message)` |
| Follow the connection | `state` and the `"stateChange"` event |
| Receive tracks, status changes and messages | the `"update"` event |
| Hear about failures | `errors` and the `"handledError"` event |

Stats and recording add a few more calls, see
[`API.md`](../crates/client/API.md) for the full list.

Discuss only passes **intent**, "I want to send this track" or "I want Alice's
camera, pinned", never how. It has no SDP, no transceiver, no request ID and
no reconnect logic.

### Behind `SfuClient`

To implement those requests, `ProtocolCore` processes events in Rust and
**returns ordered commands**. It uses no sockets or browser APIs (Sans I/O).
TypeScript executes commands sequentially because later commands may
depend on earlier effects.

```text
BrowserRuntime (TypeScript)                        │ ProtocolCore (Rust / Wasm)
───────────────────────────────────────────────────┼─────────────────────────
incoming event (user call, socket, timer)          │
      │                                            │
      ├───────── 1. input event ──────────────────>│ pure state transition
      │                                            │        │
      │<──────── 2. ordered commands ──────────────┼────────┘
      v                                            │
execute in order:                                  │
  [ cmd 1 ] ────> [ cmd 2 ] ────> [ cmd 3 ]        │
      │               │               │            │
      v               v               v            │
mutate WebRTC   send signaling    notify app       │
 (add track)     (WebSocket)    (state update)     │
───────────────────────────────────────────────────┴─────────────────────────
```

Rust provides compile-time checks for the protocol logic. TypeScript
executes the commands it produces.

We'll return to stale replies, interrupted operations and reconnects in
[Back in the browser client](#back-in-the-browser-client).

[client/README.md](../crates/client/README.md)

## A call through the architecture

Let's follow a call through these components.

Alice joins a call in Discuss and Bob requests her camera. We need to create
a room, authenticate them, negotiate media and set up forwarding.

### Create the room and join it

To create an SFU room, the tenant backend (such as an Odoo database) calls
`GET /v1/channel` and negotiates credentials. Odoo supplies connection details
and signed participant credentials to the browser. Alice connects over a
WebSocket with those credentials. After authentication, the SFU assigns a
media worker, sends the welcome state and starts media negotiation.

```text
Odoo backend                           o-sfu
     │                                   │
     ├────── GET /v1/channel ───────────>│
     │<───── room UUID + URL ────────────┘
     │
     │ room information + credentials
     v
Alice's browser                       o-sfu
     │                                   │
     ├────── open WebSocket ────────────>│
     ├────── authenticate ──────────────>│
     │<───── welcome ────────────────────┤
     │<───── SDP offer ──────────────────┤
     ├────── SDP answer ────────────────>│
     │                                   │
     │<═════ WebRTC transport ══════════>│
```

### Agree on media and connect the streams

The server sends an SDP offer over WebSocket. The browser uses its WebRTC
API to answer it. ICE and DTLS then set up the network path and encryption.
WebSocket also handles renegotiation and application messages throughout
the call.

To forward Alice's video to Bob, the SFU needs her publication and his
subscription. Room policy determines whether we can connect them and
which video quality Bob should receive.

```text
Publications + subscriptions + negotiated media settings
                         │
                         v
              Room and routing decisions
                 │                  ^
                 │                  │
                 ├──> further offer/answer when needed
                 │
                 v
          commands to media workers
```

Publishing or subscribing again can require further SDP negotiation.

### Forward packets while the call changes

Inside each media worker, a packet loop processes network input, commands
and timers. It forwards Alice's packets to Bob and Carol according to their
preferences and connections.

```text
                 WebSocket requests
                         │
                         v
                  Room decisions <────────────┐
                         │                    │
                  worker commands        observations
                         │                    │
                         v                    │
Alice ══ media ══> [ worker packet loops ] ───┘
                              │
                              ╞══ selected media ══> Bob
                              │
                              ╘══ selected media ══> Carol
```

The packet loops are lazily initialized but runs the whole time (/start on
first use, then run for the rest of the worker's life?) and also handles
transport setup commands during negotiation. They apply the current
forwarding choices **without deciding the room's policy per packet**.

Those choices change all the time: Bob might hide a tile, Alice may stop her
camera or Carols bandwith may drop. Signaling requests and worker
observations feed back into room decisions, which update the workers while
the packets keep flowing.

[server rustdoc](https://odoo.github.io/o-sfu/o_sfu/), [core rustdoc](https://odoo.github.io/o-sfu/o_sfu_core/)

## The protocols in a call

If WebRTC is new to you, all the protocol names can be a a lot (/can feel
overwhelming?), the good news is that knowing the job each one does is
enough to track a bug down.

| Term | Job in the call |
| --- | --- |
| HTTP | Lets the application backend create rooms and disconnect users |
| WebSocket | Transmits messages about joining, sending media and choosing what to recieve |
| [SDP](https://www.rfc-editor.org/rfc/rfc8866) (Session Description Protocol) | Describes the media formats and connection settings the browser and server agree to use |
| ICE (Interactive Connectivity Establishment) | Tests possible network paths and chooses one that works |
| [STUN](https://www.rfc-editor.org/rfc/rfc8489) (Session Traversal Utilities for NAT) | Supplies the connectivity-check messages used by ICE |
| [TURN](https://www.rfc-editor.org/rfc/rfc8656) (Traversal Using Relays around NAT) | Relays traffic when a direct network path isn't available |
| DTLS-SRTP (Datagram Transport Layer Security and Secure Real-time Transport Protocol) | Negotiates keys and encrypts media between a browser and the SFU |
| RTP (Real-time Transport Protocol) | Transmits encoded audio or video in packets, with information about their order and timing |
| RTCP (RTP Control Protocol) | Transmits feedback about media delivery, such as reception reports and repair requests |

In practice, they appear in roughly this order:

```text
1. WebSocket signaling    SDP offer and answer       what we could send
2. ICE (STUN, maybe TURN) connectivity checks        a network path that works
3. DTLS                   handshake                  keys for that path
4. SRTP                   RTP media, RTCP feedback   the actual audio and video
```

**Step 1 can succeed while step 2 fails**, which is how you end up with working
signaling and no video. The [deployment guide](deployment.md) explains how
to make those paths reachable.

[RFC 3550 (RTP and RTCP)](https://www.rfc-editor.org/rfc/rfc3550),
[RFC 8445 (ICE)](https://www.rfc-editor.org/rfc/rfc8445),
[RFC 5764 (DTLS-SRTP)](https://www.rfc-editor.org/rfc/rfc5764),
[RFC subcrate rustdoc](https://odoo.github.io/o-sfu/o_sfu_rfc/)

## Publications, subscriptions and routing

Alice sends media through a **producer**. A **consumer** represents Bob's use of
that producer:

```text
Alice's camera ──> producer ──┬──> consumer for Bob
                              │
                              └──> consumer for Carol
```

The [router](https://odoo.github.io/o-sfu/o_sfu_router/) describes those
relationships. It doesn't move packets: the transport forwards media to Bob.

Before a route can transmit video, the router work out what it can accept from
Alice, then match that against what Bob's connection supports (in practice
all sides support both vp8 and h264 as they are required by the webRTC
standard for compliant endpoints like browsers).

they're connections negotiate media settings independently, so some packet
identifiers need translating between them. For example:

```text
Alice's connection:  VP8 is payload type 96
Bob's connection:    VP8 is payload type 98
forwarding:          rewrite 96 ─> 98, leave the VP8 bytes alone
```

The [RTP negotiation code](https://odoo.github.io/o-sfu/o_sfu_router/model/rtp_negotiation/index.html)
shows how those parameters are matched.

### Bob can want a route that can't exist yet

Bob might ask for Alice's camera before she turns it on. We need to keep
his request until the source exists, including while Alice pauses or resumes
the camera. That's why **a subscription preference and an active consumer
route are separate informations**.

```text
Bob's intent: recieve Alice's camera
                       │
             publication exists?
                reciever ready?
             policy allows route?
                       │
                       v
             current transport route
```

The job here is **reconciliation**: take the desired state and current
capabilities, work out what can exist now, then update the parts that
differ. Turning "we can't create the route yet" into "delete Bob's
preference" would lose his request. You can think of them like switches in
series (/conditions that must all hold at once?).

Partial updates follow the same idea. **An omitted field is unchanged**,
which is a different request from switching it off. `DownloadStates`
in the [shared model](https://odoo.github.io/o-sfu/o_sfu_model/) represents
both cases:

```text
Bob's preferences for Alice:  { "audio": true,  "camera": true }

update:                       { "camera": false }
result:                       { "audio": true,  "camera": false }   audio untouched

update:                       { "camera": false, "audio": false }
result:                       { "audio": false, "camera": false }   audio switched off
```

The guideline on
[preserving meaningful outcomes](coding_guidelines/c05-preserve-meaningful-outcomes.md)
explains why the API distinguishes them. See also
[separating intent from realization](coding_guidelines/s04-separate-intent-from-current-realization.md),
[`SourceSubscriptionIntent`](https://odoo.github.io/o-sfu/o_sfu_core/engine/source_model/intent/struct.SourceSubscriptionIntent.html)
and the
[subscription transition](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/transition/subscription/index.html).

### Source identity is not an encoding choice

The client business layer (SfuClient and Discuss) knows Alice's camera as a
camera stream, next to audio and screen sharing. For the SFU, **that camera is
one source with possibly several encodings**, which Bob can switch between
without the application replacing the source. The
[application stream catalog](../src/application/stream_catalog.rs) maps one
to the other.

You'll find a source and it's encodings in
[`PublishedSourceDescriptor`](https://odoo.github.io/o-sfu/o_sfu_core/engine/source_model/descriptor/struct.PublishedSourceDescriptor.html)
and the selection among them in
[`SourceSelector`](https://odoo.github.io/o-sfu/o_sfu_core/engine/source_model/selection/enum.SourceSelector.html).

In the media protocols, a source gets more identifiers. A **MID** identifies a
negotiated media section, a **RID** distinguishes encodings of the same source
and an **SSRC** identifies an RTP source on a connection. For Alice's camera
(values made up):

```text
                     room identity      on Alice's connection
Alice's camera       source-7           MID "1"
  small version      encoding-3         RID "lo", its own SSRC
  large version      encoding-4         RID "hi", its own SSRC
```

The right-hand column lets the browser and SFU match packets with
negotiated media, but none of it replaces Alice's camera identity.
You'll see them in [browser media matching](../crates/client/src/internals/remote_media.ts),
[encoding identification](../crates/core/src/engine/media_transport/rtc/codec/rid.rs)
and [outgoing streams](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/consumer_egress/streams/index.html).

Keeping these concepts apart also give you a more precise diagnosis:
"Alice's camera is selected but paused by policy" tell you something
different from "Alice has no camera publication".

At this point you're probably wondering "does webRTC really need to be that complicated?"...
Well probably not, I think it was stitched together from whatever browser code already existed
for peer-to-peer connections and it ended up mixing unrelated concepts into one API. Maybe
in a few years we will have webTransport (/WebTransport may offer a cleaner model in a few years?).

## Choose media for each reciever

Bob wants Alice's camera enlarged. Carol wants a thumbnail and has less
bandwidth. We **choose video quality for each receiver** using
source demand, layout intent, active speakers and bandwidth observations.

### Simulcast: one camera, several versions

Sending both the large video wastes bandwidth for Carol: a smaller tile
doesn't reduce the download. Sending both the small video would make Bob's
picture blurry.

Alice's browser can encode her camera several times at different
resolutions, frame rates or bitrates. Sending these versions together is
called **simulcast**. Each version is a stream in its own right, so the SFU
can choose one per reciever:

```text
Alice's browser              SFU                         recievers
                    ┌──────────────────────┐
large version ─────>│ large for Bob        ├───────────> Bob's pinned tile
medium version ────>│ choose per reciever  │
small version ─────>│ small for Carol      ├───────────> Carol's thumbnail
                    └──────────────────────┘
```

Alice does the extra encoding and uploads more data, so we don't need to
resize or re-encode video on the SFU.

The codec defines how media is compressed and reconstructed. o-sfu enables
Opus for audio and VP8 for video by default in its
[codec options](https://odoo.github.io/o-sfu/o_sfu_core/options/codecs/index.html).
[RFC 8853](https://www.rfc-editor.org/rfc/rfc8853) describes how simulcast
is negotiated.

### Budgeting is a feedback-control problem

As Bob's network changes, the SFU measures available bandwidth and source
rates, chooses what to send, then measures the result. **Those estimates
arrive late and are noisy.**

The [video solver](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/source_policy/video/solver/index.html)
works out an effective video budget with this formula:

```text
usable total = estimated recieve bandwith * usable percentage / 100
video budget = max(0, usable total - reserved audio bandwith)
```

With an estimated 2,000,000 bit/s, 90% usable capacity and 100,000 bit/s
reserved for audio (example inputs, not repository defaults), the video
budget is 1,700,000 bit/s.

Bandwidth isn't the only limit. Policy also limits how many video routes
we can use and gives pinned cameras, thumbnails and hidden content different
priorities. These are fixed rules. We don't search for a globally optimal
allocation.

**Don't mix up the capacity in use with the capacity we want.** During
congestion, the SFU may pause a video route while sitll asking it's bandwith
controller for enough capacity to restore it. Otherwise the paused route
would lose its recovery signal. See the
[bandwith control code](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/control/bwe/index.html).

### Keep the call steady

If we change quality **too late, the call suffers. Too early and it
flickers**. We choose delays according to the cost:

| Change | When o-sfu makes it | Why |
| --- | --- | --- |
| Lower video quality | Right away | Staying would make congestion worse |
| Raise video quality | After a delay | Each upgrade costs a keyframe and the estimate may drop again |
| Pause a video route under pressure | After continuous pressure | A short dip shouldn't blank a tile |
| Stop treating Alice as speaking | After a short hold window | A pause between words isn't the end of a sentence |
| Switch to another version | At that version's next keyframe | The decoder needs a starting picture |

Suppose Carol has just enough bandwidth for the larger video. If we follow
every fluctuation, we keep switching versions and requesting keyframes.
Starting on the small version:

```text
tick                    1    2    3    4    5    6    7    8    9
large version fits?     yes  no   yes  no   yes  yes  yes  yes  no
follow every estimate   L    S    L    S    L    L    L    L    S
o-sfu                   S    S    S    S    S    S    L    L    S
                                            ├─ delay ─┤         │
                                                      │         └── down right away
                                                      └── up after a full delay
```

We call this **hysteresis**: we use the current state as well as the
latest measurement to decide when to change quality.
`VideoAdaptationTuning` sets the delays:
`upgrade_dwell` for upgrades and `soft_pause_dwell` for pausing a route.

Audio has the same problem. If we stopped forwarding as soon as the sound
level fell, we would cut speech between words. The
[active-speaker detector](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/state/route_control/active_speaker/index.html)
reads the audio level and voice activity sent with her packets and keeps her
marked as speaking for a short time after that activity drops:

```text
time ───────────────────────────────────────────────────────────>

Alice's voice:   #####  ###  ######
speaking:        [═══════════════════════════]
                                   │<───────>│
                                   hold window (250 ms)
```

`ACTIVE_SPEAKER_HOLD_WINDOW` sets that delay. In the
[audio policy](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/source_policy/audio/index.html),
we also respect receiver preferences. If Bob disables incoming audio, we
don't send it even while Alice speaks.

[input.rs](../crates/core/src/engine/room/source_policy/video/input.rs),
[projection.rs](../crates/core/src/engine/room/source_policy/video/projection.rs),
[TESTS/solver.rs](../crates/core/src/engine/room/source_policy/video/TESTS/solver.rs),
[active_speaker.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/state/route_control/active_speaker/index.html)

### Switching video need a starting picture

Carol enlarges Alice's camera tile but hasn't received earlier pictures
from that version. She can't decode changes to pictures she doesn't have.
We need a **keyframe** to start decoding:

```text
time ──────────────────────────────────────────────────────────────>

large video:    [keyframe] [changes] [changes] [keyframe] [changes]
                                               ^
Carol's choice:                 select large   │
                                    │          │
SFU forwarding:                     └── wait ──┼── forward ─────────>
                                               │
                                     a new starting picture
```

For VP8, o-sfu can ask for a keyframe and waits for its start when the
reciever need one. The
[packet handling code](../crates/core/src/engine/media_transport/rtc/codec/packet.rs)
ties that starting point to forwarding.

## Find your way around the repository

The server is a workspace of crates, each with its own compilation and
dependency boundary (although that is mostly to enforce a strict separation
of concerns between I/O, Orchestration, async, RFCs,...).

One of them is not really a "crate": `crates/client` is the TypeScript browser
package. I know... I should probably move that out eventually, here
is a complaint counter for it: 0, make a PR to increment it by 1 and at 10 I will
move that directory.

| Area | Responsibility | Start reading |
| --- | --- | --- |
| `o-sfu` | The orchestration layer. It holds the control I/O interfaces (HTTP and WebSocket) and the configuration | [Server rustdoc](https://odoo.github.io/o-sfu/o_sfu/) |
| `o-sfu-core` | Coordinates rooms. The central part of the SFU: networking logic and topology, the low-level packet loop and I/O, the difficult async parts and the routing decisions | [Core rustdoc](https://odoo.github.io/o-sfu/o_sfu_core/) |
| `o-sfu-router` | Describes who sends media to whom and matches compatible media settings | [Router rustdoc](https://odoo.github.io/o-sfu/o_sfu_router/) |
| `o-sfu-model` | Defines shared participant, stream and preference types. These are essentially the Discuss application-layer concepts | [Model rustdoc](https://odoo.github.io/o-sfu/o_sfu_model/) |
| `o-sfu-rfc` | Everything related to RFCs lives here, instead of being magic values sprinkled across the codebase | [RFC vocabulary rustdoc](https://odoo.github.io/o-sfu/o_sfu_rfc/) |
| `o-sfu-protocol` | Decides how the client respond to signaling and connection events. This is the brain of the client bundle, written in Rust and compiled to Wasm | [Protocol rustdoc](https://odoo.github.io/o-sfu/o_sfu_protocol/) |
| `o-sfu-telemetry` | The tooling used to observe the server | [Telemetry rustdoc](https://odoo.github.io/o-sfu/o_sfu_telemetry/) |
| Browser client | Provides the Odoo-facing `SfuClient` (essentially a client SDK) | [Client API](../crates/client/API.md) |

`o-sfu-core` is the hardest one to find your way in. It gets its own map in
[A map of o-sfu-core](#a-map-of-o-sfu-core).

### Read across a boundary in both directions

**Start with the caller**: what information does it have and what decision does
it need? Then follow the call into the types and errors of the next
component. The router, for example, determines media compatibility without
opening a network connection, so you can test it without sockets or a
browser. We explain this in
[keeping infrastructure out of domain crates](coding_guidelines/r07-preserve-crate-dependency-direction.md).

### Use rustdoc as a map

You don't have to read a crate from top to bottom. Open its rustdoc
introduction, pick a type from the overview and follow it's links or open
the repo in your IDE and use rust's great tooling (rust-analyzer) to nagivate the codebase.

## Identity, state and time

Alice disconnects and rejoins right away. To Bob, shes still Alice. For the
server, **these are different connections**, both with work in flight.
If we used only the user ID, old cleanup could affect the new
connection. We therefore use several kinds of identity:

| Identity | What it identifies | Where to begin |
| --- | --- | --- |
| User | The participant identity supplied by the application | [`UserId`](https://odoo.github.io/o-sfu/o_sfu_model/) |
| Connection | One accepted connection for that user | [`Router`](https://odoo.github.io/o-sfu/o_sfu_router/), [`MediaSession`](https://odoo.github.io/o-sfu/o_sfu_core/sfu/struct.MediaSession.html) |
| Room instance | A particular room lifetime, including protection against stale work after replacement | [`Room`](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/definition/index.html) |
| Published source | The logical media source and its encodings | [`source IDs`](https://odoo.github.io/o-sfu/o_sfu_core/engine/source_model/ids/index.html), [`descriptor`](https://odoo.github.io/o-sfu/o_sfu_core/engine/source_model/descriptor/struct.PublishedSourceDescriptor.html) |

An index alone has the same problem: a slot can have a new occupant by the
time old work comes back. The worker's
[`SlotHandle<Tag>`](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/state/slots/struct.SlotHandle.html)
**pairs the index with a generation**:

```text
                      remove old entry       reuse slot
slot 4:     [ old connection ] ───────> empty ───────> [ new connection ]
handle:          (4, 7)                                     (4, 8)
                    │
                    └────── delayed work: lookup (4, 7) ──────┐
                                                              │
                                                     generation differs
                                                              │
                                                            None
                                                 new connection untouched
```

That reuse problem is close to the ABA problem. Advancing the gerenation
lets the lookup distinguish thé old lifetime from the new one. The marker
tag lets the compiler tell session handles from consumer-stream handles.
Neither makes a handle valid in every store, so you still have to use it
with the store that created it.

Time values hide a similar trap. They all look like numbers, but three
clocks answer different questions:

| Question | Clock |
| --- | --- |
| How long has this policy choice been eligible? | Monotonic elapsed time |
| When did this happen, for an operators log? | Wall-clock time |
| Where does this packet sit in the media? | RTP media clock, which eventually wraps back to zero |

Before comparing two time values, **check that they belong to the same row**.
The guideline on [units, arithmetic and clock choice](coding_guidelines/c03-make-units-arithmetic-and-time-semantics-explicit.md)
covers the conventions in the code.

## Follow a participant into a room

Now let's follow Alice into a call in the code. At each step, ask what the
next component recieves and what it has to do before the call can move on:

1. The backend requests a room through the HTTP control API. Start with the
   [room creation handler](https://odoo.github.io/o-sfu/o_sfu/runtime/http_server/rooms/create/index.html) and
   its [HTTP contract](https://odoo.github.io/o-sfu/o_sfu/runtime/http_server/contract/index.html).
2. The browser connects by WebSocket to the returned server URL. We check
   participant credentials from the first message in the
   [`WebSocket handshake`](https://odoo.github.io/o-sfu/o_sfu/runtime/websocket_server/handshake/index.html)
   before we admit the participant.
3. The [WebSocket session](../src/runtime/websocket_server/session.rs)
   accepts Alice. We record membership through the
   [room manager](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/manager/index.html).
   [Placement](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/placement/index.html)
   chooses where to process media.
4. The [`application session`](../src/application/user_session/lifecycle.rs)
   sends the welcome state. We then start preparing media.
5. [`Server media negotiation`](../src/application/user_session/media.rs)
   lets the browser and server agree on what media they can send and
   recieve.
6. Alice requests publication. The
   [`publication transition`](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/transition/publication/index.html)
   prepares for media. Other participants can see the source only
   after the SDP answer confirms the required media settings.
7. Bob's reciever preferences become
   [`subscription intent`](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/transition/subscription/index.html).
   Once Alice's publication and Bob's receiving connection are ready, the
   server can connect them and start forwarding packets.

In step 6, we prepare to forward video before other participants can see
the publication. Alice can leave or reconnect before answering, so we must
**check the connection again before committing**:

```text
publish request ──> stage transport ──> send offer ──> await answer
                                                            │
                         Alice may leave or reconnect here  │
                                                            v
                                               same connection still current?
                                                  │                     │
                                                 yes                    no
                                                  │                     │
                                                  v                     v
                                          commit, source is    roll back the staged
                                          visible in the room  transport ressources
```

Someone has to see that rollback through, which leads to room effects and
obligations.

You can check this in `staged_publish_is_not_visible_in_room_graph_before_answer`
in the [publication tests](../crates/core/src/engine/room/transition/TESTS/publication.rs).
Long test names are common in Rust: we use them as test titles.

### Leases: a room lives on borrowed time

Odoo can reserve a room without sending anyone to it. We use a **lease**
to limit how long the reservation remains valid. Odoo must request the room
again to renew it. Otherwise, we remove it after the timeout.

We reserve the room for `ROOM_RESERVATION_TTL`, unless a join commits first.
When the last participant leaves, we allow `ROOM_DEPARTURE_GRACE` for a quick
reconnect. We call this **departure grace**. A reaper task checks deadlines
every 10 seconds:

```text
              renew
              ┌───┐
              v   │
create ──> Reservation ─────── join commits ───────> Alive
              │                                      │   ^
              │                          last leaves │   │ join commits
              │                                      v   │
              │ expires                              Grace
              │                                        │ expires
              v                                        │
           Closing <───────────────────────────────────┘
     (only once no work lease is held)
```

We can't remove a room just because a timeout expires while Alice is
joining. To prevent removal during an operation, we acquire a
[`RoomLifecycleLease`](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/directory/struct.RoomLifecycleLease.html).
This lease has no timeout. We release it after the operation finishes.
**We can remove an expired room only after all work leases have been
released. We can't acquire new ones once closing begins**.

The last lease to finish determines whether an empty room enters its grace.
**The lease is an `Obligation`**: if we drop it unfinished, we release it
without requesting removal, then report `room lifecycle lease`
([Let unfinished work make noise](#let-unfinished-work-make-noise)). A
canceled join can't keep the room alive forever.

## Separate decisions from effects

Bob changes which video he wants to receive. We update room state and
send the changes to a media worker. If we held the room lock until
both jobs finished, Carol would have to wait before changing subscriptions.

```text
time ───────────────────────────────────────────────────────────────────────>

lock held through the effects
room lock    [ Bob decides ─> Bob's worker ][ Carol decides ─> Carol's worker ]

lock released before the effects
room lock    [ Bob decides ][ Carol decides ]
RoomEffects                 [ Bob's worker ]
                                             [ Carol's worker ]
```

So we **release the room lock before waiting**. The pending work is returned as
[`RoomEffects`](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/effects/batch/struct.RoomEffects.html).
Here's Bob's change, simplified from the
[room state](../crates/core/src/engine/room/media_graph/subscription.rs)
and the
[subscription transition](../crates/core/src/engine/room/transition/subscription.rs):

```rust
impl RoomState {
    // the decision: a plain fn, so it can't .await a worker
    fn apply_receiver_intent(&mut self, ...) -> Option<ReceiverRouteCommit> {
        // a request from Bob's old connection stops here
        self.user_for_connection(user_id, connection_id)?;
        let work = self.plan_receiver_intent_change(...);
        Some(ReceiverRouteCommit {
            work,
            origin: ConsumerSetupOrigin::Subscribe,
            track_snapshots: Vec::new(),
        })
    }
}

impl RoomUserOperation<'_> {
    async fn apply_receiver_intent(self, ...) -> Option<()> {
        // the mutex guard is inline, so dropped at the end of the statement
        let commit = self.room.state.write().await.apply_receiver_intent(...)?;
        // the lock is free again, Bob's worker can take its time
        RoomEffects::from(commit)
            .execute(self.room, RoomEffectContext::runtime(self.media_transport))
            .await;
        Some(())
    }
}
```

**We must run the collected work in order.** Before we let packets
flow from a new publication, we have to apply forwarding policy. The
[effect runner](../crates/core/src/engine/room/effects/mod.rs) drives that
sequence, including the
[transport effects](../crates/core/src/engine/room/effects/transport.rs).

We release the room-state lock before awaiting transport work, yet a
[`SourcePolicyGuard`](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/instance/struct.SourcePolicyGuard.html) can span
those same awaits. **They protect different things.** The first protects the
room data while we change it. The second prevents a later policy decision from
overtaking the work needed to apply this one.

You may recognise the **functional core and imperative shell** pattern: work
out a decision, then apply it. It isn't a database transaction, though.
If a worker fails after the room has committed its change, the change
doesn't undo itself. And if alice reconnected meanwhile, we may need to
check the result's connection, source or policy revision again.

[guideline: pure decisions](coding_guidelines/p04-keep-decision-logic-pure.md),
[guideline: state guards and async effects](coding_guidelines/o02-make-asynchronous-commit-boundaries-explicit.md),
[guideline: derived-state validity](coding_guidelines/s03-define-how-derived-state-stays-valid.md)

## Linear types and lifecycle obligations

We've reserved resources for Alice's publication, but she leaves before
negotiation finishes. We must roll back the reservation. **Rust ownership
tracks who owns it, but doesn't require them to finish cleanup.**
[`Obligation`](https://odoo.github.io/o-sfu/o_sfu_core/engine/obligation/index.html)
adds that check.

### Affine and linear are different promises

First, a little type theory:

Once you move a non-`Copy` value into a function, you can't use that value
again in the caller. The function can pass it on or let it drop. Rust
accepts both. This is an **affine** type: a resource can be consumed at most
once. A **linear** type goes further and requires exactly once.

Immagine that you have a cinema ticket. You are free to give it to someone
else, to use it, or to throw it in the garbage and forget about it. That is "affine".

Now immagine that you have a live grenade (without the pin), you can give it to
someone else, you can disarm/"use" it. But you probably don't want to drop it
and forget about it. We must go from creation to consumption. That is "linear".

For Alice's reservation, we want that stronger promise: **commit it or
finish rolling it back**.

You might try `Drop`: release a resource when its value is
destroyed (RAII, Resource Acquisition Is Initialization). But our cleanup
needs to send commands to workers and wait for replies, possibly trying a
fallback along the way. **A Rust drop can't await those steps.**

o-sfu wraps explicit async cleanup in `Obligation`. Completion must use the
work's implementation. Abandoning it is detected at runtime, using Rust's
existing types rather than a full linear type system
(if you can come up with one, please open a PR!).

```text
                    used 0 times      used once     used twice
affine (`!Copy`)    ok (drop/forget)    ok          compile error
obligation          runtime error       ok          compile error
linear (ideal)      compile error       ok          compile error
```
Incomplete work is logged. Tests and debug builds also panic, as explained
[below](#let-unfinished-work-make-noise).

### The obligation owns its work

With a separate guard, you could mark work complete without running it.
`Obligation<W>` prevents that by **owning the work**.
You can inspect it through `work()` but cannot remove it. To complete or
transfer it, use these operations:

| Method | What it does | The guard afterwards |
| --- | --- | --- |
| `complete(context).await` | Runs the work's `Execute` implementation | Disarmed once execution returns |
| `try_complete(context)` | Tries a synchronous state change through `TryComplete` | Disarmed on success. Returns the obligation on rejection, still armed |
| `transfer()` | Calls `Transfer` to convert the work | Still armed, same label |
| `merge(other)` | Uses `Extend` to move work from `other` into this batch | Disarms `other` after moving the work |

You implement these traits for the operations a work type supports. For
Alice's reservation, we implement `TryComplete` and `Transfer`, but not
`Execute`:

```rust
impl TryComplete<PublishAcceptance<'_>> for PublishReservation { /* commit into room state */ }

impl Transfer for PublishReservation {
    type Target = TransportTeardownAction;

    fn transfer(self) -> Self::Target {
        TransportTeardownAction::RemoveMedia {
            session_key: self.descriptor.session_key,
            transport_media_id: self.media,
        }
    }
}
```

So you can't just mark a reservation finished:

```rust
let reservation = Obligation::new(PublishReservation { descriptor, media });
reservation.complete(()).await;
//          -------- `PublishReservation` doesn't implement `Execute<()>`
```

Follow Alice's reservation from start to finish:

```text
Obligation<PublishReservation>                    label "staged publication"
      │
      ├── try_complete, room accepts ──> PublishCommit, guard disarmed
      │                                  (its effects get their own obligation)
      │
      ├── try_complete, room rejects ──> Err(same obligation, still armed)
      │                                          │
      └── Alice leaves first ───────────────────>│
                                                 │
                                                 v
                                            transfer()
                                                 │
                                                 v
                    Obligation<TransportTeardownAction>, holding RemoveMedia
                    same guard, still labelled "staged publication"
                                                 │
                                                 v
                    complete(&media_transport).await
                    remove the media, fall back to closing the session
                                                 │
                                                 v
                                          guard disarmed
```

**Queuing rollback doesn't complete it.** `transfer()` converts the reservation
to [`TransportTeardown`](../crates/core/src/engine/media_transport/teardown.rs).
We disarm the guard only after cleanup and required fallback finish.
If we commit successfully, we complete the reservation before running effects.

### Batches nest

We guard room effects, the transport plan and its teardowns separately:

```text
RoomEffects: Obligation<RoomEffectWork>                  "room effects"
 ├── transport: Obligation<RoomTransportWork>            "room transport plan"
 │     ├── relays, source activity, route control
 │     ├── teardown: [TransportTeardown, ...]            "transport media teardown", ...
 │     └── consumer setup turns
 ├── output: notifications
 └── source_policy

execute: transport plan ──> output ──> source policy ──> last output
                                                              │
                                          "room effects" disarms here
```

When we `merge` batches, we move work before disarming the donor.
**The destination guard protects that work.** An empty batch starts unarmed.
We arm it with `extend` when adding work.

### Completion is not success

Cleanup can fail even when we've done everything required. If transport
teardown can't remove the media, it records the failure and closes the entire
session as a fallback. If that fails too, it logs it and returns. Then the
guard disarms and `MediaTransport::teardown` moves on to the next item.

**Completion means finishing the required attempts, including any fallback,
even if they fail.** Each `Execute` implementation defines these attempts and
handles failures before returning. The compiler can't verify that implementation.

### Let unfinished work make noise

A guard dropped before completion logs an error. With debug assertions or
`cfg(test)`, it also panics unless the thread is already unwinding:

```text
release:          error event "unfinished lifecycle obligation"
                  with obligation = "staged publication"

debug and tests:  the same event, then
                  panic "unfinished lifecycle obligation: staged publication"
```

This diagnostic does not perform cleanup.

`complete` transfers the guard into its future immediately. Canceling it
reports incomplete work even before the first poll. The test
`dropping_unpolled_publish_teardown_diagnoses_unfinished_cleanup` in the
[publication tests](../crates/core/src/engine/room/transition/TESTS/publication.rs)
drops an unpolled teardown future and expects the panic shown above.
`transfer()` preserves the original label, so the error identifies Alice's
publication even after conversion to cleanup.

`#[must_use]` warns when a value is ignored, but assigning thé guard to a
variable silences it. The destructor detects that case, but can be skipped with
[`mem::forget`](https://doc.rust-lang.org/std/mem/fn.forget.html) or by
terminating the process. This limits detection to ordinary execution.
See the
[destructor reference](https://doc.rust-lang.org/reference/destructors.html),
[`must_use` reference](https://doc.rust-lang.org/reference/attributes/diagnostics.html#the-must_use-attribute)
and
[asynchronous cleanup](https://without.boats/blog/asynchronous-clean-up/)
for the language rules behind that choice.

If you're curious about linear types, [I recommend this great video on the topic](https://www.youtube.com/watch?v=QtlkqJGdnuM). It is also a goal of the rust project to support it (eventually...): https://goals.rust-lang.org/2026/move-trait.html

## Cancellation, channels and shutdown

When you drop a future, you stop polling it. **You don't rewind its mutations
or unsend its messages.** The same goes for the losing branch of a
[`tokio::select!`](https://docs.rs/tokio/latest/tokio/macro.select.html)
race.

Canceling after a send might look like this:

```text
caller                                      worker
  │                                            │
  ├─── enqueue command ───────────────────────>│
  │                                            │ command waiting
  X  drop future                               │
     borrow ends                               │ execute command
     stop waiting                              │
                                               │ produce reply
    no reciever <..............................│

    canceling the wait did not undo the send
```

Before the send, cancellation may leave no command behind. After it, a retry
could perform the operation twice. We need to recognise retries or find out
what happened to the first attempt. The
[worker request code](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/worker/lifecycle/index.html)
makes those stages explicit.

This is why dropping `MediaSession` isn't a shortcut for closing it.
Canceling a worker doesn't help either: it stops the worker without
promising to process every command left in its queue.

Shutdown requires both a stop request and confirmation that the thread has
terminated. `RtcWorker::wait_for_shutdown` continues the join if its waiter
is canceled, allowing another caller to await it.

The [WebSocket session loop](../src/runtime/websocket_server/session.rs)
waits for an event and finishes handling it before waiting again.
Simplified, it looks like this:

```rust
loop {
    tokio::select! {
        biased;
        () = shutdown.cancelled() => { /* exit */ }
        outbound = self.outbound.recv_event() => {
            // Once this arm wins, the handler runs to completion.
            // A new socket message can't cut it short.
            self.handle_outbound_event(outbound).await;
        }
        message = self.reader.next() => {
            self.handle_socket_event(message, &mut pong).await;
        }
    }
}
```

The branches race to receive an event. Once selected, a handler completes
before that race starts again. **We choose the cancellation points.**

### What a message loses when the receiver falls behind

When choosing a channel, we need to decide what happens if we send faster
than we receive. That depends on what the message means:

**Commands must not be lost.** Losing Alice's close command would leave her
session open. A bounded `mpsc` queue makes the sender wait or returns an
error. The worker answers through a
`oneshot` reply and must not treat a failed send as an error, since the
caller may have stopped waiting.

**A snapshot can be overwritten.** A `watch` channel stores only the latest
value, like the overflow state of the
[user outbound queue](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/outbound/index.html).

A `broadcast` channel discards the oldest events for slow receivers. We need a
policy for whether they can continue or must disconnect.

A larger queue can handle a burst. **It can't fix a sender that
always produces work faster than the reciever can process it.**

[Tokio channels](https://tokio.rs/tokio/tutorial/channels),
[guideline: async delivery](coding_guidelines/o05-choose-async-primitives-by-delivery-semantics.md),
[guideline: spawned-work lifecycle](coding_guidelines/o03-give-spawned-work-a-lifecycle-policy.md)


## Follow a media packet

Now trace one of Alice's packets as the server recieves it, selects it's
destinations and forwards it.

Note that I will describe things from a UDP point of view, it is the main transport
mode of the SFU (/UDP is the SFU's main transport, so I'll follow the packet over UDP?). We did recently add support for TCP, and most of the logic is shared.

o-sfu relies on [`str0m`](https://docs.rs/str0m/0.24.1/str0m/) to interpret
packets, update protocol state and run protocol timers. We give it network
input and the current time. It gives us outgoing packets, events and
deadlines. o-sfu handles the sockets and determines when to call it again:

```text
              o-sfu worker: sockets, thread and clock
       ┌────────────────────────────────────────────────────┐
Alice ════> UDP recv                         UDP send ═════════> Bob
       │       │                                ^           │
       │       │ Input::Receive                 │ Transmit  │
       │       v                                │           │
       │  ┌───────────┐  Event(RtpPacket)  ┌───────────┐    │
       │  │ str0m Rtc │ ─────────────────> │ str0m Rtc │    │
       │  │   Alice   │    write_rtp()     │    Bob    │    │
       │  └───────────┘                    └───────────┘    │
       │    ^      │                                        │
       │    │      │ Timeout(t1)                            │
       │    └──────┘                                        │
       │    Input::Timeout(now) once t1 is due              │
       └────────────────────────────────────────────────────┘
```

Each session has an `Rtc` for both directions. The worker writes the
event from Alice's `Rtc` into Bob's with `StreamTx::write_rtp` and sends the
`Transmit` that Bob's emits. Alice's own `Rtc` also emits her ICE and DTLS
replies, RTCP feedback and subscribed media.

Every arrow leaving str0m is a `poll_output()` result. The worker calls it
until it returns a `Timeout` specifying the next deadline. The
[session drain](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/worker/session_drain/index.html)
does this for each ready session:

```rust
loop {
    match session_state.rtc.poll_output() {
        Ok(Output::Transmit(transmit)) => { /* queue for the socket */ }
        Ok(Output::Event(Event::RtpPacket(packet))) => { /* hand to forwarding */ }
        Ok(Output::Event(Event::KeyframeRequest(request))) => { /* ask the publisher */ }
        Ok(Output::Event(event)) => { /* observe and log */ }
        Ok(Output::Timeout(timeout_at)) => { /* drained: next deadline */ }
        Err(error) => { /* fail this session */ }
    }
}
```

Like Protocol and Router, Str0m is **Sans I/O**: a pure state machine that
works out what should happen without choosing the threads, sockets or async
runtime that make it happen. It doesn't even have its own sense of time.

In o-sfu, each worker runs the packet loop that joins those parts:

```text
 network      │        datagram
              │           │  demux: Rtc::accepts()
──────────────┼───────────v──────────────────────────────────────────
 str0m Alice  │      Alice's Rtc
              │           │  decrypt, parse
──────────────┼───────────v──────────────────────────────────────────
 forwarding   │   RtpPacket, rid "hi"
              │           │  source gate
              │           ├────────────────┬────────────────┐
              │           v                v                v
              │       recording      relay mailbox    Bob's session
              │                      (worker 2,             │  rewrite seq,
              │                       for Carol)            │  timestamp,
              │                                             │  SSRC and PT
──────────────┼─────────────────────────────────────────────v────────
 str0m Bob    │                                         Bob's Rtc
              │                                             │  write_rtp()
──────────────┼─────────────────────────────────────────────v────────
 network      │                                      Transmit ══> Bob
```

The **packet gates** are where room policy meet the packet path. The loop
applies the latest policy gates without recomputing layout
and bandwidth choices for each packet.

[loop_driver.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/worker/loop_driver/index.html),
[ingress_routing.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/packet_loop/ingress_routing/index.html),
[forwarding_planner.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/packet_loop/forwarding_planner/index.html)

### Open an RTP packet

Before we forward a packet from Alice, str0m decrypts it and parses the
RTP header. Here is the wire format, with 32 bits per row in the fixed
header:

```text
 0                   1                   2                   3  bytes
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|V=2|P|X|  CC   |M|     PT      |       sequence number         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           timestamp                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                             SSRC                              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| CSRC list, if present                                         |
+---------------------------------------------------------------+
| Header extensions, if present                                 |
+---------------------------------------------------------------+
| Codec payload: encoded audio or a fragment of encoded video   |
| ...                                                           |
+---------------------------------------------------------------+
| Padding, if present                                           |
+---------------------------------------------------------------+
```

The first 12 bytes are fixed. The remaining sections have variable sizes.
Compare the camera packet from Alice with what we send to Bob (example
values):

```diff
  V=2 P=0 X=1 CC=0          framing, rebuilt by str0m
  M=1                       marker: last packet of this VP8 frame
- PT=96
+ PT=102                    Bob's payload type for VP8
- seq=8812 ts=1638400
+ seq=301  ts=90000         Bob's packet order and media timeline
- SSRC=0x1a2b3c4d
+ SSRC=0x77e0c512           Bob's outgoing stream
  CSRC list                 passed on when present
  ext video-orientation     copied, like audio level and voice activity
- ext mid=1
+ ext mid=3                 Bob's media section
- ext rid=hi                names Alice's encoding, removed
- ext transport-seq=4410
+ ext transport-seq=127     numbered by str0m for Bob's connection
  VP8 payload               same encoded media, counters below
- padding                   stripped by str0m
```

We keep the encoded media, but the
[egress writer](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/consumer_egress/index.html)
has to build a new packet for Bob.

[RFC 3550 section 5.1](https://www.rfc-editor.org/rfc/rfc3550.html#section-5.1)

### Projection: same packet, different numbers

The small and large encodings use separate counters. If we forwarded those
numbers directly, switching versions could look like thousands of lost
packets. We map source numbers onto one stream for each receiver.
We call this **projection**:

```text
                  Alice        Bob
encoding "lo"      410  ────>  720   anchor: 410 is 720
                   411  ────>  721   +1 from the anchor
                   413  ────>  723   +3, so 722 stays missing like 412
switch to "hi"    9000  ────>  724   reanchor: Bob's last value + 1
                  9001  ────>  725   +1 from the new anchor
```

Within one source, Bob's numbers preserve their distance from the **anchor**,
so packet loss produces a gap he can repair. After a switch or a resumed
route, projection **reanchors** right after Bob's last number. Simplified
from the [VP8 counters](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/codec/vp8/enum.CounterProjection.html):

```rust
enum Projection {
    Empty,
    Anchored { src_anchor: u16, dst_anchor: u16 },
}

impl Projection {
    fn project(&mut self, src: u16, reanchor: bool) -> u16 {
        let dst = match *self {
            Self::Empty => src,
            // new source: continue right after Bob's last number
            Self::Anchored { dst_anchor, .. } if reanchor => {
                dst_anchor.wrapping_add(1)
            }
            // same source: keep the distance from the anchor
            Self::Anchored { src_anchor, dst_anchor } => {
                dst_anchor.wrapping_add(src.wrapping_sub(src_anchor))
            }
        };
        *self = Self::Anchored { src_anchor: src, dst_anchor: dst };
        dst
    }
}
```

Sequence numbers, PictureID and TL0PICIDX follow these rules. Timestamps
reanchor by the elapsed time instead of by one. Each consumer route has
separate state in
[`ConsumerStreamStore`](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/consumer_egress/streams/struct.ConsumerStreamStore.html),
so Bob and Carol number the same packet differently while sharing its bytes.

### Packets are not frames

Suppose you see Alice's packet counter rising, but Carol's video is frozen.
Packet counts don't tell you whether Carol can decode video. **One video
frame can span many RTP packets** and Carol may have received only some of them:

```text
one video frame
┌────────────┬────────────┬────────────┬────────────┐
│ packet 100 │ packet 101 │ packet 102 │ packet 103 │
└────────────┴────────────┴────────────┴────────────┘
Carol gets:    100          101          (lost)       103

packets recieved: 3 of 4          frames Carol can decode: 0
```

Packets can also arrive out of order. RTP sequence numbers track packet
order and RTP timestamps place packets on the media clock, which isn't the
wall clock.

A compressed video frame can also depend on earlier frames, which is why the
SFU waits for a keyframe when it switches what a reciever gets, as we saw in
[Switching video need a starting picture](#switching-video-need-a-starting-picture).
We check the VP8 payload to identify keyframes.

### Look inside the VP8 payload

A VP8 RTP payload contains a **payload descriptor**, then part of the encoded
frame. You need to parse the optional descriptor fields to find where the
encoded data starts:

```text
          7 6 5 4 3 2 1 0    bits, most significant first
         +-+-+-+-+-+-+-+-+
always:  |X|R|N|S|R| PID |  start flag (S) and partition index (PID)
         +-+-+-+-+-+-+-+-+
if X:    |I|L|T|K| RSV   |  flags say which fields follow
         +-+-+-+-+-+-+-+-+
if I:    |M| PictureID   |  picture counter, 7 or 15 bits
         +-+-+-+-+-+-+-+-+  M selects the width
if I,M:  | PictureID low |  second byte of the 15-bit form
         +-+-+-+-+-+-+-+-+
if L:    |   TL0PICIDX   |  base temporal-layer picture counter
         +-+-+-+-+-+-+-+-+
if T/K:  |TID|Y| KEYIDX  |  temporal layer, layer sync and keyframe index
         +-+-+-+-+-+-+-+-+
         | encoded frame fragment ...
```

`X` and `M` here are unrelated to RTP's `X` and marker. Temporal layers
group pictures so a decoder can use a lower frame rate.
[RFC 7741 section 4.2](https://www.rfc-editor.org/rfc/rfc7741.html#section-4.2)
describes every field.

The [VP8 code](../crates/core/src/engine/media_transport/rtc/codec/vp8.rs)
parses only the fields we need:

```text
Alice's packet, rid "hi"                   Bob's packet
┌──────────────────────┐                   ┌──────────────────────┐
│ S=1  PID=0           ├────── read ──────>│ S=1  PID=0           │
│ PictureID  20342     ├──── project ─────>│ PictureID  815       │
│ TL0PICIDX  91        ├──── project ─────>│ TL0PICIDX  17        │
│ N  TID  Y  KEYIDX    ├──────────────────>│ N  TID  Y  KEYIDX    │
│ frame fragment ...   ├──────────────────>│ frame fragment ...   │
└──────────────────────┘                   └──────────────────────┘

read     S=1 and PID=0 start a frame, so check the fragment for a
         keyframe prefix
project  map the counters of each RID onto one counter for Bob
───>     forward unchanged, o-sfu doesn't choose temporal layers (yet?!)
```

We rewrite PictureID and TL0PICIDX so Bob can follow a single sequence
across encodings with different counters. We don't change the compressed
data.

At the start of a frame, the encoded data has its own header. A keyframe
starts like this, with byte counts below the frame tag:

```text
          7 6 5 4 3 2 1 0
         +-+-+-+-+-+-+-+-+
         |Size0|H| VER |P|  frame tag, byte 1
         +-+-+-+-+-+-+-+-+
         |     Size1     |  frame tag, byte 2
         +-+-+-+-+-+-+-+-+
         |     Size2     |  frame tag, byte 3
         +---------------+
         | 9d 01 2a      |  keyframe sync code, 3 bytes
         +---------------+
         | width + scale |  2 bytes
         +---------------+
         | height + scale|  2 bytes
         +---------------+
         | compressed picture data ...

P=0: keyframe          P=1: changes based on earlier pictures
```

This `P` specifies the frame type, not RTP padding. To detect a keyframe
start, we check the frame type, version, sync code and nonzero dimensions.
See the
[keyframe probe](https://odoo.github.io/o-sfu/o_sfu_rfc/rtp/index.html) and
[VP8 frame header specification](https://www.rfc-editor.org/rfc/rfc6386.html#section-9.1).

### Fanout and cross-worker relay

We use cross-worker relay to send media between workers in the same process.
[Spread a room across workers](#spread-a-room-across-workers) explains placement.

**Relayed packets skip source observation and further relay** to prevent
duplicate counts and cycles:

```text
             < Alice >
                 │
                 v
┌───────────────────────────────────┐
│ worker 1: Alice's packet arrives  │
│   observe the source              │
│   plan local and relay delivery   │
└───────────────────────────────────┘
         │                │
  local  │                │ relay
         v                v
      < Bob >     ┌───────────────────────────────────┐
                  │ worker 2: relayed packet arrives  │
                  │   source already observed: skip   │
                  │   no relay onward                 │
                  │   write into Carol's session      │
                  └───────────────────────────────────┘
                                  │
                                  v
                              < Carol >
```

Share the `Arc<[u8]>` payload using `Arc::clone` for destinations and relay
mailboxes. That increments the reference count without copying bytes.
Immutable payloads need no lock. Copy headers before rewriting them for
receivers.

The [forwarding planner tests](../crates/core/src/engine/media_transport/rtc/TESTS/forwarding_planner_tests.rs)
exercise local recievers and relay destinations separately, without any sockets
involved.

### Keep one numbering across an encoding switch

Alice's small and large videos have separate packet histories. Bob need
one outgoing history across a switch These example sequence numbers show
the translation, assuming no packet loss:

```text
                              switch to large
                                     │
Alice's small video: ───410───411────┼- - - - - - - - >     (sequence numbers)
Alice's large video: - - - - - - - - └───9000───9001──>
                        │      │          │      │
                        v      v          v      v
Bob's stream:        ──720────721────────722────723───>
```

Passing Alice's numbers through as-is would make a quality change look
like lost or reordered packets. Timestamps need a similar translation so
Bob's media clock doesn't jump to the other encoding's timeline.

[`ConsumerStreamStore`](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/consumer_egress/streams/struct.ConsumerStreamStore.html)
projects both per consumer. The
[egress test](../crates/core/src/engine/media_transport/rtc/consumer_egress/TESTS/streams.rs)
`projected_timestamps_stay_monotonic_across_simulcast_ssrc_switches` checks
the timestamps.

### Close skipped gaps and keep lost ones

We need to distinguish packets we chose not to forward from packets lost
in transit. Only lost packets should leave gaps in the receiver's numbering,
so the receiver can request retransmission:

```text
Intentional gap: route paused and resumed

Alice: ───[10]───[11]───[12]─── [PAUSED] ───[1000]───[1001]───>
            │      │      │                    │        │
            v      v      v   (gap compacted)  v        v
Bob:   ───[50]───[51]───[52]─────────────────[53]─────[54]─────>
                               contiguous: no hole, no NACK

Unintentional gap: packet lost in transit

Alice: ───[10]───[11]─── (12 lost) ──────────[13]─────────────>
            │      │                          │
            v      v     (gap preserved)      v
Bob:   ───[50]───[51]────────────────────────[53]─────────────>
                               hole kept: missing 52 triggers NACK
```

We use a **delivery generation** to distinguish these cases. When we resume
a paused route, we start a new generation and continue numbering after the
last packet sent to Bob. Within that generation, we preserve gaps from
missing source packets. We must check the generation when retransmitting
too, so we don't send packets from a stream we've already replaced.

### Repair a lost packet with RTX

When Bob notices a gap like missing packet 52, his browser sends an
[RTCP NACK](https://www.rfc-editor.org/rfc/rfc4585) packet back to o-sfu.
If we still have the packet in the worker's egress cache, we retransmit it
over a dedicated RTX stream. We don't need to ask Alice to send it again:

```text
Bob                                             o-sfu
 │                                                │
 │<─── primary RTP (seq 51) ──────────────────────┤
 │                                                │
 │                                 X──────────────┤  packet 52 lost in transit
 │                                                │
 │<─── primary RTP (seq 53) ──────────────────────┤  detect missing 52
 │                                                │
 ├──── RTCP NACK (request seq 52) ───────────────>│
 │                                                │  lookup in RTX cache
 │<─── RTX packet (OSN 52 + payload) ─────────────┤  retransmit
 │                                                │
 v                                                v
 restore packet 52 into jitter buffer
```

We use separate SSRC and sequence numbers for RTX, so retransmissions do
not skew congestion control or packet loss estimates on the primary stream.
An
[RFC 4588](https://www.rfc-editor.org/rfc/rfc4588) RTX packet wraps the
original payload with a 2-byte prefix called the Original Sequence Number (OSN):

```text
┌─────────────────────────────────────────────────────────────────┐
│ RTX RTP header: RTX SSRC, RTX payload type, separate seq number │
├───────────────────────────────┬─────────────────────────────────┤
│ OSN: original seq number (52) │ original packet payload ...     │
└───────────────────────────────┴─────────────────────────────────┘
```

Bob reads the OSN, restores original packet 52 and continues decoding.

**Repairing a packet and refreshing a decoder are separate fixes.** If the
packet has already expired from the cache or the decoder lacks the reference
picture it needs, the SFU requests a keyframe instead. Read
[recovery](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/recovery/index.html) alongside
[consumer egress](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/consumer_egress/index.html)
to follow those two paths.

## Workers, scheduling and network backends

The main Tokio runtime handles HTTP, WebSocket and orchestration work. RTC
media workers run the packet loops.

Each media worker has a dedicated OS thread and **is the only one to change
its packet state**. Other tasks send it commands through a mailbox. This
resembles an actor (/This is close to the actor model?).

```text
main Tokio runtime                    media worker (its own OS thread)
HTTP, WebSocket, rooms               ┌─────────────────────────────┐
        │                            │ mailbox commands            │
        ├─────── command ──────────> │ network packets             │ <══ packets in
        │                            │ relay packets               │
        └<─── reply, observation ─── │ timers                      │ ══> packets out
                                     └─────────────────────────────┘
```

Commands share each worker turn with network packets, relay packets and
timeouts. A turn does a limited amount of work, so a flood of packets can't
stop the worker from noticing a command or timer. On the main runtime,
writing `async` doesn't stop a long CPU loop from hogging an executor thread
either.

[worker/mod.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/worker/index.html),
[lifecycle.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/worker/lifecycle/index.html),
[input.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/worker/input/index.html)

### Throughput and latency answer different questions

Batching spreads a syscall's cost over several datagrams but adds latency
while the batch fills. A queue absorbs a short burst, but if input keep
exceeding what you can process, you get delay or loss. In a call, **hearing
Alice a second late is often worse than losing a few packets**. When you
propose an optimization, name the cost you're reducing for a defined
workload
([packet-path costs](coding_guidelines/r05-keep-media-hot-paths-cheap.md)).

### Spread a room across workers

`RoomWorkerPolicy` controls whether later joins can use additional workers.
We use workers from the same process.

For the first join, we search workers in a cycle from the room instance ID
modulo the worker count. This distributes initial placements across workers.
We stop at the first healthy worker. If we can't find one, we use the
shortest delay among workers still running.

To check health, we schedule a heartbeat every 100 ms and compare the
scheduled time with when we actually run it. We report the smaller of
the last two delays to ignore isolated slow turns. If we miss a heartbeat
or get a delay of at least 100 ms, we can't report a usable measurement:

```text
heartbeat            1      2      3      4      5
reached late by      0 ms   30 ms  25 ms  3 ms   (missed)
reported delay       0      0      25     3      none
healthy (< 20 ms)    yes    yes    no     yes    no
```

Later joins go through a short list of preferences:

```text
Carol joins Alice's room
        │
        v
an assigned worker under the threshold?   ── yes ──>  join the least-delayed one
        │ no
        v
room under its router cap and an unused   ── yes ──>  new placement on that worker
healthy worker?
        │ no
        v
join the least-delayed running assigned worker
```

`ROOM_MAX_LOCAL_ROUTERS` defaults to 1, so we keep using the assigned
worker even with delays, unless it stops. We can raise the limit up to the
worker count to allow more placements.

We use `Router` (from `o-sfu-router`) in room state, outside the workers.
We map router IDs to media workers through placements. To subscribe across
placements, we need relay, as when Carol subscribes to Alice below:

```text
Router: one per room, kept in room state, no I/O
┌────────────────────────────────────┬───────────────────────────────┐
│ router 1 ─> worker 1               │ router 2 ─> worker 2          │
│   Alice  home   camera producer    │   Carol  home                 │
│                      ├──> Bob      │                               │
│                      └──> Carol    │                               │
│                           (shadow) │                               │
│   Bob    home                      │                               │
└────────────────────────────────────┴───────────────────────────────┘
                  │                                         │
                  │ worker 1's routes                       │ worker 2's routes
                  v                                         v
Alice ══> worker 1 packet loop ─── relay mailbox ───> worker 2 packet loop ══> Carol
                  │
                  ╘══> Bob
```

We create a home session to publish media within a placement. To subscribe
to a producer in a different placement, we need a **shadow session** there.
In this example, we add Carol's shadow and consumer under router 1, next to
Alice's producer. We translate this into relay from worker 1 in `o-sfu-core`.
When we remove Carol's home session, we remove the shadow too.

[placement.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/placement/index.html),
[routing.rs](https://odoo.github.io/o-sfu/o_sfu_core/options/routing/index.html),
[delay.rs](../crates/core/src/engine/media_transport/rtc/worker/delay.rs),
[topology/mod.rs](https://odoo.github.io/o-sfu/o_sfu_router/model/topology/index.html)

### UDP backends and TCP fallback

| Backend or transport | Idea to understand |
| --- | --- |
| `tokio` UDP | Use the async runtime to wait for socket readiness |
| Linux `tokio_batch` UDP | Send several datagrams together to reduce syscall overhead |
| Linux `io_uring` UDP | Submit operations and recieve they're completion events |
| Optional RTC TCP | Offer another media path when UDP connectivity is unavailable |

The [deployment guide](deployment.md) covers backend selection. For a
learner, the interesting difference is what the kernel tells you and when.

With **readiness-based I/O**, a worker waits until the socket reports that a
read can probably make progress, then attempts the read. With
**completion-based I/O**, it submits the operation and later recieves its
result:

```text
readiness                             completion (io_uring)

worker              kernel            worker                  kernel
  ├── wait ───────────>│                ├── submit read(buf) ────>│
  │<── readable ───────┤                │                         │  kernel is
  ├── read(buf) ──────>│                │                         │  using buf
  │<── data ───────────┘                │<── read completed ──────┘
```

That change what we can do with a buffer. Once a read is submitted, **we can't
assume the kernel is done with its memory** just because our task wants to
cancel or shut down. Here too, the lifetime of the work matters as much as
the lifetime of the Rust value that represent it.

## A map of o-sfu-core

Use this map to find code in `o-sfu-core`.

Use `MediaTransport` to communicate between room code and RTC workers.
Room code runs on the main Tokio runtime. It records receiver intent and
chooses what to forward. RTC workers manage str0m sessions and forward packets.

```text
                                                             
        MediaSession               dirty rooms, snapshots <───────────┐
     one per connection                       │                       │
              │                               │                       │
              v                               v                       │
         transition                      policy turn                  │
              │                               │                       │
              ├────────> RoomState <──────────┤                       │
              │          one RwLock           │                       │
              v                               v                       │
         RoomEffects                  gates, activity,                │
              │                       bandwidth targets               │
              │                               │                       │
              └───────> MediaTransport <──────┘                       │
                              │                                       │
                              │ commands                              │ [[ main runtime ]] tokio tasks on thread pool
- - - - - - - - - - - - - - - │ - one mailbox per worker - - - - - - -│- - -
                              │                                       │ [[ media workers ]] 1 worker per thread
         ┌────────────────────┼─────────────────────┐                 │
         │                    v                     │                 │
         │                 control                  │                 │
         │                    │ writes gates        │                 │
         │                    v                     │                 │
         │             PacketLoopState              │                 │
         │                    │ read per packet     │                 │
         │                    v                     │                 │
UDP/Relay ════════════> packet_loop ═══════════════════> UDP, relay   │
 Input   │                    │                     │      Output     │
         │                    v                     │                 │
         │              observations ───────────────┼─────────────────┘
         │                                          │
         └──── RtcWorker, one OS thread each ───────┘
```

Paths below are under `crates/core/src/engine/`, with `rtc/` short for
`media_transport/rtc/`. `sfu.rs` is at the crate root.

| Purpose | Module | Runtime |
| --- | --- | --- |
| Connection operations | `sfu.rs`: `MediaSession` | Main runtime |
| Which rooms exist, admission and placement | `room/manager/`, `room/placement.rs` | Main runtime |
| Receiver preferences | `room/state/`, `room/media_graph/` (holds the `Router`) | Main runtime |
| Updating room state | `room/transition/`, then `room/effects/` | Main runtime |
| Choosing streams for receivers | `room/source_policy/` | Main runtime |
| Communicating with workers | `media_transport/` | Main runtime |
| Worker commands | `rtc/commands.rs` | Both |
| SDP, media and route controls inside a worker | `rtc/control/` | Worker |
| Running worker turns | `rtc/worker/loop_driver.rs` | Worker |
| Demux, observe, plan and flush | `rtc/packet_loop/` | Worker |
| Sessions, routes, gates and relays | `rtc/state/` | Worker |
| Rewriting packets for a receiver, RTX | `rtc/consumer_egress/` | Worker |
| Keyframe requests | `rtc/recovery/` | Worker |
| VP8, H.264 and RID rules | `rtc/codec/` | Worker |
| Sending datagrams | `rtc/egress/` | Worker |

### Commands go down

A transition updates `RoomState` under the lock and returns `RoomEffects`,
which call `MediaTransport` once the lock is released
([Separate decisions from effects](#separate-decisions-from-effects)).
`MediaTransport` sends an `RtcWorkerCommand` to the worker running that
session. When we need a response, for example when applying an SDP answer,
we include a `oneshot` channel. Route controls are best effort: we may have
already removed the target relay or session.

**Commands are the only way in.** Nothing outside the worker thread touches
`PacketLoopState`, so the packet path doesn't need a lock.

### Packets stay below

In `packet_loop`, we find the session for a datagram and let str0m
decrypt it. We process RTP packets through three steps in
`PacketForwarder`, before processing another packet can change route state:

```text
observe   resolve source and encoding, count bitrate, audio activity, keyframes
plan      read RouteTable: local destinations, relay targets, gates
flush     packet sinks ─> relay mailboxes ─> local sessions (consumer_egress)
```

In `RouteTable`, we use `PacketLayerGate` to choose which source encodings
(RIDs) we may send to a destination. We first check the union of destination
gates, then check delivery separately for each destination. To switch
encodings, we wait for a keyframe. We send requests through `recovery`,
using the producer's worker if the source is on a different worker.

**We write gates only in room policy. The packet loop uses them without changes.**

### Observations go up

We collect observations on workers without calling room code:

- **Snapshots:** we record receiver bandwidth, transport health and media
  quality in `RtcSnapshotState`, using a mutex outside the packet path.
  Room code obtains these through `MediaTransport`, along with source
  bitrates. We request active speakers from workers with a cold-path command.
- **Wakeups:** when speaker ranking or estimated bandwidth changes, or we
  have a new one-second bitrate sample, we mark the room dirty on
  `SourcePolicySignal` so policy can reconsider which streams to forward.

In `spawn_source_packet_policy_update_task`, we wait for those wakeups,
read snapshots and plan under the room state read lock. We then send
commands to update gates, route activity and bandwidth targets. We also
schedule policy deadlines, so the upgrade delay from
[Keep the call steady](#keep-the-call-steady) can expire without another
observation.

```text
                          [[ main runtime ]]

  SourcePolicySignal   ───────── wakeup ─────────>  policy turn
  coalesced per room   <─────── deadline ─────────  one per room at a time
                                                    reads snapshots, plans
          ^                                             │
          │ marks room dirty                            │ gates, activity,
          │                                             │ bandwidth targets
- - - - - │ - - - - - - - - - - - - - - - - - - - - - - │ - - - - - - - - - - -
          │                                             v
     observations    <────────────────────────────  packet loop
     bitrate: per media packet                      applies the gates
     speech: per audio packet
     bandwidth: per str0m estimate

                          [[ media worker ]]
```

This implements the feedback loop from
[Budgeting is a feedback-control problem](#budgeting-is-a-feedback-control-problem).
**We must check room state before acting on observations** because snapshots
can race packet processing or teardown.

**We update only worker-local state per packet.** We need locks, wakeups
and planning only for changes:

- We count bitrate with a few atomic operations. Only the worker writes
  that counter. We track speech in `RouteTable` without locking
  per packet.
- When we receive the first packet after registration or a full idle window,
  complete a bitrate window or change speaker ranking, we mark the room dirty.
  We deduplicate room IDs across a packet batch, locking `SourcePolicySignal`
  at most once.
- Bandwidth takes the snapshot mutex once per str0m estimate.
- We wake the policy task only when adding to an empty set of pending
  rooms. Repeated updates for a pending room need only a single turn.
- We send commands only if selections change. Otherwise, we skip commands
  and the room state write lock.

### The special subsystems

| Subsystem | Policy | Implementation |
| --- | --- | --- |
| Cross-worker relay | `RoomTopology`: we create a shadow consumer for subscriptions across placements | Through `MediaTransport`, we add a relay target on the producer worker using the subscriber worker's mailbox. We keep it inactive until enabling it through route control |
| Keyframe recovery | Decoder gates in `RouteTable` | `rtc/recovery/`: we retry on the consumer worker and request keyframes from the publisher through the producer worker |
| Packet sinks (like recording) | `RoomPacketSinkRegistry` | We call sinks synchronously on the origin worker before relaying. We don't filter by gates and sinks must not block |
| Placement | `room/placement.rs` | See [Spread a room across workers](#spread-a-room-across-workers) |
| Diagnostics | `room/read_model.rs` | We collect room data under the lock and transport snapshots after releasing it. These don't form an atomic snapshot |

[core rustdoc](https://odoo.github.io/o-sfu/o_sfu_core/),
[rtc/mod.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/index.html),
[route_table.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/state/route_table/index.html),
[policy_invalidation.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/policy_invalidation/index.html)

## Back in the browser client

You can apply what we've learned on the server to
[The browser client](#the-browser-client) too.

### Messages, requests and correlated answers

When handling Bob's SDP answer, we need to know which offer it belongs to.
**Valid SDP isn't enough**: we can't complete the current negotiation with
a response to an older offer.

We therefore identify signaling requests so we can match responses to them.
In the message format, `q` identifies the request and `r` refers to it:

```text
o-sfu ─> Bob   {"t": "offer", "q": "server-3", "p": {"sdp": "..."}}
Bob ─> o-sfu   {"t": "offer", "r": "server-3", "p": {"sdp": "..."}}
                              ───────────────
                              answers request server-3
```

If the server is waiting for `server-3`, an answer with `"r": "server-2"` is
rejected as unknown or stale. Events don't need an answer, so they contain
neither field. [envelope.rs](../crates/protocol/src/signaling/envelope.rs)
defines the format.

The [request tracker](../crates/protocol/src/core/request_tracker.rs) tracks
requests until they recieve a reply or time out.
[Server media negotiation](../src/application/user_session/media.rs) uses
that correlation when handling SDP offers and answers.

### Ordered turns and reentrancy

Suppose we start an operation on Bob's connection, then replace that
connection while awaiting a Promise. We now have a result for a connection
we're no longer using. JavaScript runs one callback at a time, but
**other events can change state while we await a Promise**.

[`TurnQueue`](../crates/client/src/internals/turn_queue.ts) makes turns run
in order. When an interrupted operation resumes, it can check whether
it should continue:

```text
turn:      replaceTrack() on peer 1 ─── await ...................... resolves
                                                                       │
recovery:                 peer 1 replaced by peer 2                    │
                                                                       v
                                                   isCurrent()? no, drop the result
```

[`PeerSession`](../crates/client/src/internals/peer_session.ts) and
[`LocalUploads`](../crates/client/src/internals/local_uploads.ts) also check
which generation of the peer an operation belongs to, like Alice's
connection identity on the server.

Order matters within a turn too. The application can observe whether a
public update comes before or after browser teardown. Running the commands
as parallel Promises would change that, even if each command still ran once.

### Join tracks with their meaning

When Bob receives a media track, we know whether it contains audio or video.
We don't know who published it or whether it represents a microphone, camera
or screen share. We allow multiple streams of each type. To identify the
track, we need a binding snapshot from the server, which may arrive before
or after the track.

Both halves include a MID. The browser's `track` event gives the MID of its
transceiver. Each server binding maps that MID to a participant and stream
type and indicates whether the stream is active.
[`RemoteMedia`](../crates/client/src/internals/remote_media.ts) maintains one
slot per MID and tells Discuss about a track only once its slot has both:

```text
MID "3" in Bob's browser

   event                      slot track   slot binding     Discuss gets
1  binding: Alice, camera     -            Alice, camera    nothing yet
2  track event                track A      Alice, camera    TRACK: Alice's camera is track A
3  binding: Carol, camera     - (dropped)  Carol, camera    nothing yet
4  next track event           track C      Carol, camera    TRACK: Carol's camera is track C
```

We can receive steps 1 and 2 in either order. In step 3, the server reuses
the MID. We discard the old track so we don't show Alice's video as Carol's.

Snapshots contain all bindings. We remove slots for MIDs missing from a new
snapshot. We also apply Bob's preferences to `active`: if Bob disabled
Alice's camera with `subscribe`, we report it as inactive even if the server
binding says active.

### Recovery replays state selectively

After a brief connection loss, we keep Bob's intent to publish or subscribe.
We **restore subscriptions but don't repeat broadcasts**, which would
repeat application actions:

| After reconnecting | The client replays |
| --- | --- |
| Authentication succeeds | Subscriptions and participant information |
| Media is ready again | Active publications |
| Never | Broadcasts |

How the connection closed determines whether there is a reconnect at all:

```text
recoverable close                                ─> retry with increasing delays
explicit disconnect                              ─> clear intent, no recovery
auth failure, protocol failure, kick, room full  ─> stop, no endless retry loop
```

[Connection lifecycle](https://odoo.github.io/o-sfu/o_sfu_protocol/core/connection_lifecycle/index.html)
and [sticky replay](../crates/protocol/src/core/sticky_replay.rs) implement
these rules.

**`connect()` can return before authentication and media negotiation finish.**
Wait for the documented events to know when you can use the connection.

## Conclusion

Back to the first question: what happens when someone turns on their camera?
Let's follow Alice one last time:

1. Alice calls `sfu.publish("camera", track)` and the client and server
   negotiate it with SDP
   ([agree on media](#agree-on-media-and-connect-the-streams)).
2. We reserve media before receiving the SDP answer. We must either commit
   that reservation or clean it up
   ([obligations](#linear-types-and-lifecycle-obligations)).
3. After the SDP answer confirms the media settings, we commit the
   publication so other participants can see it
   ([follow a participant into a room](#follow-a-participant-into-a-room)).
4. Bob's pending request for the camera
   ([Bob can want a route that can't exist yet](#bob-can-want-a-route-that-cant-exist-yet))
   becomes a route. We apply forwarding policy before sending packets
   ([separate decisions from effects](#separate-decisions-from-effects)).
5. We send the large version to Bob and the small version to Carol
   ([simulcast](#simulcast-one-camera-several-versions)), each starting at a
   keyframe
   ([a starting picture](#switching-video-need-a-starting-picture)).
6. Alice's packets go through str0m in her session, get forwarded, then go
   through str0m again in Bob's and Carol's sessions with their own
   numbering ([follow a media packet](#follow-a-media-packet)).

And if Alice reconnects in the middle of all this, her new connection gets
a new identity while the old connection's cleanup still has to finish. The
client replays her publication once media is ready again.

Well done, you've reached the end. Don't hesitate to come back to it if you're
working on a system in particular, sometimes it's easier to read about a system
only after you thinkered with it.

Want to read more?! Check the [coding guidelines](coding_guidelines.md).

## Rust learning ressources

### Talks

- ["Considering Rust" by Jon Gjengset](https://www.youtube.com/watch?v=DnT-LUQgc7s)
- ["Rust for Everyone!" by Will Crichton](https://www.youtube.com/watch?v=R0dP-QR5wQo)
- ["The Rust Programming Language" by Aaron Turon](https://youtu.be/O5vzLKg7y-k)
- ["Crust of Rust async/await" by Jon Gjengset](https://www.youtube.com/watch?v=ThjvMReOXYM)
- ["Rust makes cents" by No Boilerplate](https://www.youtube.com/watch?v=4dvf6kM70qM)

### General overview

- [The Rust Book](https://doc.rust-lang.org/book/)
- [Comprehensive Rust by Google](https://google.github.io/comprehensive-rust)

### Good References

- [Canonical's rust best practices](https://canonical.github.io/rust-best-practices/)
- [The Rustonomicon (unsafe/advanced)](https://doc.rust-lang.org/nomicon/)
- [Rust cheat sheet](https://cheats.rs/#data-structures)
- [Clippy](https://rust-lang.github.io/rust-clippy/)
- [Rust by Example](https://doc.rust-lang.org/rust-by-example/)
- [Rust Cookbook](https://github.com/rust-lang-nursery/rust-cookbook/)
- [Rust Atomics and Locks](https://mara.nl/atomics/)
- [The Tokio doc](https://docs.rs/tokio/latest/tokio/)
- [Idiomatic Rust snippets](https://idiomatic-rust-snippets.org/)
- [The Rust Performance Book by N. Nethercote](https://nnethercote.github.io/perf-book/introduction.html)
