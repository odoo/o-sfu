# Hello?? Can you see my screen?

It's going to be long, but it's a synthesis of what I think is important to learn
if you really want to dive deep into (/understand the internals of?) the sfu.

Suppose you want to change what happen when someone turns on their camera.
Past the browser method that handles the request, a few more questions come
up. What the other participants want to see? When does the server start
forwarding packets? Which version of the video does each of them get?

Answer those and you've visited a large part of o-sfu. This guide takes that
visit with you (/walks that path with you?) and stops to explain the features along the way.

For the examples, we will follow a call with 3 participants that looks like this:

```text
Alice   publishes audio and camera video, then reconnecst later
Bob     pins Alices camera
Carol   watches on a weaker connection
```

(note that pinning is not a SFU concept, but a Discuss feature that the SFU can express
and use to take decisions (/use when choosing what to forward?), it's for example when you're watching a single stream in full screen)

The reconnect happens while the server is still cleaning up Alice's old
connection.

That reconnect hints to what makes an SFU hard: **everything in a call happens
at the same time** and moves forward on its own. The server only sees messages
about a browser's state, which can arrive late, out of order or not at all.

It also happen very fast, on a clock nobody can pause (/in real time, with
no way to pause?). Alice's camera produces a frame every 33ms, often spread
over several packets. [ITU-T G.114](https://www.itu.int/rec/T-REC-G.114)
puts the one-way delay budget of a natural conversation around 150ms,
network included. A worker forwarding 100k packets per second has 10µs for
each one.

That changes what "slow" means. A packet that arrives after its frame has
been played is useless to Bob and queuing it only delays the packets behind
it. That's why the repair cache keeps packets only briefly and why a decoder
that has fallen too far behind gets a keyframe instead.

Moreover (/On top of that?), there is no single "now" in a system like this.
Leslie Lamport's
[Time, Clocks, and the Ordering of Events in a Distributed System](https://lamport.azurewebsites.net/pubs/time-clocks.pdf)
shows that without a shared clock, **the only order you can trust is
causality**. Two events with no causal link can reach the server in either
order, so the code has to be correct for both.

Here is what a few seconds in a call could look like:

```text
time ------------------------------------------------------------------->

Alice     publish(camera) ...... SDP answer .......... Wi-Fi drops
Bob              subscribe(Alice's camera, pinned)     disconnect()
Carol                             connect() ....... Error .......... recovery
network          Carol's packets   Bob's NACK       RTCP reports
timers                     ICE check due        keyframe request due
server    reserve media ... commit? ......................... cleanup?
packet loop     packet - command - packet - packet - packet - packet
```

This is **a concurrency problem before it's a parallelism one**. Even with a single
thread, Alice can reconnect between the two lines of your code that sit on
either side of an `.await`.

You'll meet these consequences throughout the guide:

- **A message describes the past**. Check that it still holds for the current
  connection ([identities and generations](#identity-state-and-time)).
- **Keep what people want, not only what exists right now**. Bob's request for
  Alice's camera has to wait for the camera
  ([intent and reconciliation](#bob-can-want-a-route-that-cant-exist-yet)).
- **Work you start is work you owe**, even if the participant who asked for it
  has left ([obligations](#linear-types-and-lifecycle-obligations)).
- **Don't hold a lock while you wait**. Decide, release the lock, then carry the
  decision out ([decisions and effects](#separate-decisions-from-effects)).

The focus is o-sfus features and the mechanisms we use to express it (/to
implement them?). The rules above span functions, tasks or machines, so we
move each one somewhere it can be checked: IDs that can't be mixed up,
states that can't exist at the same time, work that complains when it's
dropped unfinished. Code that looks odd is often such a rule (which is
hopefully commented).

For the Rust basics, see the [learning ressources](#rust-learning-library)
at the end.

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
20. [Rust in o-sfu](#rust-in-o-sfu)

## What an SFU does

Picture Alice and Bob in a call. Then Carol joins. In a mesh (P2P) call,
Alice sends her media to both. Every new person means it cost her one more
upload, so `n` participants means `n - 1` uploads (+encryption):

```text
Alice's uploads in a mesh call

2 people:  Alice --> Bob                               1 upload
3 people:  Alice --> Bob, Carol                        2 upload
6 people:  Alice --> Bob, Carol, Didier, Eric, Frank   5 upload
```

With a SFU, Alice sends her audio and video once. The server decide what to
forward to Bob and Carol and makes the copies itself, which is called
**fanout**. Now its the SFU pays its network cost instead of Alice.

```text
                           +--> Bob: Alice's camera at higher quality
Alice: audio + camera --> SFU
                           +--> Carol: Alice's camera at lower quality
```

Alice may also send several qualities of her camera, so each reciever can
get a version that fits.

You may also have heard of a MCU (multipoint control unit), which decodes
participants media and re-encodes it into a new output. An SFU forwards the
streams as they were sent and each reciever arranges they're own view of the
call (also a SFU is less CPU(/+GPU?)-expensive since there is no transcoding).

Inside the SFU there are really two flows (/two paths?). It helps to follow them
separately:

```text
Control: join the room, turn on a camera, choose what to recieve
              -> the server decides who should recieve what

Media:   participants send their audio and video
              -> the server forwards the selected streams
```

Control decides what should flow. Media does the flowing. **The two can fail
independently**, which is how you end up in a room with everyone in the
participant list and no video.

## Tenants and rooms

(I will mention Odoo/Discuss a few times below, but o-sfu is tenant agnostic, it does not need
to communicate with an Odoo "DB" specifically)

When you join a call in Discuss, Odoo decide which call you belong to and
gives you the credentials to join it (Odoo negotiate these credentials with
the SFU, one room at a time). One SFU process can serve many tenants, such
as Odoo databases, each with many rooms:

```text
o-sfu process
   |
   +-- tenant: Odoo database A
   |     +-- room: [<Alice>, <Bob>, <Carol>]
   |     +-- room: [another call]
   |     +-- ...
   | 
   +-- tenant: Odoo database B
         +-- room: ...
```

note: o-sfu does not "spawn" odoo servers, here it just means that o-sfu's rooms can be "managed/owned" by different tenants

## The browser client

Discuss (or any front-end) never directly manipulates the WebRTC API. It
runs the whole call through one object, `SfuClient`, from the browser
bundle. Here's Alice joining with her microphone and camera:

```js
import { SfuClient } from "/bundle/odoo_sfu.js";

const sfu = new SfuClient();
sfu.connect(serverUrl, jsonWebToken, { channelUUID });
sfu.publish("audio", microphoneTrack);
sfu.publish("camera", cameraTrack);
```

And here's Bob pinning her camera and waiting for the track to arrive:

```js
sfu.subscribe(aliceSessionId, { audio: true, camera: true, cameraLayout: "pinned" });
// by default you are subscribed to all participants
// (but still subject to SFU limits decisions)

sfu.addEventListener("update", ({ detail: { name, playload } }) => {
    switch(name) {
        case CLIENT_UPDATE.TRACK:
            const { sessionId, type, track, active } = payload;
    }
});
```

Carol issues the same `subscribe` call with `cameraLayout: "visible_thumbnail"`.
That one field is you can tell the SFU you don't mind a smaller picture (/That one field tells the SFU she doesn't mind a smaller picture?)

The public API is small and very simple (/The whole public API fits in one table?):

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

All that "how" still has to happen somewhere. A Rust state machine,
`ProtocolCore`, **receives an event and returns an ordered list of commands**
without touching a socket or a browser API (that's Sans I/O, and we'll see
it in several other places in the codebase). A TypeScript runtime carries
out the commands in order, because a later command may depend on an earlier
effect.

```text
BrowserRuntime (TypeScript)                        | ProtocolCore (Rust / Wasm)
---------------------------------------------------+-------------------------
incoming event (user call, socket, timer)          |
      |                                            |
      |--------- 1. input event ------------------>| pure state transition
      |                                            |        |
      |<-------- 2. ordered commands --------------|--------+
      v                                            |
execute in order:                                  |
  [ cmd 1 ] ----> [ cmd 2 ] ----> [ cmd 3 ]        |
      |               |               |            |
      v               v               v            |
mutate WebRTC   send signaling    notify app       |
 (add track)     (WebSocket)    (state update)     |
---------------------------------------------------+-------------------------
```

The protocol is in Rust to get the compiler's stronger guarantees for the
client's critical logic. The typescript part is just a "simple" executor
(/only carries out the commands it's given?).

The client faces the same problems as the server: stale answers, interrupted
operations and reconnects. We'll come back to it in
[Back in the browser client](#back-in-the-browser-client).

[client/README.md](../crates/client/README.md)

## A call through the architecture

Now that we've covered the big picture, let's dive into the flow (/follow the call?) step by step,
system by system.

Alice joins an call in Discuss and Bob wants her camera. Room creation gives
them somewhere to meet, authentication lets each of them in, negotiation
agrees on media settings and routing connects what they send to what they
recieve. Media workers then carry out those decisions as packets arrive.

### Create the room and join it

The tenant (like a odoo DB) backend calls `GET /v1/channel` to create a SFU
room and negotiate its credentials. Odoo then (/must first?) gives the
browser the connection details and participant signed credentials it need.
Alice's browser sends them over a websocket. The SFU authenticates her,
assigns a media worker, sends the welcome state and begins media
negotiation.

```text
Odoo backend                           o-sfu
     |                                   |
     |------ GET /v1/channel ----------->|
     |<----- room UUID + URL ------------|
     |
     | room information + credentials
     v
Alice's browser                       o-sfu
     |                                   |
     |------ open WebSocket ------------>|
     |------ authenticate -------------->|
     |<----- welcome --------------------|
     |<----- SDP offer ------------------|
     |------ SDP answer ---------------->|
     |                                   |
     |<===== WebRTC transport ==========>|
```

### Agree on media and connect the streams

The server send an SDP offer over the websocket and the browser answers it
(this is mostly handled by the webRTC API of the browser). ICE and DTLS then
set up the network path and its encryption. The websocket stays open
afterwards, because the connection can change at any time (be re
negotiated), and also some business-level messages go through websocket
(/and application messages also travel over it?).

**Nothing flows** until Alice publishes her camera and Bob subscribes to it. The
room then works out which routes can exist and reciever policy decide what
to forward, including the video qualité that match what bob needs.

```text
Publications + subscriptions + negotiated media settings
                         |
                         v
              Room and routing decisions
                 |                  ^
                 |                  |
                 +--> further offer/answer when needed
                 |
                 v
          commands to media workers
```

Negotiation and routing don't stop after the first round: a new publication
or subscription can require another offer and answer.

### Forward packets while the call changes

Inside each media worker, a packet loop processes network input, commands
and timers. It forwards Alices packets to Bob and sends Carol a copy chosen
based her own preferences and connection.

```text
                 WebSocket requests
                         |
                         v
                  Room decisions <------------+
                         |                    |
                  worker commands        observations
                         |                    |
                         v                    |
Alice == media ==> [ worker packet loops ] ---+
                              |
                              +== selected media ==> Bob
                              |
                              +== selected media ==> Carol
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
| WebSocket | Carries messages about joining, sending media and choosing what to recieve |
| [SDP](https://www.rfc-editor.org/rfc/rfc8866) (Session Description Protocol) | Describes the media formats and connection settings the browser and server agree to use |
| ICE (Interactive Connectivity Establishment) | Tests possible network paths and chooses one that works |
| [STUN](https://www.rfc-editor.org/rfc/rfc8489) (Session Traversal Utilities for NAT) | Supplies the connectivity-check messages used by ICE |
| [TURN](https://www.rfc-editor.org/rfc/rfc8656) (Traversal Using Relays around NAT) | Relays traffic when a direct network path isn't available |
| DTLS-SRTP (Datagram Transport Layer Security and Secure Real-time Transport Protocol) | Negotiates keys and encrypts media between a browser and the SFU |
| RTP (Real-time Transport Protocol) | Carries encoded audio or video in packets, with information about their order and timing |
| RTCP (RTP Control Protocol) | Carries feedback about media delivery, such as reception reports and repair requests |

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
Alice's camera --> producer --+--> consumer for Bob
                              |
                              +--> consumer for Carol
```

The [router](https://odoo.github.io/o-sfu/o_sfu_router/) describes those
relationships. It doesn't move packets: the transport forwards media to Bob.

Before a route can carry video, the router work out what it can accept from
Alice, then match that against what Bob's connection supports (in practice
all sides support both vp8 and h264 as they are required by the webRTC
standard for compliant endpoints like browsers).

they're connections negotiate media settings independently, so some packet
identifiers need translating between them. For example:

```text
Alice's connection:  VP8 is payload type 96
Bob's connection:    VP8 is payload type 98
forwarding:          rewrite 96 -> 98, leave the VP8 bytes alone
```

The [RTP negotiation code](https://odoo.github.io/o-sfu/o_sfu_router/model/rtp_negotiation/index.html)
shows how those parameters are matched.

### Bob can want a route that can't exist yet

Bob might ask for Alice's camera before she turns it on. His request has to
be recorded until the source exists and survive Alice pausing and resuming
her caemra. That's why **a subscription preference and an active consumer
route are separate informations**.

```text
Bob's intent: recieve Alice's camera
                       |
             publication exists?
                reciever ready?
             policy admits route?
                       |
                       v
             current transport route
```

The job here is **reconciliation**: take the desired state and current
capabilities, work out what can exist now, then update the parts that
differ. Turning "we can't create the route yet" into "delete Bob's
preference" would lose his request. You can think of them like switches in
series (/conditions that must all hold at once?).

Partial updates follow the same idea. **A field Bob doesn't mention keeps its
value**, which is a different request from switching it off. `DownloadStates`
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
explains why the API keeps them distinct. See also
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

Bob pins Alice's camera while Carol sees it as a thumbnail on a weaker
network. One video qualtiy for the whole room can't suit both of them, so
**policy chooses for each reciever** using source demand, layout intent, active
speakers and bandwith observations.

### Simulcast: one camera, several versions

Sending them both the large video wastes Carol's bandwith, since shrinking
her tile doesn't shrink what she downloads. The small one would leave Bob
with a blurry picture.

Alice's browser can encode her camera several times at different
resolutions, frame rates or bitrates. Sending these versions together is
called **simulcast**. Each version is a stream in its own right, so the SFU
can choose one per reciever:

```text
Alice's browser              SFU                         recievers
                    +----------------------+
large version ----->| large for Bob        |-----------> Bob's pinned tile
medium version ---->| choose per reciever  |
small version ----->| small for Carol      |-----------> Carol's thumbnail
                    +----------------------+
```

Alice pays for the extra encoding and upload bandwith, so the SFU doesn't
have to resize or re-encode the video.

The codec defines how media is compressed and reconstructed. o-sfu enables
Opus for audio and VP8 for video by default in its
[codec options](https://odoo.github.io/o-sfu/o_sfu_core/options/codecs/index.html).
[RFC 8853](https://www.rfc-editor.org/rfc/rfc8853) describes how simulcast
is negotiated.

### Budgeting is a feedback-control problem

As Bob's network changes, the SFU observes his bandwith and the source
rates, chooses what to deliver, then observes the result. **Those estimates
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

bandwith isn't the only limit. The policy also caps the number of video
routes and gives a pinned camera, thumbnails and hidden content different
priorities. These choices follow a defined policy (/a fixed set of rules?)
rather than a search for a globally optimal allocation.

**Don't mix up the capacity in use with the capacity we want.** During
congestion, the SFU may pause a video route while sitll asking it's bandwith
controller for enough capacity to restore it. Otherwise the paused route
would lose its recovery signal. See the
[bandwith control code](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/control/bwe/index.html).

### Keep the call steady

Each change can go wrong in one direction or the other: **too late and the
call suffers, too early and it flickers**. That cost sets how quickly o-sfu
acts:

| Change | When o-sfu makes it | Why |
| --- | --- | --- |
| Lower video quality | Right away | Staying would make congestion worse |
| Raise video quality | After a dwell | Each upgrade costs a keyframe and the estimate may drop again |
| Pause a video route under pressure | After continuous pressure | A short dip shouldn't blank a tile |
| Stop treating Alice as speaking | After a short hold window | A pause between words isn't the end of a sentence |
| Switch to another version | At that version's next keyframe | The decoder needs a starting picture |

Imagine Carol's bandwith estimate hovering around what Alice's larger video
needs. Following every fluctuation would switch her back and forth, with a
keyframe request to Alice on each upgrade. Starting on the small version:

```text
tick                    1    2    3    4    5    6    7    8    9
large version fits?     yes  no   yes  no   yes  yes  yes  yes  no
follow every estimate   L    S    L    S    L    L    L    L    S
o-sfu                   S    S    S    S    S    S    L    L    S
                                            |- dwell -|         |
                                                      |         +-- down right away
                                                      +-- up after a full dwell
```

This is **hysteresis**: the decision depends on where you are now, as well
as the latest measurement. `VideoAdaptationTuning` sets the delays:
`upgrade_dwell` for upgrades and `soft_pause_dwell` for pausing a route.

Audio has the same problem. Dropping Alice's audio as soon as her sound
level fell would make her speech choppy. The
[active-speaker detector](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/state/route_control/active_speaker/index.html)
reads the audio level and voice activity sent with her packets and keeps her
marked as speaking for a short time after that activity drops:

```text
time ----------------------------------------------------------->

Alice's voice:   #####  ###  ######
speaking:        [===========================]
                                   |<------->|
                                   hold window (250 ms)
```

The window is `ACTIVE_SPEAKER_HOLD_WINDOW`. The room's
[audio policy](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/source_policy/audio/index.html)
combines it with what each reciever wants to hear: if Bob has muted incoming
audio, Alice speaking doesn't override his choice.

[input.rs](../crates/core/src/engine/room/source_policy/video/input.rs),
[projection.rs](../crates/core/src/engine/room/source_policy/video/projection.rs),
[TESTS/solver.rs](../crates/core/src/engine/room/source_policy/video/TESTS/solver.rs),
[active_speaker.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/state/route_control/active_speaker/index.html)

### Switching video need a starting picture

Carol opens Alice's camera in a larger tile. She hasn't recieved the earlier
pictures from that version, so changes based on those pictures won't help
her. A **keyframe** gives her a clean start:

```text
time -------------------------------------------------------------->

large video:    [keyframe] [changes] [changes] [keyframe] [changes]
                                               ^
Carol's choice:                 select large   |
                                    |          |
SFU forwarding:                     +-- wait --+-- forward --------->
                                               |
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
component. The router, for example, decides media compatibility without
opening a network connection, so you can test it without sockets or a
browser. That's the reason behind
[keeping infrastructure out of domain crates](coding_guidelines/r07-preserve-crate-dependency-direction.md).

### Use rustdoc as a map

You don't have to read a crate from top to bottom. Open its rustdoc
introduction, pick a type from the overview and follow it's links or open
the repo in your IDE and use rust's great tooling (rust-analyzer) to nagivate the codebase.

## Identity, state and time

Alice disconnects and rejoins right away. To Bob, shes still Alice. To the
server, **her old and new connections are different things**, both with work in
flight. With only her user ID, an old cleanup could act on the new
connection, so the code has several kinds of identity:

| Identity | What it identifies | Where to begin |
| --- | --- | --- |
| User | The participant identity supplied by the application | [`UserId`](https://odoo.github.io/o-sfu/o_sfu_model/) |
| Connection | One admitted connection for that user | [`Router`](https://odoo.github.io/o-sfu/o_sfu_router/), [`MediaSession`](https://odoo.github.io/o-sfu/o_sfu_core/sfu/struct.MediaSession.html) |
| Room instance | A particular room lifetime, including protection against stale work after replacement | [`Room`](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/definition/index.html) |
| Published source | The logical media source and its encodings | [`source IDs`](https://odoo.github.io/o-sfu/o_sfu_core/engine/source_model/ids/index.html), [`descriptor`](https://odoo.github.io/o-sfu/o_sfu_core/engine/source_model/descriptor/struct.PublishedSourceDescriptor.html) |

An index alone has the same problem: a slot can have a new occupant by the
time old work comes back. The worker's
[`SlotHandle<Tag>`](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/state/slots/struct.SlotHandle.html)
**pairs the index with a generation**:

```text
                      remove old entry       reuse slot
slot 4:     [ old connection ] -------> empty -------> [ new connection ]
handle:          (4, 7)                                     (4, 8)
                    |
                    +------ delayed work: lookup (4, 7) ------+
                                                              |
                                                     generation differs
                                                              |
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
2. The browser opens a websocket to the returned server URL. It's first
   message carries thé participant's credentials, which the
   [`WebSocket handshake`](https://odoo.github.io/o-sfu/o_sfu/runtime/websocket_server/handshake/index.html)
   checks before admission.
3. The [WebSocket session](../src/runtime/websocket_server/session.rs)
   admits Alice into the room. The
   [room manager](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/manager/index.html) records her
   membership and [placement](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/placement/index.html)
   chooses which worker will handle her media.
4. The [`application session`](../src/application/user_session/lifecycle.rs)
   sends Alice the room's welcome state and starts preparing media.
5. [`Server media negotiation`](../src/application/user_session/media.rs)
   lets the browser and server agree on what media they can send and
   recieve.
6. Alice requests publication. The
   [`publication transition`](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/transition/publication/index.html)
   prepares for her media, then makes the source available in the room only
   after the answer confirms the required media settings.
7. Bob's reciever preferences become
   [`subscription intent`](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/transition/subscription/index.html).
   Once Alice's publication and Bob's receiving connection are ready, the
   server can connect them and start forwarding packets.

Step 6 has a spécificité worth a closer look. The server prepares to carry
Alice's video before her publication is visible to other participants. While
it waits for her answer, Alice can leave or reconnect, so **publication must
check the connection again before committing**:

```text
publish request --> stage transport --> send offer --> await answer
                                                            |
                         Alice may leave or reconnect here  |
                                                            v
                                               same connection still current?
                                                  |                     |
                                                 yes                    no
                                                  |                     |
                                                  v                     v
                                          commit, source is    roll back the staged
                                          visible in the room  transport ressources
```

Someone has to see that rollback through, which leads to room effects and
obligations.

The test `staged_publish_is_not_visible_in_room_graph_before_answer` (it's
not an ugly name! it's "rust-idiomatic" to name test functions as the test
title) in the
[publication tests](../crates/core/src/engine/room/transition/TESTS/publication.rs)
covers this boundary.

## Separate decisions from effects

Bob changes which video he wants to recieve. The room need to update its
decision and ask a media worker to carry it out. Holding the room lock until
both jobs are done would hide the unfinished change, but Carol would then
wait on Bob's worker to change her own, unrelated subscriptions.

So we **release the room lock before waiting**. Now a decision is recorded but
its work hasn't happened yet, with no lock left to remind us. In o-sfu, that
pending work becomes a value called
[`RoomEffects`](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/effects/batch/struct.RoomEffects.html):

```text
acquire room state
  validate current identities
  commit the permitted transition
  collect the work required by that transition
release room state
  execute the collected work in its required order
  report completion after all required effects finish
```

**The collected work still runs in a fixed order.** When Alice's publication
become active, the forwarding policy has to apply before we let packets
flow. The [effect runner](../crates/core/src/engine/room/effects/mod.rs)
drive that sequence, including the
[transport effects](../crates/core/src/engine/room/effects/transport.rs).

This also explains an aparent contradiction you may think (/This also clears up something that may look contradictory?). We
release the room-state lock before awaiting transport work, yet a
[`SourcePolicyGuard`](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/instance/struct.SourcePolicyGuard.html) can span
those same awaits. **They protect different things.** thé first protects the
room data while we change it. The second prevents a later policy decsiion from
overtaking the work needed to apply this one.

You may recognise the **functional core and imperative shell** pattern: work
out a decision, then carry it out. It isn't a database transaction, though.
If a worker fails after the room has committed its change, the change
doesn't undo itself. And if alice reconnected meanwhile, we may need to
check the result's connection, source or policy revision again.

[guideline: pure decisions](coding_guidelines/p04-keep-decision-logic-pure.md),
[guideline: state guards and async effects](coding_guidelines/o02-make-asynchronous-commit-boundaries-explicit.md),
[guideline: derived-state validity](coding_guidelines/s03-define-how-derived-state-stays-valid.md)

## Linear types and lifecycle obligations

We've reserved ressources for Alice;s publication, but she leaves before
negotiation finishes. Someone has to roll back that reservation and see the
cleanup through to the end. **Rust ownership knows who holds the reservation,
but not whether they finished the job.** That's the problem
[`Obligations`](https://odoo.github.io/o-sfu/o_sfu_core/engine/obligation/index.html) addresse.

### Affine and linear are different promises

First, we need to dive a little bit into type theory (/First, a little type theory?):

Once you move a non-`Copy` value into a function, you can't use that value
again in the caller. The function can pass it on or let it drop. Rust
accepts both. This is an **affine** type: a resource can be consumed at most
once. A **linear** type goes further and requires exactly once.

Immagine that you have a cinema ticket. You are free to give it to someone
else, to use it, or to throw it in the garbage and forget about it. That is "affine".

Now immagine that you have a live grenade (without the pin), you can give it to
someone else, you can disarm/"use" it. But you probably don't want to drop it
and forget about it. We must go from creation to consumption. That is "linear".

Like a live grenade, when doing async cleanup, dropping the resource isn't always safe (not in the rust sense but in the business logic sense (/Rust allows it, but the call ends up in a broken state?)):

For Alice's reservation, we want that stronger promise: **commit it or
finish rolling it back**.

At first, `Drop` seems enough: release a resource when its value is
destroyed (RAII, Resource Acquisition Is Initialization). But our cleanup
need to send commands to workers and wait for replies, possibly trying a
fallback along the way. **A Rust drop can't await those steps.**

So o-sfu keeps the async cleanup explicit and wraps the work in an
`Obligation`. The compiler ensures the only way to finish an obligation is
to run its work. Noticing that someone abandoned it is a runtime check,
built with Rust's existing types rather than a new full linear type system
(if you can come up with one, please open a PR!).

```text
                    used 0 times      used once     used twice
affine (`!Copy`)    ok (drop/forget)    ok          compile error
obligation          runtime error       ok          compile error
linear (ideal)      compile error       ok          compile error
```
note: "runtime error" only happens in tests or debug builds, in relese mode it is
only logged as an unfulfilled obligation.

### The obligation owns its work

A guard that only sits next to the work they are supposed to enforce (/A
guard kept beside the work it protects?) can easily be misused: nothing
stops someone from marking it complete without doing the work. So
`Obligation<W>` **holds the work itself** (a bit like Mutexes wrap their value).
You can read it through `work()` but not take it out. An armed obligation
disappears quietly only through one of these:

| Method | What it does | The guard afterwards |
| --- | --- | --- |
| `complete(context).await` | Runs the work's `Execute` implementation | Disarmed once execution returns |
| `try_complete(context)` | Tries a synchronous state change through `TryComplete` | Disarmed on success. A rejection hands the obligation back, still armed |
| `transfer()` | Turns the work into its next form through `Transfer` | Still armed, same label |
| `merge(other)` | Moves `other`'s work into this batch through `Extend` | `other`'s guard disarms once its work has moved |

Each work type chooses the operations it supports by implementing those traits
beside its own code. Alice's reservation can be committed or turned into
cleanup, but there is nothing to execute:

```rust
impl Work for PublishReservation {
    fn label(&self) -> &'static str {
        "staged publication"
    }
}

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
      |
      +-- try_complete, room accepts --> PublishCommit, guard disarmed
      |                                  (its effects get their own obligation)
      |
      +-- try_complete, room rejects --> Err(same obligation, still armed)
      |                                          |
      +-- Alice leaves first ------------------->+
                                                 |
                                                 v
                                            transfer()
                                                 |
                                                 v
                    Obligation<TransportTeardownAction>, holding RemoveMedia
                    same guard, still labelled "staged publication"
                                                 |
                                                 v
                    complete(&media_transport).await
                    remove the media, fall back to closing the session
                                                 |
                                                 v
                                          guard disarmed
```

**A rollback that's only queued isn't finished**, so the guard doesn't stop at
`transfer()`. The work becomes
[`TransportTeardown`](../crates/core/src/engine/media_transport/teardown.rs)
under the same guard, which disarms only after the worker has answered,
fallback included. On the commit side, the reservation is done as soon as
the room accepts Alice's media, before any of the commit's effects run.

### Batches nest

The same logic applies one level up. Room effects hold a transport plan,
which holds teardowns. Each level has its own guard:

```text
RoomEffects: Obligation<RoomEffectWork>                  "room effects"
 +-- transport: Obligation<RoomTransportWork>            "room transport plan"
 |     +-- relays, source activity, route control
 |     +-- teardown: [TransportTeardown, ...]            "transport media teardown", ...
 |     +-- consumer setup turns
 +-- output: notifications
 +-- source_policy

execute: transport plan --> output --> source policy --> last output
                                                              |
                                          "room effects" disarms here
```

`merge` moves the donor's work into the destination before disarming the
donor's guard, so **work never exists without an armed guard**. Empty batches
are the exception: they start unarmed and adding work with `extend` arms
them.

### Completion is not success

Cleanup can fail even when we've done everything required. If transport
teardown can't remove the media, it records the failure and closes the whole
session as a fallback. If that fails too, it logs it and returns. Then the
guard disarms and `MediaTransport::teardown` moves on to the next item.

So **completing means we followed the required path to the end, not that it
succeeded**. Because `complete` runs the work itself, it can't be called too
early. What the compiler can't check is each `Execute` implementation, which
decides what "finished" means and handles failures before returning. Review
moves from every call site to one implementation per work type.

### Let unfinished work make noise

A guard dropped before completion logs an error. With debug assertions or
`cfg(test)`, it also panics unless the thread is already unwinding:

```text
release:          error event "unfinished lifecycle obligation"
                  with obligation = "staged publication"

debug and tests:  the same event, then
                  panic "unfinished lifecycle obligation: staged publication"
```

The guard reports the abandoned work, but it doesn't perform the cleanup
itself.

Cancellation is caught the same way. `complete` moves the guard into the
future it returns, so dropping that future drops an armed guard, even before
its first poll. The test
`dropping_unpolled_publish_teardown_diagnoses_unfinished_cleanup` in the
[publication tests](../crates/core/src/engine/room/transition/TESTS/publication.rs)
drops a teardown future without polling it and expects this panic:

```text
unfinished lifecycle obligation: staged publication
```

The dropped work was a teardown, but it started as Alice's reservation.
`transfer()` keeps the original label, so the diagnostic points to where the
responsibility came from.

`#[must_use]` warns when a value is ignored, but assigning thé guard to a
variable silences it. The destructor catches that case too. It can still be
skipped with
[`mem::forget`](https://doc.rust-lang.org/std/mem/fn.forget.html) or by
terminating the process, so we haven't secretly added linear types to Rust.
We've made an otherwise silent mistake visible during ordinary execution.
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
  |                                            |
  |--- enqueue command ----------------------->|
  |                                            | command waiting
  X  drop future                               |
     borrow ends                               | execute command
     stop waiting                              |
                                               | produce reply
    no reciever <..............................|

    canceling the wait did not undo the send
```

Before the send, cancellation may leave no command behind. After it, a retry
could perform the operation twice, so the caller need a way to recognise a
retry or find out what happened to the first attempt. The
[worker request code](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/worker/lifecycle/index.html)
makes those stages explicit.

This is why dropping `MediaSession` isn't a shortcut for closing it.
Canceling a worker doesn't help either: it stops the worker without
promising to process every command left in its queue.

Even shutdown has two sides: asking the worker to stop and waiting until it
has. `RtcWorker::wait_for_shutdown` keeps the thread join running if a
waiter is canceled, so another caller can await it.

The [WebSocket session loop](../src/runtime/websocket_server/session.rs)
selects an event, finishes handling it and then selects again. Simplifd, it
looks like this:

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

The branches just race to recieve the next event,nothing races a handler
halfway through its work. So **cancellation occurs where we chose to allow
it**, not wherever an `.await` happens to appear.

### What a message loses when the receiver falls behind

Every channel must define what happens when the sender is faster than the
receiver, the answer depends on what the message means:

**A command must never vanish.** If the queue dropped a command to close
Alice's session, her session would stay open. A bounded `mpsc` queue makes
the sender wait or get an error instead. The worker answers through a
`oneshot` reply and must not treat a failed send as an error, since the
caller may have stopped waiting.

**A snapshot can be overwritten.** A `watch` channel holds only the latest
value, like the overflow state of the
[user outbound queue](https://odoo.github.io/o-sfu/o_sfu_core/engine/room/outbound/index.html).

**An event for several observers needs a rule for the slow one.** A
`broadcast` channel drops the oldest events for a receiver that falls
behind, so we decide whether it can continue or must be disconnected.

| Message | When the reciever falls behind | Tokio building block |
| --- | --- | --- |
| Close Alices session | Wait or reject, but never lose it silently, then reply with the result | Bounded `mpsc` plus a `oneshot` reply |
| Latest state snapshot | Replace the older snapshot | `watch` |
| Event for several observers | Decide what a slow observer misses or disconnect it | `broadcast` |

Making the queue larger buys time for a busrt. **It can't fix a sender that
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
deadlines. o-sfu handles the sockets and decides when to call it again:

```text
              o-sfu worker: sockets, thread and clock
       +----------------------------------------------------+
Alice ====> UDP recv                         UDP send =========> Bob
       |       |                                ^           |
       |       | Input::Receive                 | Transmit  |
       |       v                                |           |
       |  +-----------+  Event(RtpPacket)  +-----------+    |
       |  | str0m Rtc | -----------------> | str0m Rtc |    |
       |  |   Alice   |    write_rtp()     |    Bob    |    |
       |  +-----------+                    +-----------+    |
       |    ^      |                                        |
       |    |      | Timeout(t1)                            |
       |    +------+                                        |
       |    Input::Timeout(now) once t1 is due              |
       +----------------------------------------------------+
```

Each session has its own `Rtc` for both directions. The worker writes the
event from Alice's `Rtc` into Bob's with `StreamTx::write_rtp` and sends the
`Transmit` that Bob's emits. Alice's own `Rtc` also emits her ICE and DTLS
replies, RTCP feedback and subscribed media.

Every arrow leaving str0m is a `poll_output()` result. The worker keeps
calling it until str0m answers with a `Timeout`, which also says when to
return. The
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
 network      |        datagram
              |           |  demux: Rtc::accepts()
--------------+-----------v------------------------------------------
 str0m Alice  |      Alice's Rtc
              |           |  decrypt, parse
--------------+-----------v------------------------------------------
 forwarding   |   RtpPacket, rid "hi"
              |           |  source gate
              |           +----------------+----------------+
              |           v                v                v
              |       recording      relay mailbox    Bob's session
              |                      (worker 2,             |  rewrite seq,
              |                       for Carol)            |  timestamp,
              |                                             |  SSRC and PT
--------------+---------------------------------------------v--------
 str0m Bob    |                                         Bob's Rtc
              |                                             |  write_rtp()
--------------+---------------------------------------------v--------
 network      |                                      Transmit ==> Bob
```

The **packet gates** are where room policy meet the packet path. The loop
apply whichever gates policy selected last instead of recomputing layout
and bandwith choices for every packet.

[loop_driver.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/worker/loop_driver/index.html),
[ingress_routing.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/packet_loop/ingress_routing/index.html),
[forwarding_planner.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/packet_loop/forwarding_planner/index.html)

### Open an RTP packet

By the time forwarding sees Alice's packet, str0m has decrypted it and
parsed its RTP header. On the wire, with 32 bits per row in the fixed
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

The first 12 bytes are fixed. The sections after them have variable sizes
Here's Alice's camera packet diffed against the copy Bob gets (example
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

So this isn't a byte-for-byte copy of Alice's packet. The
[egress writer](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/consumer_egress/index.html)
build a packet for Bob's connection carrying the same encoded media.

[RFC 3550 section 5.1](https://www.rfc-editor.org/rfc/rfc3550.html#section-5.1)

### Projection: same packet, different numbers

The diff above gave Bob numbers Alice never sent. Her small and large
encodings count on their own, so passing them through would make a switch
look like thousands of lost packets. Instead, each receiver gets one stream
and o-sfu maps every source number onto it. We call this **projection**:

```text
                  Alice        Bob
encoding "lo"      410  ---->  720   anchor: 410 is 720
                   411  ---->  721   +1 from the anchor
                   413  ---->  723   +3, so 722 stays missing like 412
switch to "hi"    9000  ---->  724   reanchor: Bob's last value + 1
                  9001  ---->  725   +1 from the new anchor
```

Within one source, Bob's numbers keep their distance from the **anchor**, so
a lost packet stays a gap he can still repair. After a switch or a resumed
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
reanchor by the elapsed time instead of by one. Each consumer route keeps
its own state in
[`ConsumerStreamStore`](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/consumer_egress/streams/struct.ConsumerStreamStore.html),
so Bob and Carol number the same packet differently while sharing its bytes.

### Packets are not frames

Suppose you see Alice's packet counter rising, but Carol's video is frozen.
Both observations can be true at once. **One video frame can span many RTP
packets** and Carol may have recieved only some of them:

```text
one video frame
+------------+------------+------------+------------+
| packet 100 | packet 101 | packet 102 | packet 103 |
+------------+------------+------------+------------+
Carol gets:    100          101          (lost)       103

packets recieved: 3 of 4          frames Carol can decode: 0
```

Packets can also arrive out of order. RTP sequence numbers track packet
order and RTP timestamps place packets on the media clock, which isn't the
wall clock.

A compressed video frame can also depend on earlier frames, which is why the
SFU waits for a keyframe when it switches what a reciever gets, as we saw in
[Switching video need a starting picture](#switching-video-need-a-starting-picture).
The VP8 payload is where o-sfu recognises it.

### Look inside the VP8 payload

A VP8 RTP payload starts with a **payload descriptor**, followed by a fragment
of the encoded frame. The descriptor travels with every packet. Its optional
fields explain why the picture data doesn't always start at the same offset:

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
reads only the fields it need:

```text
Alice's packet, rid "hi"                   Bob's packet
+----------------------+                   +----------------------+
| S=1  PID=0           |------ read ------>| S=1  PID=0           |
| PictureID  20342     |---- project ----->| PictureID  815       |
| TL0PICIDX  91        |---- project ----->| TL0PICIDX  17        |
| N  TID  Y  KEYIDX    |------------------>| N  TID  Y  KEYIDX    |
| frame fragment ...   |------------------>| frame fragment ...   |
+----------------------+                   +----------------------+

read     S=1 and PID=0 start a frame, so check the fragment for a
         keyframe prefix
project  map the counters of each RID onto one counter for Bob
--->     forward unchanged, o-sfu doesn't choose temporal layers (yet?!)
```

Rewriting PictureID and TL0PICIDX lets Bob follow one (monotically
increasing) sequence of picture ids even when Alice's small and large
encodings use unrelated counters. The compressed picture data itself is
unchanged.

At the start of a frame, the encoded data has its own header. A keyframe
starts like this, with byte counts beside the fields below the frame tag:

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

This `P` is the frame type, unrelated to RTP padding. o-sfu reads just
enough of this prefix to recognise a keyframe start: the frame type,
version, sync code and nonzero dimensions. See the
[keyframe probe](https://odoo.github.io/o-sfu/o_sfu_rfc/rtp/index.html) and
[VP8 frame header specification](https://www.rfc-editor.org/rfc/rfc6386.html#section-9.1).

### Fanout and cross-worker relay

Bob might share Alice's worker while Carol's session runs on another worker
in the same process. Cross-worker relay carries the packet to Carol's worker
so it can write into her reciever session.
[Spread a room across workers](#spread-a-room-across-workers) explains how a
room ends up on several workers.

**Relaying doesn't turn that worker into another source of the same packet.** It
must not count the source observation again or relay the packet onward,
which could duplicate observations or form a cycle:

```text
             < Alice >
                 |
                 v
+-----------------------------------+
| worker 1: Alice's packet arrives  |
|   observe the source              |
|   plan local and relay delivery   |
+-----------------------------------+
         |                |
  local  |                | relay
         v                v
      < Bob >     +-----------------------------------+
                  | worker 2: relayed packet arrives  |
                  |   source already observed: skip   |
                  |   no relay onward                 |
                  |   write into Carol's session      |
                  +-----------------------------------+
                                  |
                                  v
                              < Carol >
```

All of that while avoiding copying the bytes of the payload, even across threads (/The payload bytes aren't copied along the way, even across threads?).

The payload is an `Arc<[u8]>`, so each destination and the relay mailbox get
a handle with `Arc::clone`, which only increments a reference count. The
bytes are immutable, so worker 2 reads them without a lock. Each destination
rewrites only its own copy of the header.

The [forwarding planner tests](../crates/core/src/engine/media_transport/rtc/TESTS/forwarding_planner_tests.rs)
exercise local recievers and relay destinations separately, without any sockets
involved.

### Keep one numbering across an encoding switch

Alice's small and large videos have separate packet histories. Bob need
one outgoing history across a switch These example sequence numbers show
the translation, assuming no packet loss:

```text
                              switch to large
                                     |
Alice's small video: ---410---411----|- - - - - - - - >     (sequence numbers)
Alice's large video: - - - - - - - - |---9000---9001-->
                        |      |          |      |
                        v      v          v      v
Bob's stream:        --720----721--------722----723--->
```

Passing Alice's numbers through as-is would make a qualité change look
like lost or reordered packets. Timestamps need a similar translation so
Bob's media clock doesn't jump to the other encoding's timeline.

[`ConsumerStreamStore`](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/consumer_egress/streams/struct.ConsumerStreamStore.html)
projects both per consumer. The
[egress test](../crates/core/src/engine/media_transport/rtc/consumer_egress/TESTS/streams.rs)
`projected_timestamps_stay_monotonic_across_simulcast_ssrc_switches` checks
the timestamps.

### Close skipped gaps and keep lost ones

Bob's numbering can have two kinds of gap. A packet the SFU skipped
deliberately shouldn't leave a hole. A packet the network lost should, so it can
still be repaired (example numbers again):

```text
Intentional gap: route paused and resumed

Alice: ---[10]---[11]---[12]--- [PAUSED] ---[1000]---[1001]--->
            |      |      |                    |        |
            v      v      v   (gap compacted)  v        v
Bob:   ---[50]---[51]---[52]-----------------[53]-----[54]----->
                               contiguous: no hole, no NACK

Unintentional gap: packet lost in transit

Alice: ---[10]---[11]--- (12 lost) ----------[13]------------->
            |      |                          |
            v      v     (gap preserved)      v
Bob:   ---[50]---[51]------------------------[53]------------->
                               hole kept: missing 52 triggers NACK
```

The SFU tells the two apart with a **delivery generation**. Pausing and resuming
a route moves it to a new generation, whose first packet continues Bob's
numbering directly after his last one. Within one generation, a missing
source number stays missing on Bob's side. Retransmission state must also
match the current delivery generation, otherwise an old repair request could
revive a packet from a stream that's been replaced.

### Repair a lost packet with RTX

When Bob notices a gap like missing packet 52, his browser sends an
[RTCP NACK](https://www.rfc-editor.org/rfc/rfc4585) packet back to o-sfu.
If that packet is still in the worker's egress cache, o-sfu repairs it over
a dedicated RTX stream without requesting a retransmission from Alice:

```text
Bob                                             o-sfu
 |                                                |
 |<--- primary RTP (seq 51) ----------------------|
 |                                                |
 |                                 X--------------|  packet 52 lost in transit
 |                                                |
 |<--- primary RTP (seq 53) ----------------------|  detect missing 52
 |                                                |
 |---- RTCP NACK (request seq 52) --------------->|
 |                                                |  lookup in RTX cache
 |<--- RTX packet (OSN 52 + payload) -------------|  retransmit
 |                                                |
 v                                                v
 restore packet 52 into jitter buffer
```

RTX uses its own SSRC and sequence numbers so retransmissions do not skew
congestion control or packet loss estimates on the primary stream. An
[RFC 4588](https://www.rfc-editor.org/rfc/rfc4588) RTX packet wraps the
original payload with a 2-byte prefix called the Original Sequence Number (OSN):

```text
+-----------------------------------------------------------------+
| RTX RTP header: RTX SSRC, RTX payload type, separate seq number |
+-------------------------------+---------------------------------+
| OSN: original seq number (52) | original packet payload ...     |
+-------------------------------+---------------------------------+
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
HTTP, WebSocket, rooms               +-----------------------------+
        |                            | mailbox commands            |
        +------- command ----------> | network packets             | <== packets in
        |                            | relay packets               |
        +<--- reply, observation --- | timers                      | ==> packets out
                                     +-----------------------------+
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

Alice's room starts on one worker. Later joins can go to other workers
according to `RoomWorkerPolicy`. Topology and relay keep it one logical
room, so Bob on worker 1 can still see Carol on worker 2. This distribution
currently happens within one process.

The first join searches the workers in a cycle, starting from the room
instance ID modulo the worker count, so different rooms tend to start on
different workers. It takes the first healthy worker, or the least-delayed
running one if none is healthy.

Health is measured, every 100 ms the packet loop has a heartbeat deadline
and records how late it reached it. The worker reports the smaller of its
last two delays, so one slow turn doesn't count but a loop that stays late
does. A delay of 100 ms or more, or a missed heartbeat, reports no value:

```text
heartbeat            1      2      3      4      5
reached late by      0 ms   30 ms  25 ms  3 ms   (missed)
reported delay       0      0      25     3      none
healthy (< 20 ms)    yes    yes    no     yes    no
```

Later joins go through a short list of preferences:

```text
Carol joins Alice's room
        |
        v
an assigned worker under the threshold?   -- yes -->  join the least-delayed one
        | no
        v
room under its router cap and an unused   -- yes -->  new placement on that worker
healthy worker?
        | no
        v
join the least-delayed running assigned worker
```

The cap is `ROOM_MAX_LOCAL_ROUTERS` (limited by the worker count) and it
defaults to 1. So **by default a room doesn't spread**: everyone joins the same
worker even when it's late, unless that worker has stopped.

The room has one `Router` (from `o-sfu-router`), kept in room state and
never run on a worker. Each placement in it pairs a router ID with the media
worker that carries its users' media. Here's Carol subscribing to Alice's
camera in a room with two placements:

```text
Router: one per room, kept in room state, no I/O
+------------------------------------+-------------------------------+
| router 1 -> worker 1               | router 2 -> worker 2          |
|   Alice  home   camera producer    |   Carol  home                 |
|                      +--> Bob      |                               |
|                      +--> Carol    |                               |
|                           (shadow) |                               |
|   Bob    home                      |                               |
+------------------------------------+-------------------------------+
                  |                                         |
                  | worker 1's routes                       | worker 2's routes
                  v                                         v
Alice ==> worker 1 packet loop --- relay mailbox ---> worker 2 packet loop ==> Carol
                  |
                  +==> Bob
```

Each user has a home session under their placement's router ID, where their
producers live. To give Carol Alice's camera, the `Router` creates a **shadow
session** for Carol under router 1 next to Alice's producer and puts the
consumer there. `o-sfu-core` turns that into a relay destination on worker
1. When Carol leaves, the shadow goes with her home session.

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
  |-- wait ----------->|                |-- submit read(buf) ---->|
  |<-- readable -------|                |                         |  kernel is
  |-- read(buf) ------>|                |                         |  using buf
  |<-- data -----------|                |<-- read completed ------|
```

That change what we can do with a buffer. Once a read is submitted, **we can't
assume the kernel is done with its memory** just because our task wants to
cancel or shut down. Here too, the lifetime of the work matters as much as
the lifetime of the Rust value that represent it.

## A map of o-sfu-core

`o-sfu-core` is where most of the hard SFU logic lives and where most people
get lost. This chapter puts the pieces you've met on one page.

The crate has two halves. The room side runs on the main Tokio runtime. It
holds what each room wants and decides what each receiver should get. The
media side runs in the RTC workers. It holds the str0m sessions and moves
the packets. `MediaTransport` is **the only door between them**. When you're
lost, ask which half you're in and which way the information moves: **commands
go down, packets stay below and observations come back up**.

```text
                                                             
        MediaSession               dirty rooms, snapshots <-----------+
     one per connection                       |                       |
              |                               |                       |
              v                               v                       |
         transition                      policy turn                  |
              |                               |                       |
              +--------> RoomState <----------+                       |
              |          one RwLock           |                       |
              v                               v                       |
         RoomEffects                  gates, activity,                |
              |                       bandwidth targets               |
              |                               |                       |
              +-------> MediaTransport <------+                       |
                              |                                       |
                              | commands                              | [[ main runtime ]] tokio tasks on thread pool
- - - - - - - - - - - - - - - | - one mailbox per worker - - - - - - -|- - -
                              |                                       | [[ media workers ]] 1 worker per thread
         +--------------------|---------------------+                 |
         |                    v                     |                 |
         |                 control                  |                 |
         |                    | writes gates        |                 |
         |                    v                     |                 |
         |             PacketLoopState              |                 |
         |                    | read per packet     |                 |
         |                    v                     |                 |
UDP/Relay ============> packet_loop ===================> UDP, relay   |
 Input   |                    |                     |      Output     |
         |                    v                     |                 |
         |              observations ---------------|-----------------+
         |                                          |
         +---- RtcWorker, one OS thread each -------+
```

Paths below are under `crates/core/src/engine/`, with `rtc/` short for
`media_transport/rtc/`. `sfu.rs` sits at the crate root.

| Looking for | Start in | Runs on |
| --- | --- | --- |
| What a connection can do | `sfu.rs`: `MediaSession` | Main runtime |
| Which rooms exist, admission and placement | `room/manager/`, `room/placement.rs` | Main runtime |
| What the room wants | `room/state/`, `room/media_graph/` (holds the `Router`) | Main runtime |
| How that changes | `room/transition/`, then `room/effects/` | Main runtime |
| What each receiver gets | `room/source_policy/` | Main runtime |
| The door to the workers | `media_transport/` | Main runtime |
| What a worker accepts | `rtc/commands.rs` | Both |
| SDP, media and route controls inside a worker | `rtc/control/` | Worker |
| The order of a worker turn | `rtc/worker/loop_driver.rs` | Worker |
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
`MediaTransport` sends an `RtcWorkerCommand` to the mailbox of the session's
worker. Commands that need an answer, like applying an SDP answer, carry a
`oneshot` reply. Route controls are best effort, because the worker may
already have torn down the relay or session they target.

**Commands are the only way in.** Nothing outside the worker thread touches
`PacketLoopState`, so the packet path doesn't need a lock.

### Packets stay below

In module terms, `packet_loop` matches a datagram to a session and str0m
decrypts it. Each RTP packet then goes through three steps in
`PacketForwarder` before the next packet can change route state:

```text
observe   resolve source and encoding, count bitrate, audio activity, keyframes
plan      read RouteTable: local destinations, relay targets, gates
flush     packet sinks -> relay mailboxes -> local sessions (consumer_egress)
```

The packet gates live in `RouteTable`. A `PacketLayerGate` says which
encodings (RIDs) of a source a destination may receive. The source keeps the
union of its destinations' gates as a first check, then each destination's
gate decides its own delivery. A switch to another encoding stays pending
until a keyframe arrives. `recovery` sends the keyframe requests, through
the producer's worker when the source lives on another one.

**Room policy writes the gates. The packet loop only reads them.**

### Observations go up

Workers never call room code. What they notice goes to two places:

- **Snapshots:** receiver bandwidth, transport health and media quality go
  into `RtcSnapshotState`, behind a mutex off the packet path. Room code
  reads them back through `MediaTransport`, along with source bitrates.
  Active speakers come from a cold-path command to each worker.
- **Wakeups:** when an observation may change what policy would decide, such
  as a change in the speaker ranking, a new one-second bitrate sample or a
  changed bandwidth estimate, the worker marks the room dirty on
  `SourcePolicySignal`.

A task in the server, `spawn_source_packet_policy_update_task`, waits on
those wakeups. A turn reads the snapshots, plans under the room state read
lock, then sends gates, route activity and bandwidth targets back down as
commands. Policy can also set its own deadline, which is how the upgrade
dwell from [Keep the call steady](#keep-the-call-steady) comes back without
a new observation.

```text
                          [[ main runtime ]]

  SourcePolicySignal   --------- wakeup --------->  policy turn
  coalesced per room   <------- deadline ---------  one per room at a time
                                                    reads snapshots, plans
          ^                                             |
          | marks room dirty                            | gates, activity,
          |                                             | bandwidth targets
- - - - - | - - - - - - - - - - - - - - - - - - - - - - | - - - - - - - - - - -
          |                                             v
     observations    <----------------------------  packet loop
     bitrate: per media packet                      applies the gates
     speech: per audio packet
     bandwidth: per str0m estimate

                          [[ media worker ]]
```

That's the feedback loop from
[Budgeting is a feedback-control problem](#budgeting-is-a-feedback-control-problem),
drawn across the thread boundary. **An observation is only a hint**: a snapshot
can race packet processing or teardown, so the room checks its own state
before acting.

**Per packet, the loop only updates worker-local state.** Locks, wakeups and
planning happen only when something changes:

- Bitrate is a few atomic operations on a counter that only its worker
  writes. Speech state sits in the worker's own `RouteTable`. No mutex is
  taken per packet.
- A packet marks its room dirty only when a one-second bitrate window closes
  or the speaker ranking changes. The worker deduplicates those room IDs
  over one packet batch and locks `SourcePolicySignal` once, or not at all.
- Bandwidth takes the snapshot mutex once per str0m estimate.
- The policy task is woken only when the set of pending rooms goes from
  empty to non-empty, so many wakeups for one room give a single turn.
- A turn sends only what differs from the current selections. A turn that
  changes nothing sends no command and skips the room state write lock.

### The special subsystems

| Subsystem | Decided by | Carried out by |
| --- | --- | --- |
| Cross-worker relay | `RoomTopology`: a subscriber on another placement gets a shadow consumer | `MediaTransport` installs a relay target on the producer's worker with the subscriber worker's mailbox. It stays inactive until route control enables it |
| Keyframe recovery | Decoder gates in `RouteTable` | `rtc/recovery/`: the consumer's worker owns the retries and the producer's worker asks the publisher |
| Packet sinks (like recording) | `RoomPacketSinkRegistry` | The origin worker, synchronously and before relays. Gates don't filter it. A sink must not block |
| Placement | `room/placement.rs` | See [Spread a room across workers](#spread-a-room-across-workers) |
| Diagnostics | `room/read_model.rs` | Room facts captured under the lock, transport snapshots after. The result isn't one atomic snapshot |

[core rustdoc](https://odoo.github.io/o-sfu/o_sfu_core/),
[rtc/mod.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/index.html),
[route_table.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/state/route_table/index.html),
[policy_invalidation.rs](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/policy_invalidation/index.html)

## Back in the browser client

The client from [The browser client](#the-browser-client) needs the same
tools you've seen on the server.

### Messages, requests and correlated answers

When the server gets Bob's SDP answer, it needs to know which offer he
answered. **Valid SDP isn't enough**: an answer to an older offer won't complete
the current negotiation.

So a signaling request carries an identity that it's reply can refer to.
On the wire, `q` names the request and `r` points back at it:

```text
o-sfu -> Bob   {"t": "offer", "q": "server-3", "p": {"sdp": "..."}}
Bob -> o-sfu   {"t": "offer", "r": "server-3", "p": {"sdp": "..."}}
                              ---------------
                              answers request server-3
```

If the server is waiting for `server-3`, an answer with `"r": "server-2"` is
rejected as unknown or stale. Events don't need an answer, so they carry
neither field. [envelope.rs](../crates/protocol/src/signaling/envelope.rs)
defines the format.

The [request tracker](../crates/protocol/src/core/request_tracker.rs) tracks
requests until they recieve a reply or time out.
[Server media negotiation](../src/application/user_session/media.rs) uses
that correlation when handling SDP offers and answers.

### Ordered turns and reentrancy

A browser operation starts on Bob's connection, then the connection gets
replaced while that operation is waiting. When its Promise resolves, we have
a result for a connection we're no longer using. JavaScript run one callback
at a time, but **awaiting a Promise gave other events a chance to change the
state**.

[`TurnQueue`](../crates/client/src/internals/turn_queue.ts) makes turns run
in order. When an interrupted operation resumes, it can check whether
it should sitll carry on:

```text
turn:      replaceTrack() on peer 1 --- await ...................... resolves
                                                                       |
recovery:                 peer 1 replaced by peer 2                    |
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

Alice's camera reaches Bob's browser as a media track that doesn't say whose
it is or whether it's a camera, microphone or screen share (we technically
know whether it's audio or video but that's not enough, the SFU does not
assume that there is at most only one stream of each type). The server sends
a binding snapshot with that infomration, but the snapshot and track can
arrive in either order.

Both halves carry a MID. The browser's `track` event gives the MID of its
transceiver and each server binding says which participant and stream type a
MID carries and whether it's active.
[`RemoteMedia`](../crates/client/src/internals/remote_media.ts) keeps one
slot per MID and tells Discuss about a track only once its slot has both:

```text
MID "3" in Bob's browser

   event                      slot track   slot binding     Discuss gets
1  binding: Alice, camera     -            Alice, camera    nothing yet
2  track event                track A      Alice, camera    TRACK: Alice's camera is track A
3  binding: Carol, camera     - (dropped)  Carol, camera    nothing yet
4  next track event           track C      Carol, camera    TRACK: Carol's camera is track C
```

Steps 1 and 2 could arrive in either order. Step 3 is the server reusing the
MID for another stream, so `RemoteMedia` drops Alice's old track rather than
show her video as Carol's.

Each snapshot lists every binding, so a MID missing from the latest one
loses its slot. The `active` flag also reflects Bob's preferences: if he
turned off Alice's camera with `subscribe`, it arrives inactive even though
the server binding says active.

### Recovery replays state selectively

After a brief connection loss, the client keeps Bob's intent to publish or
subscribe, but **it doesn't replay everything he did**. Restoring a subscription
recreate desired state. Repeating a broadcast would cause a second
application action:

| After reconnecting | The client replays |
| --- | --- |
| Authentication succeeds | Subscriptions and participant information |
| Media is ready again | Active publications |
| Never | Broadcasts |

How the connection closed determines whether there is a reconnect at all:

```text
recoverable close                                -> retry with increasing delays
explicit disconnect                              -> clear intent, no recovery
auth failure, protocol failure, kick, room full  -> stop, no endless retry loop
```

[Connection lifecycle](https://odoo.github.io/o-sfu/o_sfu_protocol/core/connection_lifecycle/index.html)
and [sticky replay](../crates/protocol/src/core/sticky_replay.rs) implement
these rules.

**Don't take `connect()` returning as a sign that authentication and media
negotiation have finished.** Wait for the documented events before treating
the connection as ready.

## Conclusion

Back to the first question: what happens when someone turns on their camera?
Let's follow Alice one last time:

1. Alice calls `sfu.publish("camera", track)` and the client and server
   negotiate it with SDP
   ([agree on media](#agree-on-media-and-connect-the-streams)).
2. The server reserves media for her camera before the answer arrives. That
   reservation is an obligation: it either commits or turns into cleanup
   ([obligations](#linear-types-and-lifecycle-obligations)).
3. The answer confirms the media settings and the room commits her
   publication. Only then can the other participants see it
   ([follow a participant into a room](#follow-a-participant-into-a-room)).
4. Bob's pending intent for her camera
   ([Bob can want a route that can't exist yet](#bob-can-want-a-route-that-cant-exist-yet))
   becomes a route and the forwarding policy applies before packets flow
   ([separate decisions from effects](#separate-decisions-from-effects)).
5. Bob gets the large version for his pinned tile and Carol the small one
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

The [coding guidelines](coding_guidelines.md) are the next thing to read.
The ressources below cover the Rust side.

## Rust learning ressources

### Talks

- ["Considering Rust" by Jon Gjengset](https://www.youtube.com/watch?v=DnT-LUQgc7s)
- ["Rust for Everyone!" by Will Crichton](https://www.youtube.com/watch?v=R0dP-QR5wQo)
- ["The Rust Programming Language" by Aaron Turon](https://youtu.be/O5vzLKg7y-k)
- ["Crust of Rust async/await" by Jon Gjengset](https://www.youtube.com/watch?v=ThjvMReOXYM)
- ["Rust makes cents" by No Boilerplate](https://www.youtube.com/watch?v=4dvf6kM70qM)

### Comprehensive overview

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

## Rust in o-sfu

Some rust/rust-ecosystem concepts that may be interesting (will find a better catch phrase later...)

### Give domain values distinct types

Alices camera has an ID and so does each version of the video she sends. As
plain `u64` values, a function expecting a camera ID would silently accept
an encoding ID. **The numbers fit. The meanings don't.**

The [source model](https://odoo.github.io/o-sfu/o_sfu_core/engine/source_model/ids/index.html) gives them
distinct types:

```rust
pub struct PublishedSourceId(u64);
pub struct SourceEncodingId(u64);
```

Both wrap a `u64` (and will be compiled as such), yet
`PublishedSourceDescriptor::encoding` only accepts a `SourceEncodingId`, so
mixing them up is a compile error:

```rust
descriptor.encoding(source_id);
//                  --------- expected `SourceEncodingId`, found `PublishedSourceId`
```

That's the **newtype pattern**. A type alias such as
`type SourceEncodingId = u64` would only rename the number and the call
above would compile.

Some protocol values only accept a certain range. If a constructor reject
values outside it, the rest of the code can use the resulting type without
checking that rule again. See the
[RFC vocabulary](https://odoo.github.io/o-sfu/o_sfu_rfc/) for examples.

Enums. They're everywhere, they're powerful (/Enums are everywhere in o-sfu, and for good reason?). They are algebraic/composable (unlike
those of Python and JS). You will love them (unless you're stubborn like Didier,
in which you can skip this section, because if you boil every program down to a tape
turing machine, everything is "just a list with extra steps").

They give another set of compile time guarantees, for example they can represent
data in only the valid combinations (example: [std::net::IpAddr](https://doc.rust-lang.org/std/net/enum.IpAddr.html)).

When you add a variant, **an exhaustive `match` forces an explicit choice**
wherever the enum is used. Here's one from
`MediaSession::deactivate_publication`:

```rust
match self.room_operation().deactivate_publication(&intent).await {
    DeactivateIntentOutcome::RolledBack => {
        self.phase.mark_follow_up_renegotiation();
    }
    DeactivateIntentOutcome::Deactivated | DeactivateIntentOutcome::Noop => {}
}
```

Add a 4th outcome and this `match` stops compiling until someone decides
whether it need a follow-up renegotiation (a `_ => {}` arm can still let it
pass silently, so use it only when you really mean that any other option can
be ignored).

Both the ["compiler-enforced correctness" rule](coding_guidelines/c01-let-the-compiler-enforce-correctness.md)
and the ["explicit domain changes" rule](coding_guidelines/c02-make-domain-evolution-compiler-visible.md)
extend these ideas: put enough meaning in the types that a change helps you
find the code it affects.

### Keep async operations tied to they're session

An operation on Alice's session sends work to a media worker and waits for
its reply. Other tasks can run meanwhile, but we don't want two method calls
changing the same session midway through each other's work.

[`MediaSession`](https://odoo.github.io/o-sfu/o_sfu_core/sfu/struct.MediaSession.html) uses `&mut self` to make those
calls take turns. An async method keeps it's mutable borrow while it waits,
so you can't begin another mutable operation through that session value
at thé same time:

```rust
let publishing = session.publish(intent); // borrows `session` mutably until awaited
session.close().await;                    // ERROR: second mutable borrow of `session`
publishing.await;
```

So **the borrow checker checks an ordering rule for us**, with no lock around
every method (/without a lock in every method?).

Now suppose we give up waiting and drop the first future. Its borrow ends
and Rust lets us use the session again, but thé command we sent to the
worker is already in flight. Dropping our future doesn't take it back.

That's what makes these methods **not cancellation safe**: to close Alice's
session, the caller has to account for that work and finish the close
protocol. More on that in
[obligations](#linear-types-and-lifecycle-obligations) and
[cancellation](#cancellation-channels-and-shutdown).

### Shared access doesn't make old work current

If several tasks need some session data, we can share it through an `Arc` so
it won't vanish underneath them. But Alice can reconnect while a callback is
waiting. When the callback resumes, it's `Arc` still works fine. It just
describes her old connection:

```text
time ------------------------------------------------------------------>

callback:  clone Arc ----- await ......................... resume, use Arc
Alice:     connection 1 ------- drops --- connection 2 -------------------
                                                           ^
                                    the Arc still holds connection 1's data
```

We kept thé data **reachable without making it current**. The callback still
need to check that this is the connection it should act on, which is the job
of [identities and generations](#identity-state-and-time). The
[synchronization guideline](coding_guidelines/s02-match-locks-and-atomics-to-the-state.md)
explains how these checks fit alongside locks and atomics.

### Errors for callers and errors for people

o-sfu uses two error crates, one for each audience
([guideline](coding_guidelines/e01-make-errors-easy-to-handle-and-diagnose.md)).

**When a caller must react differently to each failure**, the error is an enum
derived with [`thiserror`](https://docs.rs/thiserror/latest/thiserror/).
Joining a room can fail in four ways:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RoomManagerJoinError {
    #[error("room not found")]
    MissingRoom,
    #[error("room is full")]
    RoomFull,
    #[error("no usable media worker")]
    NoUsableWorker,
    #[error("router state error")]
    RouterState,
}
```

`o-sfu-core` knows nothing about WebSocket close codes. The
[WebSocket session](../src/runtime/websocket_server/session.rs) maps each
variant to one **at the edge**:

```rust
let code = match error {
    RoomManagerJoinError::RoomFull => CloseCode::RoomFull,
    RoomManagerJoinError::MissingRoom => CloseCode::AuthFailed,
    RoomManagerJoinError::NoUsableWorker | RoomManagerJoinError::RouterState => {
        CloseCode::Error
    }
};
```

The client's [recovery rules](#recovery-replays-state-selectively) read that
code, so a full room stops the reconnect loop. A new variant breaks this
`match` until someone picks a close code for it.

**When a person reads the error and no code matches on it**, such as a failed
startup, o-sfu uses [`anyhow`](https://docs.rs/anyhow/latest/anyhow/).
[`run`](https://odoo.github.io/o-sfu/o_sfu/runtime/fn.run.html) returns
`anyhow::Result<()>` and each step adds only the context the underlying
error lacks:

```rust
let value = self
    .load()
    .with_context(|| format!("{} env variable is required", self.key))?;
```

### Two kinds of Tokio runtime

Every async task in o-sfu runs on [Tokio](https://tokio.rs/), **but not all on
the same runtime**. HTTP, WebSocket and room work share one multi-thread
runtime. Each media worker builds its own current-thread runtime on a
dedicated OS thread.

Simplified:

```rust
thread::Builder::new().name(thread_name).spawn(move || {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        match TokioRuntimeBuilder::new_current_thread().enable_io().enable_time().build() {
            Ok(runtime) => runtime.block_on(startup.run(backend, startup_tx, &started)),
            Err(error) => { /* report that the worker failed to start */ }
        }
    }));
    mark_thread_exit(&terminal, started.get(), &shutdown, &rtc_metrics, media_worker_id, outcome);
})
```

**A worker's packets never wait behind HTTP or room tasks** and its state never
leaves its thread. `catch_unwind` keeps a panic in the packet loop inside the
thread and `mark_thread_exit` marks the worker terminal, so placement stops
choosing it.

Shutdown relies on two [`tokio_util`](https://docs.rs/tokio-util/latest/tokio_util/)
types:

- A [`CancellationToken`](https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html)
  tells a worker to stop.
  [`RtcWorker::wait_for_shutdown`](https://odoo.github.io/o-sfu/o_sfu_core/engine/media_transport/rtc/worker/struct.RtcWorker.html#method.wait_for_shutdown)
  then joins the OS thread inside `spawn_blocking`, because a blocking join
  would hold an executor thread for as long as it takes
  ([guideline](coding_guidelines/o01-keep-blocking-work-off-async-executors.md)).
- A [`TaskTracker`](https://docs.rs/tokio-util/latest/tokio_util/task/task_tracker/struct.TaskTracker.html)
  holds every WebSocket session task. On shutdown the server closes it and
  waits for the last session to finish
  ([guideline](coding_guidelines/o03-give-spawned-work-a-lifecycle-policy.md)).
