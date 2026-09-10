# Recording

> Design proposal. Recording is disabled in the inspected O-SFU backend.
> Approving this design does not enable the feature.

## What recording provides

Participants with permission can record an Odoo call, request a transcript or do
both. O-SFU captures the call while forwarding continues, then prepares and sends
the result after recording stops. This avoids running a media encoder for every
captured stream during the call.

The first version runs on one SFU node and accepts Opus audio with VP8 or H.264
video. It delivers at most one media file and, when requested, submits audio for
transcription. With usable requested media, successful processing produces:

| Request | Result |
| --- | --- |
| Audio and video | MP4 with mixed audio, or Ogg audio if video is absent |
| Audio only | One Ogg audio recording |
| Video without audio | One silent MP4 recording |
| Transcription only | Audio sent for transcription, with no media upload |
| Recording with transcription | The requested media file and a separate transcription request |

The Odoo dialog offers audio with video, optionally with transcription, or
transcription alone. Other callers can use the API for audio-only or silent-video
recordings.

Video shows the most recently available screen. When no screen is available, it
shows up to four of the most recently available cameras in a grid. Sources keep
their aspect ratio. The output profile is 1280 x 720 at 30 fps, encoded as AV1 in
MP4. Audio output uses Opus in Ogg at 32 kbit/s.

Capture audio throughout the recording so transcription can be enabled later.
The final transcription choice applies to the whole recording. Capturing this
audio does not permit adding it to a video requested without audio.

Recordings shorter than five seconds from start admission are discarded. The
60-minute limit begins when preparation succeeds. A queued recording that has
not started processing expires 24 hours after stop.

## API and visible state

The existing client API exposes capabilities through
`sfu.availableFeatures.recording` and accepts partial output choices:

```ts
setRecording(options: {
    audio?: boolean
    video?: boolean
    transcription?: boolean
}): Promise<boolean>
```

For example, a permitted caller can start a recording, enable transcription and
stop it:

```ts
await sfu.setRecording({ audio: true, video: true, transcription: false });
await sfu.setRecording({ transcription: true });
await sfu.setRecording({ audio: false, video: false, transcription: false });
```

To start transcription alone from idle:

```ts
await sfu.setRecording({ audio: false, video: false, transcription: true });
```

Omitted flags keep their values when the request executes. Requests are processed
in order, including while a previous start or stop is still being prepared.
The server verifies caller identity and permissions when the request arrives.
Advertised capabilities help the UI, but the server enforces permission checks.
Video and transcription capability both require audio capability.

Starting requires at least two SFU sessions. While recording, audio and video
choices cannot change except by stopping. A permitted caller may change the
transcription choice. Setting all flags to false stops recording. A caller with
no recording capability cannot stop it or submit an unchanged request.

`true` means the request was accepted, including an allowed no-op. It does not
mean preparation, file creation or delivery has completed. `false` means refusal,
such as unavailable recording, insufficient permission, too few sessions or an
unsupported change. Disconnection, request timeout and bus closure reject the
promise. Existing deprecated start/stop methods keep their legacy behavior.

The client exposes all three current flags in `sfu.recordingState`. State updates
contain a full snapshot. Stopping clears those flags immediately, while the job
keeps the output choices needed to finish the recording. Preparation, capture and
finalization failures are reported through state events and cannot retract an
earlier acknowledgement.

| Stop code | Meaning |
| --- | --- |
| `user_request` | A permitted caller stopped recording |
| `channel_closed` | One session remains or the channel closes |
| `recording_timeout` | The maximum duration was reached |
| `disk_space_exhausted` | Storage could not be reserved during preparation |
| `recording_failed` | Preparation, capture or finalization failed |

A departure that leaves one session schedules a stop. A later join does not undo
it. Processing or delivery of an older recording must never change the state of
a newer recording in the same room.

## Architecture

The call and the recording share incoming packets. Media processing happens after
capture, outside the forwarding path. The controller can run in the SFU process,
as shown. The media compiler must run in a separate restricted OS process.

```text
              +-----------------------------------------+
              | SFU process                             |
Publishers -->| Forwarding -----------------------------|--> listeners
              |     |                                   |
              |     v                                   |
              | Bounded capture --> local files         |
              |                          | after stop   |
              |                          v              |
              | Trusted job controller                  |
              |     |              |                    |
              +-----|--------------|--------------------+
                    |              +-- authenticated HTTP --> Odoo / storage
                    | sealed job files
                    v
              +-----------------------------------------+
              | Separate restricted OS process          |
              | Media compiler (worker)                 |
              | Reconstruct -> decode -> mix -> encode  |
              | No credentials or network access        |
              +--------------------|--------------------+
                                   |
                                   +-- artifacts --> controller
```

| Component | Responsibility |
| --- | --- |
| Room recording control | Authorize requests, order transitions and publish state |
| Capture service | Select source streams and write their packets and timing information |
| Local storage | Hold completed captures until processing and remove expired or failed work |
| Trusted job controller | Claim a recording, supervise workers and deliver the results |
| Restricted media worker | Reconstruct tracks, mix audio and encode the selected output |

The capture service records original incoming RTP, the packet format used to
transport audio and video. It does not reuse an individual receiver's stream
selection. It records one video encoding per selected source and the eligible
audio sources, together with the codec information and timing needed to reconstruct
them. Selected VP8 and H.264 video must be retained in mixed calls, including
sources that join or replace an earlier source after capture starts. Recording
must not omit a selected video source because it uses either accepted codec.

Capture uses bounded queues. The packet path copies and queues data without
waiting for disk writes or decoding media. If storage fails or capture cannot
keep up, the affected recording fails while forwarding continues.

Stopping closes capture and finishes accepted writes. A recording becomes
*sealed* only when its files are complete and durably published. Workers never
process an unfinished capture. A crash during capture discards that capture.

```text
Room:       Idle --> Starting --> Recording --> Stopping --> Idle
                                                 |
                                                 v
Job:                                      sealed and queued
                                                 |
                                                 v
                                          processing once
                                                 |
                                                 v
                                         delivered or failed
                                                 |
                                                 v
                                               cleanup
```

The room can accept a new recording after the previous capture is sealed or
discarded. It does not wait for encoding or upload. Each recording has its own
identity so delayed completions and cleanup cannot affect its successor.

This design keeps media encoding out of the call, but it requires local disk space
and reconstruction of packet order, timing and missing media afterward. It also
means users receive the result after recording stops, rather than during the call.

## Media processing and delivery

The controller claims a queued recording before starting media processing or
HTTP requests. It may defer an unclaimed job when the server lacks capacity.
Once claimed, the job gets one attempt. The restricted worker decodes captured VP8
and H.264 directly before composition and AV1 encoding. H.264 input requires no
intermediate VP8 transcode or additional publisher track.

Processing follows this order:

```text
Media worker process
  Prepare audio, if needed
          |
          v
Trusted controller
  Submit transcription, if requested and usable audio exists
          |
          v
Media worker process
  Prepare requested video and select one media file
          |
          v
Trusted controller
  Request upload destination --> upload --> confirm completion, if required
```

Transcription-only stops after its audio submission, then cleans up. When media
is requested, choose usable video first. If no video artifact is available, use
audio only when audio output was requested. A video-only request cannot acquire
an audio fallback. A compilation error ends the attempt rather than triggering
this fallback.

Transcription preparation or HTTP failure prevents later media delivery. If no
usable audio exists, transcription is skipped without failing the attempt.
An HTTP acknowledgement means Odoo accepted the input, not that a transcript is
ready. A later asynchronous transcription failure does not block media delivery.

For media delivery, the controller asks Odoo for the upload destination, follows
the returned method and headers and sends completion only when Odoo requires it.
Callbacks retain the original channel credentials and the starter's Odoo identity.
Callback credentials must not be forwarded to the storage destination. Recording
time bounds are sent as epoch milliseconds so Odoo can place the result on the
call timeline.

A failed or interrupted claimed job is discarded without replay. After restart,
only still-queued unexpired jobs may begin processing. Cleanup may retry deleting
files, but it must not repeat media processing or HTTP requests.
If an upload succeeds and its response is lost, the remote result can remain
unknown. One processing attempt cannot guarantee exactly-once remote effects.

## Why media work has its own process

Encoded media comes from participants and must be treated as untrusted input.
FFmpeg, ffprobe and native codec calls run in a separate restricted OS process
from the SFU and trusted controller. Another Tokio runtime in the same process
would still share its memory and privileges. A worker mode of the same executable
is allowed. A separate machine or network service is not required.

The worker receives only the files for its current job. It cannot access channel
keys, other recordings or the network. The controller retains credentials and
performs all HTTP requests. OS-enforced limits bound the worker's CPU, memory,
disk use and execution time. If the required restrictions cannot be established,
media execution must fail without an unrestricted fallback.

The controller treats returned artifacts as untrusted, accepts only the expected
bounded files and prevents later worker writes before delivery. Cancellation and
controller failure must stop the full worker process tree before cleanup. Native
media inspection stays inside the worker. FFmpeg and its linked codec libraries
still need a maintained security-update path.

## Scope and readiness

The first release does not include remote recording nodes, other input codecs,
custom layouts, generic packet replay or recovery of unfinished captures.

The capture format, clock reconstruction, executable packaging, sandbox mechanism
and numeric resource limits still need detailed design and evidence before
dependent implementation begins. The supported H.264 profiles and packetization
modes also need an explicit contract before implementation. The separate restricted
process and the API and delivery behavior described here are requirements.

Before enablement, demonstrate:

- Permission checks, ordered start/stop behavior and correct UI state.
- Usable Odoo playback and transcription, including synchronization and seeking.
- Isolated media execution, bounded resource use and acceptable forwarding impact.
- Correct cleanup after failures and restarts, with no replay of claimed jobs.
- Configured storage and transcription providers, followed by release-owner acceptance.

The behavior references are the pinned [Node recorder][node-recorder],
[media compiler][node-compiler], [processing sequence][node-processor] and
[Odoo callback handling][odoo-routing], inspected on 2026-10-21. The existing
[O-SFU client API][client-api] is the integration point. These source references
do not establish runtime recording readiness.

[node-recorder]: https://github.com/odoo/sfu/blob/c241eca2367ecbc6dc930a59024f19de521aa1d4/src/recording/models/recorder.ts
[node-compiler]: https://github.com/odoo/sfu/blob/c241eca2367ecbc6dc930a59024f19de521aa1d4/src/recording/models/media_compiler.ts
[node-processor]: https://github.com/odoo/sfu/blob/c241eca2367ecbc6dc930a59024f19de521aa1d4/src/recording/models/recording_processor.ts#L31-L98
[odoo-routing]: https://github.com/odoo/odoo/blob/c6b576cd416c36053f67ac3be5410c22176d22f4/addons/mail/controllers/discuss/rtc.py#L227-L268
[client-api]: https://github.com/odoo/o-sfu/blob/df102232c93c87da067c2903d7170e248d35f86d/crates/client/API.md#L416-L464
