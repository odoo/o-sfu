# Recording

> [!WARNING]
> Parts of the API depend on the choices made in: https://github.com/odoo/sfu/pull/27

This is first rough draft so that we can work on the design before starting to make big changes.

## current/old implementation

The reference is [odoo/sfu#27](https://github.com/odoo/sfu/pull/27).

Its [`MediaSink`][old-sink] connects each stream to an
FFmpeg [`MediaWriter`][old-writer]. After capture,
[`RecordingProcessor.process`][old-processor] compiles and uplods the result

```text
source RTP -> FFmpeg -> per-stream files
                              |
                              v
                    compilation -> Odoo
```

O-SFU should follow the same external contract (and general behavior decisions).

interaction from the clietn:

```ts
setRecording(options: {
    audio?: boolean
    video?: boolean
    transcription?: boolean
}): Promise<boolean>
```

- Starting requires permission for every enabled output. Every request requires
  at least one effective recording capability. Video and transcription
  capabilities depend on audio capability.
- Audio and video are fixed once recording starts. Reject a request that changes
  either while keeping any output enabled. An all-false result is the stop request.
- Only transcription may change while audio or video remains active, with
  transcription permission. Its final value selects transcription of the whole
  recording. Enabling it midway includes earlier captured audio. Disabling it
  cancels that output.
- Authorized empty or unchanged requests return `true` without changing the
  starter, timer or recording state. Repeating an unchanged flag does not require
  permission for that flag beyond the reference request rules above.
- Stop preserves the final output flags before clearing visible state. It never
  turns an existing recording into an empty output selection.
- The boolean acknowledges acceptance before startup or stop/finalization finishes.
  State events report the resulting recording state. Later requests wait for the
  current transition rather than modifying preparation or overtaking stop.

```ts
const admitted = await sfuClient.setRecording({ audio: true, video: true })
const updated = await sfuClient.setRecording({ transcription: true })
const stopped = await sfuClient.setRecording({
    audio: false,
    video: false,
    transcription: false,
})
```

The required audio contract is that `audio: false` produces silent video. Odoo
normally requests audio or audio plus video. Audio may still be captured for
whole-recording transcription without being included in the delivered video.
The inspected [Node-SFU compiler][old-compiler] currently requests its audio mix
for video regardless of the `audio` flag. That is a reference mismatch to reconcile,
not a choice to reopen in this specification.

Reuse the [reference output settings][old-config]: Ogg Opus audio at 32 kbit/s
and MP4 video with AV1 at 1280 x 720 and 30 fps. Preserve its screen/camera layout,
5-second minimum, 60-minute maximum and 24-hour expiry for queued recordings.
Verify playback against the existing Odoo player rather than choosing a new profile.

Preserve one media upload: video when available, otherwise audio only if audio
output was requested. Transcription uses the whole recorded audio when the final
flag is true. Preserve the reference callback order: requested POST `/transcribe`,
then media POST `/routing`, the returned upload contract and conditional POST `/complete`.
The current reference makes one processing/delivery attempt and discards failures.
O-SFU must not replay that attempt after a crash or cleanup failure.

## Proposed design

Capture encoded packets during the call and build playback files afterward.
O-SFU already has an origin-side [`PacketSink::record_packet`][packet-sink],
invoked before receiver forwarding. Extend it with normalized RTP metadata and
source lifecycle information. Recording selects active publications and their
encodings independently of receiver policy.
[Recording is not implemented yet][recording-gate].

```text
during recording:
publisher -> accepted origin RTP -> receiver forwarding
                        |
                        v
                  bounded capture queue
                        |
                        v
                 session capture owner
                        |
                   packet files

after stop (once per recording):
drain queue -> finalize capture -> compiler process -> Odoo
```

- **Room control** authorizes requests and owns the visible recording state.
  Extend the existing [room commit/effect pattern][room-effects]. Every command
  and completion carries a unique recording ID and room generation.
- **Capture** retains packets, sender reports and ordered source changes.
  Packet workers only perform bounded enqueue work. One serial capture owner per
  recording writes files and metadata on a fixed I/O pool. Queue or storage
  exhaustion fails that recording while forwarding continues.
- **Compilation** reconstructs tracks and their shared timeline, then uses
  FFmpeg for mixing, composition and final encoding. A separate process owns
  compilation and delivery with explicit resource limits. The router remains
  independent of files, FFmpeg and HTTP.

The first version would capture locally with Opus and VP8, one video encoding per
selected source and plain FFmpeg composition. Start with one screen if present,
otherwise up to four cameras. Styled layouts and remote recorder nodes can follow
later.

## API concepts

### Room commands and completion

Expose one `MediaSession::set_recording` command. Room code resolves the partial
update under its state guard and executes the resulting effects afterward:

```rust
let commit = {
    let mut state = self.state.write().await;
    state.set_recording(caller, options)
};

let _ = acknowledgement.send(commit.admitted());
RoomEffects::from_recording(commit)
    .execute(self, context)
    .await;
```

`set_recording` applies the reference authorization and immutable audio/video
rules before choosing no-op, start, transcription update or stop. Only an
idle-to-enabled transition freezes starter identity, reserves `Starting` and
takes the source snapshot with its ordered subscription. The prepare effect
schedules capture work without delaying the acceptance acknowledgement.
Subsequent commands wait for that transition to finish. A transcription update
changes the final output flag on the same recording without resetting its timer.

The scheduled work reports a session-scoped result. Completion handling resolves
the room generation and builds a fresh effect context. Recording IDs remain unique
across process restarts:

```rust
struct RecordingTarget {
    room: RoomInstanceId,
    session: RecordingSessionId,
}

let prepared: Result<PreparedCapture, CaptureError> =
    capture.prepare(start).await;
room.recording_prepared(target, prepared).await;
```

`PreparedCapture` is unregistered with closed ingress. Inside
`recording_prepared`, the room revalidates the target:

```rust
let commit = {
    let mut state = self.state.write().await;
    state.finish_recording_start(target, prepared)
};
RoomEffects::from_recording(commit)
    .execute(self, context)
    .await;
```

Registration and activation also reject a closed or superseded target. Delayed
preparation cannot replace a newer sink. Cancelled or stale results produce cleanup
effects. Recording events retain commit order through effects.
`CaptureError` distinguishes capacity, overload, storage I/O, source binding,
unsupported media, cancellation and interruption. Operational failure reports a
room event only for the current session, including sealing failure during `Stopping`.

### Closing capture

When an all-false request reaches an active session, preserve its output flags
and commit `Stopping`. An idle request remains a no-op. A request received during
startup waits for the preceding transition. Stop acknowledges acceptance before
the capture service completes closure and finalization:

```rust
let closed: ClosedCapture = capture.stop(target);
room.recording_closed(closed, context).await;
```

- `stop` closes packet admission and detaches the sink, even when the queue is full.
- `ClosedCapture` reports internal ingress closure. It is not the client acknowledgement.
- Keep later commands queued until this stop transition has sealed or discarded
  the old capture. Compilation and delivery proceed independently afterward.
- [Cached handles][packet-sink] reject packets after closure. Old cleanup and results
  cannot affect a new recording.

### Packet sidee

Keep the existing `PacketSink` boundary and pass a borrowed packet observation:

```rust
struct OriginRtp<'a> {
    binding: SourceBindingId,
    received_at: Instant,
    metadata: NormalizedRtpMetadata,
    payload: &'a [u8],
}

sink.record_packet(OriginRtp {
    binding,
    received_at,
    metadata,
    payload,
});
```

- `binding` identifies the transport and format version. `metadata` preserves RTP
  timing, ordering and orientation.
- Transport, SSRC or format changes start a new stream epoch.
- `record_packet` filters and enqueues within fixed limits, without waiting or I/O.
  Overflow fails the recording through a separate control path.

### media compiler

ORTP will store encoded media packets with the timing and source metadata needed
to reconstruct the recording after the call. The file format and reconstruction
rules will be specified later.

Share the capture file format and finalization rules through one library:

```text
server runtime -> core
server runtime -> recording library <- compiler
```

The runtime writes capture files using the shared library. The compiler reads
them after finalization:

```rust
if let Some(mut job) = spool.claim_next_finalized()? {
    let result = processor.process_once(&mut job, &limits).await;
    spool.finish(job, result).await?;
}
```

The exclusive claim durably consumes the queued attempt before processing starts.
A restart or failed cleanup must not make it eligible again. `process_once`
follows the reference order: compile audio when required, submit requested transcription,
compile requested video, upload the selected media and complete the upload when
required. A failure stops that attempt. `finish` removes failed jobs and performs
the reference cleanup for successful jobs.

Capture files, FFmpeg work and callback requests remain outside room control.
Their outcomes cannot overwrite the state of a newer recording. A transcription
HTTP acknowledgement does not guarantee that a transcript was generated.


## note

Before enabling the feature, we can probably test the full flow on one of our odoo servers
since we will be able to hot-swap SFUs since: https://github.com/odoo/odoo/pull/279672

[old-sink]: https://github.com/odoo/sfu/blob/cf463386f9b1b8cdf6a3bddd437e9ccae786477d/src/recording/models/media_sink.ts#L93-L178
[old-writer]: https://github.com/odoo/sfu/blob/cf463386f9b1b8cdf6a3bddd437e9ccae786477d/src/recording/models/media_writer.ts#L105-L115
[old-processor]: https://github.com/odoo/sfu/blob/cf463386f9b1b8cdf6a3bddd437e9ccae786477d/src/recording/models/recording_processor.ts#L36-L65
[packet-sink]: https://github.com/odoo/o-sfu/blob/7b0bb4edceba18b18889af83b9ba78bc74b49914/crates/core/src/engine/packet_sink_registry.rs
[recording-gate]: https://github.com/odoo/o-sfu/blob/7b0bb4edceba18b18889af83b9ba78bc74b49914/crates/core/src/engine/room/definition.rs#L23-L29
[room-effects]: https://github.com/odoo/o-sfu/blob/7b0bb4edceba18b18889af83b9ba78bc74b49914/crates/core/src/engine/room/effects/batch.rs

[old-config]: https://github.com/odoo/sfu/blob/cf463386f9b1b8cdf6a3bddd437e9ccae786477d/src/config.ts#L238-L270
[old-compiler]: https://github.com/odoo/sfu/blob/cf463386f9b1b8cdf6a3bddd437e9ccae786477d/src/recording/models/media_compiler.ts#L330-L334
