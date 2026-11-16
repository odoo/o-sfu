import assert from "node:assert/strict";
import test from "node:test";
import { CLIENT_UPDATE } from "../../dist/public_api.js";
import { COMMAND_KIND } from "../../dist/protocol_contract.js";
import { FakeMediaTrack, FakePeerConnection } from "../support/browser_fakes.mjs";
import { FakeProtocolCore, tick } from "../support/protocol_fakes.mjs";
import {
    createCameraTrack,
    createSfuClientHarness,
    emitOfferWithBinding
} from "../support/sfu_client_harness.mjs";
import {
    audioMedia,
    audioUploadSlot,
    sdp,
    videoMedia,
    videoUploadSlot
} from "../support/negotiation_fixtures.mjs";

test("track metadata updates re-emit track state for existing remote tracks", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    const receiverTrack = createCameraTrack("track-1");
    peerConnections[0].emitTrack(receiverTrack, "0");
    await tick();

    await emitMessage("inactive-track-binding");

    const [track] = receiverTrack.clones;
    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 42,
                track,
                type: "camera"
            }
        },
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: false,
                sessionId: 42,
                track,
                type: "camera"
            }
        }
    ]);
    assert.equal(client._consumers.get(42).camera.track, track);
});

test("track events wait for later binding snapshots before publishing", async () => {
    const { client, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();
    await emitMessage("offer");

    const receiverTrack = createCameraTrack("track-1");
    peerConnections[0].emitTrack(receiverTrack, "0");
    await tick();

    assert.deepEqual(updates, []);
    assert.equal(client._consumers.size, 0);

    await emitMessage("inactive-track-binding");

    const [track] = receiverTrack.clones;
    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: false,
                sessionId: 42,
                track,
                type: "camera"
            }
        }
    ]);
    assert.equal(client._consumers.get(42).camera.track, track);
});

test("initial peer creation keeps earlier binding snapshots", async () => {
    const core = new FakeProtocolCore();
    const onWsMessage = core.onWsMessage.bind(core);
    core.onWsMessage = (frame) => {
        if (frame === "offer-without-track-bindings") {
            return core._withPendingNegotiationKind([
                {
                    kind: COMMAND_KIND.APPLY_NEGOTIATION,
                    negotiationKind: "offer",
                    requestId: "7",
                    sdp: sdp(audioMedia("0"), videoMedia("1")),
                    uploadSlots: [audioUploadSlot("0"), videoUploadSlot("1")]
                }
            ]);
        }
        return onWsMessage(frame);
    };
    const { client, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness({ protocolCore: core });

    await connectWithWelcome();
    await emitMessage("inactive-track-binding");
    await emitMessage("offer-without-track-bindings");

    const receiverTrack = createCameraTrack("track-1");
    peerConnections[0].emitTrack(receiverTrack, "0");
    await tick();

    const [track] = receiverTrack.clones;
    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: false,
                sessionId: 42,
                track,
                type: "camera"
            }
        }
    ]);
    assert.equal(client._consumers.get(42).camera.track, track);
});

test("track-only slots survive empty binding snapshots before publishing", async () => {
    const { client, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();
    await emitMessage("offer");

    const receiverTrack = createCameraTrack("track-1");
    peerConnections[0].emitTrack(receiverTrack, "0");
    await tick();

    await emitMessage("clear-track-bindings");
    await emitMessage("inactive-track-binding");

    const [track] = receiverTrack.clones;
    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: false,
                sessionId: 42,
                track,
                type: "camera"
            }
        }
    ]);
    assert.equal(client._consumers.get(42).camera.track, track);
});

test("subscribe overlays local download state onto existing remote tracks", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    const receiverTrack = createCameraTrack("track-1");
    peerConnections[0].emitTrack(receiverTrack, "0");
    await tick();

    client.subscribe(42, { camera: false });
    await tick();
    await tick();
    client.subscribe(42, { camera: undefined });
    await tick();
    await tick();
    client.subscribe(42, { camera: true });
    await tick();
    await tick();

    const [track] = receiverTrack.clones;
    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 42,
                track,
                type: "camera"
            }
        },
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: false,
                sessionId: 42,
                track,
                type: "camera"
            }
        },
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 42,
                track,
                type: "camera"
            }
        }
    ]);
});

test("subscribe preferences apply to future remote track bindings", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    client.subscribe(42, { camera: false });
    await tick();
    await tick();

    await emitOfferWithBinding({ core, emitMessage });

    const receiverTrack = createCameraTrack("track-1");
    peerConnections[0].emitTrack(receiverTrack, "0");
    await tick();

    const [track] = receiverTrack.clones;
    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: false,
                sessionId: 42,
                track,
                type: "camera"
            }
        }
    ]);
    assert.equal(client._consumers.get(42).camera.track, track);
});

test("fresh connect clears subscription overlays from the previous session", async () => {
    const { client, core, connectWithWelcome, emitMessage, peerConnections, updates } =
        createSfuClientHarness();

    client.subscribe(42, { camera: false });
    await tick();
    await connectWithWelcome();
    await emitOfferWithBinding({ core, emitMessage });
    peerConnections[0].emitTrack(createCameraTrack("camera-track"), "0");
    await tick();

    assert.equal(updates.at(-1).payload.active, true);
});

test("recovery retains subscription overlays for rebound tracks", async () => {
    const core = new FakeProtocolCore();
    core.transportFailureState = "recovering";
    const onWsClose = core.onWsClose.bind(core);
    core.onWsClose = (code) => [
        { kind: "closePeerConnection" },
        ...onWsClose(code),
        { kind: "connect", url: "ws://example.test/recovery" }
    ];
    const { client, connectWithWelcome, emitMessage, open, peerConnections, sockets, updates } =
        createSfuClientHarness({ protocolCore: core });

    await connectWithWelcome();
    client.subscribe(42, { camera: false });
    await tick();
    sockets[0].emitClose(1011);
    await tick();
    await open(1);
    await emitMessage("welcome", 1);
    core.trackBindings.set("0", { active: true, mid: "0", sessionId: 42, type: "camera" });
    await emitMessage("offer", 1);
    peerConnections[0].emitTrack(createCameraTrack("rebound-camera"), "0");
    await tick();

    assert.equal(updates.at(-1).payload.active, false);
});

test("peer teardown clears bindings before the next peer can emit tracks", async () => {
    const core = new FakeProtocolCore();
    const earlyTrack = new FakeMediaTrack({
        id: "fresh-before-binding",
        kind: "video"
    });
    const { client, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness({
            protocolCore: core,
            createPeerConnection(config, index) {
                const peerConnection = new FakePeerConnection(config);
                if (index === 1) {
                    const setRemoteDescription =
                        peerConnection.setRemoteDescription.bind(peerConnection);
                    peerConnection.setRemoteDescription = async (description) => {
                        await setRemoteDescription(description);
                        peerConnection.emitTrack(earlyTrack, "0");
                    };
                }
                return peerConnection;
            }
        });

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    peerConnections[0].emitTrack(createCameraTrack("track-1"), "0");
    await tick();
    updates.length = 0;

    await emitOfferWithBinding({ core, emitMessage }, { sessionId: 84, type: "screen" });

    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 84,
                track: earlyTrack.clones[0],
                type: "screen"
            }
        }
    ]);
    assert.equal(client._consumers.has(42), false);
    assert.equal(client._consumers.get(84).screen.track, earlyTrack.clones[0]);
});

test("a reused mid gives each owner a track of its own", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    const receiverTrack = new FakeMediaTrack({ id: "track-1", kind: "video" });
    peerConnections[0].emitTrack(receiverTrack, "0");
    await tick();

    // One snapshot renames the mid. Its section stayed sendonly, so the
    // browser fires no track event.
    await emitMessage("track-rebind");

    const [cameraTrack, screenTrack] = receiverTrack.clones;
    assert.equal(client._consumers.has(42), false);
    assert.equal(client._consumers.get(84).screen.track, screenTrack);
    // The previous owner cannot play what the mid receives next.
    assert.equal(cameraTrack.readyState, "ended");

    // A retracted owner hears nothing more of the track. Odoo stops the tracks
    // of a session that left.
    await emitMessage("clear-track-bindings");
    screenTrack.stop();
    receiverTrack.setMuted(true);
    await tick();

    assert.equal(client._consumers.size, 0);

    // The section was inactive in between, so the browser repeats the event.
    await emitMessage("inactive-track-binding");
    peerConnections[0].emitTrack(receiverTrack, "0");
    await tick();

    const reboundTrack = receiverTrack.clones[2];
    assert.deepEqual(
        updates.map(({ payload }) => [payload.sessionId, payload.type, payload.active]),
        [
            [42, "camera", true],
            [84, "screen", true],
            [42, "camera", false]
        ]
    );
    assert.deepEqual(
        updates.map(({ payload }) => payload.track),
        [cameraTrack, screenTrack, reboundTrack]
    );
    assert.equal(client._consumers.get(42).camera.track, reboundTrack);
    assert.equal(reboundTrack.readyState, "live");
    assert.equal(receiverTrack.clones.length, 3);

    // Both mids change owners in one snapshot, without another track event.
    core.trackBindings.set("1", { active: true, mid: "1", sessionId: 84, type: "screen" });
    await emitMessage("inactive-track-binding");
    const secondReceiverTrack = createCameraTrack("track-2");
    peerConnections[0].emitTrack(secondReceiverTrack, "1");
    await tick();

    core.trackBindings.set("1", { active: false, mid: "1", sessionId: 42, type: "camera" });
    await emitMessage("track-rebind");

    assert.equal(client._consumers.get(84)?.screen?.track, receiverTrack.clones[3]);
    assert.equal(client._consumers.get(42)?.camera?.track, secondReceiverTrack.clones[1]);
    assert.equal(reboundTrack.readyState, "ended");
    assert.equal(secondReceiverTrack.clones[0].readyState, "ended");
    assert.deepEqual(
        updates.slice(-2).map(({ payload }) => [payload.sessionId, payload.type, payload.track]),
        [
            [84, "screen", receiverTrack.clones[3]],
            [42, "camera", secondReceiverTrack.clones[1]]
        ]
    );
});

test("peer departure clears remote-track state before disconnect update", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();
    const consumerPresenceAtDisconnect = [];
    client.addEventListener("update", (event) => {
        if (event.detail.name === CLIENT_UPDATE.DISCONNECT) {
            consumerPresenceAtDisconnect.push(client._consumers.has(42));
        }
    });

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    const receiverTrack = createCameraTrack("track-1");
    peerConnections[0].emitTrack(receiverTrack, "0");
    await tick();

    await emitMessage("peer-left");

    assert.deepEqual(consumerPresenceAtDisconnect, [false]);
    assert.deepEqual(updates.at(-1), {
        name: CLIENT_UPDATE.DISCONNECT,
        payload: {
            sessionId: 42
        }
    });
});

test("peer connection teardown clears stale remote consumer state", async () => {
    const { client, core, emitMessage, peerConnections, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    peerConnections[0].emitTrack(createCameraTrack("track-1"), "0");
    await tick();

    assert.equal(client._consumers.get(42).camera.track.id, "track-1-clone-0");

    await emitMessage("close-peer-connection");

    assert.equal(peerConnections[0].closed, true);
    assert.equal(client._consumers.size, 0);
});

test("remote track lifecycle updates re-emit when the browser unmutes the track", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    // What a caller reads from the track at the time it is told.
    const mutedAtUpdate = [];
    client.addEventListener("update", ({ detail }) => {
        mutedAtUpdate.push(detail.payload.track.muted);
    });

    const receiverTrack = new FakeMediaTrack({
        id: "track-1",
        kind: "video",
        muted: true
    });
    peerConnections[0].emitTrack(receiverTrack, "0");
    await tick();

    receiverTrack.setMuted(false);
    await tick();

    const [track] = receiverTrack.clones;
    assert.deepEqual(mutedAtUpdate, [true, false]);
    assert.deepEqual(updates.at(-1), {
        name: CLIENT_UPDATE.TRACK,
        payload: {
            active: true,
            sessionId: 42,
            track,
            type: "camera"
        }
    });
    assert.equal(client._consumers.get(42).camera.track, track);
    assert.equal(client._consumers.get(42).camera.track.muted, false);
});

test("duplicate remote track events keep one lifecycle listener", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    const receiverTrack = new FakeMediaTrack({
        id: "track-1",
        kind: "video",
        muted: true
    });
    peerConnections[0].emitTrack(receiverTrack, "0");
    await tick();
    peerConnections[0].emitTrack(receiverTrack, "0");
    await tick();

    assert.equal(updates.length, 1);

    receiverTrack.setMuted(false);
    await tick();

    assert.equal(updates.length, 2);
    assert.equal(client._consumers.get(42).camera.track.muted, false);
});
