import assert from "node:assert/strict";
import test from "node:test";
import { CLIENT_UPDATE } from "../../dist/public_api.js";
import { COMMAND_KIND } from "../../dist/protocol_contract.js";
import { FakeMediaTrack, FakePeerConnection } from "../support/browser_fakes.mjs";
import { FakeProtocolCore, tick } from "../support/protocol_fakes.mjs";
import {
    createCameraTrack,
    createSfuClientHarness,
    createScreenTrack,
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

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");
    await tick();

    await emitMessage("inactive-track-binding");

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

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");
    await tick();

    assert.deepEqual(updates, []);
    assert.equal(client._consumers.size, 0);

    await emitMessage("inactive-track-binding");

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

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");
    await tick();

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

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");
    await tick();

    await emitMessage("clear-track-bindings");
    await emitMessage("inactive-track-binding");

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

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");
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

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");
    await tick();

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
                track: earlyTrack,
                type: "screen"
            }
        }
    ]);
    assert.equal(client._consumers.has(42), false);
    assert.equal(client._consumers.get(84).screen.track, earlyTrack);
});

test("track rebinding waits for a fresh track event before re-emitting state", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    const firstTrack = createCameraTrack("track-1");
    peerConnections[0].emitTrack(firstTrack, "0");
    await tick();

    await emitMessage("track-rebind");

    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 42,
                track: firstTrack,
                type: "camera"
            }
        }
    ]);
    assert.equal(client._consumers.has(42), false);
    assert.equal(client._consumers.has(84), false);

    const reboundTrack = createScreenTrack("track-2");
    peerConnections[0].emitTrack(reboundTrack, "0");
    await tick();

    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 42,
                track: firstTrack,
                type: "camera"
            }
        },
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 84,
                track: reboundTrack,
                type: "screen"
            }
        }
    ]);
    assert.equal(client._consumers.get(84).screen.track, reboundTrack);
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

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");
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

    assert.equal(client._consumers.get(42).camera.track.id, "track-1");

    await emitMessage("close-peer-connection");

    assert.equal(peerConnections[0].closed, true);
    assert.equal(client._consumers.size, 0);
});

test("remote track lifecycle updates re-emit when the browser unmutes the track", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    const track = new FakeMediaTrack({
        id: "track-1",
        kind: "video",
        muted: true
    });
    peerConnections[0].emitTrack(track, "0");
    await tick();

    track.setMuted(false);
    await tick();

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

    const track = new FakeMediaTrack({
        id: "track-1",
        kind: "video",
        muted: true
    });
    peerConnections[0].emitTrack(track, "0");
    await tick();
    peerConnections[0].emitTrack(track, "0");
    await tick();

    assert.equal(updates.length, 1);

    track.setMuted(false);
    await tick();

    assert.equal(updates.length, 2);
    assert.equal(client._consumers.get(42).camera.track.muted, false);
});
