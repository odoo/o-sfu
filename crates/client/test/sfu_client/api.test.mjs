import assert from "node:assert/strict";
import test from "node:test";
import { FakePeerConnection, FakeSender } from "../support/browser_fakes.mjs";
import { tick } from "../support/protocol_fakes.mjs";
import { createCameraTrack, createSfuClientHarness } from "../support/sfu_client_harness.mjs";

test("subscribe forwards additive video layout intent to the protocol core", async () => {
    const { client, core } = createSfuClientHarness();
    const states = {
        camera: true,
        cameraLayout: "pinned",
        screenLayout: "hidden"
    };

    client.subscribe(42, states);
    states.camera = false;
    await tick();

    assert.deepEqual(core.subscriptionUpdates, [
        {
            sessionId: 42,
            states: {
                camera: true,
                cameraLayout: "pinned",
                screenLayout: "hidden"
            }
        }
    ]);
});

test("subscribe rejects invalid download state fields", () => {
    const { client, core } = createSfuClientHarness();

    for (const states of [{ cameraLayout: "floating" }, { camera: true, video: false }]) {
        assert.throws(() => client.subscribe(42, states), Error);
    }
    assert.deepEqual(core.subscriptionUpdates, []);
});

test("getStats exposes compatibility-shaped transport and producer stats", async () => {
    const peerConnectionStats = new Map([["transport", { type: "transport" }]]);
    const cameraProducerStats = new Map([["outbound-rtp", { type: "outbound-rtp" }]]);
    const { client, peerConnections, emitMessage, connectWithWelcome } = createSfuClientHarness({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config, {
                peerConnectionStats
            });
            peerConnection.transceivers[1].sender = new FakeSender(cameraProducerStats);
            return peerConnection;
        }
    });

    await connectWithWelcome();

    client.publish("camera", createCameraTrack("camera-track"));
    await tick();

    await emitMessage("offer");

    const stats = await client.getStats();

    assert.equal(peerConnections.length, 1);
    assert.equal(stats.uploadStats, peerConnectionStats);
    assert.equal(stats.downloadStats, peerConnectionStats);
    assert.equal(stats.camera, cameraProducerStats);
    assert.equal(stats.audio, undefined);
    assert.equal(stats.screen, undefined);
});

test("updateInfo keeps the legacy needRefresh option as a compatibility no-op", async () => {
    const { client, core } = createSfuClientHarness();
    const info = { isCameraOn: true, isRaisingHand: true };

    client.updateInfo(info, { needRefresh: true });
    info.isCameraOn = false;
    await tick();

    assert.deepEqual(core.updateInfoCalls, [
        {
            isCameraOn: true,
            isRaisingHand: true
        }
    ]);
});

test("broadcast snapshots nested payloads at call time", async () => {
    const { client, core } = createSfuClientHarness();
    const message = { metadata: { label: "before" } };

    client.broadcast(message);
    message.metadata.label = "after";
    await tick();

    assert.deepEqual(core.broadcasts, [{ metadata: { label: "before" } }]);
});

test("publish rejects stream-kind mismatches", () => {
    const { client } = createSfuClientHarness();

    assert.throws(() => {
        client.publish("camera", {
            id: "audio-track",
            kind: "audio"
        });
    }, Error);
});

test("publish rejects invalid stream types", () => {
    const { client, core } = createSfuClientHarness();

    assert.throws(() => {
        client.publish("slides", null);
    }, Error);
    assert.deepEqual(core.publicationUpdates, []);
});

test("deprecated updateUpload and updateDownload delegate to publish and subscribe", async () => {
    const { client, core } = createSfuClientHarness();

    client.updateUpload("camera", createCameraTrack("camera-track-compat"));
    client.updateDownload(7, { audio: true });
    await tick();

    assert.deepEqual(core.publicationUpdates, [{ active: true, type: "camera" }]);
    assert.equal(core.subscriptionUpdates.length, 1);
});
