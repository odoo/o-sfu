import assert from "node:assert/strict";
import test from "node:test";
import {
    buildWelcomeFrame,
    decodeSentFrame,
    sentPublishCount,
    tick
} from "../support/protocol_fakes.mjs";
import {
    authenticateRecovery,
    connectRealWithWelcome,
    createCameraTrack,
    createRecoveryHarness,
    createSfuClientHarness,
    createScreenTrack
} from "../support/sfu_client_harness.mjs";
import {
    buildNegotiationFrame,
    buildVideoRenegotiationFrame,
    sdp,
    videoMedia,
    videoUploadSlot
} from "../support/negotiation_fixtures.mjs";

test("authenticated publication waits for transport readiness", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, sockets } = harness;

    await connectRealWithWelcome(harness);
    client.publish("camera", createCameraTrack("camera-track"));
    await tick();

    assert.equal(sentPublishCount(sockets[0]), 0);
    await emitMessage(buildNegotiationFrame("offer", "server-initial", "1"));
    assert.equal(sentPublishCount(sockets[0]), 1);
});

test("publish replaces an already attached local sender track without re-publishing", async () => {
    const { client, core, emitMessage, peerConnections, connectWithWelcome } =
        createSfuClientHarness();

    const firstTrack = createCameraTrack("camera-track-1");
    const secondTrack = createCameraTrack("camera-track-2");

    await connectWithWelcome();

    client.publish("camera", firstTrack);
    await tick();

    await emitMessage("offer");

    assert.equal(peerConnections[0].transceivers[1].sender.track, firstTrack);
    assert.deepEqual(core.publicationUpdates, [{ active: true, type: "camera" }]);

    client.publish("camera", secondTrack);
    await tick();

    assert.equal(peerConnections[0].transceivers[1].sender.track, secondTrack);
    assert.deepEqual(
        core.publicationUpdates,
        [{ active: true, type: "camera" }],
        "replacing a live local track should stay local once the sender is bound"
    );
});

test("cancelled track replacement cannot retain a stale peer binding", async () => {
    const { promise: replacementGate, resolve: releaseReplacement } = Promise.withResolvers();
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections, sockets } = harness;
    const firstTrack = createCameraTrack("camera-before-recovery");
    const secondTrack = createCameraTrack("camera-after-recovery");

    await connectRealWithWelcome(harness);
    client.publish("camera", firstTrack);
    await tick();
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    const sender = peerConnections[0].transceivers[1].sender;
    const replaceTrack = sender.replaceTrack.bind(sender);
    let replacementStarted = false;
    sender.replaceTrack = async (track) => {
        if (track === secondTrack) {
            replacementStarted = true;
            await replacementGate;
        }
        await replaceTrack(track);
    };

    client.publish("camera", secondTrack);
    await tick();
    assert.equal(replacementStarted, true);
    sockets[0].emitClose(1011);
    await tick();
    releaseReplacement();
    await tick();

    await authenticateRecovery(harness);
    await emitMessage(buildNegotiationFrame("offer", "recovery-offer", "1"), 1);
    await emitMessage(
        buildVideoRenegotiationFrame("recovery-renegotiation", {
            simulcastEncodings: []
        }),
        1
    );

    assert.equal(
        peerConnections[1].transceivers.find((transceiver) => transceiver.mid === "2").sender.track,
        secondTrack
    );
});

test("cancelled track detach cannot clear a recovered peer binding", async () => {
    const { promise: detachGate, resolve: releaseDetach } = Promise.withResolvers();
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections, sockets } = harness;
    const secondTrack = createCameraTrack("camera-after-recovery");
    const thirdTrack = createCameraTrack("camera-after-stale-detach");

    await connectRealWithWelcome(harness);
    client.publish("camera", createCameraTrack("camera-before-recovery"));
    await tick();
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    const sender = peerConnections[0].transceivers[1].sender;
    const replaceTrack = sender.replaceTrack.bind(sender);
    let detachStarted = false;
    sender.replaceTrack = async (track) => {
        if (track === null) {
            detachStarted = true;
            await detachGate;
        }
        await replaceTrack(track);
    };

    client.publish("camera", null);
    await tick();
    assert.equal(detachStarted, true);
    sockets[0].emitClose(1011);
    await tick();
    client.publish("camera", secondTrack);
    await tick();

    await authenticateRecovery(harness);
    await emitMessage(buildNegotiationFrame("offer", "recovery-offer", "1"), 1);
    await emitMessage(buildVideoRenegotiationFrame("recovery-renegotiation", { mid: "2" }), 1);
    releaseDetach();
    await tick();

    client.publish("camera", thirdTrack);
    await tick();

    const recoveredSender = peerConnections[1].transceivers.find(
        (transceiver) => transceiver.mid === "2"
    ).sender;
    assert.equal(recoveredSender.track, thirdTrack);
});

test("updateUpload pauses then reuses the negotiated sender without another answer", async () => {
    const track = createCameraTrack("camera-track-1");
    const resumedTrack = createCameraTrack("camera-track-2");
    const { client, core, emitMessage, open, peerConnections, sockets, connect } =
        createSfuClientHarness();
    const originalPublish = core.publish.bind(core);
    core.publish = (type, active) => {
        const commands = originalPublish(type, active);
        if (!active) {
            commands.push({ frame: `unpublish:${type}`, kind: "sendWebSocket" });
        }
        return commands;
    };

    await connect();
    await open();
    await emitMessage("welcome");

    client.publish("camera", track);
    await tick();
    await emitMessage("offer");

    assert.equal(peerConnections[0].transceivers[1].sender.track, track);
    assert.deepEqual(core.publicationUpdates, [{ active: true, type: "camera" }]);

    const peer = peerConnections[0];
    const transceiver = peerConnections[0].transceivers[1];
    const answerCount = peer.answerSnapshots.length;
    const direction = transceiver.direction;
    const mid = transceiver.mid;
    const transceiverCount = peer.transceivers.length;
    const pauseOrder = [];
    const socket = sockets[0];
    const originalSend = socket.send.bind(socket);
    socket.send = (frame) => {
        pauseOrder.push(`send:${frame}`);
        originalSend(frame);
    };
    const sender = transceiver.sender;
    const originalReplaceTrack = sender.replaceTrack.bind(sender);
    sender.replaceTrack = async (replacementTrack) => {
        assert.equal(replacementTrack, null);
        await originalReplaceTrack(replacementTrack);
        pauseOrder.push("detach");
    };

    client.updateUpload("camera", undefined);
    await tick();

    assert.deepEqual(pauseOrder, ["detach", "send:unpublish:camera"]);
    assert.equal(transceiver.sender.track, null);
    assert.equal(transceiver.direction, direction);
    assert.deepEqual(core.publicationUpdates, [
        { active: true, type: "camera" },
        { active: false, type: "camera" }
    ]);

    sender.replaceTrack = originalReplaceTrack;
    client.updateUpload("camera", resumedTrack);
    await tick();

    assert.equal(transceiver.sender.track, resumedTrack);
    assert.equal(transceiver.direction, direction);
    assert.equal(transceiver.mid, mid);
    assert.equal(peer.transceivers.length, transceiverCount);
    assert.equal(peer.answerSnapshots.length, answerCount);
    assert.deepEqual(core.publicationUpdates, [
        { active: true, type: "camera" },
        { active: false, type: "camera" },
        { active: true, type: "camera" }
    ]);
});

test("rapid pause and resume converges on the latest track without negotiation", async () => {
    const { promise: detachGate, resolve: releaseDetach } = Promise.withResolvers();
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections, sockets, timers } = harness;
    const secondTrack = createCameraTrack("camera-track-same-turn-second");

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    client.publish("camera", createCameraTrack("camera-track-same-turn-first"));
    await tick();
    await emitMessage(buildVideoRenegotiationFrame("9", { mid: "2", simulcastEncodings: [] }));

    const peer = peerConnections[0];
    const transceiver = peer.transceivers.find((candidate) => candidate.mid === "2");
    assert.ok(transceiver);
    const answerCount = peer.answerSnapshots.length;
    const direction = transceiver.direction;
    const replaceTrack = transceiver.sender.replaceTrack.bind(transceiver.sender);
    let detachStarted = false;
    transceiver.sender.replaceTrack = async (track) => {
        if (track === null) {
            detachStarted = true;
            await detachGate;
        }
        await replaceTrack(track);
    };

    client.publish("camera", null);
    client.publish("camera", secondTrack);
    await tick();
    assert.equal(detachStarted, true);

    releaseDetach();
    await tick();
    timers.fireByDelay(100);
    await tick();

    assert.equal(transceiver.sender.track, secondTrack);
    assert.equal(transceiver.direction, direction);
    assert.equal(peer.answerSnapshots.length, answerCount);
    const publicationEnvelopes = sockets[0].sent
        .flatMap((_, index) => decodeSentFrame(sockets[0], index))
        .filter((envelope) => envelope.t === "publish" || envelope.t === "unpublish");
    assert.deepEqual(publicationEnvelopes.at(-1), {
        t: "publish",
        p: {
            type: "camera"
        }
    });
});

test("cleanup offer rebinds a staged camera without disturbing a bound screen", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections } = harness;
    const screenTrack = createScreenTrack("screen-track");
    const latestTrack = createCameraTrack("camera-track-latest");

    await connectRealWithWelcome(harness);
    client.publish("screen", screenTrack);
    await tick();
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    client.publish("camera", createCameraTrack("camera-track-cancelled"));
    client.publish("camera", null);
    client.publish("camera", latestTrack);
    await tick();

    await emitMessage(
        buildNegotiationFrame("renegotiate", "9", {
            sdp: sdp(
                videoMedia("1", { direction: "recvonly" }),
                videoMedia("2", { direction: "recvonly" })
            ),
            uploadSlots: [videoUploadSlot("2", { simulcastEncodings: [] })]
        })
    );
    const peer = peerConnections[0];
    const screenTransceiver = peer.transceivers.find((candidate) => candidate.mid === "1");
    const oldTransceiver = peer.transceivers.find((candidate) => candidate.mid === "2");
    assert.equal(screenTransceiver.sender.track, screenTrack);
    assert.equal(oldTransceiver.sender.track, latestTrack);

    await emitMessage(
        buildNegotiationFrame("renegotiate", "10", {
            sdp: sdp(
                videoMedia("1", { direction: "recvonly" }),
                videoMedia("2", { direction: "inactive" }),
                videoMedia("3", { direction: "recvonly" })
            ),
            uploadSlots: [videoUploadSlot("3", { simulcastEncodings: [] })]
        })
    );

    const freshTransceiver = peer.transceivers.find((candidate) => candidate.mid === "3");
    assert.equal(screenTransceiver.sender.track, screenTrack);
    assert.equal(oldTransceiver.sender.track, null);
    assert.equal(freshTransceiver.sender.track, latestTrack);
    assert.equal(
        peer.answerSnapshots.at(-1).find((snapshot) => snapshot.mid === "3")?.senderTrack,
        latestTrack
    );
});

test("failed sender resume does not signal an active publication", async () => {
    const { client, core, emitMessage, handledErrors, open, peerConnections, sockets, connect } =
        createSfuClientHarness();
    const originalPublish = core.publish.bind(core);
    core.publish = (type, active) => {
        const commands = originalPublish(type, active);
        commands.push({ frame: `publication:${active}`, kind: "sendWebSocket" });
        return commands;
    };

    await connect();
    await open();
    await emitMessage("welcome");

    client.publish("camera", createCameraTrack("camera-track-first"));
    await tick();
    await emitMessage("offer");
    client.publish("camera", null);
    await tick();

    const sender = peerConnections[0].transceivers[1].sender;
    const secondTrack = createCameraTrack("camera-track-second");
    const replaceTrack = sender.replaceTrack.bind(sender);
    sender.replaceTrack = async (track) => {
        if (track === secondTrack) {
            throw new Error("sender rejected replacement");
        }
        await replaceTrack(track);
    };
    sockets[0].sent.length = 0;

    client.publish("camera", secondTrack);
    await tick();

    assert.deepEqual(sockets[0].sent, []);
    assert.equal(handledErrors.length, 1);
    assert.equal(handledErrors[0].message, "sender rejected replacement");
});

test("explicit disconnect clears publication intent before reconnect", async () => {
    const harness = createRecoveryHarness();
    const { client, connect, emitMessage, open, peerConnections, sockets, timers } = harness;
    const track = createCameraTrack("camera-track-after-disconnect");

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    client.publish("camera", track);
    await tick();
    timers.fireByDelay(100);
    await tick();
    await emitMessage(buildVideoRenegotiationFrame("9", { mid: "2", simulcastEncodings: [] }));

    client.disconnect();
    await tick();

    await connect("ws://example.test/ws", "jwt-token", { channelUUID: "channel-a" });
    await open(1);
    await emitMessage(buildWelcomeFrame(), 1);
    await emitMessage(buildNegotiationFrame("offer", "restart-offer", "1"), 1);

    assert.equal(
        peerConnections
            .at(-1)
            .answerSnapshots.at(-1)
            .some((section) => section.senderTrack === track),
        false
    );
    assert.equal(sentPublishCount(sockets[1]), 0);

    client.publish("camera", track);
    await tick();
    timers.fireByDelay(100);
    await tick();

    assert.equal(sentPublishCount(sockets[1]), 1);

    await emitMessage(buildVideoRenegotiationFrame("10", { mid: "2", simulcastEncodings: [] }), 1);

    const transceiver = peerConnections
        .at(-1)
        .transceivers.find((candidate) => candidate.mid === "2");
    assert.ok(transceiver);
    assert.equal(transceiver.sender.track, track);
});

test("pre-connect publish does not survive a fresh connect", async () => {
    const harness = createRecoveryHarness();
    const { client, connect, emitMessage, open, peerConnections, sockets } = harness;
    const track = createCameraTrack("camera-track-before-connect");

    client.publish("camera", track);
    await tick();

    await connect("ws://example.test/ws", "jwt-token", { channelUUID: "channel-a" });
    await open();
    await emitMessage(buildWelcomeFrame());
    await emitMessage(buildNegotiationFrame("offer", "server-initial", "1"));

    assert.equal(
        peerConnections
            .at(-1)
            .answerSnapshots.at(-1)
            .some((section) => section.senderTrack === track),
        false
    );
    assert.equal(sentPublishCount(sockets[0]), 0);
});

test("canceling pending camera publish does not detach an attached screen sender", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections } = harness;
    const screenTrack = createScreenTrack("screen-track");
    const cameraTrack = createCameraTrack("camera-track");

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    client.publish("screen", screenTrack);
    await tick();
    await emitMessage(buildVideoRenegotiationFrame("9", { mid: "2", simulcastEncodings: [] }));

    client.publish("camera", cameraTrack);
    client.publish("camera", null);
    await tick();

    const transceiver = peerConnections[0].transceivers.find((candidate) => candidate.mid === "2");
    assert.ok(transceiver);
    assert.equal(transceiver.sender.track, screenTrack);
});
