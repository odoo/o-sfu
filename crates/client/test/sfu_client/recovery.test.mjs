import assert from "node:assert/strict";
import test from "node:test";
import { CLIENT_UPDATE } from "../../dist/public_api.js";
import { WS_CLOSE_CODE } from "../../dist/protocol_contract.js";
import { FakePeerConnection } from "../support/browser_fakes.mjs";
import {
    FakeProtocolCore,
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
    emitOfferWithBinding
} from "../support/sfu_client_harness.mjs";
import {
    buildNegotiationFrame,
    buildVideoRenegotiationFrame
} from "../support/negotiation_fixtures.mjs";

test("real protocol core replays sticky publish after recovery transport readiness", async () => {
    const harness = createRecoveryHarness();
    const { client, sockets, connect, emitMessage, open, peerConnections } = harness;

    const cameraTrack = createCameraTrack("camera-track-1");

    await connect("ws://example.test/ws", "jwt-token", {
        channelUUID: "channel-a"
    });

    await open();
    await emitMessage(buildWelcomeFrame());
    await emitMessage(buildNegotiationFrame("offer", "server-initial", "1"));

    client.publish("camera", cameraTrack);
    await tick();
    await emitMessage(buildNegotiationFrame("renegotiate", "server-publish", "2"));
    client.subscribe(7, { audio: true, camera: false });
    client.updateInfo({ isCameraOn: true, isRaisingHand: true });
    await tick();

    sockets[0].emitClose(1011);
    await tick();
    await authenticateRecovery(harness);

    assert.deepEqual(decodeSentFrame(sockets[1], 0), [
        {
            t: "auth",
            p: {
                jwt: "jwt-token",
                channel: "channel-a"
            }
        }
    ]);
    assert.deepEqual(decodeSentFrame(sockets[1], 1), [
        {
            t: "subscribe",
            p: {
                sessionId: 7,
                audio: true,
                camera: false
            }
        },
        {
            t: "info",
            p: {
                isCameraOn: true,
                isRaisingHand: true
            }
        }
    ]);

    await emitMessage(buildNegotiationFrame("offer", "server-0", "1"), 1);

    assert.equal(
        peerConnections
            .at(-1)
            .answerSnapshots.at(-1)
            .some((section) => section.senderTrack === cameraTrack),
        false
    );
    assert.deepEqual(decodeSentFrame(sockets[1], 3), [
        {
            t: "publish",
            p: {
                type: "camera"
            }
        }
    ]);

    await emitMessage(buildNegotiationFrame("renegotiate", "server-republish", "2"), 1);

    const replayTransceiver = peerConnections
        .at(-1)
        .transceivers.find((candidate) => candidate.mid === "2");
    assert.ok(replayTransceiver);
    assert.equal(replayTransceiver.sender.track, cameraTrack);
    assert.equal(
        peerConnections
            .at(-1)
            .answerSnapshots.at(-1)
            .find((snapshot) => snapshot.mid === "2")?.senderTrack,
        cameraTrack
    );
});

test("real protocol core waits for transport-ready replay before binding recovery publish", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections, sockets } = harness;
    const track = createCameraTrack("camera-track-recovery-pending");

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "server-initial", "1"));

    sockets[0].emitClose(1011);
    await tick();

    client.publish("camera", track);
    await tick();

    await authenticateRecovery(harness);
    await emitMessage(buildNegotiationFrame("offer", "server-recovery", "1"), 1);

    assert.equal(
        peerConnections
            .at(-1)
            .answerSnapshots.at(-1)
            .some((section) => section.senderTrack === track),
        false
    );
    assert.equal(sentPublishCount(sockets[1]), 1);

    await emitMessage(buildVideoRenegotiationFrame("server-republish", { mid: "2" }), 1);

    const transceiver = peerConnections
        .at(-1)
        .transceivers.find((candidate) => candidate.mid === "2");
    assert.ok(transceiver);
    assert.equal(transceiver.sender.track, track);
});

test("real protocol core replays the latest sticky intents changed while recovering", async () => {
    const harness = createRecoveryHarness();
    const { client, sockets, connect, emitMessage, open } = harness;

    await connect();

    await open();
    await emitMessage(buildWelcomeFrame());

    client.publish("camera", createCameraTrack("camera-track-2"));
    client.subscribe(7, { audio: true });
    await tick();

    sockets[0].emitClose(1011);
    await tick();

    client.publish("camera", null);
    client.subscribe(7, { audio: false, camera: true });
    client.updateInfo({ isSelfMuted: true });
    await tick();

    await authenticateRecovery(harness);

    assert.deepEqual(decodeSentFrame(sockets[1], 1), [
        {
            t: "subscribe",
            p: {
                sessionId: 7,
                audio: false,
                camera: true
            }
        },
        {
            t: "info",
            p: {
                isSelfMuted: true
            }
        }
    ]);
});

test("explicit disconnect neutralizes a stale recovery timer", async () => {
    const harness = createRecoveryHarness();
    const { client, sockets, connect, emitMessage, open, timers } = harness;

    await connect();
    await open();
    await emitMessage(buildWelcomeFrame());

    sockets[0].emitClose(1011);
    await tick();
    assert.equal(timers.hasDelay(1000), true);

    client.disconnect();
    await tick();
    assert.equal(timers.hasDelay(1000), false);

    timers.fireLastByDelay(1000);
    await tick();

    assert.equal(sockets.length, 1);
});

test("new connect neutralizes a stale recovery timer", async () => {
    const harness = createRecoveryHarness();
    const { client, sockets, connect, emitMessage, open, timers } = harness;

    await connect("ws://example.test/old", "old-token");
    await open();
    await emitMessage(buildWelcomeFrame());

    sockets[0].emitClose(1011);
    await tick();
    assert.equal(timers.hasDelay(1000), true);

    client.connect("ws://example.test/new", "new-token");
    await tick();
    assert.equal(sockets.length, 2);
    assert.equal(sockets[1].url, "ws://example.test/new");
    assert.equal(timers.hasDelay(1000), false);

    timers.fireLastByDelay(1000);
    await tick();

    assert.equal(sockets.length, 2);
});

test("protocol inputs wait for an in-flight negotiation to finish", async () => {
    const { promise: answerGate, resolve: releaseAnswer } = Promise.withResolvers();
    const callOrder = [];
    class GatedPeerConnection extends FakePeerConnection {
        async createAnswer() {
            await answerGate;
            return super.createAnswer();
        }
    }
    class LoggingProtocolCore extends FakeProtocolCore {
        onWsMessage(frame) {
            callOrder.push(`onWsMessage:${frame}`);
            return super.onWsMessage(frame);
        }

        submitNegotiationAnswer(...args) {
            callOrder.push("submitNegotiationAnswer");
            super.submitNegotiationAnswer(...args);
            return [{ kind: "sendWebSocket", frame: "answer-feedback" }];
        }
    }
    const { connect, emitMessage, open, sockets } = createSfuClientHarness({
        createPeerConnection: (config) => new GatedPeerConnection(config),
        protocolCore: new LoggingProtocolCore()
    });

    await connect();
    await open();
    await emitMessage("welcome");
    const send = sockets[0].send.bind(sockets[0]);
    sockets[0].send = (frame) => {
        callOrder.push(`send:${frame}`);
        send(frame);
    };
    await emitMessage("offer");
    sockets[0].emitMessage("peer-left");
    await tick();

    assert.equal(callOrder.includes("onWsMessage:peer-left"), false);

    releaseAnswer();
    await tick();

    assert.deepEqual(callOrder.slice(-3), [
        "submitNegotiationAnswer",
        "send:answer-feedback",
        "onWsMessage:peer-left"
    ]);
});

test("reentrant disconnect cannot reorder a subscription behind cleanup", async () => {
    const core = new FakeProtocolCore();
    const calls = [];
    const subscribe = core.subscribe.bind(core);
    core.subscribe = (...args) => {
        calls.push("subscribe");
        return subscribe(...args);
    };
    const { client, connectWithWelcome, emitMessage, peerConnections } = createSfuClientHarness({
        protocolCore: core
    });

    await connectWithWelcome();
    await emitOfferWithBinding({ core, emitMessage });
    peerConnections[0].emitTrack(createCameraTrack("camera-track"), "0");
    await tick();

    client.addEventListener("update", (event) => {
        if (event.detail.name === CLIENT_UPDATE.TRACK && !event.detail.payload.active) {
            calls.push("disconnect");
            client.disconnect();
        }
    });
    client.subscribe(42, { camera: false });
    await tick();

    assert.deepEqual(calls, ["subscribe", "disconnect"]);
    assert.equal(core.disconnectCalls, 1);
});

test("reentrant disconnect cancels publication signaling", async () => {
    const core = new FakeProtocolCore();
    const { client, connectWithWelcome } = createSfuClientHarness({ protocolCore: core });

    await connectWithWelcome();
    client.addEventListener("log", () => client.disconnect(), { once: true });
    client.publish("camera", createCameraTrack("camera-track"));
    await tick();

    assert.equal(core.disconnectCalls, 1);
    assert.deepEqual(core.publicationUpdates, []);
});

test("socket close cancels an in-flight negotiation before recovery", async () => {
    const { promise: answerGate, resolve: releaseAnswer } = Promise.withResolvers();
    const core = new FakeProtocolCore();
    core.transportFailureState = "recovering";
    const onWsClose = core.onWsClose.bind(core);
    core.onWsClose = (code) => [{ kind: "closePeerConnection" }, ...onWsClose(code)];
    class GatedPeerConnection extends FakePeerConnection {
        async createAnswer() {
            await answerGate;
            return super.createAnswer();
        }
    }
    const { client, connectWithWelcome, emitMessage, handledErrors, peerConnections, sockets } =
        createSfuClientHarness({
            createPeerConnection: (config) => new GatedPeerConnection(config),
            protocolCore: core
        });

    await connectWithWelcome();
    await emitMessage("offer");
    const socket = sockets[0];
    socket.close = (code) => {
        socket.closeCode = code;
        socket.readyState = 2;
    };
    peerConnections[0].emitConnectionState("failed");
    releaseAnswer();
    await tick();

    assert.equal(client.state, "recovering");
    assert.equal(peerConnections[0].closed, true);
    assert.deepEqual(core.wsCloseCodes, [4000]);
    assert.deepEqual(core.submittedAnswers, []);
    assert.deepEqual(handledErrors, []);

    socket.readyState = 3;
    socket.onclose?.({ code: socket.closeCode });
    await tick();

    assert.deepEqual(core.wsCloseCodes, [4000]);
});

test("same-turn recovery retains queued sticky inputs", async () => {
    const core = new FakeProtocolCore();
    core.transportFailureState = "recovering";
    const { client, connectWithWelcome, sockets } = createSfuClientHarness({ protocolCore: core });

    await connectWithWelcome();
    client.publish("camera", createCameraTrack("camera-before-recovery"));
    client.subscribe(42, { camera: false });
    client.updateInfo({ isCameraOn: false });
    client.broadcast({ dropped: true });
    sockets[0].emitClose(1011);
    await tick();

    assert.deepEqual(core.publicationUpdates, [{ active: true, type: "camera" }]);
    assert.deepEqual(core.subscriptionUpdates, [{ sessionId: 42, states: { camera: false } }]);
    assert.deepEqual(core.updateInfoCalls, [{ isCameraOn: false }]);
    assert.deepEqual(core.broadcasts, []);
});

test(
    "disconnect cancels a stalled negotiation and ignores late failures",
    { timeout: 2_000 },
    async () => {
        const { promise: remoteDescription, reject: rejectRemoteDescription } =
            Promise.withResolvers();
        const harness = createRecoveryHarness({
            createPeerConnection(config) {
                const peerConnection = new FakePeerConnection(config);
                peerConnection.setRemoteDescription = () => remoteDescription;
                return peerConnection;
            }
        });
        const { client, emitMessage, handledErrors, peerConnections, sockets } = harness;

        await connectRealWithWelcome(harness);
        await emitMessage(buildNegotiationFrame("offer", "7", "1"));

        client.disconnect();
        await tick();

        assert.equal(client.state, "disconnected");
        assert.equal(peerConnections[0].closed, true);
        assert.equal(sockets[0].closeCode, WS_CLOSE_CODE.CLEAN);
        assert.equal(handledErrors.length, 0);

        rejectRemoteDescription(new Error("late negotiation failure"));
        await tick();

        assert.equal(handledErrors.length, 0);
    }
);

test("disconnect from peer creation stops negotiation before WebRTC effects", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, handledErrors, peerConnections, sockets } = harness;
    client.addEventListener("log", (event) => {
        if (event.detail.message === "created RTCPeerConnection") {
            client.disconnect();
        }
    });

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    assert.equal(peerConnections[0].remoteDescriptions.length, 0);
    assert.equal(client.state, "disconnected");
    assert.equal(peerConnections[0].closed, true);
    assert.equal(sockets[0].sent.length, 1);
    assert.deepEqual(handledErrors, []);
});
