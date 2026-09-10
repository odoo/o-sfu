import assert from "node:assert/strict";
import test from "node:test";
import { COMMAND_KIND, WS_CLOSE_CODE } from "../../dist/protocol_contract.js";
import { FakePeerConnection } from "../support/browser_fakes.mjs";
import {
    EMPTY_FEATURES,
    FakeProtocolCore,
    buildWelcomeFrame,
    createManualTimers,
    tick
} from "../support/protocol_fakes.mjs";
import {
    connectRealWithWelcome,
    createCameraTrack,
    createRecoveryHarness,
    createSfuClientHarness,
    emitOfferWithBinding
} from "../support/sfu_client_harness.mjs";
import { buildNegotiationFrame } from "../support/negotiation_fixtures.mjs";

test("oversized server text frames close before protocol decoding", async () => {
    const core = new FakeProtocolCore();
    let decoded = false;
    core.onWsMessage = () => {
        decoded = true;
        return [];
    };
    const { connect, open, sockets } = createSfuClientHarness({ protocolCore: core });

    await connect();
    await open();

    sockets[0].emitMessage("x".repeat(256 * 1024 + 1));

    assert.equal(decoded, false);
    assert.notEqual(sockets[0].closeCode, WS_CLOSE_CODE.PROTOCOL_ERROR);
    assert.equal(sockets[0].closeCode >= 3000 && sockets[0].closeCode <= 4999, true);
    assert.deepEqual(core.wsCloseCodes, [WS_CLOSE_CODE.PROTOCOL_ERROR]);
});

test("cyclic broadcast input leaves the real protocol core reusable", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, handledErrors, open, sockets } = harness;
    const message = {};
    message.self = message;

    await connectRealWithWelcome(harness);
    client.broadcast(message);
    await tick();

    assert.equal(handledErrors.length, 1);
    assert.equal(handledErrors[0].name, "TypeError");
    assert.equal(client.state, "disconnected");
    assert.deepEqual(client.availableFeatures, EMPTY_FEATURES);
    assert.deepEqual(client.recordingState, {});

    client.connect("ws://example.test/ws", "jwt-token", { channelUUID: "channel-a" });
    await tick();
    assert.equal(sockets.length, 2);
    await open(1);
    await emitMessage(buildWelcomeFrame(), 1);

    assert.equal(client.state, "authenticated");
});

test("broadcast serialization failures use the runtime error boundary", async () => {
    const { client, handledErrors } = createSfuClientHarness();

    client.broadcast(() => undefined);
    await tick();

    assert.equal(handledErrors.length, 1);
    assert.equal(handledErrors[0].name, "TypeError");
});

test("same-turn disconnect preserves fatal cleanup effects", async () => {
    const harness = createRecoveryHarness();
    const { client, handledErrors } = harness;
    const stateChanges = [];
    client.addEventListener("stateChange", (event) => stateChanges.push(event.detail.state));

    await connectRealWithWelcome(harness);
    client.broadcast(() => undefined);
    client.disconnect();
    await tick();

    assert.equal(handledErrors.length, 1);
    assert.equal(stateChanges.at(-1), "disconnected");
});

test("fatal input after same-turn disconnect observes the installed cleanup state", async () => {
    const harness = createRecoveryHarness();
    const { client } = harness;
    const handledErrorSurfaces = [];

    await connectRealWithWelcome(harness);
    client.addEventListener("handledError", () => {
        handledErrorSurfaces.push({
            availableFeatures: client.availableFeatures,
            recordingState: client.recordingState,
            state: client.state
        });
    });

    client.disconnect();
    client.broadcast(() => undefined);
    await tick();

    assert.deepEqual(handledErrorSurfaces, [
        {
            availableFeatures: EMPTY_FEATURES,
            recordingState: {},
            state: "disconnected"
        }
    ]);
});

test("fatal cleanup runs before a reconnect requested by teardown callbacks", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, sockets } = harness;

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));
    client.addEventListener("log", (event) => {
        if (event.detail.message === "closed RTCPeerConnection") {
            client.connect("ws://other.example.test/ws", "jwt-token", {
                channelUUID: "channel-b"
            });
        }
    });

    client.broadcast(() => undefined);
    await tick();

    assert.equal(sockets.length, 2);
    assert.equal(sockets[1].closeCode, null);
});

test("repeated fatal inputs preserve installed cleanup effects", async () => {
    const harness = createRecoveryHarness();
    const { client, handledErrors } = harness;
    const stateChanges = [];
    client.addEventListener("stateChange", (event) => stateChanges.push(event.detail.state));

    await connectRealWithWelcome(harness);
    client.broadcast(() => undefined);
    client.broadcast(() => undefined);
    await tick();

    assert.equal(handledErrors.length, 2);
    assert.equal(stateChanges.at(-1), "disconnected");
});

test("fatal teardown clears the active peer before logging", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, handledErrors } = harness;
    let closeLogs = 0;

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));
    client.addEventListener("log", (event) => {
        if (event.detail.message !== "closed RTCPeerConnection") {
            return;
        }
        closeLogs += 1;
        if (closeLogs === 1) {
            client.broadcast(() => undefined);
        }
    });
    client.broadcast(() => undefined);
    await tick();

    assert.equal(closeLogs, 1);
    assert.equal(handledErrors.length, 2);
});

test("disconnect exposes cleanup state before peer-close callbacks", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage } = harness;
    const handledErrorSurfaces = [];

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));
    client.addEventListener("log", (event) => {
        if (event.detail.message === "closed RTCPeerConnection") {
            client.broadcast(() => undefined);
        }
    });
    client.addEventListener("handledError", () => {
        handledErrorSurfaces.push({
            availableFeatures: client.availableFeatures,
            recordingState: client.recordingState,
            state: client.state
        });
    });

    client.disconnect();
    await tick();

    assert.deepEqual(handledErrorSurfaces, [
        {
            availableFeatures: EMPTY_FEATURES,
            recordingState: {},
            state: "disconnected"
        }
    ]);
});

test("fatal runtime errors reset the public client surface", async () => {
    const { client, core, emitMessage, handledErrors, open, peerConnections, sockets, connect } =
        createSfuClientHarness();
    const stateChanges = [];
    const handledErrorSurfaces = [];
    client.addEventListener("stateChange", (event) => {
        stateChanges.push(event.detail);
    });
    client.addEventListener("handledError", () => {
        handledErrorSurfaces.push({
            availableFeatures: client.availableFeatures,
            recordingState: client.recordingState,
            state: client.state
        });
    });

    await connect();
    await open();
    await emitMessage("welcome");

    await emitOfferWithBinding({ core, emitMessage });
    peerConnections[0].emitTrack(createCameraTrack("track-1"), "0");

    await emitMessage("explode");

    assert.equal(core.disconnectCalls, 1);
    assert.equal(client.state, "disconnected");
    assert.deepEqual(client.availableFeatures, EMPTY_FEATURES);
    assert.deepEqual(client.recordingState, {});
    assert.equal(client._consumers.size, 0);
    assert.equal(stateChanges.at(-1).state, "disconnected");
    assert.equal(client.errors.length, 1);
    assert.equal(client.errors[0] instanceof Error, true);
    assert.equal(handledErrors[0], client.errors[0]);
    assert.deepEqual(handledErrorSurfaces, [
        {
            availableFeatures: EMPTY_FEATURES,
            recordingState: {},
            state: "disconnected"
        }
    ]);
    assert.equal(sockets[0].closeCode, 4000);
    assert.equal(sockets[0].readyState, 3);
    assert.deepEqual(core.wsCloseCodes, []);
});

test("fatal abort ignores late negotiation failures", async () => {
    const { promise: remoteDescription, reject: rejectRemoteDescription } = Promise.withResolvers();
    const { client, connectWithWelcome, emitMessage, handledErrors } = createSfuClientHarness({
        createPeerConnection(config) {
            const peerConnection = new FakePeerConnection(config);
            peerConnection.setRemoteDescription = () => remoteDescription;
            return peerConnection;
        }
    });

    await connectWithWelcome();
    await emitMessage("offer");
    client.broadcast(() => undefined);
    rejectRemoteDescription(new Error("late negotiation failure"));
    await tick();

    assert.equal(handledErrors.length, 1);
});

test("fatal runtime errors keep the original error when protocol disconnect fails", async () => {
    const core = new FakeProtocolCore();
    const disconnect = core.disconnect.bind(core);
    core.disconnect = () => {
        disconnect();
        throw new Error("disconnect failure");
    };
    const { client, connect, emitMessage, handledErrors, open, sockets } = createSfuClientHarness({
        protocolCore: core
    });
    const handledErrorSurfaces = [];
    const stateChanges = [];
    client.addEventListener("stateChange", (event) => stateChanges.push(event.detail.state));
    client.addEventListener("handledError", () => {
        handledErrorSurfaces.push({
            availableFeatures: client.availableFeatures,
            recordingState: client.recordingState,
            state: client.state
        });
    });

    await connect();
    await open();
    await emitMessage("welcome");

    await emitMessage("explode");

    assert.equal(client.errors.length, 1);
    assert.equal(client.errors[0].message, "boom");
    assert.equal(handledErrors[0], client.errors[0]);
    assert.equal(core.disconnectCalls, 1);
    assert.equal(sockets[0].closeCode, 4000);
    assert.equal(sockets[0].readyState, 3);
    assert.equal(stateChanges.at(-1), "disconnected");
    assert.deepEqual(handledErrorSurfaces, [
        {
            availableFeatures: EMPTY_FEATURES,
            recordingState: {},
            state: "disconnected"
        }
    ]);
});

test("fatal errors invalidate active recovery cleanup when disconnect fails", async () => {
    const core = new FakeProtocolCore();
    core.onWsClose = () => {
        core.state = "recovering";
        return [
            { kind: COMMAND_KIND.CLOSE_PEER_CONNECTION },
            { kind: COMMAND_KIND.EMIT_STATE_CHANGE, state: "recovering" },
            { kind: COMMAND_KIND.SCHEDULE_TIMER, id: 1, ms: 1000 }
        ];
    };
    const disconnect = core.disconnect.bind(core);
    core.disconnect = () => {
        disconnect();
        throw new Error("disconnect failure");
    };
    const timers = createManualTimers();
    const harness = createSfuClientHarness({
        clearTimer: timers.clearTimer,
        protocolCore: core,
        setTimer: timers.setTimer
    });
    const { client, connect, emitMessage, handledErrors, open, sockets } = harness;

    await connect();
    await open();
    await emitMessage("welcome");
    await emitMessage("offer");
    client.addEventListener("log", (event) => {
        if (event.detail.message === "closed RTCPeerConnection") {
            client.broadcast(() => undefined);
        }
    });

    sockets[0].emitClose(1011);
    await tick();

    assert.equal(client.state, "disconnected");
    assert.equal(timers.hasDelay(1000), false);
    assert.equal(handledErrors.length, 1);
});

test("fatal runtime errors drop already queued browser commands", async () => {
    const { connect, handledErrors, open, peerConnections, sockets, updates } =
        createSfuClientHarness({
            createPeerConnection(config) {
                const peerConnection = new FakePeerConnection(config);
                peerConnection.setRemoteDescription = async () => {
                    throw new Error("broken remote offer");
                };
                return peerConnection;
            }
        });

    await connect();
    await open();

    sockets[0].emitMessage("offer");
    sockets[0].emitMessage("peer-left");
    await tick();

    assert.equal(handledErrors.length, 1);
    assert.equal(peerConnections[0].closed, true);
    assert.deepEqual(updates, []);
});
