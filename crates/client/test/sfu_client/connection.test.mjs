import assert from "node:assert/strict";
import test from "node:test";
import { CLIENT_UPDATE } from "../../dist/public_api.js";
import { WS_CLOSE_CODE } from "../../dist/protocol_contract.js";
import {
    EMPTY_FEATURES,
    FakeProtocolCore,
    buildWelcomeFrame,
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

const WELCOME_FEATURES = {
    rtc: true,
    transcription: false,
    audioRecording: false,
    videoRecording: true
};

const WELCOME_RECORDING_STATE = {
    recording: false,
    audio: false,
    transcription: false,
    video: false
};

test("connect normalizes the URL and sends auth on WebSocket open", async () => {
    const { sockets, connect, open } = createSfuClientHarness();

    await connect("https://example.test/ws", "jwt-token", {
        channelUUID: "channel-a",
        iceServers: [{ urls: "stun:stun.example.test" }]
    });

    assert.equal(sockets[0].url, "wss://example.test/ws");

    await open();

    assert.deepEqual(sockets[0].sent, ["auth-frame"]);
});

test("ignored duplicate connect keeps the accepted ICE server config", async () => {
    const harness = createRecoveryHarness();
    const { client, connect, emitMessage, open, peerConnections } = harness;
    const iceServers = [{ urls: "stun:first.example.test" }];

    await connect("ws://example.test/ws", "jwt-token", {
        channelUUID: "channel-a",
        iceServers
    });
    client.connect("ws://example.test/ws", "jwt-token", {
        channelUUID: "channel-a",
        iceServers: [{ urls: "stun:second.example.test" }]
    });
    await tick();
    await open();
    await emitMessage(buildWelcomeFrame());
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    assert.deepEqual(peerConnections[0].config.iceServers, iceServers);
});

test("public state changes are visible before their events", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, sockets } = harness;
    const stateSnapshots = [];
    let recordingStateDuringUpdate;
    client.addEventListener("stateChange", (event) => {
        stateSnapshots.push({
            state: event.detail.state,
            availableFeatures: client.availableFeatures,
            recordingState: client.recordingState
        });
    });
    client.addEventListener("update", (event) => {
        if (event.detail.name === CLIENT_UPDATE.CHANNEL_INFO_CHANGE) {
            recordingStateDuringUpdate = client.recordingState;
        }
    });

    await connectRealWithWelcome(harness);
    assert.deepEqual(stateSnapshots.at(-1), {
        state: "authenticated",
        availableFeatures: WELCOME_FEATURES,
        recordingState: WELCOME_RECORDING_STATE
    });

    const activeRecordingState = {
        recording: true,
        audio: true,
        transcription: false,
        video: true
    };
    await emitMessage(
        JSON.stringify([
            {
                t: "recordingchange",
                p: { state: activeRecordingState }
            }
        ])
    );
    assert.deepEqual(recordingStateDuringUpdate, activeRecordingState);

    sockets[0].emitClose(1011);
    await tick();
    assert.deepEqual(stateSnapshots.at(-1), {
        state: "recovering",
        availableFeatures: WELCOME_FEATURES,
        recordingState: activeRecordingState
    });

    client.disconnect();
    await tick();
    assert.deepEqual(stateSnapshots.at(-1), {
        state: "disconnected",
        availableFeatures: EMPTY_FEATURES,
        recordingState: {}
    });
});

test("batched public state exposes ordered snapshots", async () => {
    const harness = createRecoveryHarness();
    const { client, connect, emitMessage, open } = harness;
    const snapshots = [];
    const activeRecordingState = {
        recording: true,
        audio: true,
        transcription: false,
        video: true
    };
    client.addEventListener("stateChange", (event) => {
        if (event.detail.state === "authenticated") {
            snapshots.push({ event: "authenticated", recordingState: client.recordingState });
        }
    });
    client.addEventListener("update", (event) => {
        if (event.detail.name === CLIENT_UPDATE.CHANNEL_INFO_CHANGE) {
            snapshots.push({ event: "recording", recordingState: client.recordingState });
        }
    });

    await connect("ws://example.test/ws", "jwt-token", { channelUUID: "channel-a" });
    await open();
    const [welcome] = JSON.parse(buildWelcomeFrame());
    await emitMessage(
        JSON.stringify([
            welcome,
            {
                t: "recordingchange",
                p: { state: activeRecordingState }
            }
        ])
    );

    assert.deepEqual(snapshots, [
        { event: "authenticated", recordingState: WELCOME_RECORDING_STATE },
        { event: "recording", recordingState: activeRecordingState }
    ]);
});

test("welcome public state is atomic across microtasks", async () => {
    const harness = createRecoveryHarness();
    const { client, connect, open, sockets } = harness;
    const snapshots = [];

    await connect("ws://example.test/ws", "jwt-token", { channelUUID: "channel-a" });
    await open();
    sockets[0].emitMessage(buildWelcomeFrame());
    queueMicrotask(() => {
        snapshots.push({
            availableFeatures: client.availableFeatures,
            recordingState: client.recordingState,
            state: client.state
        });
    });
    await tick();

    assert.deepEqual(snapshots, [
        {
            availableFeatures: WELCOME_FEATURES,
            recordingState: WELCOME_RECORDING_STATE,
            state: "authenticated"
        }
    ]);
});

test("immediate disconnect prevents pending connect and subscription", async () => {
    const core = new FakeProtocolCore();
    core.disconnect = () => [];
    const { client, sockets } = createSfuClientHarness({ protocolCore: core });

    client.connect("ws://example.test/ws", "jwt-token", { channelUUID: "channel-a" });
    client.subscribe(42, { camera: false });
    client.disconnect();
    await tick();

    assert.deepEqual(sockets, []);
    assert.deepEqual(core.subscriptionUpdates, []);
});

test("last same-turn disconnect drops a queued reconnect without canceling cleanup", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections, sockets } = harness;

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    client.disconnect();
    client.connect("ws://other.example.test/ws", "jwt-token", { channelUUID: "channel-b" });
    client.disconnect();
    await tick();

    assert.equal(client.state, "disconnected");
    assert.equal(sockets.length, 1);
    assert.equal(sockets[0].closeCode, WS_CLOSE_CODE.CLEAN);
    assert.equal(peerConnections[0].closed, true);
});

test("new connect closes the previous socket without feeding a stale close to the protocol core", async () => {
    const core = new FakeProtocolCore();
    const { client, sockets, connect, open } = createSfuClientHarness({ protocolCore: core });

    await connect("ws://example.test/old", "old-token");
    await open();

    client.connect("ws://example.test/new", "new-token");
    await tick();

    assert.equal(sockets[0].closeCode, 1000);
    assert.equal(sockets[1].url, "ws://example.test/new");
    assert.deepEqual(core.wsCloseCodes, []);
});

test("offer waits for peer connection transport readiness before emitting connected", async () => {
    const { client, core, emitMessage, peerConnections, connectWithWelcome } =
        createSfuClientHarness({
            peerConnectionOptions: { autoConnect: false }
        });

    await connectWithWelcome();
    assert.equal(client.state, "authenticated");

    await emitMessage("offer");

    assert.equal(core.transportReadyCalls, 0);
    assert.equal(client.state, "authenticated");
    assert.deepEqual(core.submittedAnswers, [
        {
            negotiationKind: "offer",
            requestId: "7",
            sdp: "answer-sdp"
        }
    ]);

    peerConnections[0].emitConnectionState("connected");
    await tick();

    assert.equal(core.transportReadyCalls, 1);
    assert.equal(client.state, "connected");
});

test("initial offer with only inactive media enters connected without waiting for rtc transport", async () => {
    const { client, core, emitMessage, peerConnections, connectWithWelcome } =
        createSfuClientHarness({
            peerConnectionOptions: {
                answerSdp: [
                    "v=0",
                    "o=- 1 1 IN IP4 0.0.0.0",
                    "s=-",
                    "t=0 0",
                    "m=audio 9 UDP/TLS/RTP/SAVPF 111",
                    "a=inactive",
                    "a=candidate:1 1 udp 2113937151 127.0.0.1 54400 typ host",
                    "m=video 9 UDP/TLS/RTP/SAVPF 96",
                    "a=inactive"
                ].join("\r\n"),
                autoConnect: false
            }
        });

    await connectWithWelcome();
    await emitMessage("offer");

    assert.equal(peerConnections.length, 1);
    assert.equal(core.transportReadyCalls, 1);
    assert.equal(client.state, "connected");
});

test("peer connection failed closes the websocket and enters recovery", async () => {
    const core = new FakeProtocolCore();
    core.transportFailureState = "recovering";
    const { client, sockets, emitMessage, open, peerConnections, connect } = createSfuClientHarness(
        {
            protocolCore: core
        }
    );

    await connect();
    await open();
    await emitMessage("welcome");
    await emitMessage("offer");

    peerConnections[0].emitConnectionState("failed");
    await tick();

    assert.equal(sockets[0].readyState, 3);
    assert.equal(sockets[0].closeCode, 4000);
    assert.deepEqual(core.wsCloseCodes, [4000]);
    assert.equal(client.state, "recovering");
});

test("peer connection disconnected does not tear down the websocket session", async () => {
    const core = new FakeProtocolCore();
    core.transportFailureState = "recovering";
    const { client, sockets, emitMessage, open, peerConnections, connect } = createSfuClientHarness(
        {
            protocolCore: core
        }
    );

    await connect();
    await open();
    await emitMessage("welcome");
    await emitMessage("offer");

    peerConnections[0].emitConnectionState("disconnected");
    await tick();

    assert.equal(sockets[0].readyState, 1);
    assert.equal(sockets[0].closeCode, null);
    assert.deepEqual(core.wsCloseCodes, []);
    assert.equal(client.state, "connected");
});

test("ICE candidate errors emit a warning without tearing down the session", async () => {
    const { client, connectWithWelcome, emitMessage, handledErrors, peerConnections, sockets } =
        createSfuClientHarness();
    const logs = [];
    client.addEventListener("log", (event) => logs.push(event.detail));

    await connectWithWelcome();
    await emitMessage("offer");

    peerConnections[0].emitIceCandidateError({
        errorCode: 701,
        errorText: "STUN binding request timed out.",
        url: "stun:stun.example.test:3478"
    });
    await tick();

    assert.deepEqual(logs.at(-1), {
        id: "browser_runtime",
        level: "warn",
        message: "ice candidate error: STUN binding request timed out."
    });
    assert.deepEqual(handledErrors, []);
    assert.equal(sockets[0].closeCode, null);
    assert.equal(client.state, "connected");
});

test("stale peer connection callbacks cannot affect the active session", async () => {
    const core = new FakeProtocolCore();
    core.transportFailureState = "recovering";
    const { client, emitMessage, open, peerConnections, sockets, updates, connect } =
        createSfuClientHarness({
            protocolCore: core
        });
    const logs = [];
    client.addEventListener("log", (event) => logs.push(event.detail));

    await connect();
    await open();
    await emitMessage("welcome");

    await emitOfferWithBinding({ core, emitMessage });

    const stalePeerConnection = peerConnections[0];
    assert.equal(client.state, "connected");

    await emitOfferWithBinding({ core, emitMessage }, { sessionId: 84, type: "screen" });

    assert.equal(peerConnections.length, 2);
    assert.equal(stalePeerConnection.closed, true);
    const transportReadyCalls = core.transportReadyCalls;
    const logCount = logs.length;

    stalePeerConnection.emitTrack(createCameraTrack("stale-camera"), "0");
    stalePeerConnection.emitConnectionState("connected");
    stalePeerConnection.emitConnectionState("failed");
    stalePeerConnection.emitIceCandidateError({
        errorCode: 701,
        errorText: "STUN binding request timed out.",
        url: "stun:stale.example.test:3478"
    });
    await tick();

    assert.deepEqual(updates, []);
    assert.equal(core.transportReadyCalls, transportReadyCalls);
    assert.equal(sockets.length, 1);
    assert.equal(sockets[0].closeCode, null);
    assert.equal(client.state, "connected");
    assert.equal(client._consumers.size, 0);
    assert.equal(logs.length, logCount);
});
