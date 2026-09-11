import assert from "node:assert/strict";
import test from "node:test";
import { COMMAND_KIND } from "../../dist/protocol_contract.js";
import { FakePeerConnection } from "../support/browser_fakes.mjs";
import { FakeProtocolCore, decodeSentFrame, tick } from "../support/protocol_fakes.mjs";
import {
    connectRealWithWelcome,
    createRecoveryHarness,
    createSfuClientHarness
} from "../support/sfu_client_harness.mjs";

for (const [name, startRequest, ok, expected] of [
    [
        "startRecording resolves through the protocol request lifecycle",
        (client) => client.startRecording({ audio: true }),
        true,
        true
    ],
    [
        "stopRecording resolves through the protocol request lifecycle",
        (client) => client.stopRecording(),
        true,
        true
    ],
    [
        "recording request refusal resolves false",
        (client) => client.startRecording({ audio: true }),
        false,
        false
    ]
]) {
    test(name, async () => {
        assert.equal(await resolveRealRecordingRequest(startRequest, ok), expected);
    });
}

test("recording requests without protocol registration resolve false", async () => {
    const core = new FakeProtocolCore();
    core.startRecording = (options) => {
        assert.deepEqual(options, { audio: true });
        return [];
    };
    core.stopRecording = () => [];
    const { client } = createSfuClientHarness({ protocolCore: core });
    const options = { audio: true };

    const recording = client.startRecording(options);
    options.audio = false;
    assert.equal(await recording, false);
    assert.equal(await client.stopRecording(), false);
});

test("disconnect resolves a registered recording request", { timeout: 2_000 }, async () => {
    const harness = createRecoveryHarness();
    const { client, timers } = harness;

    await connectRealWithWelcome(harness);
    const recording = client.startRecording({ audio: true });
    await tick();

    assert.equal(timers.hasDelay(5000), true);
    client.disconnect();

    assert.equal(await recording, false);
    assert.equal(timers.hasDelay(5000), false);
});

test("socket recovery resolves a registered recording request", { timeout: 2_000 }, async () => {
    const harness = createRecoveryHarness();
    const { client, sockets, timers } = harness;

    await connectRealWithWelcome(harness);
    const recording = client.startRecording({ audio: true });
    await tick();

    assert.equal(timers.hasDelay(5000), true);
    sockets[0].emitClose(1011);

    assert.equal(await recording, false);
    await tick();
    assert.equal(timers.hasDelay(5000), false);
    assert.equal(client.state, "recovering");
});

test("disconnect preserves active control cleanup", { timeout: 2_000 }, async () => {
    const harness = createRecoveryHarness();
    const { client, sockets, timers } = harness;

    await connectRealWithWelcome(harness);
    const recording = client.startRecording({ audio: true });
    await tick();

    assert.equal(timers.hasDelay(5000), true);
    client.addEventListener("log", () => queueMicrotask(() => client.disconnect()), { once: true });
    sockets[0].emitClose(1011);

    assert.equal(await recording, false);
    await tick();
    assert.equal(timers.hasDelay(5000), false);
    assert.equal(client.state, "disconnected");
});

test("duplicate recording request id is handled as a runtime error", async () => {
    const core = new FakeProtocolCore();
    core.startRecording = () => [
        {
            kind: COMMAND_KIND.BEGIN_PENDING_REQUEST,
            request: {
                requestId: "record-1",
                timeoutMs: 5000,
                timeoutTimerId: 10000
            }
        }
    ];
    const { client, handledErrors } = createSfuClientHarness({ protocolCore: core });

    const registeredPromise = client.startRecording({ audio: true });
    const registeredRejection = assert.rejects(registeredPromise, Error);
    await tick();
    await assert.rejects(client.startRecording({ audio: true }), Error);

    assert.equal(client.errors.length, 1);
    assert.equal(handledErrors[0], client.errors[0]);
    await registeredRejection;
});

test("runtime errors reject registered recording requests", async () => {
    const core = new FakeProtocolCore();
    const onWsMessage = core.onWsMessage.bind(core);
    core.onWsMessage = (frame) => {
        if (frame === "recording-runtime-failure") {
            throw new Error("recording runtime failure");
        }
        return onWsMessage(frame);
    };
    const { client, emitMessage, connectWithWelcome } = createSfuClientHarness({
        protocolCore: core
    });

    await connectWithWelcome();

    const registeredPromise = client.startRecording({ audio: true });
    await tick();
    const recordingRejection = assert.rejects(registeredPromise, /recording runtime failure/);

    await emitMessage("recording-runtime-failure");
    await recordingRejection;
});

test("runtime aborts reject stale queued recording requests", async () => {
    const { client, connectWithWelcome, sockets } = createSfuClientHarness({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnection.setRemoteDescription = async () => {
                throw new Error("broken remote offer");
            };
            return peerConnection;
        }
    });

    await connectWithWelcome();

    sockets[0].emitMessage("offer");
    const recordingRejection = assert.rejects(
        client.startRecording({ audio: true }),
        /broken remote offer/
    );
    await tick();
    await recordingRejection;
});

test("recording request timer setup failures reject through the public promise", async (t) => {
    const core = new FakeProtocolCore();
    const unhandledRejections = [];
    const trackUnhandledRejection = (reason) => {
        unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", trackUnhandledRejection);
    t.after(() => process.off("unhandledRejection", trackUnhandledRejection));
    const { client, handledErrors } = createSfuClientHarness({
        protocolCore: core,
        setTimer: () => {
            throw new Error("timer setup failed");
        }
    });

    await assert.rejects(client.startRecording({ audio: true }), /timer setup failed/);
    await tick();
    await tick();

    assert.equal(handledErrors.length, 1);
    assert.match(handledErrors[0].message, /timer setup failed/);
    assert.deepEqual(unhandledRejections, []);
});

test("startRecording rejects when the protocol core throws undefined", async () => {
    const core = new FakeProtocolCore();
    core.startRecording = () => {
        throw undefined;
    };
    const { client } = createSfuClientHarness({ protocolCore: core });

    await assert.rejects(client.startRecording(), (error) => error === undefined);
});

async function resolveRealRecordingRequest(startRequest, ok) {
    const harness = createRecoveryHarness();
    const { client, emitMessage, sockets, timers } = harness;

    await connectRealWithWelcome(harness);

    const resultPromise = startRequest(client);
    await tick();
    timers.fireByDelay(100);
    await tick();
    assert.equal(timers.hasDelay(5000), true);

    const [request] = decodeSentFrame(sockets[0], sockets[0].sent.length - 1);
    await emitMessage(JSON.stringify([{ t: request.t, r: request.q, p: { ok } }]));
    const result = await resultPromise;
    assert.equal(timers.hasDelay(5000), false);
    return result;
}
