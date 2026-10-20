import assert from "node:assert/strict";
import test from "node:test";
import { buildNegotiationFrame } from "../support/negotiation_fixtures.mjs";
import { COMMAND_KIND } from "../../dist/protocol_contract.js";
import { FakePeerConnection } from "../support/browser_fakes.mjs";
import { FakeProtocolCore, decodeSentFrame, tick } from "../support/protocol_fakes.mjs";
import {
    connectRealWithWelcome,
    createRecoveryHarness,
    createSfuClientHarness
} from "../support/sfu_client_harness.mjs";

for (const [name, startRequest, expectedRequest, ok] of [
    [
        "setRecording preserves selected true and false options",
        (client) => client.setRecording({ audio: true, video: false }),
        { t: "startrecording", p: { audio: true, video: false } },
        true
    ],
    [
        "setRecording accepts an empty partial update",
        (client) => client.setRecording({}),
        { t: "startrecording", p: {} },
        true
    ],
    [
        "setRecording forwards an all-false recording update",
        (client) => client.setRecording({ audio: false, video: false, transcription: false }),
        { t: "startrecording", p: { audio: false, video: false, transcription: false } },
        true
    ],
    [
        "startRecording resolves through the protocol request lifecycle",
        (client) => client.startRecording({ audio: true }),
        { t: "startrecording", p: { audio: true } },
        true
    ],
    [
        "stopRecording resolves through the protocol request lifecycle",
        (client) => client.stopRecording(),
        { t: "stoprecording" },
        true
    ],
    [
        "recording request refusal resolves false",
        (client) => client.setRecording({ audio: true }),
        { t: "startrecording", p: { audio: true } },
        false
    ]
]) {
    test(name, async () => {
        assert.equal(await resolveRealRecordingRequest(startRequest, expectedRequest, ok), ok);
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

for (const method of ["setRecording", "startRecording", "stopRecording"]) {
    for (const failure of ["disconnect", "recovery", "timeout"]) {
        test(
            `${method} handles ${failure} with its completion contract`,
            { timeout: 2_000 },
            async () => {
                const harness = createRecoveryHarness();
                const { client, sockets, timers, handledErrors } = harness;
                await connectRecordingClient(harness);
                const recording = client[method]({ audio: true });
                const completion =
                    method === "setRecording"
                        ? assert.rejects(
                              recording,
                              failure === "timeout" ? /bus request timed out/ : /bus closed/
                          )
                        : recording.then((result) => assert.equal(result, false));
                await tick();
                assert.equal(timers.hasDelay(5000), true);
                if (failure === "disconnect") {
                    client.disconnect();
                } else if (failure === "recovery") {
                    sockets[0].emitClose(1011);
                } else {
                    timers.fireByDelay(5000);
                }
                await completion;
                await tick();
                assert.equal(timers.hasDelay(5000), false);
                assert.deepEqual(handledErrors, []);
                if (failure === "recovery") {
                    assert.equal(client.state, "recovering");
                }
            }
        );
    }
}

test("disconnect preserves active control cleanup", { timeout: 2_000 }, async () => {
    const harness = createRecoveryHarness();
    const { client, sockets, timers } = harness;

    await connectRecordingClient(harness);
    const recording = client.setRecording({ audio: true });
    const rejection = assert.rejects(recording, /bus closed/);
    await tick();

    assert.equal(timers.hasDelay(5000), true);
    client.addEventListener("log", () => queueMicrotask(() => client.disconnect()), { once: true });
    sockets[0].emitClose(1011);

    await rejection;
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

test("setRecording rejects outside the connected state without a runtime error", async () => {
    const harness = createRecoveryHarness();
    const { client, handledErrors } = harness;
    await assert.rejects(client.setRecording({ audio: true }), {
        name: "Error",
        message: "SFU client is not connected"
    });
    await connectRealWithWelcome(harness);
    await assert.rejects(client.setRecording({}), /SFU client is not connected/);
    assert.deepEqual(handledErrors, []);
});

test("same-turn disconnect rejects a queued recording request", async () => {
    const harness = createRecoveryHarness();
    await connectRecordingClient(harness);
    const rejection = assert.rejects(
        harness.client.setRecording({ audio: true }),
        /SFU client is not connected/
    );
    harness.client.disconnect();
    await rejection;
    assert.equal(harness.timers.hasDelay(5000), false);
});

test("setRecording snapshots partial options before its queued command", async () => {
    const harness = createRecoveryHarness();
    await connectRecordingClient(harness);
    const options = { audio: true, video: false };
    const recording = harness.client.setRecording(options);
    options.audio = false;
    await tick();
    harness.timers.fireByDelay(100);
    await tick();
    const [request] = decodeSentFrame(harness.sockets[0], harness.sockets[0].sent.length - 1);
    assert.deepEqual(request.p, { audio: true, video: false });
    await harness.emitMessage(JSON.stringify([{ t: request.t, r: request.q, p: { ok: true } }]));
    assert.equal(await recording, true);
});

test("concurrent recording requests correlate replies and ignore late responses", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, sockets, timers, handledErrors } = harness;
    await connectRecordingClient(harness);
    const first = client.setRecording({ audio: true });
    const second = client.setRecording({ video: false });
    const firstRejection = assert.rejects(first, /bus request timed out/);
    await tick();
    timers.fireByDelay(100);
    await tick();
    const requests = decodeSentFrame(sockets[0], sockets[0].sent.length - 1);
    assert.equal(requests.length, 2);
    assert.notEqual(requests[0].q, requests[1].q);
    assert.deepEqual(
        requests.map((request) => request.p),
        [{ audio: true }, { video: false }]
    );
    await emitMessage(JSON.stringify([{ t: requests[1].t, r: requests[1].q, p: { ok: false } }]));
    assert.equal(await second, false);
    assert.equal(timers.hasDelay(5000), true);
    timers.fireByDelay(5000);
    await firstRejection;
    assert.equal(timers.hasDelay(5000), false);
    timers.fireLastByDelay(5000);
    await emitMessage(
        JSON.stringify(requests.map((request) => ({ t: request.t, r: request.q, p: { ok: true } })))
    );
    assert.deepEqual(handledErrors, []);
});

async function connectRecordingClient(harness) {
    await connectRealWithWelcome(harness);
    await harness.emitMessage(buildNegotiationFrame("offer", "recording-offer", "1"));
    assert.equal(harness.client.state, "connected");
}

async function resolveRealRecordingRequest(startRequest, expectedRequest, ok) {
    const harness = createRecoveryHarness();
    const { client, emitMessage, sockets, timers } = harness;
    await connectRecordingClient(harness);
    const resultPromise = startRequest(client);
    await tick();
    timers.fireByDelay(100);
    await tick();
    assert.equal(timers.hasDelay(5000), true);
    const requests = decodeSentFrame(sockets[0], sockets[0].sent.length - 1);
    assert.equal(requests.length, 1);
    const [request] = requests;
    assert.equal(request.t, expectedRequest.t);
    assert.deepEqual(request.p, expectedRequest.p);
    await emitMessage(JSON.stringify([{ t: request.t, r: request.q, p: { ok } }]));
    const result = await resultPromise;
    assert.equal(timers.hasDelay(5000), false);
    return result;
}
