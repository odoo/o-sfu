import assert from "node:assert/strict";

import { SfuClient } from "../../dist/sfu_client.js";
import { FakePeerConnection, FakeWebSocket } from "./browser_fakes.mjs";
import {
    FakeProtocolCore,
    buildWelcomeFrame,
    createManualTimers,
    tick
} from "./protocol_fakes.mjs";
import { createProtocolCore } from "./real_protocol_core.mjs";

const createTrack = ({ enabled = true, id, kind, muted = false }) => ({
    enabled,
    id,
    kind,
    muted
});

export const createCameraTrack = (id, options = {}) =>
    createTrack({ id, kind: "video", ...options });

export const createScreenTrack = (id, options = {}) =>
    createTrack({ id, kind: "video", ...options });

export const createSfuClientHarness = ({
    clearTimer,
    createPeerConnection,
    createProtocolCore,
    peerConnectionOptions = {},
    protocolCore = createProtocolCore ? null : new FakeProtocolCore(),
    setTimer
} = {}) => {
    const sockets = [];
    const peerConnections = [];
    const updates = [];
    const handledErrors = [];
    const dependencies = {
        createProtocolCore: createProtocolCore ?? (() => protocolCore),
        createPeerConnection: (config) => {
            const peerConnection = createPeerConnection
                ? createPeerConnection(config, peerConnections.length)
                : new FakePeerConnection(config, peerConnectionOptions);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    };
    if (clearTimer) {
        dependencies.clearTimer = clearTimer;
    }
    if (setTimer) {
        dependencies.setTimer = setTimer;
    }

    const client = new SfuClient(dependencies);
    client.addEventListener("update", (event) => {
        updates.push(event.detail);
    });
    client.addEventListener("handledError", (event) => {
        handledErrors.push(event.detail.error);
    });

    const connect = async (url = "ws://example.test/ws", jwt = "jwt-token", options = {}) => {
        client.connect(url, jwt, options);
        await tick();
        return sockets.at(-1);
    };
    const emitMessage = async (frame, socketIndex = 0) => {
        sockets[socketIndex].emitMessage(frame);
        await tick();
    };
    const open = async (socketIndex = 0) => {
        sockets[socketIndex].open();
        await tick();
    };
    const connectWithWelcome = async (options = {}) => {
        await connect(options.url, options.jwt, options.connectOptions ?? {});
        await emitMessage(options.welcomeFrame ?? "welcome", options.socketIndex ?? 0);
    };

    return {
        client,
        connect,
        connectWithWelcome,
        core: protocolCore,
        emitMessage,
        handledErrors,
        open,
        peerConnections,
        sockets,
        updates
    };
};

export function createRecoveryHarness(options = {}) {
    const timers = createManualTimers();
    return {
        ...createSfuClientHarness({
            clearTimer: timers.clearTimer,
            createProtocolCore,
            setTimer: timers.setTimer,
            ...options
        }),
        timers
    };
}

export async function connectRealWithWelcome(harness) {
    await harness.connect("ws://example.test/ws", "jwt-token", { channelUUID: "channel-a" });
    await harness.open();
    await harness.emitMessage(buildWelcomeFrame());
}

export async function authenticateRecovery({ emitMessage, open, sockets, timers }) {
    timers.fireByDelay(1000);
    await tick();

    assert.equal(sockets.length, 2);
    await open(1);
    await emitMessage(buildWelcomeFrame(), 1);
}

export async function emitOfferWithBinding({ core, emitMessage }, binding = {}) {
    core.trackBindings.set("0", {
        active: true,
        mid: "0",
        sessionId: 42,
        type: "camera",
        ...binding
    });
    await emitMessage("offer");
}
