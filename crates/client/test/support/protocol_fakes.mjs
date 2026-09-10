import assert from "node:assert/strict";

import { CLIENT_UPDATE } from "../../dist/public_api.js";
import {
    audioMedia,
    audioUploadSlot,
    sdp,
    videoMedia,
    videoUploadSlot
} from "./negotiation_fixtures.mjs";

export const EMPTY_FEATURES = {
    rtc: false,
    transcription: false,
    audioRecording: false,
    videoRecording: false
};

const initialOfferCommand = (requestId) => ({
    kind: "applyNegotiation",
    negotiationKind: "offer",
    requestId,
    sdp: sdp(audioMedia("0"), videoMedia("1")),
    uploadSlots: [audioUploadSlot("0"), videoUploadSlot("1")]
});

const remoteMediaUpdate = (bindings) => ({
    kind: "emitUpdate",
    update: { name: "remote_media", payload: { bindings } }
});

const setAvailableFeatures = (features) => ({ kind: "setAvailableFeatures", features });
const setRecordingState = (state) => ({ kind: "setRecordingState", state });

export class FakeProtocolCore {
    constructor() {
        this.broadcasts = [];
        this.state = "disconnected";
        this.disconnectCalls = 0;
        this.pendingNegotiationKind = null;
        this.subscriptionUpdates = [];
        this.submittedAnswers = [];
        this.publicationUpdates = [];
        this.trackBindings = new Map();
        this.transportReadyCalls = 0;
        this.transportFailureState = null;
        this.updateInfoCalls = [];
        this.wsCloseCodes = [];
    }

    broadcast(messageJson) {
        this.broadcasts.push(JSON.parse(messageJson));
        return [];
    }

    connect(url) {
        this.state = "connecting";
        return [
            setAvailableFeatures({ ...EMPTY_FEATURES }),
            setRecordingState({}),
            { kind: "emitStateChange", state: "connecting" },
            { kind: "connect", url }
        ];
    }

    disconnect() {
        this.disconnectCalls += 1;
        this.state = "disconnected";
        this.trackBindings.clear();
        return [
            setAvailableFeatures({ ...EMPTY_FEATURES }),
            setRecordingState({}),
            { kind: "emitStateChange", state: "disconnected" }
        ];
    }

    onTimer() {
        return [];
    }

    onTransportReady() {
        if (this.pendingNegotiationKind === "offer" || this.state === "connected") {
            return [];
        }
        this.transportReadyCalls += 1;
        this.state = "connected";
        return [{ kind: "emitStateChange", state: "connected" }];
    }

    onWsClose(code) {
        this.wsCloseCodes.push(code);
        if (this.transportFailureState) {
            this.state = this.transportFailureState;
            return [{ kind: "emitStateChange", state: this.transportFailureState }];
        }
        return [];
    }

    onWsMessage(frame) {
        switch (frame) {
            case "welcome":
                this.state = "authenticated";
                return [
                    setAvailableFeatures({
                        rtc: true,
                        transcription: false,
                        audioRecording: true,
                        videoRecording: false
                    }),
                    setRecordingState({
                        recording: false,
                        audio: false,
                        transcription: false,
                        video: false
                    }),
                    { kind: "emitStateChange", state: "authenticated" }
                ];
            case "offer":
                return this._withPendingNegotiationKind([
                    initialOfferCommand("7"),
                    ...this._remoteMediaSnapshot()
                ]);
            case "inactive-track-binding":
                this.trackBindings.set("0", {
                    active: false,
                    mid: "0",
                    sessionId: 42,
                    type: "camera"
                });
                return this._remoteMediaSnapshot();
            case "clear-track-bindings":
                this.trackBindings.clear();
                return this._remoteMediaSnapshot();
            case "track-rebind":
                this.trackBindings.set("0", {
                    active: true,
                    mid: "0",
                    sessionId: 84,
                    type: "screen"
                });
                return this._remoteMediaSnapshot();
            case "peer-left":
                this.trackBindings.delete("0");
                return [
                    {
                        kind: "emitUpdate",
                        update: {
                            name: CLIENT_UPDATE.DISCONNECT,
                            payload: { sessionId: 42 }
                        }
                    }
                ];
            case "close-peer-connection":
                return [{ kind: "closePeerConnection" }];
            case "explode":
                throw new Error("boom");
            default:
                return [];
        }
    }

    onWsOpen() {
        return [{ kind: "sendWebSocket", frame: "auth-frame" }];
    }

    startRecording() {
        return beginRecordingRequest();
    }

    stopRecording() {
        return beginRecordingRequest();
    }

    submitNegotiationAnswer(requestId, negotiationKind, sdp) {
        this.submittedAnswers.push({ negotiationKind, requestId, sdp });
        this.pendingNegotiationKind = null;
        return [];
    }

    subscribe(sessionId, states) {
        this.subscriptionUpdates.push({ sessionId, states });
        return [];
    }

    updateInfo(info) {
        this.updateInfoCalls.push(info);
        return [];
    }

    publish(type, active) {
        this.publicationUpdates.push({ active, type });
        return [];
    }

    _withPendingNegotiationKind(commands) {
        this.pendingNegotiationKind =
            commands.find((command) => command.kind === "applyNegotiation")?.negotiationKind ??
            null;
        return commands;
    }

    _remoteMediaSnapshot() {
        return [remoteMediaUpdate([...this.trackBindings.values()])];
    }
}

const beginRecordingRequest = () => [
    {
        kind: "beginPendingRequest",
        request: {
            requestId: "record-1",
            timeoutMs: 5000,
            timeoutTimerId: 10000
        }
    }
];

export const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

export const buildWelcomeFrame = (peers = []) =>
    JSON.stringify([
        {
            t: "welcome",
            p: {
                features: {
                    rtc: true,
                    transcription: false,
                    audioRecording: false,
                    videoRecording: true
                },
                recording: {
                    recording: false,
                    audio: false,
                    transcription: false,
                    video: false
                },
                peers
            }
        }
    ]);

export const decodeSentFrame = (socket, index) => JSON.parse(socket.sent[index]);

export const createManualTimers = () => {
    let nextHandleId = 1;
    const allHandles = [];
    const handles = new Map();
    return {
        clearTimer(handle) {
            handles.delete(handle.id);
        },
        fireLastByDelay(ms) {
            const handle = allHandles.findLast((candidate) => candidate.ms === ms);
            assert.ok(handle, `expected timer with delay ${ms}`);
            handle.callback();
        },
        fireByDelay(ms) {
            const handle = [...handles.values()].find((candidate) => candidate.ms === ms);
            assert.ok(handle, `expected timer with delay ${ms}`);
            handles.delete(handle.id);
            handle.callback();
        },
        hasDelay(ms) {
            return [...handles.values()].some((candidate) => candidate.ms === ms);
        },
        setTimer(callback, ms) {
            const handle = {
                callback,
                id: nextHandleId++,
                ms
            };
            handles.set(handle.id, handle);
            allHandles.push(handle);
            return handle;
        }
    };
};

export function sentPublishCount(socket) {
    return socket.sent
        .flatMap((_, index) => decodeSentFrame(socket, index))
        .filter((envelope) => envelope.t === "publish").length;
}
