export class FakeWebSocket {
    constructor(url) {
        this.url = url;
        this.readyState = 0;
        this.sent = [];
        this.closeCode = null;
        this.onclose = null;
        this.onerror = null;
        this.onmessage = null;
        this.onopen = null;
    }

    open() {
        this.readyState = 1;
        this.onopen?.(new Event("open"));
    }

    send(data) {
        this.sent.push(data);
    }

    emitMessage(data) {
        this.onmessage?.({ data });
    }

    emitClose(code) {
        if (this.readyState >= 2) {
            return;
        }
        this.closeCode = code;
        this.readyState = 3;
        this.onclose?.({ code });
    }

    close(code = 1000) {
        if (this.readyState < 2 && !isBrowserCloseCode(code)) {
            throw new DOMException(`invalid websocket close code ${code}`, "InvalidAccessError");
        }
        this.emitClose(code);
    }
}

function isBrowserCloseCode(code) {
    return Number.isInteger(code) && (code === 1000 || (code >= 3000 && code <= 4999));
}

export class FakeSender {
    constructor(statsReport = undefined, { rejectSetParameters = false } = {}) {
        this.parameters = { encodings: [] };
        this.rejectSetParameters = rejectSetParameters;
        this.statsReport = statsReport;
        this.track = null;
    }

    async getStats() {
        return this.statsReport;
    }

    getParameters() {
        return structuredClone(this.parameters);
    }

    async replaceTrack(track) {
        this.track = track;
    }

    async setParameters(parameters) {
        if (this.rejectSetParameters) {
            throw new Error("simulcast unsupported");
        }
        this.parameters = structuredClone(parameters);
    }
}

export class FakePeerConnection {
    constructor(
        config,
        {
            answerSdp = "answer-sdp",
            autoConnect = true,
            peerConnectionStats = undefined,
            senderOptionsByMid = {}
        } = {}
    ) {
        this.answerSdp = answerSdp;
        this.autoConnect = autoConnect;
        this.answerSnapshots = [];
        this.connectionState = "new";
        this.config = config;
        this.localDescription = null;
        this.onconnectionstatechange = null;
        this.onicecandidateerror = null;
        this.ontrack = null;
        this.peerConnectionStats = peerConnectionStats;
        this.remoteDescriptions = [];
        this.senderOptionsByMid = senderOptionsByMid;
        this.transceivers = [this._transceiver("0", "audio"), this._transceiver("1", "video")];
    }

    async createAnswer() {
        this.answerSnapshots.push(
            this.transceivers.map((transceiver) => ({
                mid: transceiver.mid,
                senderParameters: transceiver.sender.parameters
                    ? structuredClone(transceiver.sender.parameters)
                    : null,
                senderTrack: transceiver.sender.track ?? null
            }))
        );
        return { sdp: this.answerSdp, type: "answer" };
    }

    async setLocalDescription(description) {
        this.localDescription = description;
        this.transceivers.forEach((transceiver) => {
            if (transceiver.sender.track) {
                transceiver.currentDirection = transceiver.direction;
            } else if (transceiver.direction === "recvonly") {
                transceiver.currentDirection = "inactive";
            }
        });
        if (this.autoConnect) {
            this.emitConnectionState("connected");
        }
    }

    async setRemoteDescription(description) {
        this.remoteDescriptions.push(description);
        this._addRemoteTransceiver(description, "2", "video");
        this._addRemoteTransceiver(description, "3", "video");
        if (
            description.sdp.includes("a=mid:producer-audio") &&
            !this._hasTransceiver("producer-audio")
        ) {
            this.transceivers.push(
                this._transceiver("consumer-audio", "audio"),
                this._transceiver("producer-audio", "audio")
            );
        }
    }

    getTransceivers() {
        return this.transceivers;
    }

    async getStats() {
        return this.peerConnectionStats;
    }

    close() {
        this.connectionState = "closed";
        this.closed = true;
    }

    emitTrack(track, mid) {
        this.ontrack?.({
            track,
            transceiver: { mid }
        });
    }

    emitConnectionState(state) {
        this.connectionState = state;
        this.onconnectionstatechange?.();
    }

    emitIceCandidateError({ errorCode, errorText, url }) {
        this.onicecandidateerror?.({ errorCode, errorText, url });
    }

    _addRemoteTransceiver(description, mid, kind) {
        if (description.sdp.includes(`a=mid:${mid}`) && !this._hasTransceiver(mid)) {
            this.transceivers.push(this._transceiver(mid, kind));
        }
    }

    _hasTransceiver(mid) {
        return this.transceivers.some((transceiver) => transceiver.mid === mid);
    }

    _transceiver(mid, kind) {
        return {
            currentDirection: null,
            direction: "recvonly",
            mid,
            receiver: { track: { kind } },
            sender: new FakeSender(undefined, this.senderOptionsByMid[mid])
        };
    }
}

export class FakeMediaTrack extends EventTarget {
    constructor({ enabled = true, id, kind, muted = false, readyState = "live" }) {
        super();
        this.enabled = enabled;
        this.id = id;
        this.kind = kind;
        this.muted = muted;
        this.readyState = readyState;
        /** Clones in creation order. */
        this.clones = [];
    }

    clone() {
        const clone = new FakeMediaTrack({ ...this, id: `${this.id}-clone-${this.clones.length}` });
        this.clones.push(clone);
        return clone;
    }

    stop() {
        this.readyState = "ended";
    }

    // A source mutes the tracks it feeds one after the other, each with its
    // own event, in no order a caller can rely on. The clones go last here.
    setMuted(muted) {
        this.muted = muted;
        this.dispatchEvent(new Event(muted ? "mute" : "unmute"));
        for (const clone of this.clones) {
            clone.setMuted(muted);
        }
    }
}
