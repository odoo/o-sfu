import {
    CLIENT_UPDATE,
    CLIENT_LOG_LEVEL,
    type AvailableFeatures,
    type ClientLogDetail,
    type ClientUpdateDetail,
    type ConnectOptions,
    type ConnectionState,
    type ConsumersCompat,
    type DownloadStates,
    type JsonValue,
    type RecordingOptions,
    type SfuRecordingState,
    type SessionId,
    type SessionInfo,
    type SfuClientEventMap,
    type SfuStats,
    type StreamType,
    type UpdateInfoOptions
} from "./public_api.js";
import { BrowserRuntime } from "./internals/browser_runtime.js";
import type { SfuClientDependencies } from "./internals/browser_types.js";
import {
    cloneIceServers,
    normalizeWebSocketUrl,
    validateConnectOptions,
    validateDownloadStates,
    validateTrackForStreamType
} from "./internals/validation.js";

const CLIENT_LOG_SOURCE = "sfu_client";

/**
 * Browser facade for one O-SFU call session.
 *
 * The client emits `stateChange`, `update`, `handledError` and `log` events.
 * Failures caught during session processing emit `handledError` and are
 * retained in {@link errors}. A handled failure can still end the session.
 * {@link getStats} rejects directly when browser stats collection fails.
 */
export class SfuClient extends EventTarget {
    /** Runtime errors captured since the latest {@link connect} or {@link disconnect} call. */
    public errors: Error[] = [];

    /**
     * Odoo compatibility view of remote media grouped by session and stream type.
     * New integrations should consume `update` events.
     */
    public readonly _consumers: ReadonlyMap<SessionId, ConsumersCompat>;

    private readonly _runtime: BrowserRuntime;

    /** Creates a disconnected client. */
    constructor();
    constructor(dependencies: SfuClientDependencies = {}) {
        super();
        this._runtime = new BrowserRuntime(
            {
                onLog: (detail) => this._emitRuntimeLog(detail),
                onRuntimeError: (error) => this._handleRuntimeError(error),
                onStateChange: (state, cause) => this._emitStateChange(state, cause),
                onUpdate: (update) => this._emitUpdate(update)
            },
            dependencies
        );
        this._consumers = this._runtime.consumers;
    }

    /** Capabilities accepted from the latest welcome message or all `false` before one. */
    get availableFeatures(): AvailableFeatures {
        return this._runtime.availableFeatures;
    }

    /** Latest recording snapshot accepted from the server. */
    get recordingState(): SfuRecordingState {
        return this._runtime.recordingState;
    }

    /** Current signaling and transport lifecycle state. */
    get state(): ConnectionState {
        return this._runtime.state;
    }

    /**
     * Starts a connection attempt.
     *
     * `http:` and `https:` URLs are normalized to WebSocket schemes. The call
     * returns before authentication and media negotiation finish. Observe
     * `stateChange` and `handledError` for the result.
     *
     * @throws Error if `channelUUID` or an ICE server has an invalid shape.
     */
    connect(url: string, jwt: string, options: ConnectOptions = {}): void {
        validateConnectOptions(options);
        const iceServers = cloneIceServers(options.iceServers);
        this.errors = [];
        this._emitLog(
            CLIENT_LOG_LEVEL.INFO,
            `connect requested for ${options.channelUUID ? `room ${options.channelUUID}` : "implicit room"}`
        );
        this._runtime.connect(
            normalizeWebSocketUrl(url),
            jwt,
            options.channelUUID ?? null,
            iceServers
        );
    }

    /**
     * Ends the current attempt and clears caller-visible errors.
     *
     * Active sessions emit a final disconnected `stateChange`. Calling this
     * while already disconnected or closed has no protocol effect.
     */
    disconnect(): void {
        this.errors = [];
        this._emitLog(CLIENT_LOG_LEVEL.INFO, "disconnect requested");
        this._runtime.disconnect();
    }

    /**
     * Sets the desired local track for a stream.
     *
     * `null` and `undefined` pause or cancel publication. Publication intent is
     * replayed after transient recovery until an explicit disconnect or fresh
     * connection clears it.
     *
     * @throws Error if `type` is unknown or the track kind does not match it.
     */
    publish(type: StreamType, track: MediaStreamTrack | null | undefined): void {
        validateTrackForStreamType(type, track);
        this._runtime.publish(type, track ?? null);
    }

    /**
     * Applies a partial download preference update for one remote session.
     * Omitted fields keep their previous values and the merged preference is
     * replayed after transient recovery.
     *
     * @throws Error if a field name or value is invalid.
     */
    subscribe(sessionId: SessionId, states: DownloadStates): void {
        validateDownloadStates(states);
        this._emitLog(
            CLIENT_LOG_LEVEL.INFO,
            `updating download states for user ${sessionId}: ${JSON.stringify(states)}`
        );
        this._runtime.subscribe(sessionId, states);
    }

    /** @deprecated Odoo compatibility alias. Use `publish()` for new code. */
    updateUpload(type: StreamType, track: MediaStreamTrack | null | undefined): void {
        this.publish(type, track);
    }

    /** @deprecated Odoo compatibility alias. Use `subscribe()` for new code. */
    updateDownload(sessionId: SessionId, states: DownloadStates): void {
        this.subscribe(sessionId, states);
    }

    /** Sends a partial local participant-info update. */
    updateInfo(info: SessionInfo, _options: UpdateInfoOptions = {}): void {
        this._emitLog(CLIENT_LOG_LEVEL.DEBUG, `updating user info: ${JSON.stringify(info)}`);
        this._runtime.updateInfo(info);
    }

    /**
     * Returns peer-connection and local-sender WebRTC stats when available.
     * Returns an empty object before negotiation.
     *
     * Rejects with the browser error if peer-connection or sender stats
     * collection fails. The rejection does not emit `handledError` or add
     * an entry to {@link errors}.
     */
    getStats(): Promise<SfuStats> {
        return this._runtime.getStats();
    }

    /**
     * Sends a JSON snapshot of an application message.
     * Serialization failures are reported through `handledError` and can end the session.
     */
    broadcast(message: JsonValue): void {
        this._emitLog(CLIENT_LOG_LEVEL.DEBUG, "broadcast requested");
        this._runtime.broadcast(message);
    }

    /**
     * Requests recording with the selected media types.
     *
     * Resolves `true` when the server accepts the request. It resolves `false`
     * for refusal, timeout, teardown or when no request was registered. Runtime
     * failures reject the promise and also emit `handledError`.
     */
    startRecording(options: RecordingOptions = {}): Promise<boolean> {
        return this._runtime.startRecording(options);
    }

    /**
     * Requests recording shutdown with the same completion rules as
     * {@link startRecording}.
     */
    stopRecording(): Promise<boolean> {
        return this._runtime.stopRecording();
    }

    private _emitStateChange(state: ConnectionState, cause?: string): void {
        this._emitLog(
            CLIENT_LOG_LEVEL.INFO,
            cause ? `state changed to ${state} (cause: ${cause})` : `state changed to ${state}`
        );
        this._dispatch("stateChange", { cause, state });
    }

    private _emitUpdate(update: ClientUpdateDetail): void {
        this._logUpdate(update);
        this._dispatch("update", update);
    }

    private _handleRuntimeError(error: Error): void {
        this.errors.push(error);
        this._emitLog(CLIENT_LOG_LEVEL.ERROR, `runtime error: ${error.message}`);
        this._dispatch("handledError", { error });
    }

    private _emitRuntimeLog(detail: ClientLogDetail): void {
        this._dispatch("log", detail);
    }

    private _emitLog(level: ClientLogDetail["level"], message: string): void {
        this._emitRuntimeLog({
            id: CLIENT_LOG_SOURCE,
            level,
            message
        });
    }

    private _dispatch<K extends keyof SfuClientEventMap>(
        type: K,
        detail: SfuClientEventMap[K]["detail"]
    ): void {
        this.dispatchEvent(new CustomEvent(type, { detail }));
    }

    private _logUpdate(update: ClientUpdateDetail): void {
        switch (update.name) {
            case CLIENT_UPDATE.TRACK:
                return this._emitLog(
                    CLIENT_LOG_LEVEL.DEBUG,
                    `remote ${update.payload.type} track update for session ${update.payload.sessionId}: active=${update.payload.active}, muted=${update.payload.track.muted}, readyState=${update.payload.track.readyState}`
                );
            case CLIENT_UPDATE.DISCONNECT:
                return this._emitLog(
                    CLIENT_LOG_LEVEL.INFO,
                    `session ${update.payload.sessionId} disconnected`
                );
            case CLIENT_UPDATE.INFO_CHANGE:
                return this._emitLog(
                    CLIENT_LOG_LEVEL.DEBUG,
                    `received remote user info update: ${JSON.stringify(update.payload)}`
                );
            case CLIENT_UPDATE.BROADCAST:
                return this._emitLog(
                    CLIENT_LOG_LEVEL.DEBUG,
                    `received broadcast from user ${update.payload.senderId}`
                );
            case CLIENT_UPDATE.CHANNEL_INFO_CHANGE:
                return this._emitLog(CLIENT_LOG_LEVEL.DEBUG, "received channel info update");
        }
    }
}

export interface SfuClient {
    addEventListener<K extends keyof SfuClientEventMap>(
        type: K,
        listener: ((this: SfuClient, event: SfuClientEventMap[K]) => unknown) | null,
        options?: boolean | AddEventListenerOptions
    ): void;
    addEventListener(
        type: string,
        callback: EventListenerOrEventListenerObject | null,
        options?: boolean | AddEventListenerOptions
    ): void;
    removeEventListener<K extends keyof SfuClientEventMap>(
        type: K,
        listener: ((this: SfuClient, event: SfuClientEventMap[K]) => unknown) | null,
        options?: boolean | EventListenerOptions
    ): void;
    removeEventListener(
        type: string,
        callback: EventListenerOrEventListenerObject | null,
        options?: boolean | EventListenerOptions
    ): void;
}
