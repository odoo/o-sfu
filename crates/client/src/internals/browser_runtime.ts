import {
    CLIENT_LOG_LEVEL,
    CLIENT_UPDATE,
    SFU_CLIENT_STATE,
    type AvailableFeatures,
    type ClientLogDetail,
    type ClientUpdateDetail,
    type ConnectionState,
    type ConsumersCompat,
    type DownloadStates,
    type JsonValue,
    type RecordingOptions,
    type SfuRecordingState,
    type SessionId,
    type SessionInfo,
    type SfuStats,
    type StreamType
} from "../public_api.js";
import {
    COMMAND_KIND,
    createProtocolCore,
    REMOTE_MEDIA_UPDATE,
    type HostCommand,
    type ProtocolCoreBindings
} from "../protocol_contract.js";
import {
    EMPTY_FEATURES,
    type ClientPeerConnection,
    type ClientWebSocket,
    type SfuClientDependencies,
    type TimerHandle
} from "./browser_types.js";
import { PendingRequests } from "./pending_requests.js";
import { PeerSession } from "./peer_session.js";
import { RemoteMedia } from "./remote_media.js";
import { SocketSession } from "./socket_session.js";
import { TurnQueue, type TurnGuard } from "./turn_queue.js";

type BrowserRuntimeContext = {
    onLog: (detail: ClientLogDetail) => void;
    onRuntimeError: (error: Error) => void;
    onStateChange: (state: ConnectionState, cause?: string) => void;
    onUpdate: (update: ClientUpdateDetail) => void;
};

const TURN_POLICY = {
    DROP_ON_RECOVERY: "dropOnRecovery",
    RETAIN_ON_RECOVERY: "retainOnRecovery"
} as const;

type TurnPolicy = (typeof TURN_POLICY)[keyof typeof TURN_POLICY];

const CLIENT_RECOVERABLE_CLOSE_CODE = 4000;
const BROWSER_RUNTIME_LOG_SOURCE = "browser_runtime";

export class BrowserRuntime {
    private readonly _clearTimer: (handle: TimerHandle) => void;
    private readonly _core: ProtocolCoreBindings;
    private readonly _turnQueue = new TurnQueue<TurnPolicy>((error) =>
        this.handleRuntimeError(error)
    );
    private readonly _pendingRequests = new PendingRequests(
        (getCommands) =>
            this._turnQueue.enqueueAndWait(
                (isCurrent) => this.processCommands(getCommands(), isCurrent),
                TURN_POLICY.DROP_ON_RECOVERY
            ),
        (id, ms) => this.scheduleTimer(id, ms)
    );
    private readonly _media = new RemoteMedia();
    public readonly consumers: ReadonlyMap<SessionId, ConsumersCompat> = this._media.consumers;

    private readonly _peerSession: PeerSession;
    private readonly _setTimer: (callback: () => void, ms: number) => TimerHandle;
    private readonly _socketSession: SocketSession;
    private readonly _context: BrowserRuntimeContext;

    private _timerHandles = new Map<number, TimerHandle>();
    private _availableFeatures: AvailableFeatures = { ...EMPTY_FEATURES };
    private _recordingState: SfuRecordingState = {};
    private _state: ConnectionState = SFU_CLIENT_STATE.DISCONNECTED;

    constructor(context: BrowserRuntimeContext, dependencies: SfuClientDependencies = {}) {
        this._context = context;
        this._core = (dependencies.createProtocolCore ?? createProtocolCore)();
        this._setTimer = dependencies.setTimer ?? ((callback, ms) => setTimeout(callback, ms));
        this._clearTimer = dependencies.clearTimer ?? ((handle) => clearTimeout(handle));
        const log = (level: ClientLogDetail["level"], message: string) => this.log(level, message);
        this._socketSession = new SocketSession(
            dependencies.createWebSocket ?? ((url) => new WebSocket(url) as ClientWebSocket),
            log,
            () => this.enqueueProtocolCommands(() => this._core.onWsOpen()),
            (frame) => this.enqueueProtocolCommands(() => this._core.onWsMessage(frame)),
            (code) => this.handleSocketClose(code)
        );
        this._peerSession = new PeerSession(
            dependencies.createPeerConnection ??
                ((config) => new RTCPeerConnection(config) as ClientPeerConnection),
            this._media,
            this._context.onUpdate,
            () => this.enqueueProtocolCommands(() => this.onTransportReady()),
            () => {
                log(
                    CLIENT_LOG_LEVEL.WARN,
                    "closing websocket because the peer connection transport failed"
                );
                this._socketSession.close(CLIENT_RECOVERABLE_CLOSE_CODE);
            },
            log
        );
    }

    get availableFeatures(): AvailableFeatures {
        return this._availableFeatures;
    }

    get recordingState(): SfuRecordingState {
        return this._recordingState;
    }

    get state(): ConnectionState {
        return this._state;
    }

    connect(url: string, jwt: string, room: string | null, iceServers?: RTCIceServer[]): void {
        this.enqueueProtocolCommands(() => {
            const commands = this._core.connect(url, jwt, room);
            if (commands.some((command) => command.kind === COMMAND_KIND.CONNECT)) {
                this._peerSession.clearPublications();
                this._media.clearSessionState();
                this._peerSession.setIceServers(iceServers);
            }
            return commands;
        });
    }

    disconnect(): void {
        // Let the active cleanup finish before disconnecting.
        if (this._turnQueue.hasControlTurn) {
            this._turnQueue.cancelPending();
            this.enqueueProtocolCommands(() => this._core.disconnect());
            return;
        }
        const commands = this.tryControlTransition(() => this._core.disconnect());
        if (!commands) {
            return;
        }
        this.interrupt(commands);
    }

    subscribe(sessionId: SessionId, states: DownloadStates): void {
        const snapshot = { ...states };
        this._turnQueue.enqueue((isCurrent) => {
            const commands = this._core.subscribe(sessionId, snapshot);
            if (!isCurrent()) {
                return;
            }
            this._media.updateSubscriptionStates(sessionId, snapshot, this._context.onUpdate);
            return this.processCommands(commands, isCurrent);
        }, TURN_POLICY.RETAIN_ON_RECOVERY);
    }

    updateInfo(info: SessionInfo): void {
        const snapshot = { ...info };
        this.enqueueProtocolCommands(
            () => this._core.updateInfo(snapshot),
            TURN_POLICY.RETAIN_ON_RECOVERY
        );
    }

    broadcast(message: JsonValue): void {
        try {
            const messageJson = JSON.stringify(message);
            if (messageJson === undefined) {
                throw new TypeError("broadcast message must be JSON serializable");
            }
            this.enqueueProtocolCommands(() => this._core.broadcast(messageJson));
        } catch (error) {
            this.handleRuntimeError(error);
        }
    }

    startRecording(options: RecordingOptions = {}): Promise<boolean> {
        const input = { ...options };
        return this._pendingRequests.drainRequestCommands(() => this._core.startRecording(input));
    }

    stopRecording(): Promise<boolean> {
        return this._pendingRequests.drainRequestCommands(() => this._core.stopRecording());
    }

    getStats(): Promise<SfuStats> {
        return this._peerSession.getStats();
    }

    publish(type: StreamType, track: MediaStreamTrack | null): void {
        this._turnQueue.enqueue(async (isCurrent) => {
            const { active, peerTask } = this._peerSession.setPublication(type, track, this._state);
            if (!isCurrent()) {
                return;
            }
            const commands = active === undefined ? undefined : this._core.publish(type, active);
            if (peerTask) {
                await peerTask();
            }
            if (commands) {
                return this.processCommands(commands, isCurrent);
            }
        }, TURN_POLICY.RETAIN_ON_RECOVERY);
    }

    private abortResources(): void {
        this._timerHandles.forEach((handle) => this._clearTimer(handle));
        this._timerHandles.clear();
        this._peerSession.close();
        this._peerSession.clearPublications();
        this._media.clearSessionState();
        this._socketSession.abort(CLIENT_RECOVERABLE_CLOSE_CODE);
    }

    private enqueueProtocolCommands(
        getCommands: () => HostCommand[],
        policy: TurnPolicy = TURN_POLICY.DROP_ON_RECOVERY
    ): void {
        this._turnQueue.enqueue(
            (isCurrent) => this.processCommands(getCommands(), isCurrent),
            policy
        );
    }

    private handleSocketClose(code: number): void {
        const commands = this.tryControlTransition(() => this._core.onWsClose(code));
        if (!commands?.length) {
            return;
        }
        this.interrupt(commands);
    }

    private tryControlTransition(getCommands: () => HostCommand[]): HostCommand[] | undefined {
        try {
            return getCommands();
        } catch (error) {
            this.handleRuntimeError(error);
            return undefined;
        }
    }

    private interrupt(commands: HostCommand[], error?: Error): void {
        for (const command of commands) {
            this.applyPublicStateCommand(command);
        }
        const recovering = this._state === SFU_CLIENT_STATE.RECOVERING;
        if (commands.length === 0 && this._turnQueue.hasControlTurn) {
            this._turnQueue.cancelPending(error);
            return;
        }
        this._turnQueue.interrupt(
            (isCurrent) => this.processCommands(commands, isCurrent),
            (policy) => recovering && policy === TURN_POLICY.RETAIN_ON_RECOVERY,
            error
        );
    }

    private async processCommands(pending: HostCommand[], isCurrent: TurnGuard): Promise<void> {
        if (!isCurrent()) {
            return;
        }
        this.applyPublicStateBeforeTeardown(pending);
        for (let index = 0; index < pending.length; index += 1) {
            if (!isCurrent()) {
                return;
            }
            const command = pending[index];
            const commandResult = this.executeCommand(command, isCurrent);
            const followUpCommands =
                commandResult instanceof Promise ? await commandResult : commandResult;
            if (!isCurrent()) {
                return;
            }
            this.applyPublicStateBeforeTeardown(followUpCommands);
            pending.push(...followUpCommands);
        }
    }

    private executeCommand(
        command: HostCommand,
        isCurrent: TurnGuard
    ): HostCommand[] | Promise<HostCommand[]> {
        this.applyPublicStateCommand(command);
        switch (command.kind) {
            case COMMAND_KIND.SEND_WEB_SOCKET:
                this._socketSession.send(command.frame);
                return [];
            case COMMAND_KIND.APPLY_NEGOTIATION:
                return this._peerSession
                    .negotiate(
                        command.requestId,
                        command.negotiationKind,
                        command.sdp,
                        command.uploadSlots,
                        isCurrent
                    )
                    .then((answer) => {
                        if (!answer || !isCurrent()) {
                            return [];
                        }
                        const commands = this._core.submitNegotiationAnswer(
                            command.requestId,
                            command.negotiationKind,
                            answer.answerSdp
                        );
                        if (answer.shouldSignalTransportReady) {
                            commands.push(...this.onTransportReady());
                        }
                        return commands;
                    });
            case COMMAND_KIND.CLOSE_PEER_CONNECTION:
                this._peerSession.close();
                return [];
            case COMMAND_KIND.CLOSE_WEB_SOCKET:
                this._socketSession.close(command.code);
                return [];
            case COMMAND_KIND.SET_AVAILABLE_FEATURES:
            case COMMAND_KIND.SET_RECORDING_STATE:
                return [];
            case COMMAND_KIND.EMIT_STATE_CHANGE:
                if (
                    command.state === SFU_CLIENT_STATE.CLOSED ||
                    command.state === SFU_CLIENT_STATE.DISCONNECTED
                ) {
                    this._peerSession.clearPublications();
                    this._media.clearSessionState();
                }
                this._context.onStateChange(command.state, command.cause);
                return [];
            case COMMAND_KIND.EMIT_UPDATE:
                if (command.update.name === REMOTE_MEDIA_UPDATE) {
                    this._media.replaceTrackBindings(
                        command.update.payload.bindings,
                        this._context.onUpdate
                    );
                    return [];
                }
                if (command.update.name === CLIENT_UPDATE.DISCONNECT) {
                    this._media.removeSession(command.update.payload.sessionId);
                }
                this._context.onUpdate(command.update);
                return [];
            case COMMAND_KIND.BEGIN_PENDING_REQUEST:
                throw new Error("beginPendingRequest is only valid for request command drains");
            case COMMAND_KIND.COMPLETE_PENDING_REQUEST:
                this.cancelTimer(command.timeoutTimerId);
                this._pendingRequests.resolve(command.requestId, command.ok);
                return [];
            case COMMAND_KIND.SCHEDULE_TIMER:
                this.scheduleTimer(command.id, command.ms);
                return [];
            case COMMAND_KIND.CANCEL_TIMER:
                this.cancelTimer(command.id);
                return [];
            case COMMAND_KIND.CONNECT:
                this._socketSession.open(command.url);
                return [];
        }
    }

    private applyPublicStateCommand(command: HostCommand): void {
        switch (command.kind) {
            case COMMAND_KIND.SET_AVAILABLE_FEATURES:
                this._availableFeatures = command.features;
                break;
            case COMMAND_KIND.SET_RECORDING_STATE:
                this._recordingState = command.state;
                break;
            case COMMAND_KIND.EMIT_STATE_CHANGE:
                this._state = command.state;
                break;
        }
    }

    private applyPublicStateBeforeTeardown(commands: HostCommand[]): void {
        if (!commands.some((command) => command.kind === COMMAND_KIND.CLOSE_PEER_CONNECTION)) {
            return;
        }
        // The lifecycle transition is committed before host teardown can re-enter through callbacks.
        for (const command of commands) {
            this.applyPublicStateCommand(command);
        }
    }

    private onTransportReady(): HostCommand[] {
        this._peerSession.resumePublications();
        return this._core.onTransportReady();
    }

    private cancelTimer(id: number): void {
        const handle = this._timerHandles.get(id);
        if (!handle) {
            return;
        }
        this._clearTimer(handle);
        this._timerHandles.delete(id);
    }

    private scheduleTimer(id: number, ms: number): void {
        this.cancelTimer(id);
        this._timerHandles.set(
            id,
            this._setTimer(() => this.enqueueProtocolCommands(() => this._core.onTimer(id)), ms)
        );
    }

    private handleRuntimeError(error: unknown): void {
        const resolvedError = error instanceof Error ? error : new Error(String(error));
        this._pendingRequests.rejectAll(resolvedError);
        let disconnectCommands: HostCommand[];
        let disconnectFailure: { error: unknown } | undefined;
        try {
            disconnectCommands = this._core.disconnect();
        } catch (disconnectError) {
            disconnectFailure = { error: disconnectError };
            // A failed core teardown cannot emit the commands that commit browser state.
            disconnectCommands = [
                {
                    kind: COMMAND_KIND.SET_AVAILABLE_FEATURES,
                    features: { ...EMPTY_FEATURES }
                },
                { kind: COMMAND_KIND.SET_RECORDING_STATE, state: {} },
                {
                    kind: COMMAND_KIND.EMIT_STATE_CHANGE,
                    state: SFU_CLIENT_STATE.DISCONNECTED
                }
            ];
        }
        this.interrupt(disconnectCommands, resolvedError);
        if (disconnectFailure) {
            this.log(
                CLIENT_LOG_LEVEL.ERROR,
                `protocol disconnect failed: ${String(disconnectFailure.error)}`
            );
        }
        this.abortResources();
        this._context.onRuntimeError(resolvedError);
    }

    private log(level: ClientLogDetail["level"], message: string): void {
        this._context.onLog({
            id: BROWSER_RUNTIME_LOG_SOURCE,
            level,
            message
        });
    }
}
