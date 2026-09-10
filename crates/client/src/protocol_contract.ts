import type {
    AvailableFeatures,
    ClientUpdateDetail,
    ConnectionState,
    DownloadStates,
    RecordingOptions,
    SessionId,
    SessionInfo,
    SfuRecordingState,
    StreamType
} from "./public_api.js";

export const UPLOAD_KINDS = ["audio", "video"] as const;

type MediaKind = (typeof UPLOAD_KINDS)[number];
export type NegotiationUploadEncoding = {
    rid: string;
    maxBitrate?: number;
    resolutionScale?: number;
    maxFramerate?: number;
};

export type NegotiationUploadSlot = {
    mid: string;
    kind: MediaKind;
    codecs?: string[];
    simulcastEncodings?: readonly NegotiationUploadEncoding[];
};

export type TrackBinding = {
    mid: string;
    sessionId: SessionId;
    type: StreamType;
    active: boolean;
};

export const NEGOTIATION_KIND = {
    OFFER: "offer",
    RENEGOTIATE: "renegotiate"
} as const;

export type NegotiationKind = (typeof NEGOTIATION_KIND)[keyof typeof NEGOTIATION_KIND];

export const COMMAND_KIND = {
    CONNECT: "connect",
    SEND_WEB_SOCKET: "sendWebSocket",
    CLOSE_WEB_SOCKET: "closeWebSocket",
    APPLY_NEGOTIATION: "applyNegotiation",
    CLOSE_PEER_CONNECTION: "closePeerConnection",
    SET_AVAILABLE_FEATURES: "setAvailableFeatures",
    SET_RECORDING_STATE: "setRecordingState",
    EMIT_STATE_CHANGE: "emitStateChange",
    EMIT_UPDATE: "emitUpdate",
    BEGIN_PENDING_REQUEST: "beginPendingRequest",
    COMPLETE_PENDING_REQUEST: "completePendingRequest",
    SCHEDULE_TIMER: "scheduleTimer",
    CANCEL_TIMER: "cancelTimer"
} as const;

export const WS_CLOSE_CODE = {
    CLEAN: 1000,
    LEAVING: 1001,
    PROTOCOL_ERROR: 1002,
    ERROR: 1011,
    AUTH_FAILED: 4106,
    AUTH_TIMEOUT: 4107,
    KICKED: 4108,
    CHANNEL_FULL: 4109
} as const;

export const REMOTE_MEDIA_UPDATE = "remote_media";

type RemoteMediaUpdate = {
    name: typeof REMOTE_MEDIA_UPDATE;
    payload: { bindings: TrackBinding[] };
};

type HostUpdate = ClientUpdateDetail | RemoteMediaUpdate;

export type PendingRequest = {
    requestId: string;
    timeoutTimerId: number;
    timeoutMs: number;
};

export type HostCommand =
    | { kind: typeof COMMAND_KIND.SEND_WEB_SOCKET; frame: string }
    | {
          kind: typeof COMMAND_KIND.APPLY_NEGOTIATION;
          requestId: string;
          negotiationKind: NegotiationKind;
          sdp: string;
          uploadSlots: NegotiationUploadSlot[];
      }
    | { kind: typeof COMMAND_KIND.CLOSE_PEER_CONNECTION }
    | { kind: typeof COMMAND_KIND.CLOSE_WEB_SOCKET; code: number }
    | { kind: typeof COMMAND_KIND.SET_AVAILABLE_FEATURES; features: AvailableFeatures }
    | { kind: typeof COMMAND_KIND.SET_RECORDING_STATE; state: SfuRecordingState }
    | { kind: typeof COMMAND_KIND.EMIT_STATE_CHANGE; state: ConnectionState; cause?: string }
    | { kind: typeof COMMAND_KIND.EMIT_UPDATE; update: HostUpdate }
    | { kind: typeof COMMAND_KIND.BEGIN_PENDING_REQUEST; request: PendingRequest }
    | {
          kind: typeof COMMAND_KIND.COMPLETE_PENDING_REQUEST;
          requestId: string;
          timeoutTimerId: number;
          ok: boolean;
      }
    | { kind: typeof COMMAND_KIND.SCHEDULE_TIMER; id: number; ms: number }
    | { kind: typeof COMMAND_KIND.CANCEL_TIMER; id: number }
    | { kind: typeof COMMAND_KIND.CONNECT; url: string };

export interface ProtocolCoreBindings {
    connect(url: string, jwt: string, room?: string | null): HostCommand[];
    onWsOpen(): HostCommand[];
    onWsMessage(frame: string): HostCommand[];
    onTransportReady(): HostCommand[];
    onWsClose(code: number): HostCommand[];
    onTimer(timerId: number): HostCommand[];
    publish(type: StreamType, active: boolean): HostCommand[];
    subscribe(sessionId: SessionId, states: DownloadStates): HostCommand[];
    updateInfo(info: SessionInfo): HostCommand[];
    broadcast(messageJson: string): HostCommand[];
    startRecording(options?: RecordingOptions): HostCommand[];
    stopRecording(): HostCommand[];
    submitNegotiationAnswer(
        requestId: string,
        negotiationKind: NegotiationKind,
        sdp: string
    ): HostCommand[];
    disconnect(): HostCommand[];
}

export type ProtocolCoreProvider = () => ProtocolCoreBindings;

let defaultWasmProtocolCoreProvider: ProtocolCoreProvider | undefined;

export function configureDefaultWasmProtocolCoreProvider(provider: ProtocolCoreProvider): void {
    defaultWasmProtocolCoreProvider = provider;
}

export function createProtocolCore(): ProtocolCoreBindings {
    if (!defaultWasmProtocolCoreProvider) {
        throw new Error("default WASM protocol core provider is not configured");
    }
    return defaultWasmProtocolCoreProvider();
}
