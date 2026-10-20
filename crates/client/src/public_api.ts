export type SessionId = number | string;

export type JsonValue =
    string | number | boolean | null | { [key: string]: JsonValue } | JsonValue[];

export const SFU_CLIENT_STATE = {
    DISCONNECTED: "disconnected",
    CONNECTING: "connecting",
    AUTHENTICATED: "authenticated",
    CONNECTED: "connected",
    RECOVERING: "recovering",
    CLOSED: "closed"
} as const;

export type SFU_CLIENT_STATE = typeof SFU_CLIENT_STATE;

export type ConnectionState = (typeof SFU_CLIENT_STATE)[keyof typeof SFU_CLIENT_STATE];

export const STREAM_TYPES = ["audio", "camera", "screen"] as const;

export type StreamType = (typeof STREAM_TYPES)[number];

export const VIDEO_LAYOUT_INTENTS = [
    "featured",
    "pinned",
    "visible_thumbnail",
    "hidden",
    "overflow"
] as const;

export type VideoLayoutIntent = (typeof VIDEO_LAYOUT_INTENTS)[number];

export interface ConnectOptions {
    channelUUID?: string;
    iceServers?: RTCIceServer[];
}

export interface DownloadStates {
    audio?: boolean;
    camera?: boolean;
    screen?: boolean;
    cameraLayout?: VideoLayoutIntent;
    screenLayout?: VideoLayoutIntent;
}

export interface SessionInfo {
    isTalking?: boolean;
    isFeatured?: boolean;
    isCameraOn?: boolean;
    isScreenSharingOn?: boolean;
    isSelfMuted?: boolean;
    isDeaf?: boolean;
    isRaisingHand?: boolean;
}

export interface UpdateInfoOptions {
    /**
     * @deprecated The welcome payload already carries the current peer snapshot,
     * so the browser shell keeps this legacy flag as a compatibility no-op.
     */
    needRefresh?: boolean;
}

export interface RecordingFlags {
    audio: boolean;
    video: boolean;
    transcription: boolean;
}

export interface AvailableFeatures {
    rtc: boolean;
    recording: RecordingFlags;
}

/** @deprecated Use RecordingFlags. */
export type SfuRecordingState = RecordingFlags;

/** @deprecated Use Partial<RecordingFlags>. */
export type RecordingOptions = Partial<RecordingFlags>;

export const CLIENT_LOG_LEVEL = {
    DEBUG: "debug",
    INFO: "info",
    WARN: "warn",
    ERROR: "error"
} as const;

export type ClientLogLevel = (typeof CLIENT_LOG_LEVEL)[keyof typeof CLIENT_LOG_LEVEL];

export interface ClientLogDetail {
    id: string;
    level: ClientLogLevel;
    message: string;
}

export interface SfuStats {
    uploadStats?: RTCStatsReport;
    downloadStats?: RTCStatsReport;
    audio?: RTCStatsReport;
    camera?: RTCStatsReport;
    screen?: RTCStatsReport;
}

export const RECORDING_STOP_CODES = [
    "user_request",
    "channel_closed",
    "recording_timeout",
    "recording_failed",
    "disk_space_exhausted"
] as const;

export type RecordingStopCode = (typeof RECORDING_STOP_CODES)[number];

export const CLIENT_UPDATE = {
    TRACK: "track",
    DISCONNECT: "disconnect",
    INFO_CHANGE: "info_change",
    BROADCAST: "broadcast",
    CHANNEL_INFO_CHANGE: "channel_info_change"
} as const;

export type SfuClientState = ConnectionState;

export type ClientUpdateName = (typeof CLIENT_UPDATE)[keyof typeof CLIENT_UPDATE];

export interface TrackUpdateDetail {
    sessionId: SessionId;
    type: StreamType;
    track: MediaStreamTrack;
    active: boolean;
}

export interface DisconnectUpdateDetail {
    sessionId: SessionId;
}

export type InfoChangeUpdateDetail = Record<string, SessionInfo>;

export interface BroadcastUpdateDetail {
    senderId: SessionId;
    message: JsonValue;
}

export interface ChannelInfoChangeDetail {
    state: RecordingFlags;
    stopCode?: RecordingStopCode;
}

export type ClientUpdateDetail =
    | { name: typeof CLIENT_UPDATE.TRACK; payload: TrackUpdateDetail }
    | { name: typeof CLIENT_UPDATE.DISCONNECT; payload: DisconnectUpdateDetail }
    | { name: typeof CLIENT_UPDATE.INFO_CHANGE; payload: InfoChangeUpdateDetail }
    | { name: typeof CLIENT_UPDATE.BROADCAST; payload: BroadcastUpdateDetail }
    | {
          name: typeof CLIENT_UPDATE.CHANNEL_INFO_CHANGE;
          payload: ChannelInfoChangeDetail;
      };

export interface StateChangeDetail {
    state: ConnectionState;
    cause?: string;
}

export interface HandledErrorDetail {
    error: Error;
}

export interface ConsumerCompat {
    closed?: boolean;
    paused?: boolean;
    track: MediaStreamTrack | null;
}

export interface ConsumersCompat {
    audio: ConsumerCompat | null;
    camera: ConsumerCompat | null;
    screen: ConsumerCompat | null;
}

/** Event names and payloads emitted by {@link SfuClient}. */
export interface SfuClientEventMap {
    handledError: CustomEvent<HandledErrorDetail>;
    log: CustomEvent<ClientLogDetail>;
    stateChange: CustomEvent<StateChangeDetail>;
    update: CustomEvent<ClientUpdateDetail>;
}
