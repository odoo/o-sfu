import type { StreamType } from "../public_api.js";
import type { ProtocolCoreProvider } from "../protocol_contract.js";

export type MediaTrack = MediaStreamTrack;

export type TimerHandle = ReturnType<typeof globalThis.setTimeout>;

export interface ClientWebSocket {
    close(code?: number): void;
    onclose: ((event: { code: number }) => void) | null;
    onerror: ((event: Event) => void) | null;
    onmessage: ((event: { data: unknown }) => void) | null;
    onopen: ((event: Event) => void) | null;
    readonly readyState: number;
    send(data: string): void;
}

export interface PeerConnectionSender {
    getStats?(): Promise<RTCStatsReport>;
    getParameters?(): RTCRtpSendParameters;
    replaceTrack(track: MediaTrack | null): Promise<void>;
    setParameters?(parameters: RTCRtpSendParameters): Promise<void>;
    track?: MediaTrack | null;
}

export type PeerConnectionTransceiverDirection = "sendrecv" | "sendonly" | "recvonly" | "inactive";

export interface PeerConnectionTransceiver {
    mid: string | null;
    currentDirection?: PeerConnectionTransceiverDirection | null;
    direction?: PeerConnectionTransceiverDirection;
    receiver?: {
        track?: MediaTrack | null;
    };
    sender: PeerConnectionSender;
}

export interface PeerConnectionTrackEvent {
    track: MediaTrack;
    transceiver: {
        mid: string | null;
    };
}

export type ClientPeerConnectionState =
    "new" | "connecting" | "connected" | "disconnected" | "failed" | "closed";

export interface ClientPeerConnection {
    close(): void;
    connectionState?: ClientPeerConnectionState;
    createAnswer(): Promise<{ sdp: string; type: "answer" }>;
    getStats?(): Promise<RTCStatsReport>;
    getTransceivers(): PeerConnectionTransceiver[];
    localDescription?: { sdp: string; type: "answer" } | null;
    onconnectionstatechange: (() => void) | null;
    onicecandidateerror: ((event: RTCPeerConnectionIceErrorEvent) => void) | null;
    ontrack: ((event: PeerConnectionTrackEvent) => void) | null;
    setLocalDescription(description: { sdp: string; type: "answer" }): Promise<void>;
    setRemoteDescription(description: { sdp: string; type: "offer" }): Promise<void>;
}

export interface SfuClientDependencies {
    clearTimer?: (handle: TimerHandle) => void;
    createPeerConnection?: (config: RTCConfiguration) => ClientPeerConnection;
    createProtocolCore?: ProtocolCoreProvider;
    createWebSocket?: (url: string) => ClientWebSocket;
    setTimer?: (callback: () => void, ms: number) => TimerHandle;
}

export const EMPTY_FEATURES = {
    rtc: false,
    transcription: false,
    audioRecording: false,
    videoRecording: false
};

export const STREAM_KIND: Record<StreamType, "audio" | "video"> = {
    audio: "audio",
    camera: "video",
    screen: "video"
};
