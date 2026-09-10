import { configureDefaultWasmProtocolCoreProvider } from "../src/protocol_contract.js";
import { ProtocolCoreWasm, initSync } from "../generated/o_sfu_protocol.js";
import wasmModule from "../generated/o_sfu_protocol_bg.wasm";

let protocolModuleInitialized = false;

configureDefaultWasmProtocolCoreProvider(() => {
    if (!protocolModuleInitialized) {
        initSync({ module: wasmModule });
        protocolModuleInitialized = true;
    }
    return new ProtocolCoreWasm();
});

export { CLIENT_UPDATE, SFU_CLIENT_STATE } from "../src/public_api.js";
export type {
    AvailableFeatures,
    BroadcastUpdateDetail,
    ChannelInfoChangeDetail,
    ClientLogDetail,
    ClientLogLevel,
    ClientUpdateDetail,
    ClientUpdateName,
    ConnectOptions,
    ConnectionState,
    ConsumerCompat,
    ConsumersCompat,
    DisconnectUpdateDetail,
    DownloadStates,
    HandledErrorDetail,
    InfoChangeUpdateDetail,
    JsonValue,
    RecordingOptions,
    RecordingStopCode,
    SessionId,
    SessionInfo,
    SfuClientEventMap,
    SfuClientState,
    SfuRecordingState,
    SfuStats,
    StateChangeDetail,
    StreamType,
    TrackUpdateDetail,
    UpdateInfoOptions,
    VideoLayoutIntent
} from "../src/public_api.js";
export { SfuClient } from "../src/sfu_client.js";

/** Build identity for the released client artifact. */
export interface BundleInfo {
    date: string;
    hash: string;
    url: string;
    version: string;
}

/** Metadata for the source revision that produced this bundle. */
export declare const __info__: BundleInfo;
