import type { NegotiationUploadEncoding } from "../protocol_contract.js";
import type { StreamType } from "../public_api.js";
import type { PeerConnectionTransceiver } from "./browser_types.js";

const MIN_SIMULCAST_ENCODINGS = 2;

export async function applyUploadPublicationPolicy(
    streamType: StreamType,
    transceiver: PeerConnectionTransceiver,
    simulcastEncodings: readonly NegotiationUploadEncoding[]
): Promise<void> {
    if (streamType === "audio") {
        return;
    }
    if (simulcastEncodings.length < MIN_SIMULCAST_ENCODINGS) {
        return;
    }
    if (!transceiver.sender.getParameters || !transceiver.sender.setParameters) {
        return;
    }
    const parameters = transceiver.sender.getParameters();
    const previousEncodings = Array.isArray(parameters.encodings) ? parameters.encodings : [];
    const encodings: RTCRtpEncodingParameters[] = [];
    for (const [index, encoding] of simulcastEncodings.entries()) {
        if (!isValidSimulcastEncodingOffer(encoding)) {
            return;
        }
        encodings.push(buildSenderEncodingParameters(previousEncodings[index] ?? {}, encoding));
    }

    try {
        await transceiver.sender.setParameters({
            ...parameters,
            encodings
        });
    } catch {
        return;
    }
}

function buildSenderEncodingParameters(
    previousEncoding: RTCRtpEncodingParameters,
    encoding: NegotiationUploadEncoding
): RTCRtpEncodingParameters {
    const parameters: RTCRtpEncodingParameters = {
        ...previousEncoding,
        active: true,
        rid: encoding.rid
    };
    if (encoding.maxBitrate !== undefined) {
        parameters.maxBitrate = encoding.maxBitrate;
    }
    if (encoding.maxFramerate !== undefined) {
        parameters.maxFramerate = encoding.maxFramerate;
    }
    if (encoding.resolutionScale !== undefined) {
        parameters.scaleResolutionDownBy = encoding.resolutionScale;
    }
    return parameters;
}

function isValidSimulcastEncodingOffer(encoding: NegotiationUploadEncoding): boolean {
    return (
        typeof encoding.rid === "string" &&
        encoding.rid.length > 0 &&
        isOptionalPositiveInteger(encoding.maxBitrate) &&
        isOptionalPositiveInteger(encoding.maxFramerate) &&
        isOptionalPositiveNumber(encoding.resolutionScale)
    );
}

function isOptionalPositiveInteger(value: number | undefined): boolean {
    return value === undefined || (Number.isInteger(value) && value > 0);
}

function isOptionalPositiveNumber(value: number | undefined): boolean {
    return value === undefined || (Number.isFinite(value) && value > 0);
}
