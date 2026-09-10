import type { NegotiationUploadSlot } from "../protocol_contract.js";
import { STREAM_TYPES, type StreamType } from "../public_api.js";
import { STREAM_KIND, type ClientPeerConnection, type MediaTrack } from "./browser_types.js";
import { applyUploadPublicationPolicy } from "./publication_policy.js";
import { remoteDescriptionAcceptsUploadMid } from "./sdp_media_direction.js";

type UploadTransition = {
    hadTrack: boolean;
    hasTrack: boolean;
    boundMid?: string;
};

export type UploadSlot = Pick<NegotiationUploadSlot, "kind" | "mid" | "simulcastEncodings">;

export class LocalUploads {
    private _attachableTypes = new Set<StreamType>();
    private _localTracks = new Map<StreamType, MediaTrack>();
    private _peerGeneration = 0;
    private _senderMidByType = new Map<StreamType, string>();

    setPublication(
        type: StreamType,
        track: MediaStreamTrack | null,
        canAttach: boolean
    ): UploadTransition {
        const hadTrack = this._localTracks.has(type);
        const boundMid = this._senderMidByType.get(type);
        this._attachableTypes.delete(type);
        if (track) {
            this._localTracks.set(type, track);
            if (canAttach && boundMid === undefined) {
                this._attachableTypes.add(type);
            }
        } else {
            this._localTracks.delete(type);
        }
        return {
            hadTrack,
            hasTrack: track !== null,
            boundMid
        };
    }

    resumePublications(): void {
        for (const type of STREAM_TYPES) {
            if (this.hasPendingPublication(type)) {
                this._attachableTypes.add(type);
            }
        }
    }

    clearPeerConnectionState(): void {
        this._peerGeneration += 1;
        this._senderMidByType.clear();
        this._attachableTypes.clear();
    }

    clearPublications(): void {
        this._localTracks.clear();
        this._attachableTypes.clear();
    }

    boundMidFor(streamType: StreamType): string | undefined {
        return this._senderMidByType.get(streamType);
    }

    async attachTrack(
        peerConnection: ClientPeerConnection | null,
        mid: string,
        streamType: StreamType,
        uploadSlot?: UploadSlot
    ): Promise<void> {
        if (!peerConnection) {
            throw new Error("cannot attach track without an active peer connection");
        }
        const generation = this._peerGeneration;
        const track = this._localTracks.get(streamType) ?? null;
        const transceiver = peerConnection
            .getTransceivers()
            .find((candidate) => candidate.mid === mid);
        if (!transceiver) {
            throw new Error(`missing transceiver for mid ${mid}`);
        }
        await transceiver.sender.replaceTrack(track);
        if (generation !== this._peerGeneration) {
            return;
        }
        if (track) {
            enableTransceiverSend(transceiver);
            await applyUploadPublicationPolicy(
                streamType,
                transceiver,
                uploadSlot?.simulcastEncodings ?? []
            );
            if (generation !== this._peerGeneration) {
                return;
            }
        }
        this._senderMidByType.set(streamType, mid);
        this._attachableTypes.delete(streamType);
    }

    async detachTrack(
        peerConnection: ClientPeerConnection | null,
        streamType: StreamType
    ): Promise<void> {
        if (!peerConnection) {
            return;
        }
        const boundMid = this._senderMidByType.get(streamType);
        if (!boundMid) {
            return;
        }
        const transceiver = peerConnection
            .getTransceivers()
            .find((candidate) => candidate.mid === boundMid);
        if (transceiver) {
            await transceiver.sender.replaceTrack(null);
        }
    }

    async attachPendingTracks(
        peerConnection: ClientPeerConnection,
        uploadSlots: UploadSlot[],
        offerSdp: string
    ): Promise<void> {
        const generation = this._peerGeneration;
        await this.reconcileSenderBindings(peerConnection, offerSdp);
        if (generation !== this._peerGeneration) {
            return;
        }
        const pendingStreamTypes = STREAM_TYPES.filter(
            (streamType) =>
                this.hasPendingPublication(streamType) && this._attachableTypes.has(streamType)
        );
        if (pendingStreamTypes.length === 0) {
            return;
        }
        const boundMids = new Set(this._senderMidByType.values());
        const transceivers = peerConnection.getTransceivers();
        const remainingSlots = uploadSlots.filter((slot) => {
            const transceiver = transceivers.find((candidate) => candidate.mid === slot.mid);
            return (
                slot.mid.length > 0 &&
                !boundMids.has(slot.mid) &&
                transceiver !== undefined &&
                // when the sfu offers an upload slot (a=recvonly), the browser
                // sets the local transceiver to recvonly until we flip it to sendonly.
                // we also check that it's unnegotiated (currentDirection === null)
                // to be sure we only touch fresh or reset slots
                transceiver.direction === "recvonly" &&
                transceiver.currentDirection === null &&
                transceiver.sender.track == null
            );
        });
        for (const streamType of pendingStreamTypes) {
            const slotIndex = remainingSlots.findIndex(
                (slot) => slot.kind === STREAM_KIND[streamType]
            );
            if (slotIndex < 0) {
                continue;
            }
            const [slot] = remainingSlots.splice(slotIndex, 1);
            await this.attachTrack(peerConnection, slot.mid, streamType, slot);
            if (generation !== this._peerGeneration) {
                return;
            }
        }
    }

    private async reconcileSenderBindings(
        peerConnection: ClientPeerConnection,
        offerSdp: string
    ): Promise<void> {
        const generation = this._peerGeneration;
        for (const [streamType, mid] of this._senderMidByType) {
            if (remoteDescriptionAcceptsUploadMid(offerSdp, mid)) {
                continue;
            }
            const transceiver = peerConnection
                .getTransceivers()
                .find((candidate) => candidate.mid === mid);
            if (transceiver?.sender.track) {
                await transceiver.sender.replaceTrack(null);
                if (generation !== this._peerGeneration) {
                    return;
                }
            }
            this._senderMidByType.delete(streamType);
            if (this._localTracks.has(streamType)) {
                this._attachableTypes.add(streamType);
            }
        }
    }

    private hasPendingPublication(streamType: StreamType): boolean {
        return this._localTracks.has(streamType) && !this._senderMidByType.has(streamType);
    }
}

function enableTransceiverSend(
    transceiver: ReturnType<ClientPeerConnection["getTransceivers"]>[number]
): void {
    const direction = transceiver.direction;
    if (direction === "recvonly" || direction === "inactive") {
        transceiver.direction = "sendonly";
    }
}
