import type { TrackBinding } from "../protocol_contract.js";
import {
    CLIENT_UPDATE,
    type ClientUpdateDetail,
    type ConsumersCompat,
    type DownloadStates,
    type SessionId,
    type StreamType
} from "../public_api.js";
import type { MediaTrack, PeerConnectionTrackEvent } from "./browser_types.js";
import { mergeDownloadStates } from "./validation.js";

type TrackUpdateEmitter = (update: ClientUpdateDetail) => void;

type SlotBinding = Pick<TrackBinding, "active" | "sessionId" | "type">;
type RemoteMediaSlot = {
    binding?: SlotBinding;
    track?: MediaTrack;
    removeTrackListeners?: () => void;
};

export class RemoteMedia {
    public readonly consumers = new Map<SessionId, ConsumersCompat>();

    private _slots = new Map<string, RemoteMediaSlot>();
    private _subscriptionStates = new Map<SessionId, DownloadStates>();

    clearSessionState(): void {
        this.clearPeerMedia();
        this._subscriptionStates.clear();
    }

    clearPeerMedia(): void {
        this.consumers.clear();
        for (const slot of this._slots.values()) {
            this.clearSlotTrack(slot);
        }
        this._slots.clear();
    }

    replaceTrackBindings(bindings: TrackBinding[], emitUpdate: TrackUpdateEmitter): void {
        const nextBindings = new Map<string, TrackBinding>();
        for (const binding of bindings) {
            nextBindings.set(binding.mid, binding);
        }

        for (const [mid, slot] of this._slots) {
            if (slot.binding && !nextBindings.has(mid)) {
                this.removeSlot(mid);
            }
        }

        for (const [mid, binding] of nextBindings) {
            this.applyBinding(mid, binding, emitUpdate);
        }
    }

    removeSession(sessionId: SessionId): void {
        this.consumers.delete(sessionId);
        for (const [mid, slot] of this._slots) {
            if (slot.binding?.sessionId === sessionId) {
                this.removeSlot(mid);
            }
        }
    }

    updateSubscriptionStates(
        sessionId: SessionId,
        states: DownloadStates,
        emitUpdate: TrackUpdateEmitter
    ): void {
        const previousStates = this._subscriptionStates.get(sessionId);
        const nextStates = mergeDownloadStates(previousStates, states);
        if (Object.keys(nextStates).length === 0) {
            this._subscriptionStates.delete(sessionId);
        } else {
            this._subscriptionStates.set(sessionId, nextStates);
        }
        for (const slot of this._slots.values()) {
            if (slot.binding?.sessionId !== sessionId) {
                continue;
            }
            const previousActive = this.isActive(slot.binding, previousStates);
            this.projectTrackSlot(slot, previousActive, emitUpdate);
        }
    }

    handleTrackEvent(event: PeerConnectionTrackEvent, emitUpdate: TrackUpdateEmitter): void {
        const mid = event.transceiver.mid;
        if (!mid) {
            return;
        }
        const slot = this.getOrCreateSlot(mid);
        const previousActive = slot.binding ? this.effectiveActive(slot.binding) : undefined;
        this.clearSlotTrack(slot);
        slot.track = event.track;
        this.attachTrackListeners(slot, mid, event.track, emitUpdate);
        this.projectTrackSlot(slot, previousActive, emitUpdate);
    }

    private applyBinding(mid: string, binding: TrackBinding, emitUpdate: TrackUpdateEmitter): void {
        const slot = this.getOrCreateSlot(mid);
        const previousBinding = slot.binding;
        const previousActive = previousBinding ? this.effectiveActive(previousBinding) : undefined;
        const { active, sessionId, type } = binding;
        const rebinding =
            previousBinding !== undefined &&
            (previousBinding.sessionId !== sessionId || previousBinding.type !== type);
        if (rebinding) {
            this.clearConsumer(previousBinding.sessionId, previousBinding.type);
            this.clearSlotTrack(slot);
        }
        slot.binding = { active, sessionId, type };
        if (!rebinding) {
            this.projectTrackSlot(slot, previousActive, emitUpdate);
        }
    }

    private projectTrackSlot(
        slot: RemoteMediaSlot,
        previousActive: boolean | undefined,
        emitUpdate: TrackUpdateEmitter,
        forceEmit = false
    ): void {
        const { binding, track } = slot;
        if (!binding || !track) {
            return;
        }
        const active = this.effectiveActive(binding);
        if (
            !forceEmit &&
            previousActive === active &&
            this.consumers.get(binding.sessionId)?.[binding.type]?.track === track
        ) {
            return;
        }
        if (previousActive !== undefined) {
            this.clearConsumer(binding.sessionId, binding.type);
        }
        const consumers: ConsumersCompat = this.consumers.get(binding.sessionId) ?? {
            audio: null,
            camera: null,
            screen: null
        };
        consumers[binding.type] = { track };
        this.consumers.set(binding.sessionId, consumers);
        emitUpdate({
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active,
                sessionId: binding.sessionId,
                track,
                type: binding.type
            }
        });
    }

    private attachTrackListeners(
        currentSlot: RemoteMediaSlot,
        mid: string,
        track: MediaTrack,
        emitUpdate: TrackUpdateEmitter
    ): void {
        if (!("addEventListener" in track) || typeof track.addEventListener !== "function") {
            return;
        }
        const emitTrackUpdate = () => {
            const slot = this._slots.get(mid);
            if (!slot?.binding || slot.track !== track) {
                return;
            }
            const previousActive = this.effectiveActive(slot.binding);
            this.projectTrackSlot(slot, previousActive, emitUpdate, true);
        };
        track.addEventListener("mute", emitTrackUpdate);
        track.addEventListener("unmute", emitTrackUpdate);
        currentSlot.removeTrackListeners = () => {
            track.removeEventListener("mute", emitTrackUpdate);
            track.removeEventListener("unmute", emitTrackUpdate);
        };
    }

    private effectiveActive(binding: SlotBinding): boolean {
        return this.isActive(binding, this._subscriptionStates.get(binding.sessionId));
    }

    private isActive(binding: SlotBinding, states: DownloadStates | undefined): boolean {
        return binding.active && (states?.[binding.type] ?? true);
    }

    private removeSlot(mid: string): void {
        const slot = this._slots.get(mid);
        if (!slot) {
            return;
        }
        if (slot.binding) {
            this.clearConsumer(slot.binding.sessionId, slot.binding.type);
        }
        this.clearSlotTrack(slot);
        this._slots.delete(mid);
    }

    private getOrCreateSlot(mid: string): RemoteMediaSlot {
        let slot = this._slots.get(mid);
        if (slot) {
            return slot;
        }
        slot = {};
        this._slots.set(mid, slot);
        return slot;
    }

    private clearSlotTrack(slot: RemoteMediaSlot): void {
        slot.removeTrackListeners?.();
        slot.removeTrackListeners = undefined;
        slot.track = undefined;
    }

    private clearConsumer(sessionId: SessionId, streamType: StreamType): void {
        const consumers = this.consumers.get(sessionId);
        if (!consumers) {
            return;
        }
        consumers[streamType] = null;
        if (!consumers.audio && !consumers.camera && !consumers.screen) {
            this.consumers.delete(sessionId);
        }
    }
}
