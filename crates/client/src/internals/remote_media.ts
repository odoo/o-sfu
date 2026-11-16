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
// The server gives the MID of a removed track to a later one. The transceiver
// keeps one receiver track for all of them and the browser fires no `track`
// event unless the section was inactive in between, so a slot keeps it when
// its binding leaves.
//
// A caller gets a clone, never the receiver track: `stop()` ends a track for
// good and callers stop the tracks of a session that left. The clone stops
// with its binding, so the previous owner never plays the next one's media.
type RemoteMediaSlot = {
    binding?: SlotBinding;
    receiverTrack?: MediaTrack;
    /** Clone of `receiverTrack` the caller holds for `binding`. */
    track?: MediaTrack;
    /** Removes the mute and unmute listeners of `track`. */
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
            this.stopTrack(slot);
        }
        this._slots.clear();
    }

    replaceTrackBindings(bindings: TrackBinding[], emitUpdate: TrackUpdateEmitter): void {
        const nextBindings = new Map<string, TrackBinding>();
        for (const binding of bindings) {
            nextBindings.set(binding.mid, binding);
        }

        // Clear old owners before publishing replacements: a consumer can move
        // between mids, and late cleanup would delete its new entry.
        for (const [mid, slot] of this._slots) {
            const next = nextBindings.get(mid);
            if (
                !next ||
                (slot.binding &&
                    (slot.binding.sessionId !== next.sessionId || slot.binding.type !== next.type))
            ) {
                this.clearBinding(slot);
            }
        }

        for (const [mid, binding] of nextBindings) {
            this.applyBinding(mid, binding, emitUpdate);
        }
    }

    removeSession(sessionId: SessionId): void {
        this.consumers.delete(sessionId);
        for (const slot of this._slots.values()) {
            if (slot.binding?.sessionId === sessionId) {
                this.clearBinding(slot);
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
        // A section that sends again repeats the event with the same track.
        if (slot.receiverTrack !== event.track) {
            this.stopTrack(slot);
            slot.receiverTrack = event.track;
        }
        this.projectTrackSlot(slot, previousActive, emitUpdate);
    }

    private applyBinding(mid: string, binding: TrackBinding, emitUpdate: TrackUpdateEmitter): void {
        const slot = this.getOrCreateSlot(mid);
        const { active, sessionId, type } = binding;
        const previousActive = slot.binding ? this.effectiveActive(slot.binding) : undefined;
        slot.binding = { active, sessionId, type };
        this.projectTrackSlot(slot, previousActive, emitUpdate);
    }

    private projectTrackSlot(
        slot: RemoteMediaSlot,
        previousActive: boolean | undefined,
        emitUpdate: TrackUpdateEmitter,
        forceEmit = false
    ): void {
        const { binding, receiverTrack } = slot;
        if (!binding || !receiverTrack) {
            return;
        }
        const track = (slot.track ??= this.cloneTrack(slot, receiverTrack, emitUpdate));
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

    /** Clones `receiverTrack` for the binding of `slot`. */
    private cloneTrack(
        slot: RemoteMediaSlot,
        receiverTrack: MediaTrack,
        emitUpdate: TrackUpdateEmitter
    ): MediaTrack {
        const track = receiverTrack.clone();
        // The browser changes `muted` on each track of a source in turn, so
        // only the clone's own events tell when the caller's track changed.
        const emitTrackUpdate = () => {
            if (slot.binding) {
                const previousActive = this.effectiveActive(slot.binding);
                this.projectTrackSlot(slot, previousActive, emitUpdate, true);
            }
        };
        track.addEventListener("mute", emitTrackUpdate);
        track.addEventListener("unmute", emitTrackUpdate);
        slot.removeTrackListeners = () => {
            track.removeEventListener("mute", emitTrackUpdate);
            track.removeEventListener("unmute", emitTrackUpdate);
        };
        return track;
    }

    private effectiveActive(binding: SlotBinding): boolean {
        return this.isActive(binding, this._subscriptionStates.get(binding.sessionId));
    }

    private isActive(binding: SlotBinding, states: DownloadStates | undefined): boolean {
        return binding.active && (states?.[binding.type] ?? true);
    }

    private clearBinding(slot: RemoteMediaSlot): void {
        if (slot.binding) {
            this.clearConsumer(slot.binding.sessionId, slot.binding.type);
        }
        slot.binding = undefined;
        this.stopTrack(slot);
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

    private stopTrack(slot: RemoteMediaSlot): void {
        slot.removeTrackListeners?.();
        slot.removeTrackListeners = undefined;
        slot.track?.stop();
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
