import { COMMAND_KIND, type HostCommand, type PendingRequest } from "../protocol_contract.js";

type PendingRequestCallbacks = {
    reject: (error: Error) => void;
    resolve: (ok: boolean) => void;
};

export class PendingRequests {
    private _pendingRequestResolvers = new Map<string, PendingRequestCallbacks>();

    constructor(
        private readonly _enqueueRequest: (getCommands: () => HostCommand[]) => Promise<void>,
        private readonly _scheduleTimer: (timeoutTimerId: number, timeoutMs: number) => void
    ) {}

    drainRequestCommands(getCommands: () => HostCommand[]): Promise<boolean> {
        let completion: Promise<boolean> | undefined;
        return this._enqueueRequest(() => {
            const commands = getCommands();
            const begin = commands[0];
            if (begin?.kind !== COMMAND_KIND.BEGIN_PENDING_REQUEST) {
                return commands;
            }
            completion = this.register(begin.request);
            void completion.catch(() => undefined);
            this._scheduleTimer(begin.request.timeoutTimerId, begin.request.timeoutMs);
            return commands.slice(1);
        }).then(
            () => completion ?? false,
            (error) => completion ?? Promise.reject(error)
        );
    }

    private register(request: PendingRequest): Promise<boolean> {
        if (this._pendingRequestResolvers.has(request.requestId)) {
            throw new Error(`pending request ${request.requestId} is already registered`);
        }
        return new Promise<boolean>((resolve, reject) => {
            this._pendingRequestResolvers.set(request.requestId, { resolve, reject });
        });
    }

    resolve(requestId: string, ok: boolean): void {
        const callbacks = this._pendingRequestResolvers.get(requestId);
        this._pendingRequestResolvers.delete(requestId);
        callbacks?.resolve(ok);
    }

    rejectAll(error: Error): void {
        for (const callbacks of this._pendingRequestResolvers.values()) {
            callbacks.reject(error);
        }
        this._pendingRequestResolvers.clear();
    }
}
