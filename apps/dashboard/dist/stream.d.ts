import { RunEvent, StreamFreshness, ResyncControlEvent } from "./types.js";
export interface StreamErrorBanner {
    message: string | null;
    isStreamError: boolean;
}
export declare function createStreamErrorBanner(): StreamErrorBanner;
export declare function markStreamError(banner: StreamErrorBanner, message: string): void;
export declare function markGlobalError(banner: StreamErrorBanner, message: string): void;
export declare function clearStreamErrorOnLive(banner: StreamErrorBanner): boolean;
export interface StreamClientOptions {
    baseUrl?: string;
    runId: string;
    initialSequence?: number;
    staleThresholdMs?: number;
    onEvent: (event: RunEvent) => void;
    onFreshnessChange: (freshness: StreamFreshness) => void;
    onResyncRequired: (resync: ResyncControlEvent) => void;
    onError?: (err: Error) => void;
}
export declare class RunEventStreamClient {
    private runId;
    private baseUrl;
    private lastProcessedSequence;
    private staleThresholdMs;
    private freshness;
    private onEvent;
    private onFreshnessChange;
    private onResyncRequired;
    private onError?;
    private abortController;
    private staleTimer;
    private reconnectTimer;
    private reconnectAttempts;
    private stopped;
    constructor(options: StreamClientOptions);
    getLastProcessedSequence(): number;
    getFreshness(): StreamFreshness;
    setLastProcessedSequence(seq: number): void;
    start(): void;
    stop(): void;
    private setFreshness;
    private clearTimers;
    private scheduleReconnect;
    private connect;
    processSSEBlock(block: string): void;
}
//# sourceMappingURL=stream.d.ts.map