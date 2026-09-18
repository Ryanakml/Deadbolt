import { RunSnapshot, RunEvent, RunEventsResponse, StepStatus, TaskLogsResponse, StreamFreshness } from "./types.js";
export declare function shouldShowWorkerWait(stepStatus: StepStatus, waitingReason: string | null | undefined, activeCompatibleWorkers: number | undefined): boolean;
export declare function terminalStepEmptyText(stepStatus: StepStatus): string;
export interface InspectorListener {
    onSnapshotUpdated?: (snapshot: RunSnapshot) => void;
    onFreshnessChanged?: (freshness: StreamFreshness) => void;
    onLogsUpdated?: (logs: TaskLogsResponse | null, error?: string) => void;
    onEventsUpdated?: (events: RunEvent[], hasMore: boolean, nextCursor: number | null) => void;
    onError?: (err: Error) => void;
}
export declare class RunInspector {
    private snapshot;
    private streamClient;
    private baseUrl;
    private runId;
    private listeners;
    private logs;
    private logsError;
    private events;
    private eventsHasMore;
    private eventsNextCursor;
    constructor(runId: string, baseUrl?: string);
    subscribe(listener: InspectorListener): () => void;
    getSnapshot(): RunSnapshot | null;
    getEvents(): RunEvent[];
    load(): Promise<void>;
    destroy(): void;
    fetchSnapshot(): Promise<RunSnapshot>;
    fetchEvents(cursor?: number, append?: boolean): Promise<RunEventsResponse | null>;
    fetchLogs(stepId?: string, attemptId?: string, cursor?: string, append?: boolean): Promise<TaskLogsResponse | null>;
    private startStream;
    applyEvent(event: RunEvent): void;
    private notifySnapshot;
    private notifyFreshness;
    private notifyLogs;
    private notifyEvents;
    private notifyError;
}
//# sourceMappingURL=inspector.d.ts.map