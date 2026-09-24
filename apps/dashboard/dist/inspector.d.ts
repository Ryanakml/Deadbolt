import { RunSnapshot, RunStatus, RunEvent, RunEventsResponse, TaskAttempt, StepStatus, TaskLogsResponse, StreamFreshness, ReconciliationCase, ResolveAction } from "./types.js";
export declare function shouldShowWorkerWait(stepStatus: StepStatus, waitingReason: string | null | undefined, activeCompatibleWorkers: number | undefined): boolean;
export declare function terminalStepEmptyText(stepStatus: StepStatus): string;
export declare function openCaseForStep(snapshot: RunSnapshot, stepId: string): ReconciliationCase | null;
export declare function reconciliationHoldText(reason: string | null | undefined): string;
export interface ResolveActionOption {
    action: ResolveAction;
    label: string;
    hint: string;
    needsResult: boolean;
}
export declare const RESOLVE_ACTIONS: ResolveActionOption[];
export declare function terminationBannerText(status: RunStatus, terminationConfirmed: boolean | null | undefined): string | null;
export interface StatusPresentation {
    status: string;
    symbol: string;
    label: string;
    ariaLabel: string;
}
export declare function getStatusPresentation(status: string): StatusPresentation;
export interface GraphNode {
    id: string;
    nodeId: string;
    kind: string;
    status: StepStatus;
    waitReason?: string | null;
    after: string[];
    attemptsCount: number;
    output?: unknown;
    currentEpoch: number;
    completionSource?: string | null;
    level: number;
    x: number;
    y: number;
    width: number;
    height: number;
    clusterId?: string;
    isCollapsedPlaceholder?: boolean;
    collapsedCount?: number;
    collapsedNodeIds?: string[];
}
export interface GraphEdge {
    fromNodeId: string;
    toNodeId: string;
    fromX: number;
    fromY: number;
    toX: number;
    toY: number;
    isSkipped: boolean;
}
export interface GraphLayout {
    nodes: GraphNode[];
    edges: GraphEdge[];
    width: number;
    height: number;
    levels: number;
}
export declare function computeGraphLayout(steps: Array<{
    id: string;
    nodeId: string;
    kind?: string;
    status: StepStatus;
    waitReason?: string | null;
    after?: string[];
    currentEpoch?: number;
    completionSource?: string | null;
    output?: unknown;
    attempts?: TaskAttempt[];
}>, collapsedClusterIds?: Set<string>): GraphLayout;
export interface MinimapLayout {
    scale: number;
    width: number;
    height: number;
    viewport: {
        x: number;
        y: number;
        width: number;
        height: number;
    };
    nodes: Array<{
        x: number;
        y: number;
        width: number;
        height: number;
        status: StepStatus;
    }>;
}
export declare function computeMinimap(layout: GraphLayout, viewportWidth: number, viewportHeight: number, scrollLeft: number, scrollTop: number, minimapWidth?: number, minimapHeight?: number): MinimapLayout;
export declare function filterEventsForStep(events: RunEvent[], step: {
    id: string;
    nodeId: string;
    attempts?: Array<{
        id: string;
    }>;
}): RunEvent[];
export declare function virtualizeItems<T>(items: T[], startIndex: number, pageSize?: number): {
    items: T[];
    total: number;
    hasMore: number;
    offset: number;
};
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