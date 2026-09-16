export type RunStatus = "QUEUED" | "RUNNING" | "WAITING" | "PAUSING" | "PAUSED" | "CANCELLING" | "SUCCEEDED" | "FAILED" | "CANCELLED";
export type StepStatus = "BLOCKED" | "READY" | "RUNNING" | "WAITING" | "SUCCEEDED" | "FAILED" | "CANCELLED";
export type AttemptStatus = "CLAIMED" | "RUNNING" | "SUCCEEDED" | "FAILED" | "TIMED_OUT" | "LOST" | "CANCELLED";
export interface TaskAttempt {
    id: string;
    attemptNumber: number;
    status: AttemptStatus;
    workerSessionId?: string;
    ownershipEpoch?: number;
    startedAt?: string;
    completedAt?: string;
    error?: unknown;
}
export interface RunStep {
    id: string;
    nodeId: string;
    status: StepStatus;
    currentEpoch: number;
    attempts: TaskAttempt[];
}
export interface Run {
    id: string;
    organizationId?: string;
    projectId?: string;
    environmentId?: string;
    workflowName: string;
    deploymentId: string;
    status: RunStatus;
    reasonCode?: string;
    revision: number;
    createdAt: string;
    deadlineAt?: string;
}
export interface RunSnapshot extends Run {
    lastEventSequence: number;
    steps: RunStep[];
    output?: unknown;
    error?: unknown;
    waitingReason?: string | null;
    activeCompatibleWorkers?: number;
}
export interface Worker {
    id: string;
    environmentId: string;
    pool: string;
    status: "ACTIVE" | "DRAINING" | "REVOKED";
    deploymentDigests: string[];
}
export interface RunEvent {
    id: string;
    runId: string;
    sequence: number;
    schemaVersion: number;
    type: string;
    requestId?: string;
    payload: Record<string, unknown>;
    committedAt: string;
}
export interface RunEventsResponse {
    events: RunEvent[];
    hasMore: boolean;
    nextCursor?: number | null;
}
export interface TaskLogRecord {
    id: string;
    runId: string;
    stepId: string;
    attemptId: string;
    sequence: number;
    timestamp: string;
    level: "debug" | "info" | "warn" | "error";
    message: string;
}
export interface TaskLogsResponse {
    items: TaskLogRecord[];
    nextCursor?: string;
    expired: boolean;
    message?: string;
    droppedCount?: number;
    budgetExhausted?: boolean;
}
export type StreamFreshness = "LIVE" | "RECONNECTING" | "STALE" | "DISCONNECTED";
export interface ResyncControlEvent {
    reason: "RETENTION_GAP" | string;
    code?: "RESYNC_REQUIRED" | string;
    lastAvailableSequence: number;
}
//# sourceMappingURL=types.d.ts.map