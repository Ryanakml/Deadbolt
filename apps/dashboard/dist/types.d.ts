export type RunStatus = "QUEUED" | "RUNNING" | "WAITING" | "PAUSING" | "PAUSED" | "CANCELLING" | "SUCCEEDED" | "FAILED" | "CANCELLED";
export type StepStatus = "BLOCKED" | "READY" | "RUNNING" | "WAITING" | "SUCCEEDED" | "FAILED" | "CANCELLED" | "SKIPPED";
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
    kind?: string;
    status: StepStatus;
    waitReason?: string | null;
    dueAt?: string | null;
    after?: string[];
    currentEpoch: number;
    completionSource?: string | null;
    output?: unknown;
    attempts: TaskAttempt[];
}
export type StepTab = "summary" | "attempts" | "events" | "logs" | "input" | "output" | "trace";
export type InspectorViewMode = "graph" | "list";
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
    terminationConfirmed?: boolean | null;
}
export interface RunSnapshot extends Run {
    lastEventSequence: number;
    steps: RunStep[];
    reconciliationCases?: ReconciliationCase[];
    approvals?: Approval[];
    output?: unknown;
    error?: unknown;
    waitingReason?: string | null;
    activeCompatibleWorkers?: number;
}
/**
 * Approval is a durable human decision attached to a control node.
 *
 * It is never a task: there is no attempt, no lease, and no runner behind it.
 * A PENDING approval is waiting on a person, and `expiresAt` is the hard bound
 * after which the run fails with APPROVAL_EXPIRED.
 */
export type ApprovalStatus = "PENDING" | "APPROVED" | "REJECTED" | "EXPIRED" | "CANCELLED";
export interface Approval {
    id: string;
    runId: string;
    stepId: string;
    nodeId: string;
    workflowName: string;
    status: ApprovalStatus;
    requiredPermission: string;
    payload?: unknown;
    revision: number;
    expiresAt?: string | null;
    decidedAt?: string | null;
    decisionComment?: string | null;
    decisionReason?: string | null;
    actorId?: string | null;
    createdAt?: string;
}
export interface ReconciliationCase {
    id: string;
    stepId: string;
    attemptId?: string | null;
    reason: string;
    evidence?: unknown;
    status: "OPEN" | "RESOLVED";
    resolution?: string | null;
    actorId?: string | null;
    revision: number;
    resolvedAt?: string | null;
    createdAt: string;
}
export type ResolveAction = "confirm_succeeded" | "confirm_not_executed_retry" | "fail_run";
export interface ResolveReconciliationRequest {
    action: ResolveAction;
    evidence: string;
    reason: string;
    result?: unknown;
    expectedRevision: number;
}
export interface ResolveReconciliationResponse {
    caseId: string;
    resolved: boolean;
    revision: number;
}
export interface Worker {
    id: string;
    environmentId: string;
    pool: string;
    status: "ACTIVE" | "DRAINING" | "REVOKED";
    deploymentDigests: string[];
}
export interface Schedule {
    id: string;
    workflow: string;
    environment: string;
    cron: string;
    timezone: string;
    deploymentId: string | null;
    overlapPolicy: "skip-overlap";
    misfirePolicy: "coalesce-one";
    paused: boolean;
    revision: number;
    nextDueAt: string | null;
    lastOccurrenceAt: string | null;
}
export type ScheduleOccurrenceStatus = "PENDING" | "STARTED" | "SKIPPED";
export type ScheduleSkippedReason = "SKIPPED_OVERLAP" | "SKIPPED_QUOTA" | "SKIPPED_MISFIRE";
export interface ScheduleOccurrence {
    id: string;
    scheduleId: string;
    dueAt: string;
    revision: number;
    status: ScheduleOccurrenceStatus;
    skippedReason: ScheduleSkippedReason | null;
    skippedCount: number;
    runId: string | null;
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