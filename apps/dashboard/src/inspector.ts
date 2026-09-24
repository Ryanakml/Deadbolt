import {
  RunSnapshot,
  RunStatus,
  RunEvent,
  RunEventsResponse,
  TaskAttempt,
  AttemptStatus,
  StepStatus,
  StepTab,
  InspectorViewMode,
  TaskLogsResponse,
  StreamFreshness,
  ReconciliationCase,
  ResolveAction,
} from "./types.js";
import { RunEventStreamClient } from "./stream.js";
import { apiFetch } from "./api.js";

// shouldShowWorkerWait gates the "No compatible workers available" hint so
// terminal steps are never described as waiting for workers. Only steps in
// a claimable/waiting state (READY/WAITING) plus the authoritative waiting
// condition may show the recovery hint.
export function shouldShowWorkerWait(
  stepStatus: StepStatus,
  waitingReason: string | null | undefined,
  activeCompatibleWorkers: number | undefined,
): boolean {
  if (stepStatus !== "READY" && stepStatus !== "WAITING") return false;
  return (
    waitingReason === "NO_COMPATIBLE_WORKERS" || activeCompatibleWorkers === 0
  );
}

// terminalStepEmptyText is the neutral explanation for a terminal step that
// executed no attempts. It carries no worker recovery hint.
export function terminalStepEmptyText(stepStatus: StepStatus): string {
  if (stepStatus === "CANCELLED") {
    return "Cancelled before execution — no attempt executed.";
  }
  if (stepStatus === "SKIPPED") {
    return "Skipped — branch not selected or upstream dependency skipped.";
  }
  return "No attempt executed.";
}

// openCaseForStep returns the OPEN reconciliation hold for a step, if any.
// A step shows at most one actionable hold; resolved history stays visible
// through the event timeline instead.
export function openCaseForStep(
  snapshot: RunSnapshot,
  stepId: string,
): ReconciliationCase | null {
  const cases = snapshot.reconciliationCases ?? [];
  for (const c of cases) {
    if (c.stepId === stepId && c.status === "OPEN") {
      return c;
    }
  }
  return null;
}

// reconciliationHoldText explains an unknown-outcome hold without claiming
// the side effect is safe to repeat. The provider may already have received
// the operation, so the only safe actions are the audited resolutions.
export function reconciliationHoldText(
  reason: string | null | undefined,
): string {
  switch (reason) {
    case "AMBIGUOUS_OUTCOME":
      return "Outcome unknown — the provider may already have received the operation. Check the external reference, then confirm the outcome.";
    case "IDEMPOTENCY_WINDOW_INSUFFICIENT":
      return "Outcome unknown and the provider deduplication window cannot cover another attempt. Check the external reference, then confirm the outcome.";
    case "IDEMPOTENCY_WINDOW_UNKNOWN":
      return "Outcome unknown and no deduplication window is on record. Check the external reference, then confirm the outcome.";
    default:
      return "Outcome unknown — the provider may already have received the operation. Check the external reference, then confirm the outcome.";
  }
}

export interface ResolveActionOption {
  action: ResolveAction;
  label: string;
  hint: string;
  needsResult: boolean;
}

export const RESOLVE_ACTIONS: ResolveActionOption[] = [
  {
    action: "confirm_succeeded",
    label: "Confirm succeeded",
    hint: "The side effect happened. Submit the schema-valid result plus the evidence reference.",
    needsResult: true,
  },
  {
    action: "confirm_not_executed_retry",
    label: "Confirm not executed — retry",
    hint: "Declare the effect did not occur. A retry is scheduled within the remaining budget.",
    needsResult: false,
  },
  {
    action: "fail_run",
    label: "Fail run",
    hint: "Stop the workflow with a visible reason.",
    needsResult: false,
  },
];

// terminationBannerText describes cancellation settlement without ever
// promising external rollback. A null confirmation means not yet settled.
export function terminationBannerText(
  status: RunStatus,
  terminationConfirmed: boolean | null | undefined,
): string | null {
  if (status === "CANCELLING") {
    return "Cancelling — waiting for workers to acknowledge the stop or for the grace period to expire.";
  }
  if (status === "CANCELLED" && terminationConfirmed === false) {
    return "Cancellation unconfirmed — worker processes may still be running. External effects already performed were not rolled back; verify provider state.";
  }
  return null;
}

export interface StatusPresentation {
  status: string;
  symbol: string;
  label: string;
  ariaLabel: string;
}

export function getStatusPresentation(status: string): StatusPresentation {
  switch (status.toUpperCase()) {
    case "SUCCEEDED":
      return {
        status,
        symbol: "✓",
        label: "Succeeded",
        ariaLabel: "Status: Succeeded",
      };
    case "FAILED":
      return {
        status,
        symbol: "✕",
        label: "Failed",
        ariaLabel: "Status: Failed",
      };
    case "SKIPPED":
      return {
        status,
        symbol: "↷",
        label: "Skipped",
        ariaLabel: "Status: Skipped",
      };
    case "CANCELLED":
      return {
        status,
        symbol: "⊘",
        label: "Cancelled",
        ariaLabel: "Status: Cancelled",
      };
    case "RUNNING":
      return {
        status,
        symbol: "●",
        label: "Running",
        ariaLabel: "Status: Running",
      };
    case "WAITING":
      return {
        status,
        symbol: "⏳",
        label: "Waiting",
        ariaLabel: "Status: Waiting",
      };
    case "PAUSING":
      return {
        status,
        symbol: "⏸",
        label: "Pausing",
        ariaLabel: "Status: Pausing",
      };
    case "PAUSED":
      return {
        status,
        symbol: "⏸",
        label: "Paused",
        ariaLabel: "Status: Paused",
      };
    case "CANCELLING":
      return {
        status,
        symbol: "⊘",
        label: "Cancelling",
        ariaLabel: "Status: Cancelling",
      };
    case "QUEUED":
      return {
        status,
        symbol: "⋯",
        label: "Queued",
        ariaLabel: "Status: Queued",
      };
    case "READY":
      return {
        status,
        symbol: "○",
        label: "Ready",
        ariaLabel: "Status: Ready",
      };
    case "BLOCKED":
      return {
        status,
        symbol: "◌",
        label: "Blocked",
        ariaLabel: "Status: Blocked",
      };
    case "CLAIMED":
      return {
        status,
        symbol: "◷",
        label: "Claimed",
        ariaLabel: "Status: Claimed",
      };
    case "LOST":
      return {
        status,
        symbol: "⚠",
        label: "Lost",
        ariaLabel: "Status: Lost",
      };
    case "TIMED_OUT":
      return {
        status,
        symbol: "⏰",
        label: "Timed Out",
        ariaLabel: "Status: Timed Out",
      };
    default:
      return {
        status,
        symbol: "•",
        label: status,
        ariaLabel: `Status: ${status}`,
      };
  }
}

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

export function computeGraphLayout(
  steps: Array<{
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
  }>,
  collapsedClusterIds: Set<string> = new Set(),
): GraphLayout {
  if (steps.length === 0) {
    return { nodes: [], edges: [], width: 400, height: 200, levels: 0 };
  }

  // 1. Calculate topological level for each logical step
  const stepMap = new Map(steps.map((s) => [s.nodeId, s]));
  const levelMap = new Map<string, number>();

  for (const s of steps) {
    if (!s.after || s.after.length === 0) {
      levelMap.set(s.nodeId, 0);
    }
  }

  // Multi-pass relaxation to resolve dependencies
  for (let pass = 0; pass < steps.length + 1; pass++) {
    let changed = false;
    for (const s of steps) {
      const deps = s.after || [];
      if (deps.length === 0) continue;
      let maxParentLevel = 0;
      let allFound = true;
      for (const p of deps) {
        if (levelMap.has(p)) {
          maxParentLevel = Math.max(maxParentLevel, levelMap.get(p)!);
        } else {
          allFound = false;
        }
      }
      const newLevel = maxParentLevel + 1;
      if (!levelMap.has(s.nodeId) || levelMap.get(s.nodeId)! < newLevel) {
        levelMap.set(s.nodeId, newLevel);
        changed = true;
      }
    }
    if (!changed) break;
  }

  // Fallback for any disconnected nodes
  for (const s of steps) {
    if (!levelMap.has(s.nodeId)) {
      levelMap.set(s.nodeId, 0);
    }
  }

  // 2. Identify collapsible parallel groups
  // If > 4 parallel sibling nodes share identical single parent and single child,
  // or share identical dependencies, they form a candidate cluster.
  const clusterMap = new Map<string, string[]>(); // clusterId -> nodeIds
  const nodeClusterMap = new Map<string, string>(); // nodeId -> clusterId

  const siblingsByDeps = new Map<string, string[]>();
  for (const s of steps) {
    const depKey = (s.after || []).slice().sort().join(",");
    const list = siblingsByDeps.get(depKey) || [];
    list.push(s.nodeId);
    siblingsByDeps.set(depKey, list);
  }

  for (const [depKey, nodeIds] of siblingsByDeps.entries()) {
    if (nodeIds.length >= 4) {
      const clusterId = `cluster-${depKey || "root"}-${levelMap.get(nodeIds[0])}`;
      clusterMap.set(clusterId, nodeIds);
      for (const nid of nodeIds) {
        nodeClusterMap.set(nid, clusterId);
      }
    }
  }

  // 3. Layout geometry constants
  const NODE_WIDTH = 220;
  const NODE_HEIGHT = 70;
  const COL_GAP = 80;
  const ROW_GAP = 24;
  const PADDING = 40;

  // 4. Build visible nodes list (collapsing clustered nodes if collapsedClusterIds has clusterId)
  const visibleNodes: GraphNode[] = [];
  const processedClusters = new Set<string>();

  for (const s of steps) {
    const clusterId = nodeClusterMap.get(s.nodeId);
    if (clusterId && collapsedClusterIds.has(clusterId)) {
      if (!processedClusters.has(clusterId)) {
        processedClusters.add(clusterId);
        const clusterNodes = clusterMap.get(clusterId)!;
        const firstStep = stepMap.get(clusterNodes[0])!;
        const allSkipped = clusterNodes.every(
          (nid) => stepMap.get(nid)?.status === "SKIPPED",
        );
        const allSucceeded = clusterNodes.every(
          (nid) => stepMap.get(nid)?.status === "SUCCEEDED",
        );
        const anyFailed = clusterNodes.some(
          (nid) => stepMap.get(nid)?.status === "FAILED",
        );
        const anyRunning = clusterNodes.some(
          (nid) => stepMap.get(nid)?.status === "RUNNING",
        );
        let clusterStatus: StepStatus = "READY";
        if (anyFailed) clusterStatus = "FAILED";
        else if (anyRunning) clusterStatus = "RUNNING";
        else if (allSucceeded) clusterStatus = "SUCCEEDED";
        else if (allSkipped) clusterStatus = "SKIPPED";

        visibleNodes.push({
          id: clusterId,
          nodeId: `${clusterNodes.length} parallel steps (collapsed)`,
          kind: "cluster",
          status: clusterStatus,
          after: firstStep.after || [],
          attemptsCount: 0,
          currentEpoch: 0,
          level: levelMap.get(clusterNodes[0]) || 0,
          x: 0,
          y: 0,
          width: NODE_WIDTH,
          height: NODE_HEIGHT,
          clusterId,
          isCollapsedPlaceholder: true,
          collapsedCount: clusterNodes.length,
          collapsedNodeIds: clusterNodes,
        });
      }
      continue;
    }

    visibleNodes.push({
      id: s.id,
      nodeId: s.nodeId,
      kind: s.kind || "task",
      status: s.status,
      waitReason: s.waitReason,
      after: s.after || [],
      attemptsCount: s.attempts ? s.attempts.length : 0,
      output: s.output,
      currentEpoch: s.currentEpoch || 0,
      completionSource: s.completionSource,
      level: levelMap.get(s.nodeId) || 0,
      x: 0,
      y: 0,
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
      clusterId,
    });
  }

  // 5. Position nodes by column (level) and row
  const nodesByLevel = new Map<number, GraphNode[]>();
  let maxLevel = 0;
  for (const n of visibleNodes) {
    const list = nodesByLevel.get(n.level) || [];
    list.push(n);
    nodesByLevel.set(n.level, list);
    if (n.level > maxLevel) maxLevel = n.level;
  }

  let maxX = 0;
  let maxY = 0;

  for (let lvl = 0; lvl <= maxLevel; lvl++) {
    const colNodes = nodesByLevel.get(lvl) || [];
    const colX = PADDING + lvl * (NODE_WIDTH + COL_GAP);
    colNodes.forEach((node, rowIdx) => {
      const rowY = PADDING + rowIdx * (NODE_HEIGHT + ROW_GAP);
      node.x = colX;
      node.y = rowY;
      maxX = Math.max(maxX, colX + NODE_WIDTH);
      maxY = Math.max(maxY, rowY + NODE_HEIGHT);
    });
  }

  // 6. Connect edges
  const edges: GraphEdge[] = [];
  const visibleMap = new Map<string, GraphNode>();
  for (const n of visibleNodes) {
    if (n.isCollapsedPlaceholder && n.collapsedNodeIds) {
      for (const nid of n.collapsedNodeIds) {
        visibleMap.set(nid, n);
      }
    }
    visibleMap.set(n.nodeId, n);
  }

  const edgeSet = new Set<string>();
  for (const n of visibleNodes) {
    const deps = n.after || [];
    for (const parentId of deps) {
      const parentNode = visibleMap.get(parentId);
      if (!parentNode) continue;
      if (parentNode === n) continue; // avoid self-loop if inside same collapsed cluster

      const edgeKey = `${parentNode.nodeId}->${n.nodeId}`;
      if (edgeSet.has(edgeKey)) continue;
      edgeSet.add(edgeKey);

      edges.push({
        fromNodeId: parentNode.nodeId,
        toNodeId: n.nodeId,
        fromX: parentNode.x + parentNode.width,
        fromY: parentNode.y + parentNode.height / 2,
        toX: n.x,
        toY: n.y + n.height / 2,
        isSkipped: n.status === "SKIPPED" || parentNode.status === "SKIPPED",
      });
    }
  }

  return {
    nodes: visibleNodes,
    edges,
    width: Math.max(760, maxX + PADDING),
    height: Math.max(380, maxY + PADDING),
    levels: maxLevel + 1,
  };
}

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

export function computeMinimap(
  layout: GraphLayout,
  viewportWidth: number,
  viewportHeight: number,
  scrollLeft: number,
  scrollTop: number,
  minimapWidth = 160,
  minimapHeight = 100,
): MinimapLayout {
  const scale = Math.min(
    minimapWidth / Math.max(layout.width, 1),
    minimapHeight / Math.max(layout.height, 1),
  );
  const vpX = Math.max(0, scrollLeft * scale);
  const vpY = Math.max(0, scrollTop * scale);
  const vpW = Math.min(minimapWidth, Math.max(12, viewportWidth * scale));
  const vpH = Math.min(minimapHeight, Math.max(12, viewportHeight * scale));

  const nodes = layout.nodes.map((n) => ({
    x: n.x * scale,
    y: n.y * scale,
    width: Math.max(4, n.width * scale),
    height: Math.max(3, n.height * scale),
    status: n.status,
  }));

  return {
    scale,
    width: minimapWidth,
    height: minimapHeight,
    viewport: { x: vpX, y: vpY, width: vpW, height: vpH },
    nodes,
  };
}

export function filterEventsForStep(
  events: RunEvent[],
  step: { id: string; nodeId: string; attempts?: Array<{ id: string }> },
): RunEvent[] {
  const attemptIds = new Set(step.attempts?.map((a) => a.id) ?? []);
  return events.filter((e) => {
    const p = e.payload as Record<string, unknown> | undefined;
    if (!p) return false;
    if (p.stepId === step.id || p.nodeId === step.nodeId) return true;
    if (typeof p.attemptId === "string" && attemptIds.has(p.attemptId))
      return true;
    return false;
  });
}

export function virtualizeItems<T>(
  items: T[],
  startIndex: number,
  pageSize = 50,
): { items: T[]; total: number; hasMore: number; offset: number } {
  const clampedStart = Math.max(0, Math.min(startIndex, items.length));
  const sliced = items.slice(clampedStart, clampedStart + pageSize);
  return {
    items: sliced,
    total: items.length,
    hasMore: Math.max(0, items.length - (clampedStart + pageSize)),
    offset: clampedStart,
  };
}

export function getBoundedEvents(
  events: RunEvent[],
  offset: number,
  limit = 50,
): { events: RunEvent[]; total: number; hasMore: boolean; offset: number } {
  const total = events.length;
  const start = Math.max(0, Math.min(offset, total));
  const end = Math.min(start + limit, total);
  return {
    events: events.slice(start, end),
    total,
    hasMore: end < total,
    offset: start,
  };
}

export interface InspectorListener {
  onSnapshotUpdated?: (snapshot: RunSnapshot) => void;
  onFreshnessChanged?: (freshness: StreamFreshness) => void;
  onLogsUpdated?: (logs: TaskLogsResponse | null, error?: string) => void;
  onEventsUpdated?: (
    events: RunEvent[],
    hasMore: boolean,
    nextCursor: number | null,
  ) => void;
  onError?: (err: Error) => void;
}

export class RunInspector {
  private snapshot: RunSnapshot | null = null;
  private streamClient: RunEventStreamClient | null = null;
  private baseUrl: string;
  private runId: string;
  private listeners: InspectorListener[] = [];
  private logs: TaskLogsResponse | null = null;
  private logsError: string | null = null;
  private events: RunEvent[] = [];
  private eventsHasMore = false;
  private eventsNextCursor: number | null = null;

  constructor(runId: string, baseUrl = "") {
    this.runId = runId;
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  public subscribe(listener: InspectorListener): () => void {
    this.listeners.push(listener);
    if (this.snapshot && listener.onSnapshotUpdated) {
      listener.onSnapshotUpdated(this.snapshot);
    }
    if (this.streamClient && listener.onFreshnessChanged) {
      listener.onFreshnessChanged(this.streamClient.getFreshness());
    }
    if (this.events.length > 0 && listener.onEventsUpdated) {
      listener.onEventsUpdated(
        this.events,
        this.eventsHasMore,
        this.eventsNextCursor,
      );
    }
    if ((this.logs || this.logsError) && listener.onLogsUpdated) {
      listener.onLogsUpdated(this.logs, this.logsError ?? undefined);
    }
    return () => {
      this.listeners = this.listeners.filter((l) => l !== listener);
    };
  }

  public getSnapshot(): RunSnapshot | null {
    return this.snapshot;
  }

  public getEvents(): RunEvent[] {
    return this.events;
  }

  public async load(): Promise<void> {
    try {
      await this.fetchSnapshot();
      this.startStream();
      await Promise.allSettled([this.fetchEvents(), this.fetchLogs()]);
    } catch (err: unknown) {
      this.notifyError(err instanceof Error ? err : new Error(String(err)));
    }
  }

  public destroy(): void {
    if (this.streamClient) {
      this.streamClient.stop();
      this.streamClient = null;
    }
    this.listeners = [];
  }

  public async fetchSnapshot(): Promise<RunSnapshot> {
    const url = `${this.baseUrl}/v1/runs/${encodeURIComponent(this.runId)}`;
    const res = await apiFetch(url);
    if (!res.ok) {
      throw new Error(
        `Failed to load run snapshot (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    const snap = (await res.json()) as RunSnapshot;
    this.snapshot = snap;
    this.notifySnapshot();
    return snap;
  }

  public async fetchEvents(
    cursor?: number,
    append = false,
  ): Promise<RunEventsResponse | null> {
    const params = new URLSearchParams();
    if (cursor !== undefined && cursor !== null) {
      params.set("cursor", String(cursor));
    }
    const queryStr = params.toString() ? `?${params.toString()}` : "";
    const url = `${this.baseUrl}/v1/runs/${encodeURIComponent(this.runId)}/events${queryStr}`;

    try {
      const res = await apiFetch(url);
      if (!res.ok) {
        throw new Error(
          `Failed to fetch events (HTTP ${res.status}): ${res.statusText}`,
        );
      }
      const data = (await res.json()) as RunEventsResponse;
      this.eventsHasMore = data.hasMore;
      this.eventsNextCursor = data.nextCursor ?? null;

      // A live SSE event may arrive while the initial history request is in
      // flight. Always merge by authoritative sequence; replacing the array
      // would silently erase that live event from the visible timeline.
      const eventsBySequence = new Map<number, RunEvent>();
      if (append) {
        for (const ev of this.events) eventsBySequence.set(ev.sequence, ev);
      } else {
        // Keep events received after this request started as well as history.
        for (const ev of this.events) eventsBySequence.set(ev.sequence, ev);
      }
      for (const ev of data.events) {
        if (!eventsBySequence.has(ev.sequence)) {
          eventsBySequence.set(ev.sequence, ev);
        }
      }
      this.events = [...eventsBySequence.values()];
      this.events.sort((a, b) => a.sequence - b.sequence);
      this.notifyEvents();
      return data;
    } catch (err: unknown) {
      return null;
    }
  }

  public async fetchLogs(
    stepId?: string,
    attemptId?: string,
    cursor?: string,
    append = false,
  ): Promise<TaskLogsResponse | null> {
    const params = new URLSearchParams();
    if (stepId) params.set("stepId", stepId);
    if (attemptId) params.set("attemptId", attemptId);
    if (cursor) params.set("cursor", cursor);

    const queryStr = params.toString() ? `?${params.toString()}` : "";
    const url = `${this.baseUrl}/v1/runs/${encodeURIComponent(this.runId)}/logs${queryStr}`;

    try {
      const res = await apiFetch(url);
      if (res.status === 403) {
        this.logsError =
          "Diagnostic task logs require payload:read capability (redacted by tenant policy).";
        this.logs = null;
        this.notifyLogs();
        return null;
      }
      if (!res.ok) {
        throw new Error(
          `Failed to fetch logs (HTTP ${res.status}): ${res.statusText}`,
        );
      }
      const data = (await res.json()) as TaskLogsResponse;
      if (append && this.logs) {
        this.logs = {
          ...data,
          items: [...this.logs.items, ...data.items],
        };
      } else {
        this.logs = data;
      }
      this.logsError = null;
      this.notifyLogs();
      return data;
    } catch (err: unknown) {
      this.logsError = err instanceof Error ? err.message : String(err);
      if (!append) {
        this.logs = null;
      }
      this.notifyLogs();
      return null;
    }
  }

  private startStream(): void {
    if (this.streamClient) {
      this.streamClient.stop();
    }

    const initialSeq = this.snapshot ? this.snapshot.lastEventSequence : 0;

    this.streamClient = new RunEventStreamClient({
      baseUrl: this.baseUrl,
      runId: this.runId,
      initialSequence: initialSeq,
      onEvent: (event) => this.applyEvent(event),
      onFreshnessChange: (f) => this.notifyFreshness(f),
      onResyncRequired: () => {
        // Retention gap detected: refetch authoritative snapshot
        this.fetchSnapshot()
          .then((snap) => {
            if (this.streamClient) {
              this.streamClient.setLastProcessedSequence(
                snap.lastEventSequence,
              );
            }
          })
          .catch((err) => this.notifyError(err));
      },
      onError: (err) => this.notifyError(err),
    });

    this.streamClient.start();
  }

  public applyEvent(event: RunEvent): void {
    // Record into events timeline if not already recorded
    if (!this.events.some((e) => e.sequence === event.sequence)) {
      this.events.push(event);
      this.events.sort((a, b) => a.sequence - b.sequence);
      this.notifyEvents();
    }

    if (!this.snapshot) return;

    if (event.sequence > this.snapshot.lastEventSequence) {
      this.snapshot.lastEventSequence = event.sequence;
    }

    const payload = event.payload;

    switch (event.type) {
      case "run.created":
        this.snapshot.status = "QUEUED";
        break;

      case "step.ready":
        if (typeof payload.stepId === "string") {
          const step = this.snapshot.steps.find((s) => s.id === payload.stepId);
          if (step) {
            step.status = "READY";
          }
        }
        break;

      case "attempt.claimed":
        if (typeof payload.stepId === "string") {
          const step = this.snapshot.steps.find((s) => s.id === payload.stepId);
          if (step) {
            step.status = "RUNNING";
            const attemptId = payload.attemptId as string;
            let attempt = step.attempts.find((a) => a.id === attemptId);
            if (!attempt) {
              attempt = {
                id: attemptId ?? `att-${step.attempts.length + 1}`,
                attemptNumber:
                  (payload.attemptNumber as number) ?? step.attempts.length + 1,
                status: "CLAIMED",
                workerSessionId: payload.workerSessionId as string | undefined,
                ownershipEpoch: payload.epoch as number | undefined,
              };
              step.attempts.push(attempt);
            } else {
              attempt.status = "CLAIMED";
            }
          }
        }
        break;

      case "attempt.started":
        this.snapshot.status = "RUNNING";
        if (typeof payload.attemptId === "string") {
          for (const s of this.snapshot.steps) {
            const att = s.attempts.find((a) => a.id === payload.attemptId);
            if (att) {
              att.status = "RUNNING";
              s.status = "RUNNING";
              att.startedAt =
                att.startedAt ?? event.committedAt ?? new Date().toISOString();
              break;
            }
          }
        }
        break;

      case "attempt.completed":
        if (typeof payload.attemptId === "string") {
          for (const s of this.snapshot.steps) {
            const att = s.attempts.find((a) => a.id === payload.attemptId);
            if (att) {
              // TASK_COMPLETED is emitted for every terminal outcome.  Never
              // infer success from the event name: the committed outcome is
              // the authority for the inspector's transient state.
              if (typeof payload.outcome !== "string") {
                break;
              }
              const outcome = payload.outcome as AttemptStatus;
              att.status = outcome;
              if (event.committedAt) {
                att.completedAt = event.committedAt;
              }
              if (payload.error !== undefined) {
                att.error = payload.error;
              }
              if (outcome === "SUCCEEDED") {
                s.status = "SUCCEEDED";
              } else if (
                outcome === "FAILED" ||
                outcome === "TIMED_OUT" ||
                outcome === "CANCELLED"
              ) {
                s.status = "FAILED";
              } else if (outcome === "LOST") {
                att.status = "LOST";
                s.status = "WAITING";
              }
              break;
            }
          }
        }
        break;

      case "step.waiting":
        if (typeof payload.stepId === "string") {
          const s = this.snapshot.steps.find((st) => st.id === payload.stepId);
          if (s) {
            s.status = "WAITING";
          }
        }
        break;

      case "run.waiting":
        this.snapshot.status = "WAITING";
        if (typeof payload.reason === "string") {
          this.snapshot.reasonCode = payload.reason;
        }
        break;

      case "run.resumed":
        if (typeof payload.status === "string") {
          this.snapshot.status = payload.status as any;
        } else {
          this.snapshot.status = "RUNNING";
        }
        if (typeof payload.reason === "string") {
          this.snapshot.reasonCode = payload.reason;
        } else {
          delete this.snapshot.reasonCode;
        }
        // The server moved the run out of a hold or pause; converge on authority.
        void this.fetchSnapshot().catch(() => undefined);
        break;

      case "step.succeeded":
      case "step_succeeded":
        if (
          typeof payload.stepId === "string" ||
          typeof payload.nodeId === "string"
        ) {
          const s = this.snapshot.steps.find(
            (st) =>
              st.id === payload.stepId ||
              (payload.nodeId && st.nodeId === payload.nodeId),
          );
          if (s) {
            s.status = "SUCCEEDED";
            if (payload.output !== undefined) {
              s.output = payload.output;
            }
          }
        }
        break;

      case "step.skipped":
      case "step_skipped":
        if (
          typeof payload.stepId === "string" ||
          typeof payload.nodeId === "string"
        ) {
          const s = this.snapshot.steps.find(
            (st) =>
              st.id === payload.stepId ||
              (payload.nodeId && st.nodeId === payload.nodeId),
          );
          if (s) {
            s.status = "SKIPPED";
            if (typeof payload.reason === "string") {
              s.waitReason = payload.reason;
            } else if (typeof payload.waitReason === "string") {
              s.waitReason = payload.waitReason;
            }
          }
        }
        break;

      case "attempt.lost":
        if (typeof payload.attemptId === "string") {
          for (const s of this.snapshot.steps) {
            const att = s.attempts.find((a) => a.id === payload.attemptId);
            if (att) {
              att.status = "LOST";
              s.status = "WAITING";
              this.snapshot.status = "WAITING";
              break;
            }
          }
        }
        break;

      case "step.failed":
        if (typeof payload.stepId === "string") {
          const s = this.snapshot.steps.find((st) => st.id === payload.stepId);
          if (s) {
            s.status = "FAILED";
          }
        }
        break;

      case "run.succeeded":
      case "run.completed":
        this.snapshot.status = "SUCCEEDED";
        if (payload.output !== undefined) {
          this.snapshot.output = payload.output;
        }
        break;

      case "run.failed":
        this.snapshot.status = "FAILED";
        if (typeof payload.reason === "string") {
          this.snapshot.reasonCode = payload.reason;
        }
        if (payload.error !== undefined) {
          this.snapshot.error = payload.error;
        }
        break;

      case "run.cancelled":
        this.snapshot.status = "CANCELLED";
        if (typeof payload.terminationConfirmed === "boolean") {
          this.snapshot.terminationConfirmed = payload.terminationConfirmed;
        }
        break;

      case "run.cancelling":
        this.snapshot.status = "CANCELLING";
        if (typeof payload.reason === "string") {
          this.snapshot.reasonCode = payload.reason;
        }
        break;

      case "run.pausing":
        this.snapshot.status = "PAUSING";
        if (typeof payload.reason === "string") {
          this.snapshot.reasonCode = payload.reason;
        }
        break;

      case "run.paused":
        this.snapshot.status = "PAUSED";
        if (typeof payload.reason === "string") {
          this.snapshot.reasonCode = payload.reason;
        }
        break;
    }

    this.notifySnapshot();
  }

  private notifySnapshot(): void {
    if (!this.snapshot) return;
    for (const l of this.listeners) {
      if (l.onSnapshotUpdated) {
        l.onSnapshotUpdated(this.snapshot);
      }
    }
  }

  private notifyFreshness(f: StreamFreshness): void {
    for (const l of this.listeners) {
      if (l.onFreshnessChanged) {
        l.onFreshnessChanged(f);
      }
    }
  }

  private notifyLogs(): void {
    for (const l of this.listeners) {
      if (l.onLogsUpdated) {
        l.onLogsUpdated(this.logs, this.logsError ?? undefined);
      }
    }
  }

  private notifyEvents(): void {
    for (const l of this.listeners) {
      if (l.onEventsUpdated) {
        l.onEventsUpdated(
          this.events,
          this.eventsHasMore,
          this.eventsNextCursor,
        );
      }
    }
  }

  private notifyError(err: Error): void {
    for (const l of this.listeners) {
      if (l.onError) {
        l.onError(err);
      }
    }
  }
}
