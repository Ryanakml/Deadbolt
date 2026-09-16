import {
  RunSnapshot,
  RunEvent,
  TaskAttempt,
  TaskLogsResponse,
  StreamFreshness,
} from "./types.js";
import { RunEventStreamClient } from "./stream.js";

export interface InspectorListener {
  onSnapshotUpdated: (snapshot: RunSnapshot) => void;
  onFreshnessChanged: (freshness: StreamFreshness) => void;
  onLogsUpdated: (logs: TaskLogsResponse | null, error?: string) => void;
  onError: (err: Error) => void;
}

export class RunInspector {
  private snapshot: RunSnapshot | null = null;
  private streamClient: RunEventStreamClient | null = null;
  private baseUrl: string;
  private runId: string;
  private listeners: InspectorListener[] = [];
  private logs: TaskLogsResponse | null = null;
  private logsError: string | null = null;

  constructor(runId: string, baseUrl = "") {
    this.runId = runId;
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  public subscribe(listener: InspectorListener): () => void {
    this.listeners.push(listener);
    if (this.snapshot) {
      listener.onSnapshotUpdated(this.snapshot);
    }
    if (this.streamClient) {
      listener.onFreshnessChanged(this.streamClient.getFreshness());
    }
    if (this.logs || this.logsError) {
      listener.onLogsUpdated(this.logs, this.logsError ?? undefined);
    }
    return () => {
      this.listeners = this.listeners.filter((l) => l !== listener);
    };
  }

  public getSnapshot(): RunSnapshot | null {
    return this.snapshot;
  }

  public async load(): Promise<void> {
    try {
      await this.fetchSnapshot();
      this.startStream();
      await this.fetchLogs();
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
    const res = await fetch(url);
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

  public async fetchLogs(
    stepId?: string,
    attemptId?: string,
  ): Promise<TaskLogsResponse | null> {
    const params = new URLSearchParams();
    if (stepId) params.set("stepId", stepId);
    if (attemptId) params.set("attemptId", attemptId);

    const queryStr = params.toString() ? `?${params.toString()}` : "";
    const url = `${this.baseUrl}/v1/runs/${encodeURIComponent(this.runId)}/logs${queryStr}`;

    try {
      const res = await fetch(url);
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
      this.logs = data;
      this.logsError = null;
      this.notifyLogs();
      return data;
    } catch (err: unknown) {
      this.logsError = err instanceof Error ? err.message : String(err);
      this.logs = null;
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
              if (typeof payload.deadlineAt === "string") {
                att.startedAt = att.startedAt ?? new Date().toISOString();
              }
              break;
            }
          }
        }
        break;

      case "attempt.completed":
      case "step.succeeded":
        if (typeof payload.attemptId === "string") {
          for (const s of this.snapshot.steps) {
            const att = s.attempts.find((a) => a.id === payload.attemptId);
            if (att) {
              att.status = "SUCCEEDED";
              att.completedAt = new Date().toISOString();
              s.status = "SUCCEEDED";
              break;
            }
          }
        } else if (typeof payload.stepId === "string") {
          const s = this.snapshot.steps.find((st) => st.id === payload.stepId);
          if (s) {
            s.status = "SUCCEEDED";
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
        break;
    }

    this.notifySnapshot();
  }

  private notifySnapshot(): void {
    if (!this.snapshot) return;
    for (const l of this.listeners) {
      l.onSnapshotUpdated(this.snapshot);
    }
  }

  private notifyFreshness(f: StreamFreshness): void {
    for (const l of this.listeners) {
      l.onFreshnessChanged(f);
    }
  }

  private notifyLogs(): void {
    for (const l of this.listeners) {
      l.onLogsUpdated(this.logs, this.logsError ?? undefined);
    }
  }

  private notifyError(err: Error): void {
    for (const l of this.listeners) {
      l.onError(err);
    }
  }
}
