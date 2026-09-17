import { RunEvent, StreamFreshness, ResyncControlEvent } from "./types.js";
import { apiFetch } from "./api.js";

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

export class RunEventStreamClient {
  private runId: string;
  private baseUrl: string;
  private lastProcessedSequence: number;
  private staleThresholdMs: number;
  private freshness: StreamFreshness = "DISCONNECTED";
  private onEvent: (event: RunEvent) => void;
  private onFreshnessChange: (freshness: StreamFreshness) => void;
  private onResyncRequired: (resync: ResyncControlEvent) => void;
  private onError?: (err: Error) => void;

  private abortController: AbortController | null = null;
  private staleTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectAttempts = 0;
  private stopped = false;

  constructor(options: StreamClientOptions) {
    this.runId = options.runId;
    this.baseUrl = (options.baseUrl ?? "").replace(/\/$/, "");
    this.lastProcessedSequence = options.initialSequence ?? 0;
    this.staleThresholdMs = options.staleThresholdMs ?? 10000;
    this.onEvent = options.onEvent;
    this.onFreshnessChange = options.onFreshnessChange;
    this.onResyncRequired = options.onResyncRequired;
    this.onError = options.onError;
  }

  public getLastProcessedSequence(): number {
    return this.lastProcessedSequence;
  }

  public getFreshness(): StreamFreshness {
    return this.freshness;
  }

  public setLastProcessedSequence(seq: number): void {
    if (seq > this.lastProcessedSequence) {
      this.lastProcessedSequence = seq;
    }
  }

  public start(): void {
    this.stopped = false;
    this.connect();
  }

  public stop(): void {
    this.stopped = true;
    this.clearTimers();
    if (this.abortController) {
      this.abortController.abort();
      this.abortController = null;
    }
    this.setFreshness("DISCONNECTED");
  }

  private setFreshness(f: StreamFreshness): void {
    if (this.freshness !== f) {
      this.freshness = f;
      this.onFreshnessChange(f);
    }
  }

  private clearTimers(): void {
    if (this.staleTimer) {
      clearTimeout(this.staleTimer);
      this.staleTimer = null;
    }
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  private scheduleReconnect(): void {
    if (this.stopped) return;
    this.clearTimers();
    this.setFreshness("RECONNECTING");

    // After staleThresholdMs in RECONNECTING state, transition to STALE
    this.staleTimer = setTimeout(() => {
      if (this.freshness === "RECONNECTING") {
        this.setFreshness("STALE");
      }
    }, this.staleThresholdMs);

    // Exponential backoff with jitter, capped at 15 seconds
    const delay = Math.min(1000 * Math.pow(1.5, this.reconnectAttempts), 15000);
    this.reconnectAttempts++;

    this.reconnectTimer = setTimeout(() => {
      this.connect();
    }, delay);
  }

  private async connect(): Promise<void> {
    if (this.stopped) return;

    this.abortController = new AbortController();
    const url = `${this.baseUrl}/v1/runs/${encodeURIComponent(this.runId)}/stream`;

    try {
      const headers: Record<string, string> = {
        Accept: "text/event-stream",
      };
      if (this.lastProcessedSequence > 0) {
        headers["Last-Event-ID"] = String(this.lastProcessedSequence);
      }

      const response = await apiFetch(url, {
        headers,
        signal: this.abortController.signal,
      });

      if (!response.ok) {
        throw new Error(
          `SSE stream HTTP ${response.status}: ${response.statusText}`,
        );
      }

      if (!response.body) {
        throw new Error("ReadableStream not supported by environment");
      }

      // Successfully connected: reset reconnect attempts, mark LIVE
      this.reconnectAttempts = 0;
      this.clearTimers();
      this.setFreshness("LIVE");

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (!this.stopped) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }

        buffer += decoder.decode(value, { stream: true });
        const parts = buffer.split("\n\n");
        buffer = parts.pop() ?? "";

        for (const block of parts) {
          this.processSSEBlock(block);
        }
      }

      // Stream closed gracefully by server
      if (!this.stopped) {
        this.scheduleReconnect();
      }
    } catch (err: unknown) {
      if (this.stopped) return;
      if (err instanceof Error && err.name === "AbortError") return;

      if (this.onError && err instanceof Error) {
        this.onError(err);
      }
      this.scheduleReconnect();
    }
  }

  public processSSEBlock(block: string): void {
    if (!block.trim() || block.startsWith(":")) {
      // Comment or empty keepalive line
      return;
    }

    const lines = block.split("\n");
    let eventName = "message";
    let dataStr = "";
    let eventId: string | null = null;

    for (const line of lines) {
      if (line.startsWith("event:")) {
        eventName = line.slice(6).trim();
      } else if (line.startsWith("data:")) {
        const d = line.slice(5).trim();
        dataStr = dataStr ? `${dataStr}\n${d}` : d;
      } else if (line.startsWith("id:")) {
        eventId = line.slice(3).trim();
      }
    }

    // Handle resync control event (e.g. RETENTION_GAP)
    if (eventName === "resync") {
      try {
        const resyncObj = JSON.parse(dataStr) as ResyncControlEvent;
        this.onResyncRequired(resyncObj);
      } catch {
        this.onResyncRequired({
          reason: "RETENTION_GAP",
          lastAvailableSequence: 0,
        });
      }
      return;
    }

    if (!dataStr) return;

    let parsedPayload: Record<string, unknown> = {};
    try {
      parsedPayload = JSON.parse(dataStr);
    } catch {
      parsedPayload = { raw: dataStr };
    }

    const sequence = eventId
      ? parseInt(eventId, 10)
      : ((parsedPayload.sequence as number | undefined) ?? 0);

    // Duplicate detection and monotonic sequence enforcement
    if (sequence > 0 && sequence <= this.lastProcessedSequence) {
      // Ignore duplicate event
      return;
    }

    if (sequence > 0) {
      this.lastProcessedSequence = sequence;
    }

    const runEvent: RunEvent = {
      id: (parsedPayload.id as string) ?? `evt-${sequence}`,
      runId: (parsedPayload.runId as string) ?? this.runId,
      sequence,
      schemaVersion: 1,
      type: eventName,
      payload: parsedPayload,
      committedAt:
        (parsedPayload.committedAt as string) ?? new Date().toISOString(),
    };

    this.onEvent(runEvent);
  }
}
