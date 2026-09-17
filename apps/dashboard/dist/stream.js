import { apiFetch } from "./api.js";
export class RunEventStreamClient {
    runId;
    baseUrl;
    lastProcessedSequence;
    staleThresholdMs;
    freshness = "DISCONNECTED";
    onEvent;
    onFreshnessChange;
    onResyncRequired;
    onError;
    abortController = null;
    staleTimer = null;
    reconnectTimer = null;
    reconnectAttempts = 0;
    stopped = false;
    constructor(options) {
        this.runId = options.runId;
        this.baseUrl = (options.baseUrl ?? "").replace(/\/$/, "");
        this.lastProcessedSequence = options.initialSequence ?? 0;
        this.staleThresholdMs = options.staleThresholdMs ?? 10000;
        this.onEvent = options.onEvent;
        this.onFreshnessChange = options.onFreshnessChange;
        this.onResyncRequired = options.onResyncRequired;
        this.onError = options.onError;
    }
    getLastProcessedSequence() {
        return this.lastProcessedSequence;
    }
    getFreshness() {
        return this.freshness;
    }
    setLastProcessedSequence(seq) {
        if (seq > this.lastProcessedSequence) {
            this.lastProcessedSequence = seq;
        }
    }
    start() {
        this.stopped = false;
        this.connect();
    }
    stop() {
        this.stopped = true;
        this.clearTimers();
        if (this.abortController) {
            this.abortController.abort();
            this.abortController = null;
        }
        this.setFreshness("DISCONNECTED");
    }
    setFreshness(f) {
        if (this.freshness !== f) {
            this.freshness = f;
            this.onFreshnessChange(f);
        }
    }
    clearTimers() {
        if (this.staleTimer) {
            clearTimeout(this.staleTimer);
            this.staleTimer = null;
        }
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
    }
    scheduleReconnect() {
        if (this.stopped)
            return;
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
    async connect() {
        if (this.stopped)
            return;
        this.abortController = new AbortController();
        const url = `${this.baseUrl}/v1/runs/${encodeURIComponent(this.runId)}/stream`;
        try {
            const headers = {
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
                throw new Error(`SSE stream HTTP ${response.status}: ${response.statusText}`);
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
        }
        catch (err) {
            if (this.stopped)
                return;
            if (err instanceof Error && err.name === "AbortError")
                return;
            if (this.onError && err instanceof Error) {
                this.onError(err);
            }
            this.scheduleReconnect();
        }
    }
    processSSEBlock(block) {
        if (!block.trim() || block.startsWith(":")) {
            // Comment or empty keepalive line
            return;
        }
        const lines = block.split("\n");
        let eventName = "message";
        let dataStr = "";
        let eventId = null;
        for (const line of lines) {
            if (line.startsWith("event:")) {
                eventName = line.slice(6).trim();
            }
            else if (line.startsWith("data:")) {
                const d = line.slice(5).trim();
                dataStr = dataStr ? `${dataStr}\n${d}` : d;
            }
            else if (line.startsWith("id:")) {
                eventId = line.slice(3).trim();
            }
        }
        // Handle resync control event (e.g. RETENTION_GAP)
        if (eventName === "resync") {
            try {
                const resyncObj = JSON.parse(dataStr);
                this.onResyncRequired(resyncObj);
            }
            catch {
                this.onResyncRequired({
                    reason: "RETENTION_GAP",
                    lastAvailableSequence: 0,
                });
            }
            return;
        }
        if (!dataStr)
            return;
        let parsedPayload = {};
        try {
            parsedPayload = JSON.parse(dataStr);
        }
        catch {
            parsedPayload = { raw: dataStr };
        }
        const sequence = eventId
            ? parseInt(eventId, 10)
            : (parsedPayload.sequence ?? 0);
        // Duplicate detection and monotonic sequence enforcement
        if (sequence > 0 && sequence <= this.lastProcessedSequence) {
            // Ignore duplicate event
            return;
        }
        if (sequence > 0) {
            this.lastProcessedSequence = sequence;
        }
        const runEvent = {
            id: parsedPayload.id ?? `evt-${sequence}`,
            runId: parsedPayload.runId ?? this.runId,
            sequence,
            schemaVersion: 1,
            type: eventName,
            payload: parsedPayload,
            committedAt: parsedPayload.committedAt ?? new Date().toISOString(),
        };
        this.onEvent(runEvent);
    }
}
//# sourceMappingURL=stream.js.map