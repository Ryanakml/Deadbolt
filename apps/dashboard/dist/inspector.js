import { RunEventStreamClient } from "./stream.js";
export class RunInspector {
    snapshot = null;
    streamClient = null;
    baseUrl;
    runId;
    listeners = [];
    logs = null;
    logsError = null;
    events = [];
    eventsHasMore = false;
    eventsNextCursor = null;
    constructor(runId, baseUrl = "") {
        this.runId = runId;
        this.baseUrl = baseUrl.replace(/\/$/, "");
    }
    subscribe(listener) {
        this.listeners.push(listener);
        if (this.snapshot && listener.onSnapshotUpdated) {
            listener.onSnapshotUpdated(this.snapshot);
        }
        if (this.streamClient && listener.onFreshnessChanged) {
            listener.onFreshnessChanged(this.streamClient.getFreshness());
        }
        if (this.events.length > 0 && listener.onEventsUpdated) {
            listener.onEventsUpdated(this.events, this.eventsHasMore, this.eventsNextCursor);
        }
        if ((this.logs || this.logsError) && listener.onLogsUpdated) {
            listener.onLogsUpdated(this.logs, this.logsError ?? undefined);
        }
        return () => {
            this.listeners = this.listeners.filter((l) => l !== listener);
        };
    }
    getSnapshot() {
        return this.snapshot;
    }
    getEvents() {
        return this.events;
    }
    async load() {
        try {
            await this.fetchSnapshot();
            this.startStream();
            await Promise.allSettled([this.fetchEvents(), this.fetchLogs()]);
        }
        catch (err) {
            this.notifyError(err instanceof Error ? err : new Error(String(err)));
        }
    }
    destroy() {
        if (this.streamClient) {
            this.streamClient.stop();
            this.streamClient = null;
        }
        this.listeners = [];
    }
    async fetchSnapshot() {
        const url = `${this.baseUrl}/v1/runs/${encodeURIComponent(this.runId)}`;
        const res = await fetch(url);
        if (!res.ok) {
            throw new Error(`Failed to load run snapshot (HTTP ${res.status}): ${res.statusText}`);
        }
        const snap = (await res.json());
        this.snapshot = snap;
        this.notifySnapshot();
        return snap;
    }
    async fetchEvents(cursor, append = false) {
        const params = new URLSearchParams();
        if (cursor !== undefined && cursor !== null) {
            params.set("cursor", String(cursor));
        }
        const queryStr = params.toString() ? `?${params.toString()}` : "";
        const url = `${this.baseUrl}/v1/runs/${encodeURIComponent(this.runId)}/events${queryStr}`;
        try {
            const res = await fetch(url);
            if (!res.ok) {
                throw new Error(`Failed to fetch events (HTTP ${res.status}): ${res.statusText}`);
            }
            const data = (await res.json());
            this.eventsHasMore = data.hasMore;
            this.eventsNextCursor = data.nextCursor ?? null;
            if (append) {
                const existingSeqs = new Set(this.events.map((e) => e.sequence));
                for (const ev of data.events) {
                    if (!existingSeqs.has(ev.sequence)) {
                        this.events.push(ev);
                    }
                }
            }
            else {
                this.events = [...data.events];
            }
            this.events.sort((a, b) => a.sequence - b.sequence);
            this.notifyEvents();
            return data;
        }
        catch (err) {
            return null;
        }
    }
    async fetchLogs(stepId, attemptId, cursor, append = false) {
        const params = new URLSearchParams();
        if (stepId)
            params.set("stepId", stepId);
        if (attemptId)
            params.set("attemptId", attemptId);
        if (cursor)
            params.set("cursor", cursor);
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
                throw new Error(`Failed to fetch logs (HTTP ${res.status}): ${res.statusText}`);
            }
            const data = (await res.json());
            if (append && this.logs) {
                this.logs = {
                    ...data,
                    items: [...this.logs.items, ...data.items],
                };
            }
            else {
                this.logs = data;
            }
            this.logsError = null;
            this.notifyLogs();
            return data;
        }
        catch (err) {
            this.logsError = err instanceof Error ? err.message : String(err);
            if (!append) {
                this.logs = null;
            }
            this.notifyLogs();
            return null;
        }
    }
    startStream() {
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
                        this.streamClient.setLastProcessedSequence(snap.lastEventSequence);
                    }
                })
                    .catch((err) => this.notifyError(err));
            },
            onError: (err) => this.notifyError(err),
        });
        this.streamClient.start();
    }
    applyEvent(event) {
        // Record into events timeline if not already recorded
        if (!this.events.some((e) => e.sequence === event.sequence)) {
            this.events.push(event);
            this.events.sort((a, b) => a.sequence - b.sequence);
            this.notifyEvents();
        }
        if (!this.snapshot)
            return;
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
                        const attemptId = payload.attemptId;
                        let attempt = step.attempts.find((a) => a.id === attemptId);
                        if (!attempt) {
                            attempt = {
                                id: attemptId ?? `att-${step.attempts.length + 1}`,
                                attemptNumber: payload.attemptNumber ?? step.attempts.length + 1,
                                status: "CLAIMED",
                                workerSessionId: payload.workerSessionId,
                                ownershipEpoch: payload.epoch,
                            };
                            step.attempts.push(attempt);
                        }
                        else {
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
                            const outcome = payload.outcome;
                            att.status = outcome;
                            if (event.committedAt) {
                                att.completedAt = event.committedAt;
                            }
                            if (payload.error !== undefined) {
                                att.error = payload.error;
                            }
                            if (outcome === "SUCCEEDED") {
                                s.status = "SUCCEEDED";
                            }
                            else if (outcome === "FAILED" ||
                                outcome === "TIMED_OUT" ||
                                outcome === "CANCELLED") {
                                s.status = "FAILED";
                            }
                            else if (outcome === "LOST") {
                                att.status = "LOST";
                                s.status = "WAITING";
                            }
                            break;
                        }
                    }
                }
                break;
            case "step.succeeded":
                if (typeof payload.stepId === "string") {
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
    notifySnapshot() {
        if (!this.snapshot)
            return;
        for (const l of this.listeners) {
            if (l.onSnapshotUpdated) {
                l.onSnapshotUpdated(this.snapshot);
            }
        }
    }
    notifyFreshness(f) {
        for (const l of this.listeners) {
            if (l.onFreshnessChanged) {
                l.onFreshnessChanged(f);
            }
        }
    }
    notifyLogs() {
        for (const l of this.listeners) {
            if (l.onLogsUpdated) {
                l.onLogsUpdated(this.logs, this.logsError ?? undefined);
            }
        }
    }
    notifyEvents() {
        for (const l of this.listeners) {
            if (l.onEventsUpdated) {
                l.onEventsUpdated(this.events, this.eventsHasMore, this.eventsNextCursor);
            }
        }
    }
    notifyError(err) {
        for (const l of this.listeners) {
            if (l.onError) {
                l.onError(err);
            }
        }
    }
}
//# sourceMappingURL=inspector.js.map