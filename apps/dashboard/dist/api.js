export class DashboardApiClient {
    baseUrl;
    constructor(baseUrl = "") {
        this.baseUrl = baseUrl.replace(/\/$/, "");
    }
    async listRuns(environment, cursor, limit = 25) {
        const params = new URLSearchParams({ environment });
        if (cursor)
            params.set("cursor", cursor);
        if (limit)
            params.set("limit", String(limit));
        const res = await fetch(`${this.baseUrl}/v1/runs?${params.toString()}`);
        if (!res.ok) {
            throw new Error(`Failed to list runs (HTTP ${res.status}): ${res.statusText}`);
        }
        return res.json();
    }
    async getRun(runId) {
        const res = await fetch(`${this.baseUrl}/v1/runs/${encodeURIComponent(runId)}`);
        if (!res.ok) {
            throw new Error(`Failed to get run (HTTP ${res.status}): ${res.statusText}`);
        }
        return res.json();
    }
    async listWorkers(environment, cursor, limit = 25) {
        const params = new URLSearchParams({ environment });
        if (cursor)
            params.set("cursor", cursor);
        if (limit)
            params.set("limit", String(limit));
        const res = await fetch(`${this.baseUrl}/v1/workers?${params.toString()}`);
        if (!res.ok) {
            throw new Error(`Failed to list workers (HTTP ${res.status}): ${res.statusText}`);
        }
        return res.json();
    }
    async getRunEvents(runId, cursor, limit = 50) {
        const params = new URLSearchParams();
        if (cursor !== undefined && cursor !== null) {
            params.set("cursor", String(cursor));
        }
        if (limit) {
            params.set("limit", String(limit));
        }
        const q = params.toString() ? `?${params.toString()}` : "";
        const res = await fetch(`${this.baseUrl}/v1/runs/${encodeURIComponent(runId)}/events${q}`);
        if (!res.ok) {
            throw new Error(`Failed to get run events (HTTP ${res.status}): ${res.statusText}`);
        }
        return res.json();
    }
    async getRunLogs(runId, stepId, attemptId, cursor, limit = 50) {
        const params = new URLSearchParams();
        if (stepId)
            params.set("stepId", stepId);
        if (attemptId)
            params.set("attemptId", attemptId);
        if (cursor)
            params.set("cursor", cursor);
        if (limit)
            params.set("limit", String(limit));
        const q = params.toString() ? `?${params.toString()}` : "";
        const res = await fetch(`${this.baseUrl}/v1/runs/${encodeURIComponent(runId)}/logs${q}`);
        if (!res.ok) {
            throw new Error(`Failed to get run logs (HTTP ${res.status}): ${res.statusText}`);
        }
        return res.json();
    }
}
//# sourceMappingURL=api.js.map