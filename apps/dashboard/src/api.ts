import { Run, Worker, RunSnapshot } from "./types.js";

export interface ListRunsResponse {
  items: Run[];
  nextCursor?: string | null;
}

export interface ListWorkersResponse {
  items: Worker[];
  nextCursor?: string | null;
}

export class DashboardApiClient {
  private baseUrl: string;

  constructor(baseUrl = "") {
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  public async listRuns(
    environment: string,
    cursor?: string,
    limit = 25,
  ): Promise<ListRunsResponse> {
    const params = new URLSearchParams({ environment });
    if (cursor) params.set("cursor", cursor);
    if (limit) params.set("limit", String(limit));

    const res = await fetch(`${this.baseUrl}/v1/runs?${params.toString()}`);
    if (!res.ok) {
      throw new Error(
        `Failed to list runs (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<ListRunsResponse>;
  }

  public async getRun(runId: string): Promise<RunSnapshot> {
    const res = await fetch(
      `${this.baseUrl}/v1/runs/${encodeURIComponent(runId)}`,
    );
    if (!res.ok) {
      throw new Error(
        `Failed to get run (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<RunSnapshot>;
  }

  public async listWorkers(
    environment: string,
    cursor?: string,
    limit = 25,
  ): Promise<ListWorkersResponse> {
    const params = new URLSearchParams({ environment });
    if (cursor) params.set("cursor", cursor);
    if (limit) params.set("limit", String(limit));

    const res = await fetch(`${this.baseUrl}/v1/workers?${params.toString()}`);
    if (!res.ok) {
      throw new Error(
        `Failed to list workers (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<ListWorkersResponse>;
  }
}
