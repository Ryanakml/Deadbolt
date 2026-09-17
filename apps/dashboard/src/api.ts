import {
  Run,
  Worker,
  RunSnapshot,
  RunEventsResponse,
  TaskLogsResponse,
} from "./types.js";

export interface ListRunsResponse {
  items: Run[];
  nextCursor?: string | null;
}

export interface ListWorkersResponse {
  items: Worker[];
  nextCursor?: string | null;
}

export interface AuthMembership {
  OrganizationID: string;
  OrganizationName: string;
  Role: string;
  Status: string;
}

export interface AuthSession {
  user: { id: string; email: string; name: string };
  active_organization_id: string | null;
  memberships: AuthMembership[];
}

export interface SwitchOrgResponse {
  session_id: string;
  active_organization_id: string;
  csrf_token: string;
}

// apiFetch keeps every dashboard request on same-origin cookies explicitly.
// The dashboard never handles bearer tokens; authentication is the HttpOnly
// BFF session cookie.
export async function apiFetch(
  input: string,
  init: RequestInit = {},
): Promise<Response> {
  return fetch(input, { credentials: "same-origin", ...init });
}

export function isUnauthorized(err: unknown): boolean {
  return err instanceof Error && err.message.includes("HTTP 401");
}

// readCsrfToken reads the SPA-readable CSRF bootstrap cookie without ever
// touching the HttpOnly session value. Both hosted (__Host-) and local
// (non-secure) cookie names are accepted.
export function readCsrfToken(): string {
  if (typeof document === "undefined") return "";
  for (const part of document.cookie.split(";")) {
    const [rawName, ...rest] = part.trim().split("=");
    const name = rawName.trim();
    if (name === "__Host-csrf_token" || name === "deadbolt_local_csrf") {
      return decodeURIComponent(rest.join("=").trim());
    }
  }
  return "";
}

export class DashboardApiClient {
  private baseUrl: string;

  constructor(baseUrl = "") {
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  // getSession returns the current BFF session, or null when the browser is
  // unauthenticated. Other failures are thrown for the bootstrap to render.
  public async getSession(): Promise<AuthSession | null> {
    const res = await apiFetch(`${this.baseUrl}/api/auth/session`);
    if (res.status === 401) return null;
    if (!res.ok) {
      throw new Error(
        `Failed to get session (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<AuthSession>;
  }

  // switchOrganization establishes the session's active organization. The
  // server rotates session cookies; CSRF rules still apply.
  public async switchOrganization(orgId: string): Promise<SwitchOrgResponse> {
    const res = await apiFetch(`${this.baseUrl}/api/auth/switch-org`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": readCsrfToken(),
      },
      body: JSON.stringify({ organization_id: orgId }),
    });
    if (!res.ok) {
      throw new Error(
        `Failed to switch organization (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<SwitchOrgResponse>;
  }

  public async listRuns(
    environment: string,
    cursor?: string,
    limit = 25,
  ): Promise<ListRunsResponse> {
    const params = new URLSearchParams({ environment });
    if (cursor) params.set("cursor", cursor);
    if (limit) params.set("limit", String(limit));

    const res = await apiFetch(`${this.baseUrl}/v1/runs?${params.toString()}`);
    if (!res.ok) {
      throw new Error(
        `Failed to list runs (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<ListRunsResponse>;
  }

  public async getRun(runId: string): Promise<RunSnapshot> {
    const res = await apiFetch(
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

    const res = await apiFetch(
      `${this.baseUrl}/v1/workers?${params.toString()}`,
    );
    if (!res.ok) {
      throw new Error(
        `Failed to list workers (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<ListWorkersResponse>;
  }

  public async getRunEvents(
    runId: string,
    cursor?: number,
    limit = 50,
  ): Promise<RunEventsResponse> {
    const params = new URLSearchParams();
    if (cursor !== undefined && cursor !== null) {
      params.set("cursor", String(cursor));
    }
    if (limit) {
      params.set("limit", String(limit));
    }
    const q = params.toString() ? `?${params.toString()}` : "";
    const res = await apiFetch(
      `${this.baseUrl}/v1/runs/${encodeURIComponent(runId)}/events${q}`,
    );
    if (!res.ok) {
      throw new Error(
        `Failed to get run events (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<RunEventsResponse>;
  }

  public async getRunLogs(
    runId: string,
    stepId?: string,
    attemptId?: string,
    cursor?: string,
    limit = 50,
  ): Promise<TaskLogsResponse> {
    const params = new URLSearchParams();
    if (stepId) params.set("stepId", stepId);
    if (attemptId) params.set("attemptId", attemptId);
    if (cursor) params.set("cursor", cursor);
    if (limit) params.set("limit", String(limit));

    const q = params.toString() ? `?${params.toString()}` : "";
    const res = await apiFetch(
      `${this.baseUrl}/v1/runs/${encodeURIComponent(runId)}/logs${q}`,
    );
    if (!res.ok) {
      throw new Error(
        `Failed to get run logs (HTTP ${res.status}): ${res.statusText}`,
      );
    }
    return res.json() as Promise<TaskLogsResponse>;
  }
}
