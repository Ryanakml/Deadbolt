import { Run, Worker, RunSnapshot, RunEventsResponse, TaskLogsResponse } from "./types.js";
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
    user: {
        id: string;
        email: string;
        name: string;
    };
    active_organization_id: string | null;
    memberships: AuthMembership[];
}
export interface SwitchOrgResponse {
    session_id: string;
    active_organization_id: string;
    csrf_token: string;
}
export declare function apiFetch(input: string, init?: RequestInit): Promise<Response>;
export declare function isUnauthorized(err: unknown): boolean;
export declare function readCsrfToken(): string;
export declare class DashboardApiClient {
    private baseUrl;
    constructor(baseUrl?: string);
    getSession(): Promise<AuthSession | null>;
    switchOrganization(orgId: string): Promise<SwitchOrgResponse>;
    listRuns(environment: string, cursor?: string, limit?: number): Promise<ListRunsResponse>;
    getRun(runId: string): Promise<RunSnapshot>;
    listWorkers(environment: string, cursor?: string, limit?: number): Promise<ListWorkersResponse>;
    getRunEvents(runId: string, cursor?: number, limit?: number): Promise<RunEventsResponse>;
    getRunLogs(runId: string, stepId?: string, attemptId?: string, cursor?: string, limit?: number): Promise<TaskLogsResponse>;
}
//# sourceMappingURL=api.d.ts.map