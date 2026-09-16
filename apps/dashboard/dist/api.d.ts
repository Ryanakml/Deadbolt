import { Run, Worker, RunSnapshot, RunEventsResponse, TaskLogsResponse } from "./types.js";
export interface ListRunsResponse {
    items: Run[];
    nextCursor?: string | null;
}
export interface ListWorkersResponse {
    items: Worker[];
    nextCursor?: string | null;
}
export declare class DashboardApiClient {
    private baseUrl;
    constructor(baseUrl?: string);
    listRuns(environment: string, cursor?: string, limit?: number): Promise<ListRunsResponse>;
    getRun(runId: string): Promise<RunSnapshot>;
    listWorkers(environment: string, cursor?: string, limit?: number): Promise<ListWorkersResponse>;
    getRunEvents(runId: string, cursor?: number, limit?: number): Promise<RunEventsResponse>;
    getRunLogs(runId: string, stepId?: string, attemptId?: string, cursor?: string, limit?: number): Promise<TaskLogsResponse>;
}
//# sourceMappingURL=api.d.ts.map