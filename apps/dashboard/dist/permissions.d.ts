import type { AuthSession } from "./api.js";
import type { RunStatus } from "./types.js";
export declare const RUNS_CONTROL_CAPABILITY = "runs:control";
export declare function roleCanControlRuns(role: string | null | undefined): boolean;
export declare function sessionCanControlRuns(session: AuthSession | null | undefined, orgId: string | null | undefined): boolean;
export interface RunControlVisibility {
    pause: boolean;
    resume: boolean;
    cancel: boolean;
}
export declare function visibleRunControls(status: RunStatus, canControl: boolean): RunControlVisibility;
//# sourceMappingURL=permissions.d.ts.map