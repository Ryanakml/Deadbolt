import type { AuthMembership, AuthSession } from "./api.js";
export type OrgState = {
    kind: "unauthenticated";
} | {
    kind: "ready";
    orgId: string;
} | {
    kind: "switch";
    orgId: string;
    orgName: string;
} | {
    kind: "select";
    orgs: AuthMembership[];
} | {
    kind: "empty";
};
export declare function resolveOrgState(session: null): {
    kind: "unauthenticated";
};
export declare function resolveOrgState(session: AuthSession): Exclude<OrgState, {
    kind: "unauthenticated";
}>;
//# sourceMappingURL=auth.d.ts.map