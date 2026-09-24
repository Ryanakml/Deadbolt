// Inspector run-control permission data (Blueprint §23.2, §24.2).
//
// Pause, resume, and cancel all require the runs:control capability. The
// backend (RequireOrgScope) remains the security authority; this module
// mirrors the canonical role→capability map so the Inspector only renders a
// mutation CTA when the run state permits it AND the active identity is
// authorized. Authorization is derived from the BFF session membership —
// never inferred from button clicks or HTTP failures.
//
// Canonical roles (see internal/tenant/rbac.go):
//   - viewer: read-only (runs:read). No mutation CTAs.
//   - developer / operator / admin / owner: hold runs:control.
export const RUNS_CONTROL_CAPABILITY = "runs:control";
const RUNS_CONTROL_ROLES = new Set(["developer", "operator", "admin", "owner"]);
// roleCanControlRuns reports whether a canonical role grants runs:control.
export function roleCanControlRuns(role) {
    if (!role)
        return false;
    return RUNS_CONTROL_ROLES.has(role.trim().toLowerCase());
}
// sessionCanControlRuns reports whether the BFF session carries runs:control
// in the given organization via an ACTIVE membership. Unknown, inactive, or
// missing memberships fail closed.
export function sessionCanControlRuns(session, orgId) {
    if (!session || !orgId)
        return false;
    const memberships = session.memberships ?? [];
    for (const m of memberships) {
        if (!m || m.OrganizationID !== orgId)
            continue;
        if (String(m.Status ?? "").toUpperCase() !== "ACTIVE")
            continue;
        return roleCanControlRuns(m.Role);
    }
    return false;
}
// visibleRunControls is the single state+permission matrix for run mutation
// CTAs. A CTA renders only when the run state permits it and the active
// identity holds runs:control. Terminal states and CANCELLING never offer
// pause/resume; CANCELLING also hides cancel because cancellation is already
// committed (the backend stays idempotent for duplicate submits).
export function visibleRunControls(status, canControl) {
    if (!canControl)
        return { pause: false, resume: false, cancel: false };
    switch (status) {
        case "QUEUED":
        case "RUNNING":
        case "WAITING":
            return { pause: true, resume: false, cancel: true };
        case "PAUSING":
        case "PAUSED":
            return { pause: false, resume: true, cancel: true };
        default:
            return { pause: false, resume: false, cancel: false };
    }
}
//# sourceMappingURL=permissions.js.map