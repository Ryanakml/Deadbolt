function activeMemberships(session) {
    return (session.memberships ?? []).filter((m) => m &&
        m.OrganizationID &&
        String(m.Status ?? "").toUpperCase() === "ACTIVE");
}
export function resolveOrgState(session) {
    if (!session)
        return { kind: "unauthenticated" };
    if (session.active_organization_id) {
        return { kind: "ready", orgId: session.active_organization_id };
    }
    const orgs = activeMemberships(session);
    if (orgs.length === 0)
        return { kind: "empty" };
    if (orgs.length === 1) {
        return {
            kind: "switch",
            orgId: orgs[0].OrganizationID,
            orgName: orgs[0].OrganizationName,
        };
    }
    return { kind: "select", orgs };
}
//# sourceMappingURL=auth.js.map