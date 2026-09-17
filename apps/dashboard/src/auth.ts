import type { AuthMembership, AuthSession } from "./api.js";

export type OrgState =
  | { kind: "unauthenticated" }
  | { kind: "ready"; orgId: string }
  | { kind: "switch"; orgId: string; orgName: string }
  | { kind: "select"; orgs: AuthMembership[] }
  | { kind: "empty" };

function activeMemberships(session: AuthSession): AuthMembership[] {
  return (session.memberships ?? []).filter(
    (m) =>
      m &&
      m.OrganizationID &&
      String(m.Status ?? "").toUpperCase() === "ACTIVE",
  );
}

// resolveOrgState decides how the dashboard enters Issue #14 using only the
// BFF session. It never silently picks an arbitrary organization when several
// are available.
export function resolveOrgState(session: null): {
  kind: "unauthenticated";
};
export function resolveOrgState(
  session: AuthSession,
): Exclude<OrgState, { kind: "unauthenticated" }>;
export function resolveOrgState(session: AuthSession | null): OrgState {
  if (!session) return { kind: "unauthenticated" };
  if (session.active_organization_id) {
    return { kind: "ready", orgId: session.active_organization_id };
  }
  const orgs = activeMemberships(session);
  if (orgs.length === 0) return { kind: "empty" };
  if (orgs.length === 1) {
    return {
      kind: "switch",
      orgId: orgs[0].OrganizationID,
      orgName: orgs[0].OrganizationName,
    };
  }
  return { kind: "select", orgs };
}
