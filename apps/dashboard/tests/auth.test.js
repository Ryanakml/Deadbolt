import test from "node:test";
import assert from "node:assert/strict";
import { DashboardApiClient, readCsrfToken } from "../dist/api.js";
import { resolveOrgState } from "../dist/auth.js";

const orgA = {
  OrganizationID: "org-a",
  OrganizationName: "Alpha",
  Role: "Owner",
  Status: "ACTIVE",
};
const orgB = {
  OrganizationID: "org-b",
  OrganizationName: "Beta",
  Role: "Viewer",
  Status: "ACTIVE",
};

test("resolveOrgState requires login when unauthenticated", () => {
  assert.deepEqual(resolveOrgState(null), { kind: "unauthenticated" });
});

test("resolveOrgState enters when the session already has an active org", () => {
  assert.deepEqual(
    resolveOrgState({
      user: { id: "u", email: "e", name: "n" },
      active_organization_id: "org-a",
      memberships: [orgA],
    }),
    { kind: "ready", orgId: "org-a" },
  );
});

test("resolveOrgState switches deterministically with one membership", () => {
  assert.deepEqual(
    resolveOrgState({
      user: { id: "u", email: "e", name: "n" },
      active_organization_id: null,
      memberships: [orgA],
    }),
    { kind: "switch", orgId: "org-a", orgName: "Alpha" },
  );
});

test("resolveOrgState requires explicit selection with several memberships", () => {
  const state = resolveOrgState({
    user: { id: "u", email: "e", name: "n" },
    active_organization_id: null,
    memberships: [orgA, orgB],
  });
  assert.equal(state.kind, "select");
  assert.equal(state.orgs.length, 2);
});

test("resolveOrgState reports empty with no active memberships", () => {
  assert.deepEqual(
    resolveOrgState({
      user: { id: "u", email: "e", name: "n" },
      active_organization_id: null,
      memberships: [{ ...orgA, Status: "SUSPENDED" }],
    }),
    { kind: "empty" },
  );
  assert.deepEqual(
    resolveOrgState({
      user: { id: "u", email: "e", name: "n" },
      active_organization_id: null,
      memberships: [],
    }),
    { kind: "empty" },
  );
});

test("getSession maps 401 to unauthenticated without throwing", async () => {
  const origFetch = globalThis.fetch;
  const seen = [];
  globalThis.fetch = async (input, init) => {
    seen.push([input, init]);
    return new Response("{}", { status: 401 });
  };
  try {
    const client = new DashboardApiClient();
    assert.equal(await client.getSession(), null);
    assert.equal(seen.length, 1);
    assert.equal(seen[0][0], "/api/auth/session");
    assert.equal(seen[0][1]?.credentials, "same-origin");
  } finally {
    globalThis.fetch = origFetch;
  }
});

test("getSession returns the session when authenticated", async () => {
  const origFetch = globalThis.fetch;
  const session = {
    user: { id: "u", email: "e@x.io", name: "n" },
    active_organization_id: "org-a",
    memberships: [orgA],
  };
  globalThis.fetch = async () =>
    new Response(JSON.stringify(session), { status: 200 });
  try {
    const client = new DashboardApiClient();
    assert.deepEqual(await client.getSession(), session);
  } finally {
    globalThis.fetch = origFetch;
  }
});

test("switchOrganization posts org with CSRF token and parses response", async () => {
  const origFetch = globalThis.fetch;
  const origDocument = globalThis.document;
  globalThis.document = { cookie: "__Host-csrf_token=csrf-123; other=x" };
  const seen = [];
  globalThis.fetch = async (input, init) => {
    seen.push([input, init]);
    return new Response(
      JSON.stringify({
        session_id: "s",
        active_organization_id: "org-b",
        csrf_token: "csrf-456",
      }),
      { status: 200 },
    );
  };
  try {
    const client = new DashboardApiClient();
    const out = await client.switchOrganization("org-b");
    assert.equal(out.active_organization_id, "org-b");
    const [url, init] = seen[0];
    assert.equal(url, "/api/auth/switch-org");
    assert.equal(init.method, "POST");
    assert.equal(init.headers["X-CSRF-Token"], "csrf-123");
    assert.deepEqual(JSON.parse(init.body), { organization_id: "org-b" });
  } finally {
    globalThis.fetch = origFetch;
    if (origDocument === undefined) delete globalThis.document;
    else globalThis.document = origDocument;
  }
});

test("readCsrfToken accepts hosted and local cookie names only", () => {
  const origDocument = globalThis.document;
  try {
    globalThis.document = { cookie: "a=b; __Host-csrf_token=host-csrf" };
    assert.equal(readCsrfToken(), "host-csrf");
    globalThis.document = { cookie: "deadbolt_local_csrf=local-csrf" };
    assert.equal(readCsrfToken(), "local-csrf");
    globalThis.document = { cookie: "session=abc" };
    assert.equal(readCsrfToken(), "");
  } finally {
    if (origDocument === undefined) delete globalThis.document;
    else globalThis.document = origDocument;
  }
});
