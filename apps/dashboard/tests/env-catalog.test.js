import test from "node:test";
import assert from "node:assert/strict";
import {
  DashboardApiClient,
  createEnvironmentSelection,
  formatEnvironmentLabel,
  getSelectedEnvironmentId,
  selectEnvironment,
  setSelectionCatalog,
  setSelectionOrganization,
} from "../dist/api.js";

const UUID_A = "11111111-1111-4111-8111-111111111111";
const UUID_B = "22222222-2222-4222-8222-222222222222";

function installFetch(routes) {
  const origFetch = globalThis.fetch;
  const seen = [];
  globalThis.fetch = async (input, init) => {
    const url = String(input);
    seen.push(url);
    for (const [match, handler] of routes) {
      if (typeof match === "string" ? url === match : match.test(url)) {
        return handler(url, init);
      }
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
  return {
    seen,
    restore() {
      globalThis.fetch = origFetch;
    },
  };
}

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// Two different projects both contain an environment named "staging".
// The catalog MUST keep both with distinct canonical UUIDs and
// project-disambiguated labels.
test("catalog keeps ambiguous staging names as distinct UUID options", async () => {
  const mock = installFetch([
    [
      "/v1/projects",
      () =>
        jsonResponse({
          projects: [
            { id: "proj-a", name: "order-service" },
            { id: "proj-b", name: "billing-service" },
          ],
        }),
    ],
    [
      /\/v1\/projects\/proj-a\/environments/,
      () => jsonResponse({ environments: [{ id: UUID_A, name: "staging" }] }),
    ],
    [
      /\/v1\/projects\/proj-b\/environments/,
      () => jsonResponse({ environments: [{ id: UUID_B, name: "staging" }] }),
    ],
  ]);
  try {
    const client = new DashboardApiClient();
    const catalog = await client.loadEnvironmentCatalog();
    assert.equal(catalog.length, 2);
    const ids = catalog.map((e) => e.environmentId).sort();
    assert.deepEqual(ids, [UUID_A, UUID_B].sort());
    // Labels carry project context so the operator can disambiguate.
    const labels = catalog.map(formatEnvironmentLabel).sort();
    assert.ok(labels[0].includes("billing-service"));
    assert.ok(labels[1].includes("order-service"));
    for (const label of labels) assert.ok(label.includes("staging"));

    // Selection is by UUID, never the ambiguous bare name.
    const sel = createEnvironmentSelection();
    setSelectionOrganization(sel, "org-1");
    setSelectionCatalog(sel, catalog);
    assert.equal(getSelectedEnvironmentId(sel), catalog[0].environmentId);
    assert.ok(
      getSelectedEnvironmentId(sel) === UUID_A ||
        getSelectedEnvironmentId(sel) === UUID_B,
    );
    assert.notEqual(getSelectedEnvironmentId(sel), "staging");
  } finally {
    mock.restore();
  }
});

test("listRuns uses environment=<uuid>, never the ambiguous name", async () => {
  const mock = installFetch([
    [
      /\/v1\/runs\?/,
      (url) => {
        assert.ok(
          url.includes(`environment=${encodeURIComponent(UUID_A)}`),
          `runs URL must carry UUID, got ${url}`,
        );
        assert.ok(
          !url.includes("environment=staging"),
          `runs URL must not carry bare name, got ${url}`,
        );
        return jsonResponse({ items: [] });
      },
    ],
  ]);
  try {
    const client = new DashboardApiClient();
    await client.listRuns(UUID_A);
    assert.equal(mock.seen.length, 1);
    assert.ok(mock.seen[0].includes("/v1/runs?"));
  } finally {
    mock.restore();
  }
});

test("listWorkers uses environment=<uuid>, never the ambiguous name", async () => {
  const mock = installFetch([
    [
      /\/v1\/workers\?/,
      (url) => {
        assert.ok(
          url.includes(`environment=${encodeURIComponent(UUID_B)}`),
          `workers URL must carry UUID, got ${url}`,
        );
        assert.ok(
          !url.includes("environment=staging"),
          `workers URL must not carry bare name, got ${url}`,
        );
        return jsonResponse({ items: [] });
      },
    ],
  ]);
  try {
    const client = new DashboardApiClient();
    await client.listWorkers(UUID_B);
    assert.equal(mock.seen.length, 1);
    assert.ok(mock.seen[0].includes("/v1/workers?"));
  } finally {
    mock.restore();
  }
});

test("switching organizations cannot retain a previous-org environment ID", () => {
  const sel = createEnvironmentSelection();
  setSelectionOrganization(sel, "org-a");
  setSelectionCatalog(sel, [
    {
      projectId: "proj-a",
      projectName: "order-service",
      environmentId: UUID_A,
      environmentName: "staging",
    },
  ]);
  selectEnvironment(sel, UUID_A);
  assert.equal(getSelectedEnvironmentId(sel), UUID_A);

  // Org switch clears catalog and selection.
  setSelectionOrganization(sel, "org-b");
  assert.equal(getSelectedEnvironmentId(sel), null);
  assert.deepEqual(sel.catalog, []);

  // Old UUID is no longer selectable under the new org.
  setSelectionCatalog(sel, [
    {
      projectId: "proj-c",
      projectName: "other",
      environmentId: UUID_B,
      environmentName: "staging",
    },
  ]);
  const kept = selectEnvironment(sel, UUID_A);
  assert.notEqual(kept, UUID_A);
  assert.equal(getSelectedEnvironmentId(sel), UUID_B);
});

test("zero-environment state selects nothing and issues no runs/workers request", async () => {
  const mock = installFetch([
    ["/v1/projects", () => jsonResponse({ projects: [] })],
  ]);
  try {
    const client = new DashboardApiClient();
    const catalog = await client.loadEnvironmentCatalog();
    assert.deepEqual(catalog, []);

    const sel = createEnvironmentSelection();
    setSelectionOrganization(sel, "org-1");
    const selected = setSelectionCatalog(sel, catalog);
    assert.equal(selected, null);
    assert.equal(getSelectedEnvironmentId(sel), null);

    // Documented UI flow: with no selection, no protected list request is
    // issued. Prove it by observing zero /v1/runs and /v1/workers calls.
    const runsOrWorkers = mock.seen.filter(
      (u) => u.includes("/v1/runs") || u.includes("/v1/workers"),
    );
    assert.deepEqual(runsOrWorkers, []);
    // Only the catalog discovery request happened.
    assert.ok(mock.seen.includes("/v1/projects"));
  } finally {
    mock.restore();
  }
});

test("zero environments inside existing projects also selects nothing", async () => {
  const mock = installFetch([
    [
      "/v1/projects",
      () =>
        jsonResponse({ projects: [{ id: "proj-a", name: "order-service" }] }),
    ],
    [
      /\/v1\/projects\/proj-a\/environments/,
      () => jsonResponse({ environments: [] }),
    ],
  ]);
  try {
    const client = new DashboardApiClient();
    const catalog = await client.loadEnvironmentCatalog();
    assert.deepEqual(catalog, []);
    const sel = createEnvironmentSelection();
    setSelectionOrganization(sel, "org-1");
    setSelectionCatalog(sel, catalog);
    assert.equal(getSelectedEnvironmentId(sel), null);
    const runsOrWorkers = mock.seen.filter(
      (u) => u.includes("/v1/runs") || u.includes("/v1/workers"),
    );
    assert.deepEqual(runsOrWorkers, []);
  } finally {
    mock.restore();
  }
});
