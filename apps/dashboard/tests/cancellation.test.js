import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { RunInspector, terminationBannerText } from "../dist/inspector.js";
import { DashboardApiClient } from "../dist/api.js";

describe("cancellation", () => {
  test("terminationBannerText distinguishes settling from unconfirmed", () => {
    const settling = terminationBannerText("CANCELLING", null);
    assert.ok(settling && /waiting for workers/i.test(settling));
    const unconfirmed = terminationBannerText("CANCELLED", false);
    assert.ok(unconfirmed && /unconfirmed/i.test(unconfirmed));
    assert.ok(/not.*rolled back/i.test(unconfirmed));
    assert.equal(terminationBannerText("CANCELLED", true), null);
    assert.equal(terminationBannerText("RUNNING", null), null);
    assert.equal(terminationBannerText("CANCELLED", null), null);
  });

  test("cancelRun sends revision with an Idempotency-Key", async () => {
    const seen = [];
    const origFetch = globalThis.fetch;
    globalThis.fetch = async (input, init) => {
      seen.push([input, init]);
      return new Response(
        JSON.stringify({ id: "run-1", status: "CANCELLING" }),
        {
          status: 200,
        },
      );
    };
    try {
      const client = new DashboardApiClient();
      const first = await client.cancelRun("run-1", 3, "cancel-key-1");
      const second = await client.cancelRun("run-1", 3, "cancel-key-1");
      assert.equal(first.status, "CANCELLING");
      assert.equal(seen.length, 2);
      const bodies = seen.map(([, init]) => JSON.parse(init.body));
      assert.deepEqual(bodies[0], { expectedRevision: 3 });
      assert.equal(seen[0][1].headers["Idempotency-Key"], "cancel-key-1");
      assert.equal(seen[1][1].headers["Idempotency-Key"], "cancel-key-1");
      assert.ok(String(seen[0][0]).endsWith("/v1/runs/run-1/cancel"));
      assert.deepEqual(second, first);
    } finally {
      globalThis.fetch = origFetch;
    }
  });

  test("run.cancelling and run.cancelled converge without rollback promises", () => {
    const inspector = new RunInspector("run-1");
    const snap = {
      id: "run-1",
      workflowName: "w",
      deploymentId: "dep-1",
      status: "RUNNING",
      revision: 2,
      lastEventSequence: 4,
      createdAt: new Date().toISOString(),
      steps: [],
    };
    inspector["snapshot"] = snap;
    const at = new Date().toISOString();
    inspector.applyEvent({
      id: "ev-5",
      runId: "run-1",
      sequence: 5,
      schemaVersion: 1,
      type: "run.cancelling",
      payload: { reason: "CANCEL_REQUESTED" },
      committedAt: at,
    });
    assert.equal(inspector["snapshot"].status, "CANCELLING");
    inspector.applyEvent({
      id: "ev-6",
      runId: "run-1",
      sequence: 6,
      schemaVersion: 1,
      type: "run.cancelled",
      payload: { reason: "CANCEL_REQUESTED", terminationConfirmed: false },
      committedAt: at,
    });
    assert.equal(inspector["snapshot"].status, "CANCELLED");
    assert.equal(inspector["snapshot"].terminationConfirmed, false);
  });
});
