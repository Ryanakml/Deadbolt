import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { RunInspector } from "../dist/inspector.js";
import { DashboardApiClient } from "../dist/api.js";

describe("pause and resume controls", () => {
  test("pauseRun sends revision with an Idempotency-Key", async () => {
    const seen = [];
    const origFetch = globalThis.fetch;
    globalThis.fetch = async (input, init) => {
      seen.push([input, init]);
      return new Response(
        JSON.stringify({ id: "run-1", status: "PAUSING", revision: 4 }),
        { status: 200 },
      );
    };
    try {
      const client = new DashboardApiClient();
      const first = await client.pauseRun("run-1", 3, "pause-key-1");
      assert.equal(first.status, "PAUSING");
      assert.equal(seen.length, 1);
      const body = JSON.parse(seen[0][1].body);
      assert.deepEqual(body, { expectedRevision: 3 });
      assert.equal(seen[0][1].headers["Idempotency-Key"], "pause-key-1");
      assert.ok(String(seen[0][0]).endsWith("/v1/runs/run-1/pause"));
    } finally {
      globalThis.fetch = origFetch;
    }
  });

  test("resumeRun sends revision with an Idempotency-Key", async () => {
    const seen = [];
    const origFetch = globalThis.fetch;
    globalThis.fetch = async (input, init) => {
      seen.push([input, init]);
      return new Response(
        JSON.stringify({ id: "run-1", status: "RUNNING", revision: 5 }),
        { status: 200 },
      );
    };
    try {
      const client = new DashboardApiClient();
      const first = await client.resumeRun("run-1", 4, "resume-key-1");
      assert.equal(first.status, "RUNNING");
      assert.equal(seen.length, 1);
      const body = JSON.parse(seen[0][1].body);
      assert.deepEqual(body, { expectedRevision: 4 });
      assert.equal(seen[0][1].headers["Idempotency-Key"], "resume-key-1");
      assert.ok(String(seen[0][0]).endsWith("/v1/runs/run-1/resume"));
    } finally {
      globalThis.fetch = origFetch;
    }
  });

  test("inspector applies run.pausing, run.paused, and run.resumed events", () => {
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

    // 1. run.pausing
    inspector.applyEvent({
      id: "ev-5",
      runId: "run-1",
      sequence: 5,
      schemaVersion: 1,
      type: "run.pausing",
      payload: { reason: "PAUSE_REQUESTED", activeAttempts: 1 },
      committedAt: at,
    });
    assert.equal(inspector["snapshot"].status, "PAUSING");
    assert.equal(inspector["snapshot"].reasonCode, "PAUSE_REQUESTED");

    // 2. run.paused
    inspector.applyEvent({
      id: "ev-6",
      runId: "run-1",
      sequence: 6,
      schemaVersion: 1,
      type: "run.paused",
      payload: { reason: "PAUSE_REQUESTED" },
      committedAt: at,
    });
    assert.equal(inspector["snapshot"].status, "PAUSED");

    // 3. run.resumed
    inspector.applyEvent({
      id: "ev-7",
      runId: "run-1",
      sequence: 7,
      schemaVersion: 1,
      type: "run.resumed",
      payload: { status: "RUNNING" },
      committedAt: at,
    });
    assert.equal(inspector["snapshot"].status, "RUNNING");
    assert.equal(inspector["snapshot"].reasonCode, undefined);
  });
});
