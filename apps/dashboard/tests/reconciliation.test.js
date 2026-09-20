import { test, describe } from "node:test";
import assert from "node:assert/strict";
import {
  RunInspector,
  openCaseForStep,
  reconciliationHoldText,
  RESOLVE_ACTIONS,
} from "../dist/inspector.js";
import {
  DashboardApiClient,
  isConflict,
  newIdempotencyKey,
} from "../dist/api.js";

function holdSnapshot() {
  return {
    id: "run-001",
    workflowName: "research-report",
    deploymentId: "dep-001",
    status: "WAITING",
    reasonCode: "RECONCILIATION",
    revision: 1,
    lastEventSequence: 7,
    createdAt: new Date().toISOString(),
    steps: [
      {
        id: "step-1",
        nodeId: "charge",
        status: "WAITING",
        currentEpoch: 1,
        attempts: [{ id: "att-1", attemptNumber: 1, status: "LOST" }],
      },
    ],
    reconciliationCases: [
      {
        id: "case-1",
        stepId: "step-1",
        attemptId: "att-1",
        reason: "AMBIGUOUS_OUTCOME",
        evidence: { operationId: "op_abc" },
        status: "OPEN",
        revision: 1,
        createdAt: new Date().toISOString(),
      },
      {
        id: "case-old",
        stepId: "step-1",
        reason: "AMBIGUOUS_OUTCOME",
        status: "RESOLVED",
        resolution: "RETRY",
        revision: 2,
        createdAt: new Date().toISOString(),
      },
    ],
  };
}

describe("reconciliation holds", () => {
  test("openCaseForStep returns only the OPEN case for the step", () => {
    const snap = holdSnapshot();
    const found = openCaseForStep(snap, "step-1");
    assert.equal(found?.id, "case-1");
    assert.equal(openCaseForStep(snap, "step-2"), null);
    assert.equal(
      openCaseForStep({ ...snap, reconciliationCases: [] }, "step-1"),
      null,
    );
    assert.equal(
      openCaseForStep({ ...snap, reconciliationCases: undefined }, "step-1"),
      null,
    );
  });

  test("reconciliationHoldText never claims retry is safe", () => {
    for (const reason of [
      "AMBIGUOUS_OUTCOME",
      "IDEMPOTENCY_WINDOW_INSUFFICIENT",
      "IDEMPOTENCY_WINDOW_UNKNOWN",
      undefined,
      "SOMETHING_NEW",
    ]) {
      const text = reconciliationHoldText(reason);
      assert.ok(text.length > 0);
      assert.ok(!/safe to retry/i.test(text));
      assert.ok(/unknown/i.test(text));
    }
  });

  test("RESOLVE_ACTIONS exposes exactly the three audited decisions", () => {
    assert.deepEqual(
      RESOLVE_ACTIONS.map((a) => a.action),
      ["confirm_succeeded", "confirm_not_executed_retry", "fail_run"],
    );
    assert.deepEqual(
      RESOLVE_ACTIONS.map((a) => a.needsResult),
      [true, false, false],
    );
  });

  test("isConflict detects 409 revision conflicts for dialog refresh", () => {
    assert.equal(
      isConflict(new Error("Failed to resolve case (HTTP 409): Conflict")),
      true,
    );
    assert.equal(
      isConflict(new Error("Failed to resolve case (HTTP 422): Unprocessable")),
      false,
    );
  });

  test("resolve sends a unique Idempotency-Key and reuses the dialog key", async () => {
    const seen = [];
    const origFetch = globalThis.fetch;
    globalThis.fetch = async (input, init) => {
      seen.push([input, init]);
      return new Response(
        JSON.stringify({ caseId: "case-1", resolved: true, revision: 2 }),
        { status: 200 },
      );
    };
    try {
      const client = new DashboardApiClient();
      const body = {
        action: "fail_run",
        evidence: "prov-x",
        reason: "duplicate confirmed",
        expectedRevision: 1,
      };
      await client.resolveReconciliationCase("case-1", body);
      await client.resolveReconciliationCase("case-1", body, "dialog-key-1");
      await client.resolveReconciliationCase("case-1", body, "dialog-key-1");
      assert.equal(seen.length, 3);
      const keys = seen.map(([, init]) => init.headers["Idempotency-Key"]);
      assert.ok(keys[0] && typeof keys[0] === "string");
      assert.equal(keys[1], "dialog-key-1");
      assert.equal(keys[2], "dialog-key-1");
      assert.notEqual(keys[0], "dialog-key-1");
      assert.equal(newIdempotencyKey() === newIdempotencyKey(), false);
    } finally {
      globalThis.fetch = origFetch;
    }
  });

  test("step.waiting parks the step without inventing success", () => {
    const inspector = new RunInspector("run-001");
    const snap = holdSnapshot();
    snap.steps[0].status = "RUNNING";
    inspector["snapshot"] = snap;
    inspector.applyEvent({
      id: "ev-8",
      runId: "run-001",
      sequence: 8,
      schemaVersion: 1,
      type: "step.waiting",
      payload: { stepId: "step-1", reason: "RECONCILIATION" },
      committedAt: new Date().toISOString(),
    });
    assert.equal(inspector["snapshot"].steps[0].status, "WAITING");
  });

  test("run.resumed converges on the authoritative snapshot", async () => {
    const inspector = new RunInspector("run-001");
    const snap = holdSnapshot();
    inspector["snapshot"] = snap;
    const refreshed = { ...snap, status: "RUNNING", lastEventSequence: 9 };
    const origFetch = globalThis.fetch;
    globalThis.fetch = async () => ({
      ok: true,
      json: async () => refreshed,
    });
    try {
      inspector.applyEvent({
        id: "ev-9",
        runId: "run-001",
        sequence: 9,
        schemaVersion: 1,
        type: "run.resumed",
        payload: { trigger: "confirm_not_executed_retry" },
        committedAt: new Date().toISOString(),
      });
      await new Promise((r) => setTimeout(r, 20));
      assert.equal(inspector["snapshot"].status, "RUNNING");
    } finally {
      globalThis.fetch = origFetch;
    }
  });
});
