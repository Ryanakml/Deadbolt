import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { RunInspector } from "../dist/inspector.js";

describe("RunInspector", () => {
  test("increments snapshot state monotonically and updates attempts", () => {
    const inspector = new RunInspector("run-001");

    // Simulate loaded snapshot
    const initialSnapshot = {
      id: "run-001",
      workflowName: "order-processing",
      deploymentId: "dep-001",
      status: "QUEUED",
      revision: 1,
      lastEventSequence: 1,
      createdAt: new Date().toISOString(),
      steps: [
        {
          id: "step-1",
          nodeId: "charge",
          status: "READY",
          currentEpoch: 0,
          attempts: [],
        },
      ],
    };

    let updatedSnapshot = null;
    inspector.subscribe({
      onSnapshotUpdated: (s) => {
        updatedSnapshot = s;
      },
      onFreshnessChanged: () => {},
      onLogsUpdated: () => {},
      onError: () => {},
    });

    // Directly set snapshot for testing event reducer
    inspector["snapshot"] = initialSnapshot;

    // Apply attempt.claimed
    inspector.applyEvent({
      id: "evt-2",
      runId: "run-001",
      sequence: 2,
      schemaVersion: 1,
      type: "attempt.claimed",
      payload: {
        stepId: "step-1",
        attemptId: "att-1",
        attemptNumber: 1,
        epoch: 1,
        workerSessionId: "sess-abc",
      },
      committedAt: new Date().toISOString(),
    });

    assert.ok(updatedSnapshot);
    assert.equal(updatedSnapshot.lastEventSequence, 2);
    assert.equal(updatedSnapshot.steps[0].attempts.length, 1);
    assert.equal(updatedSnapshot.steps[0].attempts[0].status, "CLAIMED");
    assert.equal(
      updatedSnapshot.steps[0].attempts[0].workerSessionId,
      "sess-abc",
    );

    // Apply attempt.started
    inspector.applyEvent({
      id: "evt-3",
      runId: "run-001",
      sequence: 3,
      schemaVersion: 1,
      type: "attempt.started",
      payload: {
        attemptId: "att-1",
        epoch: 1,
        deadlineAt: new Date(Date.now() + 60000).toISOString(),
      },
      committedAt: new Date().toISOString(),
    });

    assert.equal(updatedSnapshot.status, "RUNNING");
    assert.equal(updatedSnapshot.steps[0].status, "RUNNING");
    assert.equal(updatedSnapshot.steps[0].attempts[0].status, "RUNNING");

    // Apply attempt.lost (worker abandoned/lost)
    inspector.applyEvent({
      id: "evt-4",
      runId: "run-001",
      sequence: 4,
      schemaVersion: 1,
      type: "attempt.lost",
      payload: {
        attemptId: "att-1",
        reason: "HEARTBEAT_EXPIRED",
        recovery: "HANDOFF_REQUIRED",
      },
      committedAt: new Date().toISOString(),
    });

    // Invariant: Honest handoff, never fabricating recovery before it commits
    assert.equal(updatedSnapshot.status, "WAITING");
    assert.equal(updatedSnapshot.steps[0].status, "WAITING");
    assert.equal(updatedSnapshot.steps[0].attempts[0].status, "LOST");

    // Apply attempt #2 claimed
    inspector.applyEvent({
      id: "evt-5",
      runId: "run-001",
      sequence: 5,
      schemaVersion: 1,
      type: "attempt.claimed",
      payload: {
        stepId: "step-1",
        attemptId: "att-2",
        attemptNumber: 2,
        epoch: 2,
        workerSessionId: "sess-def",
      },
      committedAt: new Date().toISOString(),
    });

    assert.equal(updatedSnapshot.steps[0].attempts.length, 2);
    assert.equal(updatedSnapshot.steps[0].attempts[1].status, "CLAIMED");
    assert.equal(updatedSnapshot.steps[0].attempts[1].attemptNumber, 2);

    // Apply attempt #2 completed
    inspector.applyEvent({
      id: "evt-6",
      runId: "run-001",
      sequence: 6,
      schemaVersion: 1,
      type: "attempt.completed",
      payload: {
        attemptId: "att-2",
      },
      committedAt: new Date().toISOString(),
    });

    assert.equal(updatedSnapshot.steps[0].attempts[1].status, "SUCCEEDED");
    assert.equal(updatedSnapshot.steps[0].status, "SUCCEEDED");

    // Apply run.succeeded
    inspector.applyEvent({
      id: "evt-7",
      runId: "run-001",
      sequence: 7,
      schemaVersion: 1,
      type: "run.succeeded",
      payload: {
        output: { chargeId: "ch_123" },
      },
      committedAt: new Date().toISOString(),
    });

    assert.equal(updatedSnapshot.status, "SUCCEEDED");
    assert.deepEqual(updatedSnapshot.output, { chargeId: "ch_123" });
  });
});
