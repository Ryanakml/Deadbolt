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
        outcome: "SUCCEEDED",
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

  test("correctly maps attempt.completed outcomes (FAILED, TIMED_OUT, CANCELLED) and authoritative committedAt", () => {
    const inspector = new RunInspector("run-002");
    const authoritativeTime = "2026-09-16T12:00:00.000Z";

    let snapshot = {
      id: "run-002",
      workflowName: "failover-flow",
      deploymentId: "dep-001",
      status: "RUNNING",
      revision: 1,
      lastEventSequence: 1,
      createdAt: new Date().toISOString(),
      steps: [
        {
          id: "step-1",
          nodeId: "task-node",
          status: "RUNNING",
          currentEpoch: 1,
          attempts: [
            {
              id: "att-1",
              attemptNumber: 1,
              status: "RUNNING",
              workerSessionId: "sess-1",
            },
          ],
        },
      ],
    };

    inspector["snapshot"] = snapshot;

    // 1. FAILED outcome
    inspector.applyEvent({
      id: "evt-failed",
      runId: "run-002",
      sequence: 2,
      schemaVersion: 1,
      type: "attempt.completed",
      payload: {
        attemptId: "att-1",
        outcome: "FAILED",
        error: { code: "TASK_FAILED", message: "boom" },
      },
      committedAt: authoritativeTime,
    });

    assert.equal(snapshot.steps[0].attempts[0].status, "FAILED");
    assert.equal(snapshot.steps[0].status, "FAILED");
    assert.equal(snapshot.steps[0].attempts[0].completedAt, authoritativeTime);
    assert.deepEqual(snapshot.steps[0].attempts[0].error, {
      code: "TASK_FAILED",
      message: "boom",
    });

    // 2. TIMED_OUT outcome
    snapshot.steps[0].attempts[0].status = "RUNNING";
    inspector.applyEvent({
      id: "evt-timeout",
      runId: "run-002",
      sequence: 3,
      schemaVersion: 1,
      type: "attempt.completed",
      payload: {
        attemptId: "att-1",
        outcome: "TIMED_OUT",
      },
      committedAt: authoritativeTime,
    });

    assert.equal(snapshot.steps[0].attempts[0].status, "TIMED_OUT");
    assert.equal(snapshot.steps[0].status, "FAILED");

    // 3. CANCELLED outcome
    snapshot.steps[0].attempts[0].status = "RUNNING";
    inspector.applyEvent({
      id: "evt-cancel",
      runId: "run-002",
      sequence: 4,
      schemaVersion: 1,
      type: "attempt.completed",
      payload: {
        attemptId: "att-1",
        outcome: "CANCELLED",
      },
      committedAt: authoritativeTime,
    });

    assert.equal(snapshot.steps[0].attempts[0].status, "CANCELLED");
    assert.equal(snapshot.steps[0].status, "FAILED");
  });

  test("does not fabricate success when a malformed completion event lacks its committed outcome", () => {
    const inspector = new RunInspector("run-unknown-outcome");
    const snapshot = {
      id: "run-unknown-outcome",
      workflowName: "flow",
      deploymentId: "dep-001",
      status: "RUNNING",
      revision: 1,
      lastEventSequence: 1,
      createdAt: "2026-09-16T12:00:00.000Z",
      steps: [
        {
          id: "step-1",
          nodeId: "task",
          status: "RUNNING",
          currentEpoch: 1,
          attempts: [{ id: "att-1", attemptNumber: 1, status: "RUNNING" }],
        },
      ],
    };
    inspector["snapshot"] = snapshot;

    inspector.applyEvent({
      id: "evt-2",
      runId: "run-unknown-outcome",
      sequence: 2,
      schemaVersion: 1,
      type: "attempt.completed",
      payload: { attemptId: "att-1" },
      committedAt: "2026-09-16T12:00:01.000Z",
    });

    assert.equal(snapshot.steps[0].attempts[0].status, "RUNNING");
    assert.equal(snapshot.steps[0].status, "RUNNING");
    assert.equal(snapshot.steps[0].attempts[0].completedAt, undefined);
  });

  test("deduplicates and sorts events in timeline", () => {
    const inspector = new RunInspector("run-003");
    let receivedEvents = [];

    inspector.subscribe({
      onEventsUpdated: (evs) => {
        receivedEvents = evs;
      },
    });

    inspector.applyEvent({
      id: "e-1",
      runId: "run-003",
      sequence: 1,
      schemaVersion: 1,
      type: "run.created",
      payload: {},
      committedAt: "2026-09-16T12:00:01.000Z",
    });

    inspector.applyEvent({
      id: "e-3",
      runId: "run-003",
      sequence: 3,
      schemaVersion: 1,
      type: "step.ready",
      payload: {},
      committedAt: "2026-09-16T12:00:03.000Z",
    });

    // Duplicate event #1 should not be added again
    inspector.applyEvent({
      id: "e-1-dup",
      runId: "run-003",
      sequence: 1,
      schemaVersion: 1,
      type: "run.created",
      payload: {},
      committedAt: "2026-09-16T12:00:01.000Z",
    });

    // Event #2 arriving out of order should be sorted in
    inspector.applyEvent({
      id: "e-2",
      runId: "run-003",
      sequence: 2,
      schemaVersion: 1,
      type: "step.ready",
      payload: {},
      committedAt: "2026-09-16T12:00:02.000Z",
    });

    assert.equal(receivedEvents.length, 3);
    assert.equal(receivedEvents[0].sequence, 1);
    assert.equal(receivedEvents[1].sequence, 2);
    assert.equal(receivedEvents[2].sequence, 3);
  });

  test("merges history with an SSE event received before the history request resolves", async () => {
    const inspector = new RunInspector("run-history-race");
    const originalFetch = globalThis.fetch;
    let resolveHistory;
    const history = new Promise((resolve) => {
      resolveHistory = resolve;
    });
    globalThis.fetch = async () => ({
      ok: true,
      statusText: "OK",
      json: async () => history,
    });

    try {
      const historyRequest = inspector.fetchEvents();
      inspector.applyEvent({
        id: "live-2",
        runId: "run-history-race",
        sequence: 2,
        schemaVersion: 1,
        type: "step.ready",
        payload: { stepId: "step-1" },
        committedAt: "2026-09-16T12:00:02.000Z",
      });
      resolveHistory({
        events: [
          {
            id: "history-1",
            runId: "run-history-race",
            sequence: 1,
            schemaVersion: 1,
            type: "run.created",
            payload: {},
            committedAt: "2026-09-16T12:00:01.000Z",
          },
        ],
        hasMore: false,
        nextCursor: null,
      });
      await historyRequest;

      assert.deepEqual(
        inspector.getEvents().map((event) => event.sequence),
        [1, 2],
      );
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});
