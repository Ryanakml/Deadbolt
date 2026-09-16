import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { RunEventStreamClient } from "../dist/stream.js";

describe("RunEventStreamClient", () => {
  test("discards duplicate events with sequence <= lastProcessedSequence", () => {
    const received = [];
    const client = new RunEventStreamClient({
      runId: "run-123",
      initialSequence: 5,
      onEvent: (ev) => received.push(ev),
      onFreshnessChange: () => {},
      onResyncRequired: () => {},
    });

    // Event with sequence 4 (older than initialSequence 5)
    client.processSSEBlock(
      `id: 4\nevent: step.ready\ndata: {"sequence":4,"stepId":"step-1"}`,
    );
    // Event with sequence 5 (equal to initialSequence 5)
    client.processSSEBlock(
      `id: 5\nevent: step.ready\ndata: {"sequence":5,"stepId":"step-1"}`,
    );
    // Event with sequence 6 (new)
    client.processSSEBlock(
      `id: 6\nevent: attempt.started\ndata: {"sequence":6,"stepId":"step-1","attemptId":"att-1"}`,
    );
    // Duplicate of sequence 6
    client.processSSEBlock(
      `id: 6\nevent: attempt.started\ndata: {"sequence":6,"stepId":"step-1","attemptId":"att-1"}`,
    );
    // Event with sequence 7 (new)
    client.processSSEBlock(
      `id: 7\nevent: attempt.completed\ndata: {"sequence":7,"stepId":"step-1","attemptId":"att-1"}`,
    );

    assert.equal(received.length, 2);
    assert.equal(received[0].sequence, 6);
    assert.equal(received[0].type, "attempt.started");
    assert.equal(received[1].sequence, 7);
    assert.equal(received[1].type, "attempt.completed");
    assert.equal(client.getLastProcessedSequence(), 7);
  });

  test("handles event: resync and invokes onResyncRequired", () => {
    let resyncEvent = null;
    const client = new RunEventStreamClient({
      runId: "run-456",
      initialSequence: 1,
      onEvent: () => {},
      onFreshnessChange: () => {},
      onResyncRequired: (r) => {
        resyncEvent = r;
      },
    });

    client.processSSEBlock(
      `event: resync\ndata: {"reason":"RETENTION_GAP","code":"RESYNC_REQUIRED","lastAvailableSequence":10}`,
    );

    assert.ok(resyncEvent);
    assert.equal(resyncEvent.reason, "RETENTION_GAP");
    assert.equal(resyncEvent.code, "RESYNC_REQUIRED");
    assert.equal(resyncEvent.lastAvailableSequence, 10);
  });

  test("safely ignores keepalive comment lines and empty lines", () => {
    const received = [];
    const client = new RunEventStreamClient({
      runId: "run-789",
      onEvent: (ev) => received.push(ev),
      onFreshnessChange: () => {},
      onResyncRequired: () => {},
    });

    client.processSSEBlock(": keepalive");
    client.processSSEBlock("   ");
    client.processSSEBlock(": ping");

    assert.equal(received.length, 0);
  });
});
