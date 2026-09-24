import test from "node:test";
import assert from "node:assert/strict";
import {
  computeGraphLayout,
  computeMinimap,
  filterEventsForStep,
  getStatusPresentation,
  virtualizeItems,
  RunInspector,
} from "../dist/inspector.js";

test("Logical Graph: exactly one node per logical step; retry attempts contained within step", () => {
  const steps = [
    {
      id: "step-1",
      nodeId: "fetch-profile",
      kind: "task",
      status: "SUCCEEDED",
      currentEpoch: 2,
      attempts: [
        {
          id: "att-1",
          epoch: 1,
          status: "FAILED",
          error: "Connection reset by peer",
          startedAt: "2026-09-24T10:00:00Z",
          completedAt: "2026-09-24T10:00:02Z",
        },
        {
          id: "att-2",
          epoch: 2,
          status: "SUCCEEDED",
          startedAt: "2026-09-24T10:00:05Z",
          completedAt: "2026-09-24T10:00:07Z",
        },
      ],
    },
    {
      id: "step-2",
      nodeId: "charge-card",
      kind: "task",
      status: "FAILED",
      currentEpoch: 3,
      after: ["fetch-profile"],
      attempts: [
        { id: "att-2-1", epoch: 1, status: "FAILED", error: "Card timeout" },
        {
          id: "att-2-2",
          epoch: 2,
          status: "FAILED",
          error: "Insufficient funds",
        },
        { id: "att-2-3", epoch: 3, status: "FAILED", error: "Bank rejected" },
      ],
    },
  ];

  const layout = computeGraphLayout(steps);

  // Exactly 2 nodes in graph despite 5 total retry attempts across steps
  assert.equal(layout.nodes.length, 2);
  assert.equal(layout.nodes[0].nodeId, "fetch-profile");
  assert.equal(layout.nodes[0].attemptsCount, 2);
  assert.equal(layout.nodes[1].nodeId, "charge-card");
  assert.equal(layout.nodes[1].attemptsCount, 3);

  // Edge connects step-1 to step-2
  assert.equal(layout.edges.length, 1);
  assert.equal(layout.edges[0].fromNodeId, "fetch-profile");
  assert.equal(layout.edges[0].toNodeId, "charge-card");
  assert.equal(layout.edges[0].isSkipped, false);
});

test("Logical Graph: selected branch vs skipped branch matches DB wait reasons", () => {
  const steps = [
    {
      id: "step-root",
      nodeId: "evaluate-fraud",
      kind: "choice",
      status: "SUCCEEDED",
    },
    {
      id: "step-branch-approve",
      nodeId: "auto-approve",
      kind: "task",
      status: "SUCCEEDED",
      after: ["evaluate-fraud"],
    },
    {
      id: "step-branch-review",
      nodeId: "manual-review",
      kind: "approval",
      status: "SKIPPED",
      waitReason: "BRANCH_NOT_SELECTED",
      after: ["evaluate-fraud"],
    },
    {
      id: "step-branch-review-notify",
      nodeId: "notify-compliance",
      kind: "task",
      status: "SKIPPED",
      waitReason: "DEPENDENCY_SKIPPED",
      after: ["manual-review"],
    },
  ];

  const layout = computeGraphLayout(steps);
  assert.equal(layout.nodes.length, 4);

  const approveNode = layout.nodes.find((n) => n.nodeId === "auto-approve");
  const reviewNode = layout.nodes.find((n) => n.nodeId === "manual-review");
  const notifyNode = layout.nodes.find((n) => n.nodeId === "notify-compliance");

  assert.equal(approveNode.status, "SUCCEEDED");
  assert.equal(reviewNode.status, "SKIPPED");
  assert.equal(reviewNode.waitReason, "BRANCH_NOT_SELECTED");
  assert.equal(notifyNode.status, "SKIPPED");
  assert.equal(notifyNode.waitReason, "DEPENDENCY_SKIPPED");

  // Skipped branches have isSkipped: true on edges
  const skippedEdge1 = layout.edges.find((e) => e.toNodeId === "manual-review");
  const skippedEdge2 = layout.edges.find(
    (e) => e.toNodeId === "notify-compliance",
  );
  const activeEdge = layout.edges.find((e) => e.toNodeId === "auto-approve");

  assert.ok(skippedEdge1, "Skipped edge 1 must exist");
  assert.ok(skippedEdge2, "Skipped edge 2 must exist");
  assert.ok(activeEdge, "Active edge must exist");
  assert.equal(skippedEdge1.isSkipped, true);
  assert.equal(skippedEdge2.isSkipped, true);
  assert.equal(activeEdge.isSkipped, false);
});

test("200-Node Fixture: topological layout, minimap scaling, and parallel collapsing", () => {
  // Construct 200 nodes: 1 root -> 198 fan-out parallel steps -> 1 aggregate join step
  const steps = [
    {
      id: "node-root",
      nodeId: "root",
      kind: "task",
      status: "SUCCEEDED",
    },
  ];

  for (let i = 1; i <= 198; i++) {
    steps.push({
      id: `node-parallel-${i}`,
      nodeId: `parallel-${i}`,
      kind: "task",
      status: i % 10 === 0 ? "SKIPPED" : "SUCCEEDED",
      waitReason: i % 10 === 0 ? "BRANCH_NOT_SELECTED" : undefined,
      after: ["root"],
    });
  }

  steps.push({
    id: "node-join",
    nodeId: "join-all",
    kind: "task",
    status: "WAITING",
    after: steps.slice(1, 199).map((s) => s.nodeId),
  });

  assert.equal(steps.length, 200);

  // Measure layout calculation time
  const t0 = performance.now();
  const fullLayout = computeGraphLayout(steps);
  const duration = performance.now() - t0;

  // Must compute in under 250ms for 200 nodes
  assert.ok(
    duration < 250,
    `Layout computation took ${duration}ms, expected < 250ms`,
  );
  assert.equal(fullLayout.nodes.length, 200);
  assert.equal(fullLayout.levels, 3); // level 0: root, level 1: 198 parallel, level 2: join

  // Minimap scaling test
  const minimap = computeMinimap(fullLayout, 800, 600, 0, 0);

  assert.ok(minimap.scale > 0, "Minimap scale must be strictly positive");
  assert.ok(
    minimap.viewport.width > 0,
    "Minimap viewport width must be strictly positive",
  );
  assert.ok(
    minimap.viewport.height > 0,
    "Minimap viewport height must be strictly positive",
  );
  assert.ok(Number.isFinite(minimap.viewport.x));
  assert.ok(Number.isFinite(minimap.viewport.y));

  // Collapsing candidate parallel cluster
  // Siblings sharing identical dependencies form a candidate cluster
  const candidateClusterId = "cluster-root-1";
  const collapsedLayout = computeGraphLayout(
    steps,
    new Set([candidateClusterId]),
  );

  // 198 nodes collapsed into 1 cluster placeholder node + root + join = 3 visible nodes
  assert.equal(collapsedLayout.nodes.length, 3);
  const clusterNode = collapsedLayout.nodes.find(
    (n) => n.isCollapsedPlaceholder,
  );
  assert.ok(clusterNode);
  assert.equal(clusterNode.collapsedCount, 198);
  assert.equal(clusterNode.collapsedNodeIds.length, 198);
});

test("Accessibility: WCAG 2.2 AA non-color status differentiation (symbols + text)", () => {
  const statuses = [
    "SUCCEEDED",
    "FAILED",
    "SKIPPED",
    "CANCELLED",
    "RUNNING",
    "WAITING",
    "READY",
    "BLOCKED",
  ];

  const presentations = new Map();

  for (const st of statuses) {
    const p = getStatusPresentation(st);
    assert.ok(p.symbol, `Status ${st} must have a non-color symbol`);
    assert.ok(p.label, `Status ${st} must have a text label`);
    assert.ok(p.ariaLabel, `Status ${st} must have an ariaLabel`);
    presentations.set(st, p);
  }

  // Ensure every status has a distinct, recognizable symbol
  const symbols = new Set(
    Array.from(presentations.values()).map((p) => p.symbol),
  );
  assert.equal(
    symbols.size,
    statuses.length,
    "All status symbols must be distinct",
  );

  assert.equal(presentations.get("SUCCEEDED").symbol, "✓");
  assert.equal(presentations.get("FAILED").symbol, "✕");
  assert.equal(presentations.get("SKIPPED").symbol, "↷");
  assert.equal(presentations.get("CANCELLED").symbol, "⊘");
  assert.equal(presentations.get("RUNNING").symbol, "●");
  assert.equal(presentations.get("WAITING").symbol, "⏳");
  assert.equal(presentations.get("READY").symbol, "○");
  assert.equal(presentations.get("BLOCKED").symbol, "◌");
});

test("List View Virtualization: virtualizeItems slices 200 nodes correctly based on scroll", () => {
  const items = Array.from({ length: 200 }, (_, i) => ({ id: `item-${i}` }));

  // Page 1: 0..50
  const page1 = virtualizeItems(items, 0, 50);
  assert.equal(page1.offset, 0);
  assert.equal(page1.items.length, 50);
  assert.equal(page1.total, 200);
  assert.equal(page1.hasMore, 150);

  // Page 2: 50..100
  const page2 = virtualizeItems(items, 50, 50);
  assert.equal(page2.offset, 50);
  assert.equal(page2.items.length, 50);
  assert.equal(page2.hasMore, 100);

  // Final page: 150..200
  const page4 = virtualizeItems(items, 150, 50);
  assert.equal(page4.offset, 150);
  assert.equal(page4.items.length, 50);
  assert.equal(page4.hasMore, 0);
});

test("Step Inspector: filterEventsForStep filters run events by stepId and nodeId", () => {
  const events = [
    { id: "e1", type: "run.started", sequence: 1 },
    {
      id: "e2",
      type: "step.started",
      sequence: 2,
      payload: { stepId: "s-1", nodeId: "fetch-user" },
    },
    {
      id: "e3",
      type: "step.started",
      sequence: 3,
      payload: { stepId: "s-2", nodeId: "send-email" },
    },
    {
      id: "e4",
      type: "step.succeeded",
      sequence: 4,
      payload: { stepId: "s-1", nodeId: "fetch-user" },
    },
  ];

  const s1Events = filterEventsForStep(events, {
    id: "s-1",
    nodeId: "fetch-user",
  });
  assert.equal(s1Events.length, 2);
  assert.equal(s1Events[0].id, "e2");
  assert.equal(s1Events[1].id, "e4");

  const s2Events = filterEventsForStep(events, {
    id: "s-2",
    nodeId: "send-email",
  });
  assert.equal(s2Events.length, 1);
  assert.equal(s2Events[0].id, "e3");
});

test("Stream convergence: applyEvent handles step.skipped and step.succeeded correctly", () => {
  const inspector = new RunInspector("run-test-conv");
  const initialSteps = [
    {
      id: "s-1",
      nodeId: "check-fraud",
      kind: "task",
      status: "RUNNING",
    },
    {
      id: "s-2",
      nodeId: "flag-account",
      kind: "task",
      status: "WAITING",
    },
  ];

  inspector["snapshot"] = {
    revision: 1,
    status: "RUNNING",
    steps: initialSteps,
    attempts: [],
    events: [],
    logs: [],
    lastEventSequence: 1,
  };

  // Apply step.succeeded
  inspector.applyEvent({
    id: "evt-1",
    runId: "run-test-conv",
    type: "step.succeeded",
    sequence: 2,
    schemaVersion: 1,
    committedAt: "2026-09-24T10:00:00Z",
    payload: {
      stepId: "s-1",
      nodeId: "check-fraud",
      output: { score: 0.1 },
    },
  });

  assert.equal(inspector["snapshot"].steps[0].status, "SUCCEEDED");
  assert.deepEqual(inspector["snapshot"].steps[0].output, { score: 0.1 });

  // Apply step.skipped
  inspector.applyEvent({
    id: "evt-2",
    runId: "run-test-conv",
    type: "step.skipped",
    sequence: 3,
    schemaVersion: 1,
    committedAt: "2026-09-24T10:00:01Z",
    payload: {
      stepId: "s-2",
      nodeId: "flag-account",
      waitReason: "BRANCH_NOT_SELECTED",
    },
  });

  assert.equal(inspector["snapshot"].steps[1].status, "SKIPPED");
  assert.equal(
    inspector["snapshot"].steps[1].waitReason,
    "BRANCH_NOT_SELECTED",
  );
});
