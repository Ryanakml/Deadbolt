import test from "node:test";
import assert from "node:assert/strict";
import {
  defineTask,
  defineWorkflow,
  input,
  output,
  literal,
  ContractError,
} from "../dist/index.js";

const payloadSchemaString = {
  type: "object",
  properties: { val: { type: "string" } },
  required: ["val"],
  additionalProperties: false,
};

const taskA = defineTask({
  name: "task-a",
  inputSchema: payloadSchemaString,
  outputSchema: payloadSchemaString,
  recovery: "safe",
});

const taskB = defineTask({
  name: "task-b",
  inputSchema: payloadSchemaString,
  outputSchema: payloadSchemaString,
  recovery: "safe",
});

const taskC = defineTask({
  name: "task-c",
  inputSchema: payloadSchemaString,
  outputSchema: payloadSchemaString,
  recovery: "idempotent",
  timeoutMs: 10000,
  idempotencyWindowMs: 20000,
});

test("defineWorkflow creates valid linear A -> B -> C workflow", () => {
  const wf = defineWorkflow({
    name: "linear-pipeline",
    inputSchema: payloadSchemaString,
    outputSchema: payloadSchemaString,
    nodes: [
      {
        id: "stepA",
        type: "task",
        task: taskA,
        input: { val: input("/val") },
      },
      {
        id: "stepB",
        type: "task",
        task: taskB,
        after: ["stepA"],
        input: { val: output("stepA", "/val") },
      },
      {
        id: "stepC",
        type: "task",
        task: taskC,
        after: ["stepB"],
        input: { val: output("stepB", "/val") },
      },
    ],
    output: { val: output("stepC", "/val") },
  });

  assert.equal(wf.name, "linear-pipeline");
  assert.equal(wf.nodes.length, 3);
  assert.equal(wf.tasks.length, 3);

  const manifest = wf.toManifest();
  assert.equal(manifest.manifestVersion, 1);
  assert.equal(manifest.name, "linear-pipeline");
  assert.equal(manifest.nodes.length, 3);
  assert.equal(manifest.nodes[0].task, "task-a");
  assert.equal(manifest.nodes[1].task, "task-b");
  assert.equal(manifest.nodes[2].task, "task-c");
  assert.deepEqual(manifest.nodes[1].after, ["stepA"]);
  assert.deepEqual(manifest.nodes[2].after, ["stepB"]);
});

test("mapping helpers construct valid reference descriptors", () => {
  assert.deepEqual(input("/user/id"), {
    $ref: "run.input",
    pointer: "/user/id",
  });
  assert.deepEqual(input("/user/name", "anonymous"), {
    $ref: "run.input",
    pointer: "/user/name",
    default: "anonymous",
  });
  assert.deepEqual(output("step1", "/data"), {
    $ref: "step.output",
    stepId: "step1",
    pointer: "/data",
  });
  assert.deepEqual(output("step1", "/data", null), {
    $ref: "step.output",
    stepId: "step1",
    pointer: "/data",
    default: null,
  });
  assert.deepEqual(literal({ count: 42 }), {
    literal: { count: 42 },
  });
});

test("defineWorkflow rejects unresolved task reference", () => {
  assert.throws(
    () =>
      defineWorkflow({
        name: "missing-task-wf",
        inputSchema: payloadSchemaString,
        outputSchema: payloadSchemaString,
        nodes: [
          {
            id: "step1",
            type: "task",
            task: "non-existent-task",
            input: { val: input("/val") },
          },
        ],
        output: { val: output("step1", "/val") },
      }),
    (err) => err instanceof ContractError && err.code === "MISSING_TASK_REF",
  );
});

test("defineWorkflow accepts static parallel fan-out graphs", () => {
  assert.doesNotThrow(() =>
    defineWorkflow({
      name: "fan-out-wf",
      inputSchema: payloadSchemaString,
      outputSchema: payloadSchemaString,
      nodes: [
        {
          id: "root",
          type: "task",
          task: taskA,
          input: { val: input("/val") },
        },
        {
          id: "branch1",
          type: "task",
          task: taskB,
          after: ["root"],
          input: { val: output("root", "/val") },
        },
        {
          id: "branch2",
          type: "task",
          task: taskC,
          after: ["root"],
          input: { val: output("root", "/val") },
          sideEffect: true,
        },
      ],
      output: { val: output("branch1", "/val") },
    }),
  );
});

test("defineWorkflow rejects cycles", () => {
  assert.throws(
    () =>
      defineWorkflow({
        name: "cyclic-wf",
        inputSchema: payloadSchemaString,
        outputSchema: payloadSchemaString,
        nodes: [
          {
            id: "step1",
            type: "task",
            task: taskA,
            after: ["step2"],
            input: { val: output("step2", "/val") },
          },
          {
            id: "step2",
            type: "task",
            task: taskB,
            after: ["step1"],
            input: { val: output("step1", "/val") },
          },
        ],
        output: { val: output("step1", "/val") },
      }),
    (err) => err instanceof ContractError && err.code === "CYCLE_DETECTED",
  );
});

test("defineWorkflow rejects duplicate node IDs", () => {
  assert.throws(
    () =>
      defineWorkflow({
        name: "duplicate-id-wf",
        inputSchema: payloadSchemaString,
        outputSchema: payloadSchemaString,
        nodes: [
          {
            id: "dup",
            type: "task",
            task: taskA,
            input: { val: input("/val") },
          },
          {
            id: "dup",
            type: "task",
            task: taskB,
            input: { val: input("/val") },
          },
        ],
        output: { val: output("dup", "/val") },
      }),
    (err) => err instanceof ContractError && err.code === "DUPLICATE_NODE_ID",
  );
});

test("defineWorkflow rejects orphaned leaves", () => {
  assert.throws(
    () =>
      defineWorkflow({
        name: "orphan-wf",
        inputSchema: payloadSchemaString,
        outputSchema: payloadSchemaString,
        nodes: [
          {
            id: "stepA",
            type: "task",
            task: taskA,
            input: { val: input("/val") },
          },
          {
            id: "stepB",
            type: "task",
            task: taskB,
            after: ["stepA"],
            input: { val: output("stepA", "/val") },
            // stepB has no successors, is not declared sideEffect: true, and is not used in output!
          },
        ],
        output: { val: input("/val") }, // output doesn't use stepB
      }),
    (err) => err instanceof ContractError && err.code === "ORPHAN_LEAF",
  );
});

test("defineWorkflow allows leaf declared with sideEffect: true", () => {
  const wf = defineWorkflow({
    name: "side-effect-leaf-wf",
    inputSchema: payloadSchemaString,
    outputSchema: payloadSchemaString,
    nodes: [
      {
        id: "stepA",
        type: "task",
        task: taskA,
        input: { val: input("/val") },
        sideEffect: true, // explicit sideEffect leaf
      },
    ],
    output: { val: input("/val") },
  });
  assert.equal(wf.nodes[0].sideEffect, true);
});
