import test from "node:test";
import assert from "node:assert/strict";
import { defineTask, ContractError } from "../dist/index.js";

const sampleInputSchema = {
  type: "object",
  properties: { query: { type: "string" } },
  required: ["query"],
  additionalProperties: false,
};

const sampleOutputSchema = {
  type: "object",
  properties: { results: { type: "array", items: { type: "string" } } },
  required: ["results"],
  additionalProperties: false,
};

test("defineTask creates valid safe task with normalized defaults", () => {
  const task = defineTask({
    name: "fetch-data",
    inputSchema: sampleInputSchema,
    outputSchema: sampleOutputSchema,
    recovery: "safe",
    handler: async (input, ctx) => ({ results: [input.query] }),
  });

  assert.equal(task.name, "fetch-data");
  assert.equal(task.recovery, "safe");
  assert.equal(task.timeoutMs, 300000); // default
  assert.deepEqual(task.retry, {
    maxAttempts: 3,
    initialDelayMs: 1000,
    maxDelayMs: 30000,
  });
  assert.equal(task.entrypoint, "./tasks/fetch-data.js");

  const manifest = task.toManifest();
  assert.equal(manifest.name, "fetch-data");
  assert.equal(manifest.recovery, "safe");
  assert.equal(manifest.entrypoint, "./tasks/fetch-data.js");
});

test("defineTask creates valid idempotent task when idempotencyWindowMs is sufficient", () => {
  const task = defineTask({
    name: "charge-card",
    inputSchema: sampleInputSchema,
    outputSchema: sampleOutputSchema,
    recovery: "idempotent",
    timeoutMs: 10000,
    idempotencyWindowMs: 15000, // exactly 5000 + 10000
  });

  assert.equal(task.recovery, "idempotent");
  assert.equal(task.idempotencyWindowMs, 15000);
  assert.equal(task.timeoutMs, 10000);

  const manifest = task.toManifest();
  assert.equal(manifest.idempotencyWindowMs, 15000);
});

test("defineTask rejects idempotent task when idempotencyWindowMs < 5000 + timeoutMs", () => {
  // default timeoutMs is 300000, so min window is 305000
  assert.throws(
    () =>
      defineTask({
        name: "charge-card",
        inputSchema: sampleInputSchema,
        outputSchema: sampleOutputSchema,
        recovery: "idempotent",
        idempotencyWindowMs: 304999, // 1 ms below 305000
      }),
    (err) => err instanceof ContractError && err.code === "INVALID_TASK",
  );

  // Custom timeoutMs: 20000, min window is 25000
  assert.throws(
    () =>
      defineTask({
        name: "charge-card",
        inputSchema: sampleInputSchema,
        outputSchema: sampleOutputSchema,
        recovery: "idempotent",
        timeoutMs: 20000,
        idempotencyWindowMs: 24999,
      }),
    (err) => err instanceof ContractError && err.code === "INVALID_TASK",
  );
});

test("defineTask creates valid reconcile task", () => {
  const task = defineTask({
    name: "external-transfer",
    inputSchema: sampleInputSchema,
    outputSchema: sampleOutputSchema,
    recovery: "reconcile",
    timeoutMs: 60000,
  });

  assert.equal(task.recovery, "reconcile");
  assert.equal(task.timeoutMs, 60000);
});

test("defineTask rejects invalid retry delay configuration", () => {
  assert.throws(
    () =>
      defineTask({
        name: "bad-retry",
        inputSchema: sampleInputSchema,
        outputSchema: sampleOutputSchema,
        recovery: "safe",
        retry: {
          initialDelayMs: 40000,
          maxDelayMs: 10000, // initial > max
        },
      }),
    (err) => err instanceof ContractError && err.code === "INVALID_TASK",
  );
});

test("defineTask rejects invalid task names", () => {
  for (const badName of [
    "has space",
    "has/slash",
    "has.dot",
    "",
    "a".repeat(65), // > 64 chars
  ]) {
    assert.throws(
      () =>
        defineTask({
          name: badName,
          inputSchema: sampleInputSchema,
          outputSchema: sampleOutputSchema,
          recovery: "safe",
        }),
      (err) => err instanceof ContractError,
    );
  }
});

test("defineTask allows custom entrypoint override in toManifest", () => {
  const task = defineTask({
    name: "custom-entry",
    inputSchema: sampleInputSchema,
    outputSchema: sampleOutputSchema,
    recovery: "safe",
    entrypoint: "./dist/custom.js",
  });

  assert.equal(task.entrypoint, "./dist/custom.js");
  assert.equal(
    task.toManifest("./overridden.js").entrypoint,
    "./overridden.js",
  );
});
