import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { executeTask, resolveHandler } from "../dist/runner.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

test("Resolve handler from module exports", async () => {
  const fixturePath = path.resolve(__dirname, "fixtures/sample-task.js");
  const handler = await resolveHandler(fixturePath, "sampleTask");
  assert.equal(typeof handler, "function");
});

test("Execute successful task and capture structured completion", async () => {
  const fixturePath = path.resolve(__dirname, "fixtures/sample-task.js");
  let capturedResult = "";
  const writeResult = (data) => {
    capturedResult = data;
  };

  const taskInput = {
    attemptId: "att_001",
    operationId: "op_001",
    taskName: "sampleTask",
    entrypoint: fixturePath,
    input: { x: 10, y: 20 },
  };

  const completion = await executeTask(taskInput, writeResult);

  assert.equal(completion.status, "SUCCEEDED");
  assert.deepEqual(completion.output, { sum: 30 });
  assert.equal(completion.attemptId, "att_001");
  assert.ok(completion.metrics.durationMs >= 0);

  const parsedJson = JSON.parse(capturedResult);
  assert.equal(parsedJson.status, "SUCCEEDED");
  assert.deepEqual(parsedJson.output, { sum: 30 });
});

test("Execute failing task and capture typed error envelope", async () => {
  const fixturePath = path.resolve(__dirname, "fixtures/sample-task.js");
  let capturedResult = "";
  const writeResult = (data) => {
    capturedResult = data;
  };

  const taskInput = {
    attemptId: "att_002",
    operationId: "op_002",
    taskName: "failingTask",
    entrypoint: fixturePath,
    input: {},
  };

  const completion = await executeTask(taskInput, writeResult);

  assert.equal(completion.status, "FAILED");
  assert.ok(completion.error);
  assert.equal(completion.error.code, "INTENTIONAL_FAILURE");
  assert.equal(completion.error.message, "This task intentionally failed");
  assert.equal(completion.error.retryable, true);
});

test("Arbitrary stdout and stderr from user task do NOT pollute structured result", async () => {
  const fixturePath = path.resolve(__dirname, "fixtures/sample-task.js");
  let capturedResult = "";
  const writeResult = (data) => {
    capturedResult = data;
  };

  const taskInput = {
    attemptId: "att_003",
    operationId: "op_003",
    taskName: "noisyTask",
    entrypoint: fixturePath,
    input: {},
  };

  const completion = await executeTask(taskInput, writeResult);

  assert.equal(completion.status, "SUCCEEDED");
  assert.deepEqual(completion.output, { clean: true });

  // Verify the structured result channel received strictly valid JSON
  const parsed = JSON.parse(capturedResult);
  assert.equal(parsed.status, "SUCCEEDED");
  assert.deepEqual(parsed.output, { clean: true });
});

test("Task timeout triggers AbortSignal and marks completion FAILED", async () => {
  const fixturePath = path.resolve(__dirname, "fixtures/sample-task.js");
  let capturedResult = "";
  const writeResult = (data) => {
    capturedResult = data;
  };

  const taskInput = {
    attemptId: "att_004",
    operationId: "op_004",
    taskName: "slowTask",
    entrypoint: fixturePath,
    input: {},
    timeoutMs: 50,
  };

  const completion = await executeTask(taskInput, writeResult);

  assert.equal(completion.status, "FAILED");
  assert.ok(completion.error);
  assert.equal(completion.error.code, "ABORTED");
});
