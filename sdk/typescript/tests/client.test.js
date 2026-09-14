import test from "node:test";
import assert from "node:assert/strict";
import {
  DeadboltClient,
  DeadboltApiError,
  DeadboltExecutionError,
  DeadboltTimeoutError,
  ContractError,
} from "../dist/index.js";

test("DeadboltClient requires idempotencyKey on create run", async () => {
  const client = new DeadboltClient({ baseUrl: "http://example.com" });

  await assert.rejects(
    () =>
      client.runs.create({
        workflow: "my-wf",
        input: { x: 1 },
        idempotencyKey: "", // empty
      }),
    (err) =>
      err instanceof ContractError && err.code === "MISSING_IDEMPOTENCY_KEY",
  );

  await assert.rejects(
    () =>
      client.runs.create({
        workflow: "my-wf",
        input: { x: 1 },
        // @ts-ignore
        idempotencyKey: undefined,
      }),
    (err) =>
      err instanceof ContractError && err.code === "MISSING_IDEMPOTENCY_KEY",
  );
});

test("DeadboltClient sends Idempotency-Key and reports 202 Accepted as persisted acceptance", async () => {
  let capturedUrl = "";
  let capturedHeaders = {};
  let capturedBody = "";

  const mockFetch = async (url, init) => {
    capturedUrl = url;
    capturedHeaders = init.headers;
    capturedBody = JSON.parse(init.body);

    return {
      ok: true,
      status: 202,
      statusText: "Accepted",
      text: async () =>
        JSON.stringify({
          id: "run-12345",
          workflowName: "data-pipeline",
          deploymentId: "dep-987",
          status: "QUEUED",
          revision: 1,
          createdAt: "2026-09-12T00:00:00Z",
        }),
    };
  };

  const client = new DeadboltClient({
    baseUrl: "https://api.deadbolt.internal",
    apiKey: "apikey-tenant-test-123",
    environment: "staging",
    fetch: mockFetch,
  });

  const run = await client.runs.create({
    workflow: "data-pipeline",
    input: { query: "search terms" },
    idempotencyKey: "req-key-abcdef",
  });

  assert.equal(
    capturedUrl,
    "https://api.deadbolt.internal/v1/workflows/data-pipeline/runs",
  );
  assert.equal(capturedHeaders["Idempotency-Key"], "req-key-abcdef");
  assert.equal(
    capturedHeaders["Authorization"],
    "Bearer apikey-tenant-test-123",
  );
  assert.equal(capturedBody.environment, "staging");
  assert.deepEqual(capturedBody.input, { query: "search terms" });

  assert.equal(run.id, "run-12345");
  assert.equal(run.status, "QUEUED");
  assert.equal(run.isAccepted, true); // Persisted acceptance in control plane
});

test("DeadboltClient throws DeadboltApiError on 409 conflict or other errors", async () => {
  const mockFetch = async () => ({
    ok: false,
    status: 409,
    statusText: "Conflict",
    text: async () =>
      JSON.stringify({
        code: "IDEMPOTENCY_CONFLICT",
        message: "Key already used with different payload",
        requestId: "req-999",
      }),
  });

  const client = new DeadboltClient({
    baseUrl: "http://example.com",
    fetch: mockFetch,
  });

  await assert.rejects(
    () =>
      client.runs.create({
        workflow: "wf",
        input: {},
        idempotencyKey: "same-key",
      }),
    (err) => {
      assert.ok(err instanceof DeadboltApiError);
      assert.equal(err.status, 409);
      assert.equal(err.code, "IDEMPOTENCY_CONFLICT");
      assert.equal(err.requestId, "req-999");
      return true;
    },
  );
});

test("DeadboltClient pollResult returns output when status becomes SUCCEEDED", async () => {
  let pollCount = 0;
  const mockFetch = async (url) => {
    pollCount++;
    const status = pollCount < 3 ? "RUNNING" : "SUCCEEDED";
    const output = pollCount >= 3 ? { result: "done", score: 99 } : undefined;

    return {
      ok: true,
      status: 200,
      text: async () =>
        JSON.stringify({
          id: "run-1",
          workflowName: "wf",
          deploymentId: "dep-1",
          status,
          revision: pollCount,
          createdAt: "2026-09-12T00:00:00Z",
          lastEventSequence: pollCount,
          steps: [],
          output,
        }),
    };
  };

  const client = new DeadboltClient({
    baseUrl: "http://example.com",
    fetch: mockFetch,
    pollIntervalMs: 10,
  });

  const result = await client.runs.pollResult("run-1", {
    intervalMs: 5,
    timeoutMs: 1000,
  });

  assert.deepEqual(result, { result: "done", score: 99 });
  assert.equal(pollCount, 3);
});

test("DeadboltClient pollResult throws DeadboltExecutionError when status is FAILED", async () => {
  const mockFetch = async () => ({
    ok: true,
    status: 200,
    text: async () =>
      JSON.stringify({
        id: "run-failed",
        workflowName: "wf",
        deploymentId: "dep-1",
        status: "FAILED",
        reasonCode: "PROVIDER_DOWN",
        revision: 2,
        createdAt: "2026-09-12T00:00:00Z",
        lastEventSequence: 2,
        steps: [],
        error: {
          code: "PROVIDER_DOWN",
          message: "Downstream API unavailable",
        },
      }),
  });

  const client = new DeadboltClient({
    baseUrl: "http://example.com",
    fetch: mockFetch,
  });

  await assert.rejects(
    () => client.runs.pollResult("run-failed", { intervalMs: 5 }),
    (err) => {
      assert.ok(err instanceof DeadboltExecutionError);
      assert.equal(err.code, "RUN_FAILED");
      assert.match(err.message, /PROVIDER_DOWN/);
      return true;
    },
  );
});

test("DeadboltClient pollResult throws DeadboltTimeoutError when timeout is reached", async () => {
  const mockFetch = async () => ({
    ok: true,
    status: 200,
    text: async () =>
      JSON.stringify({
        id: "run-slow",
        workflowName: "wf",
        deploymentId: "dep-1",
        status: "RUNNING",
        revision: 1,
        createdAt: "2026-09-12T00:00:00Z",
        lastEventSequence: 1,
        steps: [],
      }),
  });

  const client = new DeadboltClient({
    baseUrl: "http://example.com",
    fetch: mockFetch,
  });

  await assert.rejects(
    () =>
      client.runs.pollResult("run-slow", {
        intervalMs: 10,
        timeoutMs: 30, // 30ms timeout
      }),
    (err) => {
      assert.ok(err instanceof DeadboltTimeoutError);
      assert.match(err.message, /Timed out after/);
      return true;
    },
  );
});
