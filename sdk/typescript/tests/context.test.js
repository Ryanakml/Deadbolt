import test from "node:test";
import assert from "node:assert/strict";
import {
  createTaskContext,
  createRedactionAwareLogger,
  redactSensitiveData,
} from "../dist/index.js";

test("createTaskContext sets all required execution context properties", () => {
  const controller = new AbortController();
  const ctx = createTaskContext({
    stepId: "step-1",
    attemptId: "att-123",
    operationId: "op-456",
    signal: controller.signal,
    env: { DB_PORT: "5432" },
  });

  assert.equal(ctx.stepId, "step-1");
  assert.equal(ctx.attemptId, "att-123");
  assert.equal(ctx.operationId, "op-456");
  assert.equal(ctx.signal, controller.signal);
  assert.equal(typeof ctx.logger.info, "function");
  assert.equal(ctx.log, ctx.logger); // alias
  assert.equal(ctx.env.DB_PORT, "5432");
});

test("createTaskContext rejects missing stepId instead of aliasing operationId", () => {
  assert.throws(
    () =>
      createTaskContext({
        attemptId: "att-123",
        operationId: "op-456",
      }),
    /stepId is required/,
  );
});

test("redactSensitiveData masks sensitive keys and patterns", () => {
  const sensitiveObj = {
    username: "alice",
    password: "supersecretpassword",
    api_key: "test_secret_api_key_to_redact",
    nested: {
      token: "secret-token-value",
      safe: "hello world",
      authHeader: "Bearer test_bearer_token_xyz",
    },
  };

  const redacted = redactSensitiveData(sensitiveObj);

  assert.equal(redacted.username, "alice");
  assert.equal(redacted.password, "[REDACTED]");
  assert.equal(redacted.api_key, "[REDACTED]");
  assert.equal(redacted.nested.token, "[REDACTED]");
  assert.equal(redacted.nested.safe, "hello world");
  assert.match(redacted.nested.authHeader, /Bearer \[REDACTED\]/);
});

test("redactSensitiveData handles circular references without crashing", () => {
  const circular = { name: "test" };
  circular.self = circular;

  const result = redactSensitiveData(circular);
  assert.equal(result.name, "test");
  assert.equal(result.self, "[Circular]");
});

test("createRedactionAwareLogger delivers sanitized logs to sink", () => {
  const logs = [];
  const logger = createRedactionAwareLogger("step-test", (level, msg, meta) => {
    logs.push({ level, msg, meta });
  });

  logger.info("Connecting to server with Bearer mysecrettoken123456789012345", {
    password: "raw_password",
    safeData: 100,
  });

  assert.equal(logs.length, 1);
  assert.equal(logs[0].level, "info");
  assert.match(logs[0].msg, /Bearer \[REDACTED\]/);
  assert.equal(logs[0].meta[0].password, "[REDACTED]");
  assert.equal(logs[0].meta[0].safeData, 100);
});
