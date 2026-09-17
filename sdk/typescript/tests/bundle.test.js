import test from "node:test";
import assert from "node:assert/strict";
import {
  defineTask,
  defineWorkflow,
  input,
  output,
  buildDeploymentBundle,
  validateDeployment,
  canonicalize,
  ContractError,
} from "../dist/index.js";

const payloadSchema = {
  type: "object",
  properties: { msg: { type: "string" } },
  required: ["msg"],
  additionalProperties: false,
};

const task1 = defineTask({
  name: "task-one",
  inputSchema: payloadSchema,
  outputSchema: payloadSchema,
  recovery: "safe",
});

const task2 = defineTask({
  name: "task-two",
  inputSchema: payloadSchema,
  outputSchema: payloadSchema,
  recovery: "safe",
});

const workflow = defineWorkflow({
  name: "pipeline",
  inputSchema: payloadSchema,
  outputSchema: payloadSchema,
  nodes: [
    {
      id: "step1",
      type: "task",
      task: task1,
      input: { msg: input("/msg") },
    },
    {
      id: "step2",
      type: "task",
      task: task2,
      after: ["step1"],
      input: { msg: output("step1", "/msg") },
    },
  ],
  output: { msg: output("step2", "/msg") },
});

test("buildDeploymentBundle generates valid deployment manifest and pinned digests", () => {
  const bundle = buildDeploymentBundle({
    workflows: [workflow],
    tasks: [task1, task2],
    targetArchitecture: "amd64",
    secretNames: ["DATABASE_URL", "STRIPE_API_KEY"],
    dependencyLockContent: "lockfile-content-v1",
    bundleFiles: {
      "index.js": "console.log('hello');",
    },
  });

  assert.ok(bundle.manifest);
  assert.equal(bundle.manifest.manifestVersion, 1);
  assert.equal(bundle.manifest.protocolMajor, 1);
  assert.equal(bundle.manifest.nodeRuntimeMajor, 24);
  assert.equal(bundle.manifest.targetOS, "linux");
  assert.equal(bundle.manifest.targetArchitecture, "amd64");
  assert.match(bundle.bundleDigest, /^[0-9a-f]{64}$/);
  assert.match(bundle.dependencyLockDigest, /^[0-9a-f]{64}$/);
  assert.deepEqual(bundle.manifest.secretNames, [
    "DATABASE_URL",
    "STRIPE_API_KEY",
  ]);

  // Verify against deployment.schema.json
  const validation = validateDeployment(bundle.manifest);
  assert.equal(validation.valid, true);
  assert.deepEqual(validation.errors, []);
});

test("buildDeploymentBundle automatically includes tasks referenced in workflows", () => {
  const bundle = buildDeploymentBundle({
    workflows: [workflow], // tasks not explicitly passed; should be inferred from workflow
    targetArchitecture: "arm64",
    dependencyLockContent: "lockfile-content-v1",
    bundleFiles: {
      "tasks.js": "export const task = async () => ({ msg: 'ok' });",
    },
  });

  assert.equal(bundle.manifest.targetArchitecture, "arm64");
  assert.equal(bundle.manifest.tasks.length, 2);
  const taskNames = bundle.manifest.tasks.map((t) => t.name);
  assert.ok(taskNames.includes("task-one"));
  assert.ok(taskNames.includes("task-two"));
});

test("buildDeploymentBundle rejects secret values and invalid secret names", () => {
  for (const badSecret of [
    "KEY=value123", // secret value assignment
    "SECRET_KEY=raw_secret_value",
    "has space",
    "has-hyphen", // deployment.schema.json requires ^[A-Za-z_][A-Za-z0-9_]*$
    "1startsWithNumber",
  ]) {
    assert.throws(
      () =>
        buildDeploymentBundle({
          workflows: [workflow],
          secretNames: [badSecret],
          dependencyLockContent: "lockfile-content-v1",
          bundleFiles: {
            "tasks.js": "export const task = async () => ({ msg: 'ok' });",
          },
        }),
      (err) => err instanceof ContractError && err.code === "INVALID_MANIFEST",
    );
  }
});

test("buildDeploymentBundle rejects unsupported target architecture or OS", () => {
  assert.throws(
    () =>
      buildDeploymentBundle({
        workflows: [workflow],
        targetOS: "windows",
        dependencyLockContent: "lockfile-content-v1",
        bundleFiles: {
          "tasks.js": "export const task = async () => ({ msg: 'ok' });",
        },
      }),
    (err) =>
      err instanceof ContractError && err.code === "UNSUPPORTED_CAPABILITY",
  );

  assert.throws(
    () =>
      buildDeploymentBundle({
        workflows: [workflow],
        targetArchitecture: "x86",
        dependencyLockContent: "lockfile-content-v1",
        bundleFiles: {
          "tasks.js": "export const task = async () => ({ msg: 'ok' });",
        },
      }),
    (err) =>
      err instanceof ContractError && err.code === "UNSUPPORTED_CAPABILITY",
  );
});

test("buildDeploymentBundle rejects empty workflows or tasks", () => {
  assert.throws(
    () =>
      buildDeploymentBundle({
        workflows: [],
        tasks: [task1],
        dependencyLockContent: "lockfile-content-v1",
        bundleFiles: {
          "tasks.js": "export const task = async () => ({ msg: 'ok' });",
        },
      }),
    (err) => err instanceof ContractError,
  );
});

test("buildDeploymentBundle fails closed without real dependency lock material", () => {
  assert.throws(
    () =>
      buildDeploymentBundle({
        workflows: [workflow],
        bundleFiles: {
          "tasks.js": "export const task = async () => ({ msg: 'ok' });",
        },
      }),
    (err) => err instanceof ContractError && err.code === "INVALID_MANIFEST",
  );
});

test("buildDeploymentBundle fails closed without executable bundle material", () => {
  assert.throws(
    () =>
      buildDeploymentBundle({
        workflows: [workflow],
        dependencyLockContent: "lockfile-content-v1",
      }),
    (err) => err instanceof ContractError && err.code === "INVALID_MANIFEST",
  );
});

test("buildDeploymentBundle digests are stable for identical bytes and change with byte changes", () => {
  const baseOptions = {
    workflows: [workflow],
    dependencyLockContent: "lockfile-content-v1",
    bundleFiles: {
      "tasks.js": "export const task = async () => ({ msg: 'ok' });",
    },
  };

  const first = buildDeploymentBundle(baseOptions);
  const second = buildDeploymentBundle(baseOptions);
  const changedBundle = buildDeploymentBundle({
    ...baseOptions,
    bundleFiles: {
      "tasks.js": "export const task = async () => ({ msg: 'changed' });",
    },
  });
  const changedLock = buildDeploymentBundle({
    ...baseOptions,
    dependencyLockContent: "lockfile-content-v2",
  });

  assert.equal(first.bundleDigest, second.bundleDigest);
  assert.equal(first.dependencyLockDigest, second.dependencyLockDigest);
  assert.notEqual(first.bundleDigest, changedBundle.bundleDigest);
  assert.notEqual(first.dependencyLockDigest, changedLock.dependencyLockDigest);
});

test("buildDeploymentBundle architecture changes produce distinct bundle digests", () => {
  const baseOptions = {
    workflows: [workflow],
    dependencyLockContent: "lockfile-content-v1",
    bundleFiles: {
      "tasks.js": "export const task = async () => ({ msg: 'ok' });",
    },
  };

  const amd64Bundle = buildDeploymentBundle({
    ...baseOptions,
    targetArchitecture: "amd64",
  });
  const arm64Bundle = buildDeploymentBundle({
    ...baseOptions,
    targetArchitecture: "arm64",
  });

  assert.notEqual(amd64Bundle.bundleDigest, arm64Bundle.bundleDigest);
  assert.equal(
    amd64Bundle.dependencyLockDigest,
    arm64Bundle.dependencyLockDigest,
  );
});

test("buildDeploymentBundle embeds canonical platform identity matching the manifest target", () => {
  const bundle = buildDeploymentBundle({
    workflows: [workflow],
    targetArchitecture: "arm64",
    dependencyLockContent: "lockfile-content-v1",
    bundleFiles: {
      "tasks.js": "export const task = async () => ({ msg: 'ok' });",
    },
  });

  const raw = bundle.files[".deadbolt/platform.json"];
  assert.ok(raw, "expected .deadbolt/platform.json in bundle files");
  const platformJson = new TextDecoder().decode(raw);
  assert.deepEqual(JSON.parse(platformJson), {
    targetArchitecture: "arm64",
    targetOS: "linux",
  });
  // Embedded bytes must be the deterministic canonical form and agree with
  // the manifest target, so identity cannot drift from the manifest.
  assert.equal(
    platformJson,
    canonicalize({ targetArchitecture: "arm64", targetOS: "linux" }),
  );
  assert.equal(bundle.manifest.targetArchitecture, "arm64");
  assert.equal(bundle.manifest.targetOS, "linux");
});
