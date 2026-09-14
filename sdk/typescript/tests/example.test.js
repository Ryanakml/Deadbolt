import test from "node:test";
import assert from "node:assert/strict";
import { customerOnboardingWorkflow } from "../examples/linear-pipeline/workflow.js";
import { buildPipelineBundle } from "../examples/linear-pipeline/build.js";
import { validateDeployment } from "../dist/index.js";

test("linear pipeline example builds valid deployment bundle", () => {
  const bundle = buildPipelineBundle();

  assert.ok(bundle);
  assert.equal(bundle.manifest.manifestVersion, 1);
  assert.equal(bundle.manifest.nodeRuntimeMajor, 24);
  assert.equal(bundle.manifest.targetOS, "linux");
  assert.equal(bundle.manifest.targetArchitecture, "amd64");
  assert.match(bundle.bundleDigest, /^[0-9a-f]{64}$/);
  assert.match(bundle.dependencyLockDigest, /^[0-9a-f]{64}$/);

  // Validate entire deployment against deployment.schema.json
  const validation = validateDeployment(bundle.manifest);
  assert.equal(validation.valid, true);
  assert.deepEqual(validation.errors, []);
});

test("customerOnboardingWorkflow exports valid workflow nodes and mappings", () => {
  const manifest = customerOnboardingWorkflow.toManifest();
  assert.equal(manifest.name, "customer-onboarding");
  assert.equal(manifest.nodes.length, 3);
  assert.deepEqual(
    manifest.nodes.map((n) => n.id),
    ["validate", "provision", "send-email"],
  );
  assert.deepEqual(manifest.nodes[1].after, ["validate"]);
  assert.deepEqual(manifest.nodes[2].after, ["provision"]);
});
