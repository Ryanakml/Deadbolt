import process from "node:process";
import { buildDeploymentBundle } from "@runtime/sdk";
import { customerOnboardingWorkflow } from "./workflow.js";

export function buildPipelineBundle() {
  console.log(
    "Building immutable local bundle for customer-onboarding pipeline...",
  );

  const bundle = buildDeploymentBundle({
    workflows: [customerOnboardingWorkflow],
    targetOS: "linux",
    targetArchitecture: "amd64",
    secretNames: ["RESEND_API_KEY", "DATABASE_URL"],
    dependencyLockContent:
      "# pnpm-lock.yaml version 9.0\nlockfileVersion: '9.0'\n",
    bundleFiles: {
      "tasks.js": "// compiled tasks\n",
      "workflow.js": "// compiled workflow\n",
    },
  });

  console.log("Immutable Bundle Built Successfully!");
  console.log(`Bundle Digest (SHA-256): ${bundle.bundleDigest}`);
  console.log(
    `Dependency Lock Digest (SHA-256): ${bundle.dependencyLockDigest}`,
  );
  console.log(`Target Architecture: ${bundle.manifest.targetArchitecture}`);
  console.log(`Node Runtime Major: ${bundle.manifest.nodeRuntimeMajor}`);
  console.log(
    `Registered Tasks: ${(bundle.manifest.tasks as any[]).map((t) => t.name).join(", ")}`,
  );
  console.log("\nGenerated Deployment Manifest JSON:");
  console.log(JSON.stringify(bundle.manifest, null, 2));

  return bundle;
}

// Run directly if invoked as main script
if (
  process.argv[1]?.endsWith("build.ts") ||
  process.argv[1]?.endsWith("build.js")
) {
  buildPipelineBundle();
}
