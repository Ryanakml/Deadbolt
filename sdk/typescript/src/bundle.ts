import { createHash } from "node:crypto";
import { canonicalDigest, canonicalize } from "./canonical.js";
import { ContractError, fail, type JSONValue } from "./json.js";
import { type ObjectValue } from "./schema.js";
import type { TaskDefinition } from "./task.js";
import { validateDeployment } from "./validator.js";
import type { WorkflowDefinition } from "./workflow.js";

export interface BundleOptions {
  workflows: (WorkflowDefinition<any, any> | ObjectValue)[];
  tasks?: (TaskDefinition<any, any> | ObjectValue)[];
  targetArchitecture?: "amd64" | "arm64";
  targetOS?: "linux";
  sdkVersion?: string;
  secretNames?: string[];
  dependencyLockContent?: string | Uint8Array;
  dependencyLockDigest?: string;
  bundleFiles?: Record<string, string | Uint8Array>;
  bundleDigest?: string;
}

export interface DeploymentBundle {
  readonly manifest: ObjectValue;
  readonly manifestJson: string;
  readonly bundleDigest: string;
  readonly dependencyLockDigest: string;
  readonly files: Readonly<Record<string, Uint8Array>>;
}

const SECRET_NAME_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/;
const HEX_SHA256_REGEX = /^[0-9a-f]{64}$/;

export function calculateDigest(content: string | Uint8Array): string {
  const hash = createHash("sha256");
  if (typeof content === "string") {
    hash.update(content, "utf8");
  } else {
    hash.update(content);
  }
  return hash.digest("hex");
}

export function buildDeploymentBundle(
  options: BundleOptions,
): DeploymentBundle {
  const targetOS = options.targetOS ?? "linux";
  if (targetOS !== "linux") {
    fail("UNSUPPORTED_CAPABILITY");
  }

  const targetArchitecture = options.targetArchitecture ?? "amd64";
  if (targetArchitecture !== "amd64" && targetArchitecture !== "arm64") {
    fail("UNSUPPORTED_CAPABILITY");
  }

  // 1. Collect all tasks
  const tasksMap = new Map<string, ObjectValue>();

  if (options.tasks) {
    for (const t of options.tasks) {
      const manifest: ObjectValue =
        "toManifest" in (t as any) &&
        typeof (t as any).toManifest === "function"
          ? (t as TaskDefinition<any, any>).toManifest()
          : (t as ObjectValue);
      tasksMap.set(String(manifest.name), manifest);
    }
  }

  // 2. Collect workflows and tasks referenced in workflows
  const workflowManifests: ObjectValue[] = [];
  for (const w of options.workflows) {
    if (
      "toManifest" in (w as any) &&
      typeof (w as any).toManifest === "function"
    ) {
      workflowManifests.push((w as WorkflowDefinition<any, any>).toManifest());
      if ("tasks" in (w as any) && Array.isArray((w as any).tasks)) {
        for (const t of (w as any).tasks as (
          | TaskDefinition<any, any>
          | ObjectValue
        )[]) {
          const tManifest: ObjectValue =
            "toManifest" in (t as any) &&
            typeof (t as any).toManifest === "function"
              ? (t as TaskDefinition<any, any>).toManifest()
              : (t as ObjectValue);
          if (!tasksMap.has(String(tManifest.name))) {
            tasksMap.set(String(tManifest.name), tManifest);
          }
        }
      }
    } else {
      workflowManifests.push(w as ObjectValue);
    }
  }

  const taskManifests = Array.from(tasksMap.values());
  if (taskManifests.length === 0) {
    fail("INVALID_TASK");
  }
  if (workflowManifests.length === 0) {
    fail("EMPTY_NODES");
  }

  // Ensure each task has an entrypoint (required by deployment.schema.json)
  for (const t of taskManifests) {
    if (!t.entrypoint) {
      t.entrypoint = `./tasks/${t.name}.js`;
    }
  }

  // 3. Process secret names: strictly variable names only, never secret values
  const secretNames = options.secretNames ?? [];
  for (const name of secretNames) {
    if (typeof name !== "string" || !SECRET_NAME_REGEX.test(name)) {
      fail("INVALID_MANIFEST");
    }
    // Reject common accidental secret value injections (e.g. contains = or quotes)
    if (name.includes("=") || name.includes(" ") || name.length > 128) {
      fail("INVALID_MANIFEST");
    }
  }

  // 4. Calculate dependencyLockDigest
  let dependencyLockDigest = options.dependencyLockDigest;
  if (!dependencyLockDigest) {
    if (options.dependencyLockContent !== undefined) {
      dependencyLockDigest = calculateDigest(options.dependencyLockContent);
    } else {
      fail("INVALID_MANIFEST");
    }
  }
  if (!HEX_SHA256_REGEX.test(dependencyLockDigest)) {
    fail("INVALID_MANIFEST");
  }

  // 5. Build bundle files and calculate bundleDigest
  const files: Record<string, Uint8Array> = {};
  if (options.bundleFiles) {
    for (const [filePath, content] of Object.entries(options.bundleFiles)) {
      if (typeof content === "string") {
        files[filePath] = new TextEncoder().encode(content);
      } else {
        files[filePath] = content;
      }
    }
  }

  let bundleDigest = options.bundleDigest;
  if (!bundleDigest) {
    if (options.bundleFiles && Object.keys(options.bundleFiles).length > 0) {
      // Platform participates in immutable bundle identity: embed canonical
      // target metadata so same source + different target => different digest.
      const platformJson = canonicalize({
        targetArchitecture,
        targetOS,
      });
      files[".deadbolt/platform.json"] = new TextEncoder().encode(platformJson);

      // Deterministically sort file keys and hash canonical payload
      const sortedKeys = Object.keys(files).sort();
      const combinedHash = createHash("sha256");
      for (const k of sortedKeys) {
        combinedHash.update(`${k}\0`);
        combinedHash.update(files[k]);
      }
      bundleDigest = combinedHash.digest("hex");
    } else {
      fail("INVALID_MANIFEST");
    }
  }
  if (!HEX_SHA256_REGEX.test(bundleDigest)) {
    fail("INVALID_MANIFEST");
  }

  // 6. Assemble complete deployment manifest
  const deploymentManifest: ObjectValue = {
    manifestVersion: 1,
    sdkVersion: options.sdkVersion ?? "0.1.0",
    protocolMajor: 1,
    nodeRuntimeMajor: 24,
    targetOS: "linux",
    targetArchitecture,
    dependencyLockDigest,
    bundleDigest,
    secretNames: Array.from(new Set(secretNames)),
    tasks: taskManifests,
    workflows: workflowManifests,
  };

  // 7. Validate deployment manifest against deployment.schema.json
  const validation = validateDeployment(deploymentManifest);
  if (!validation.valid) {
    const err = validation.errors[0];
    throw new ContractError(err.code, err.message);
  }

  const { canonical } = canonicalDigest(deploymentManifest);

  return {
    manifest: deploymentManifest,
    manifestJson: canonical,
    bundleDigest,
    dependencyLockDigest,
    files: Object.freeze(files),
  };
}
