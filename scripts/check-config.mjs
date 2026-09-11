import fs from "node:fs";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
const pkg = JSON.parse(fs.readFileSync("package.json", "utf8"));
assert.equal(
  process.versions.node,
  pkg.engines.node,
  "Use the pinned Node version",
);
assert.equal(
  execFileSync("pnpm", ["--version"], { encoding: "utf8" }).trim(),
  pkg.packageManager.split("@")[1],
);
assert.equal(
  execFileSync("go", ["env", "GOVERSION"], { encoding: "utf8" }).trim(),
  "go1.27.1",
);
assert.match(fs.readFileSync("go.mod", "utf8"), /^go 1\.27\.1$/m);
assert.match(
  fs.readFileSync("go.mod", "utf8"),
  /github.com\/jackc\/pgx\/v5 v5\.11\.0/,
);
const images = JSON.parse(fs.readFileSync("deploy/images.lock.json", "utf8"));
for (const name of ["go", "node", "postgres", "nats", "local-object-store"]) {
  const p = images[name];
  assert.match(p.image, /@sha256:[0-9a-f]{64}$/);
  assert.deepEqual(p.platforms, ["linux/amd64", "linux/arm64"]);
}
const tools = JSON.parse(fs.readFileSync("tools.lock.json", "utf8"));
for (const name of ["sqlc", "goose", "gitleaks", "govulncheck"])
  assert.match(tools[name].version, /^v\d+\.\d+\.\d+$/);
const baseline = JSON.parse(
  fs.readFileSync("deploy/provisioning.json", "utf8"),
);
assert.equal(baseline.deploymentReady, false);
assert.equal(baseline.project, "deadbolt-staging");
assert(
  baseline.inputs.every(
    (x) => x.status === "unresolved" && x.name && x.purpose,
  ),
);
console.log(
  "Toolchain/image/tool pins and provisioning declaration checked. Staging readiness remains blocked by documented inputs.",
);
