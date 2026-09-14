import { readdirSync } from "node:fs";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

const roots = process.argv.slice(2);
if (roots.length === 0) {
  roots.push("tests");
}

function collectTestFiles(dir) {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      return collectTestFiles(path);
    }
    return entry.isFile() && entry.name.endsWith(".test.js") ? [path] : [];
  });
}

const files = roots.flatMap(collectTestFiles).sort();
if (files.length === 0) {
  throw new Error(`No .test.js files found under: ${roots.join(", ")}`);
}

const result = spawnSync(process.execPath, ["--test", ...files], {
  stdio: "inherit",
});

if (result.error) {
  throw result.error;
}
process.exit(result.status ?? 1);
