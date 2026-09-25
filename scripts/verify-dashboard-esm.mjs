import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";

const dashboardBaseURL = process.argv[2]?.replace(/\/$/, "");
if (!dashboardBaseURL) {
  throw new Error(
    "usage: node scripts/verify-dashboard-esm.mjs <dashboard-base-url>",
  );
}

const moduleNames = [
  "index.js",
  "api.js",
  "auth.js",
  "inspector.js",
  "permissions.js",
  "stream.js",
  "types.js",
];
const artifactDir = await mkdtemp(join(tmpdir(), "deadbolt-dashboard-esm-"));
const dashboardRootURL = new URL(`${dashboardBaseURL}/`);

function localImports(source) {
  const imports = [];
  const importPattern =
    /\b(?:from\s*|import\s*\(\s*|import\s*)["'](\.[^"']+)["']/g;
  for (const match of source.matchAll(importPattern)) {
    imports.push(match[1]);
  }
  return imports;
}

function moduleNameFor(moduleURL) {
  if (
    moduleURL.origin !== dashboardRootURL.origin ||
    !moduleURL.pathname.startsWith(dashboardRootURL.pathname)
  ) {
    throw new Error(
      `dashboard module imports an external local path: ${moduleURL}`,
    );
  }
  return decodeURIComponent(
    moduleURL.pathname.slice(dashboardRootURL.pathname.length),
  );
}

try {
  // Node uses the nearest package.json to determine whether .js is ESM. This
  // mirrors the browser module graph while keeping DOM bootstrap disabled.
  await writeFile(join(artifactDir, "package.json"), '{"type":"module"}\n');

  const pendingModules = [...moduleNames];
  const fetchedModules = new Set();
  while (pendingModules.length > 0) {
    const moduleName = pendingModules.pop();
    if (fetchedModules.has(moduleName)) {
      continue;
    }
    fetchedModules.add(moduleName);

    const response = await fetch(`${dashboardBaseURL}/${moduleName}`);
    if (!response.ok) {
      throw new Error(`GET ${moduleName} returned HTTP ${response.status}`);
    }
    const source = await response.text();
    const localPath = join(artifactDir, moduleName);
    await mkdir(dirname(localPath), { recursive: true });
    await writeFile(localPath, source);

    const sourceURL = new URL(moduleName, dashboardRootURL);
    for (const importSpecifier of localImports(source)) {
      pendingModules.push(moduleNameFor(new URL(importSpecifier, sourceURL)));
    }
  }

  globalThis.document = undefined;
  await import(pathToFileURL(join(artifactDir, "index.js")).href);
  process.stdout.write(
    "Dashboard final-image ESM module graph imported successfully\n",
  );
} finally {
  await rm(artifactDir, { recursive: true, force: true });
}
