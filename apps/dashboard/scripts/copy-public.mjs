import { cp, mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const dashboardDir = path.resolve(here, "..");
const source = path.join(dashboardDir, "public");
const destination = path.join(dashboardDir, "dist");

await mkdir(destination, { recursive: true });
await cp(source, destination, { recursive: true, force: true });
