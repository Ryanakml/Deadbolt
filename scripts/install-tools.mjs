import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";
const tools = JSON.parse(fs.readFileSync("tools.lock.json", "utf8"));
const names = process.argv.slice(2);
if (!names.length) throw Error("Specify sqlc/goose/govulncheck/gitleaks");
const bin = path.resolve("bin");
fs.mkdirSync(bin, { recursive: true });
for (const name of names) {
  const pin = tools[name];
  if (!pin) throw Error("Unknown tool");
  execFileSync("go", ["install", `${pin.module}@${pin.version}`], {
    stdio: "inherit",
    env: { ...process.env, GOBIN: bin },
  });
}
