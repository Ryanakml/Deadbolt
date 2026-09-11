import fs from "node:fs";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { evaluate } from "./lib/conformance.mjs";
const fixtures = JSON.parse(
  fs.readFileSync("contracts/fixtures/conformance.json", "utf8"),
);
const go = JSON.parse(
  execFileSync("go", ["run", "./tests/contracts"], {
    encoding: "utf8",
    maxBuffer: 4 * 1024 * 1024,
  }),
);
for (const f of fixtures) {
  const ts = evaluate(f);
  assert.deepEqual(ts, f.expected, f.id);
  assert.deepEqual(ts, go[f.id], `Go/TS parity: ${f.id}`);
}
console.log(
  `${fixtures.length} fixtures: expected values and Go/TS parity passed`,
);
