import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { digestJSON } from "../sdk/typescript/dist/index.js";
const cases = ['{"b":2,"a":1}', '{"a":1,"a":2}', '"\\ud800"'];
let mismatches = 0;
for (const raw of cases) {
  const native = JSON.stringify(JSON.parse(raw));
  try {
    if (native !== digestJSON(raw).canonical) mismatches++;
  } catch {
    mismatches++;
  }
}
assert.equal(mismatches, cases.length);
console.log(
  `JSON.parse/stringify alone: ${mismatches}/${cases.length} adversarial cases disagree with the strict JCS contract`,
);
execFileSync("go", ["run", "./tests/candidates"], { stdio: "inherit" });
