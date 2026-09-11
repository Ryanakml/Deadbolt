import fs from "node:fs";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import SwaggerParser from "@apidevtools/swagger-parser";
import YAML from "yaml";
execFileSync(process.execPath, ["scripts/generate-contracts.mjs", "--check"]);
const api = await SwaggerParser.validate(
  "contracts/openapi/control-plane.yaml",
  { resolve: { http: false } },
);
const source = YAML.parse(
  fs.readFileSync("contracts/openapi/control-plane.yaml", "utf8"),
);
for (const [key, values] of Object.entries(
  JSON.parse(fs.readFileSync("contracts/common/enums.json")),
))
  assert.deepEqual(api.components.schemas[key].enum, values);
for (const item of Object.values(source.paths))
  for (const method of ["get", "post", "patch", "delete"]) {
    const op = item[method];
    if (!op) continue;
    assert.equal(op["x-implemented"], false);
    assert(op["x-required-capability"]);
    assert(op.security ?? source.security);
    for (const code of [
      "400",
      "401",
      "403",
      "404",
      "409",
      "413",
      "422",
      "429",
      "503",
    ])
      assert(op.responses[code]);
    if (method !== "get")
      assert(op.parameters.some((p) => p.name === "Idempotency-Key"));
  }
const worker = JSON.parse(
  fs.readFileSync("contracts/worker/protocol.schema.json", "utf8"),
);
for (const name of [
  "ChallengeRequest",
  "EnrollRequest",
  "SessionRequest",
  "PollRequest",
  "StartRequest",
  "HeartbeatRequest",
  "CompleteRequest",
  "StopAckRequest",
  "LogBatchRequest",
]) {
  const shapes = worker.$defs[name].oneOf ?? [worker.$defs[name]];
  for (const schema of shapes)
    assert(
      ["protocolVersion", "requestId"].every((x) =>
        schema.required.includes(x),
      ),
    );
}
console.log(
  "OpenAPI, canonical enums, worker request metadata and generated schema consistency passed",
);
