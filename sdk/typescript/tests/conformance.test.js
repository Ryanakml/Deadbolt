import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { evaluate } from "../../../scripts/lib/conformance.mjs";
import { canonicalize, parseJSON } from "../dist/index.js";
const fixtures = JSON.parse(
  fs.readFileSync(
    new URL("../../../contracts/fixtures/conformance.json", import.meta.url),
  ),
);
for (const f of fixtures)
  test(f.id, () => assert.deepEqual(evaluate(f), f.expected));
test("Reject non-JSON JavaScript objects without invoking accessors", () => {
  for (const x of [
    new Date(),
    Buffer.from("x"),
    1n,
    () => 1,
    undefined,
    NaN,
    Infinity,
    9007199254740992,
    "\ud800",
    new Array(1),
    {
      get x() {
        throw Error("must not execute");
      },
    },
  ])
    assert.throws(() => canonicalize(x), { code: "INVALID_JSON" });
  const cycle = {};
  cycle.self = cycle;
  assert.throws(() => canonicalize(cycle), { code: "INVALID_JSON" });
});
test("Reject malformed UTF-8", () =>
  assert.throws(() => parseJSON(Uint8Array.from([34, 0xc0, 0xaf, 34])), {
    code: "INVALID_JSON",
  }));
test("Reject hidden serialization hooks and malformed arrays", () => {
  let called = false;
  const hidden = Object.defineProperty({}, "toJSON", {
    value() {
      called = true;
      return 1;
    },
  });
  const getter = Object.defineProperty({}, "toJSON", {
    get() {
      called = true;
      return () => 1;
    },
  });
  const sparse = new Array(1);
  sparse.extra = 1;
  for (const value of [hidden, getter, sparse])
    assert.throws(() => canonicalize(value), { code: "INVALID_JSON" });
  assert.equal(called, false);
});
test("Reject sparse arrays disguised by a whitespace-suffixed index", () => {
  const v = new Array(1);
  v["0\n"] = 1;
  assert.throws(() => canonicalize(v), { code: "INVALID_JSON" });
});
