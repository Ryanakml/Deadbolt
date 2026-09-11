import { canonicalize } from "./canonical.js";
import { type JSONValue, fail, assertJSON } from "./json.js";
import { object, type ObjectValue } from "./schema.js";
export function pointerParts(pointer: JSONValue): string[] {
  if (
    typeof pointer !== "string" ||
    (pointer !== "" && !pointer.startsWith("/")) ||
    /~(?![01])/.test(pointer)
  )
    fail("INPUT_MAPPING_ERROR");
  return pointer === ""
    ? []
    : pointer
        .slice(1)
        .split("/")
        .map((x) => x.replace(/~1/g, "/").replace(/~0/g, "~"));
}
export function lookup(
  value: JSONValue,
  pointer: JSONValue,
): { found: boolean; value?: JSONValue } {
  let current = value;
  for (const key of pointerParts(pointer)) {
    if (
      current === null ||
      typeof current !== "object" ||
      !Object.hasOwn(current, key)
    )
      return { found: false };
    if (
      Array.isArray(current) &&
      (!/^(0|[1-9][0-9]*)$/.test(key) || key !== String(Number(key)))
    )
      return { found: false };
    current = (current as ObjectValue)[key];
  }
  return { found: true, value: current };
}
export function reference(v: ObjectValue): void {
  if (v.$ref !== "run.input" && v.$ref !== "step.output")
    fail("INPUT_MAPPING_ERROR");
  const allowed = new Set([
    "$ref",
    "pointer",
    "default",
    ...(v.$ref === "step.output" ? ["stepId"] : []),
  ]);
  if (
    Object.keys(v).some((k) => !allowed.has(k)) ||
    (v.$ref === "step.output" && typeof v.stepId !== "string")
  )
    fail("INPUT_MAPPING_ERROR");
  pointerParts(v.pointer);
}
export function mapInput(
  mapping: JSONValue,
  input: JSONValue,
  outputs: ObjectValue,
): JSONValue {
  assertJSON(mapping);
  assertJSON(input);
  assertJSON(outputs);
  const walk = (v: JSONValue): JSONValue => {
    if (v === null || typeof v !== "object") return v;
    if (Array.isArray(v)) return v.map(walk);
    if (Object.hasOwn(v, "literal")) {
      if (Object.keys(v).length !== 1) fail("INPUT_MAPPING_ERROR");
      return v.literal;
    }
    if (Object.hasOwn(v, "$ref")) {
      reference(v);
      const source =
        v.$ref === "run.input"
          ? input
          : Object.hasOwn(outputs, String(v.stepId))
            ? outputs[String(v.stepId)]
            : undefined;
      const result =
        source === undefined ? { found: false } : lookup(source, v.pointer);
      if (result.found) return result.value!;
      if (Object.hasOwn(v, "default")) return v.default;
      return fail("INPUT_MAPPING_ERROR");
    }
    return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, walk(x)]));
  };
  return walk(mapping);
}
// This is contract conformance only; choice nodes remain capability-gated in MVP.
export function evaluateChoice(
  expression: JSONValue,
  input: JSONValue,
  outputs: ObjectValue,
): boolean {
  assertJSON(expression);
  const walk = (e: JSONValue): boolean => {
    if (e === null || typeof e !== "object" || Array.isArray(e))
      fail("INVALID_EXPRESSION");
    const v = e;
    if (
      Object.keys(v).length !== 2 ||
      typeof v.op !== "string" ||
      !Array.isArray(v.args)
    )
      fail("INVALID_EXPRESSION");
    const args = v.args;
    if (["and", "or", "not"].includes(v.op)) {
      if (
        (v.op === "not" && args.length !== 1) ||
        (v.op !== "not" && args.length < 1)
      )
        fail("INVALID_EXPRESSION");
      const values = args.map(walk);
      return v.op === "not"
        ? !values[0]
        : v.op === "and"
          ? values.every(Boolean)
          : values.some(Boolean);
    }
    if (v.op === "exists") {
      if (args.length !== 1) fail("INVALID_EXPRESSION");
      const ref = object(args[0]);
      reference(ref);
      try {
        mapInput(ref, input, outputs);
        return true;
      } catch (e) {
        if (
          e instanceof Error &&
          "code" in e &&
          e.code === "INPUT_MAPPING_ERROR"
        )
          return false;
        throw e;
      }
    }
    if (
      !["eq", "neq", "gt", "gte", "lt", "lte", "in"].includes(v.op) ||
      args.length !== 2
    )
      fail("INVALID_EXPRESSION");
    const a = mapInput(args[0], input, outputs),
      b = mapInput(args[1], input, outputs);
    const kind = (x: JSONValue) =>
      x === null ? "null" : Array.isArray(x) ? "array" : typeof x;
    const equal = (x: JSONValue, y: JSONValue): boolean => {
      if (kind(x) !== kind(y)) fail("INVALID_EXPRESSION");
      return canonicalize(x) === canonicalize(y);
    };
    if (v.op === "in") {
      if (!Array.isArray(b)) fail("INVALID_EXPRESSION");
      const values = b.map((x) => equal(a, x));
      return values.some(Boolean);
    }
    if (v.op === "eq" || v.op === "neq") {
      const eq = equal(a, b);
      return v.op === "eq" ? eq : !eq;
    }
    if (typeof a !== "number" || typeof b !== "number")
      fail("INVALID_EXPRESSION");
    return v.op === "gt"
      ? a > b
      : v.op === "gte"
        ? a >= b
        : v.op === "lt"
          ? a < b
          : a <= b;
  };
  return walk(expression);
}
