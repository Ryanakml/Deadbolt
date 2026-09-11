import { canonicalize } from "./canonical.js";
import { Validator, type Schema } from "jsonschema";
import { assertJSON, fail, type JSONValue } from "./json.js";
export type ObjectValue = Record<string, JSONValue>;
export function object(v: JSONValue): ObjectValue {
  if (v === null || typeof v !== "object" || Array.isArray(v))
    fail("INVALID_MANIFEST");
  return v;
}
export function validateAgainst(value: JSONValue, schema: JSONValue): boolean {
  return new Validator().validate(value, schema as Schema).valid;
}
export function validateSchema(schema: JSONValue, meta: JSONValue): void {
  assertJSON(schema);
  if (Buffer.byteLength(canonicalize(schema)) > 65536)
    fail("SCHEMA_SIZE_EXCEEDED");
  if (!validateAgainst(schema, meta)) fail("UNSUPPORTED_SCHEMA");
  const visit = (s: ObjectValue) => {
    if (s.oneOf) {
      const variants = s.oneOf as ObjectValue[];
      // Tagged union: one common required property has distinct const values.
      const first = object(variants[0].properties ?? {});
      const tagged = Object.keys(first).some((key) => {
        const tags: string[] = [];
        for (const variant of variants) {
          const p = object(variant.properties ?? {}),
            field = p[key];
          if (
            !Array.isArray(variant.required) ||
            !variant.required.includes(key) ||
            !field ||
            typeof field !== "object" ||
            !Object.hasOwn(field, "const")
          )
            return false;
          tags.push(canonicalize(object(field).const));
        }
        return new Set(tags).size === variants.length;
      });
      if (!tagged) fail("UNSUPPORTED_SCHEMA");
      variants.forEach(visit);
    }
    for (const child of Object.values(object(s.properties ?? {})))
      visit(object(child));
    if (s.items) visit(object(s.items));
    if (
      typeof s.additionalProperties === "object" &&
      s.additionalProperties !== null
    )
      visit(object(s.additionalProperties));
  };
  visit(object(schema));
}
export function validatePayload(
  schema: JSONValue,
  value: JSONValue,
  meta: JSONValue,
): void {
  validateSchema(schema, meta);
  assertJSON(value);
  if (!validateAgainst(value, schema)) fail("SCHEMA_VALIDATION_ERROR");
}
