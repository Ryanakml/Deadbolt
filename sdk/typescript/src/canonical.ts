import serialize from "canonicalize";
import { createHash } from "node:crypto";
import { assertJSON, parseJSON } from "./json.js";
export function canonicalize(value: unknown): string {
  assertJSON(value);
  return serialize(value)!;
}
export function sha256Hex(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}
export function canonicalDigest(value: unknown): {
  canonical: string;
  sha256: string;
} {
  const canonical = canonicalize(value);
  return { canonical, sha256: sha256Hex(canonical) };
}
export function digestJSON(raw: string | Uint8Array): {
  canonical: string;
  sha256: string;
} {
  return canonicalDigest(parseJSON(raw));
}
