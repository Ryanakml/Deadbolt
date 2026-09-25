#!/usr/bin/env node
// Count executed vs skipped tests from deterministic `go test -json` output for M3 gate.
// Reads a file containing `go test -json` output and prints:
//   "<passed> <skipped> <failed>"
// using only the structured Action/Test fields, never human-readable text.
// Package-level events (no Test field) are ignored; subtests count as
// executed tests. Exits 0 unless the input file cannot be read.
import { readFileSync } from "node:fs";

const path = process.argv[2];
if (!path) {
  console.error("usage: m3-count-results.mjs <go-test-json-file>");
  process.exit(2);
}

let raw;
try {
  raw = readFileSync(path, "utf8");
} catch (err) {
  console.error(`cannot read ${path}: ${err.message}`);
  process.exit(2);
}

let passed = 0;
let skipped = 0;
let failed = 0;
for (const line of raw.split("\n")) {
  const trimmed = line.trim();
  if (!trimmed) continue;
  let ev;
  try {
    ev = JSON.parse(trimmed);
  } catch {
    continue;
  }
  if (!ev.Test) continue;
  if (ev.Action === "pass") passed += 1;
  else if (ev.Action === "skip") skipped += 1;
  else if (ev.Action === "fail") failed += 1;
}
process.stdout.write(`${passed} ${skipped} ${failed}`);
