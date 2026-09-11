import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { canonicalize, sha256Hex, canonicalDigest } from '../dist/canonical.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const fixturesPath = path.resolve(__dirname, '../../../contracts/fixtures/canonical-json/jcs-vectors.json');
const vectors = JSON.parse(fs.readFileSync(fixturesPath, 'utf8'));

test('RFC 8785 JSON Canonicalization Scheme matches all golden vectors', () => {
  for (const vector of vectors) {
    const { canonical, sha256 } = canonicalDigest(vector.input);
    assert.equal(canonical, vector.expectedCanonical, `Canonical string mismatch for vector [${vector.id}]`);
    assert.equal(sha256, vector.expectedSha256, `SHA-256 digest mismatch for vector [${vector.id}]`);
  }
});

test('Rejection of invalid non-finite numbers per RFC 8785', () => {
  assert.throws(() => canonicalize({ bad: NaN }), /Non-finite numbers/);
  assert.throws(() => canonicalize({ bad: Infinity }), /Non-finite numbers/);
  assert.throws(() => canonicalize({ bad: -Infinity }), /Non-finite numbers/);
});

test('Rejection of undefined values per RFC 8785', () => {
  assert.throws(() => canonicalize({ bad: undefined }), /undefined value/);
});
