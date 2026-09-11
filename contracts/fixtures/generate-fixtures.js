const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// RFC 8785 JSON Canonicalization Scheme (JCS) serializer
function canonicalize(val) {
  if (val === null || typeof val !== 'object') {
    return JSON.stringify(val);
  }
  if (Array.isArray(val)) {
    return '[' + val.map(x => canonicalize(x)).join(',') + ']';
  }
  // Sort keys by UTF-16 code units (standard JS Array.prototype.sort)
  const keys = Object.keys(val).sort();
  return '{' + keys.map(k => JSON.stringify(k) + ':' + canonicalize(val[k])).join(',') + '}';
}

function sha256Hex(str) {
  return crypto.createHash('sha256').update(str, 'utf8').digest('hex');
}

const testCases = [
  { id: 'empty_object', input: {} },
  { id: 'key_sorting', input: { b: 2, a: 1, c: 3 } },
  { id: 'nested_sorting', input: { z: { y: 1, x: 2 }, a: [3, 2, 1] } },
  { id: 'unicode_and_special', input: { emoji: '🚀', accent: 'café', quote: 'say "hello"' } },
  { id: 'numbers_and_primitives', input: { zero: 0, neg: -42, float: 3.14159, flag: true, empty: null } },
  { id: 'workflow_input_example', input: { query: 'automation software market', depth: 3, filters: { activeOnly: true, minScore: 0.75 } } }
];

const vectors = testCases.map(tc => {
  const canonical = canonicalize(tc.input);
  const hash = sha256Hex(canonical);
  return {
    id: tc.id,
    input: tc.input,
    expectedCanonical: canonical,
    expectedSha256: hash
  };
});

const outPath = path.join(__dirname, 'canonical-json', 'jcs-vectors.json');
fs.writeFileSync(outPath, JSON.stringify(vectors, null, 2), 'utf8');
console.log('Successfully generated ' + vectors.length + ' JCS vectors in ' + outPath);
