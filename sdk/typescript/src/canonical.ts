import { createHash } from 'node:crypto';

/**
 * Serializes arbitrary JSON-serializable data according to RFC 8785 (JSON Canonicalization Scheme).
 * 
 * Rules:
 * 1. Whitespace stripped.
 * 2. Object keys sorted by UTF-16 code units.
 * 3. Primitives formatted per ECMAScript specification.
 * 4. NaN, Infinity, undefined, and functions rejected.
 */
export function canonicalize(val: unknown): string {
  if (val === null) {
    return 'null';
  }

  const t = typeof val;

  if (t === 'boolean') {
    return val ? 'true' : 'false';
  }

  if (t === 'number') {
    if (!Number.isFinite(val)) {
      throw new TypeError('RFC 8785: Non-finite numbers (NaN, Infinity) are forbidden');
    }
    // Safe integer or IEEE 754 float
    return JSON.stringify(val);
  }

  if (t === 'string') {
    return JSON.stringify(val);
  }

  if (Array.isArray(val)) {
    const parts = val.map((item) => canonicalize(item));
    return `[${parts.join(',')}]`;
  }

  if (t === 'object') {
    const keys = Object.keys(val as Record<string, unknown>).sort();
    const parts = keys.map((k) => {
      const v = (val as Record<string, unknown>)[k];
      if (typeof v === 'undefined') {
        throw new TypeError(`RFC 8785: undefined value forbidden for property "${k}"`);
      }
      return `${JSON.stringify(k)}:${canonicalize(v)}`;
    });
    return `{${parts.join(',')}}`;
  }

  throw new TypeError(`RFC 8785: Unsupported type "${t}"`);
}

/**
 * Computes the SHA-256 hex digest of a UTF-8 string.
 */
export function sha256Hex(str: string): string {
  return createHash('sha256').update(str, 'utf8').digest('hex');
}

/**
 * Produces the canonical digest (RFC 8785 canonical string -> SHA-256 hex).
 */
export function canonicalDigest(val: unknown): { canonical: string; sha256: string } {
  const canonical = canonicalize(val);
  return {
    canonical,
    sha256: sha256Hex(canonical),
  };
}
