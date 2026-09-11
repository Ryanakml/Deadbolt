export type JSONValue =
  | null
  | boolean
  | number
  | string
  | JSONValue[]
  | { [key: string]: JSONValue };
export class ContractError extends Error {
  constructor(
    public code: string,
    message = code,
  ) {
    super(message);
  }
}
export function fail(code: string): never {
  throw new ContractError(code);
}
export function validUnicode(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff) {
      const next = s.charCodeAt(++i);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
    } else if (c >= 0xdc00 && c <= 0xdfff) return false;
  }
  return true;
}
export function assertJSON(v: unknown, depth = 0): asserts v is JSONValue {
  if (typeof v === "string") {
    if (!validUnicode(v)) fail("INVALID_JSON");
    return;
  }
  if (typeof v === "number") {
    if (
      !Number.isFinite(v) ||
      (Number.isInteger(v) && !Number.isSafeInteger(v))
    )
      fail("INVALID_JSON");
    return;
  }
  if (v === null || typeof v === "boolean") return;
  if (typeof v !== "object") fail("INVALID_JSON");
  if (++depth > 32) fail("INVALID_JSON");
  if (Array.isArray(v) && Object.getPrototypeOf(v) !== Array.prototype)
    fail("INVALID_JSON");
  if (
    !Array.isArray(v) &&
    Object.getPrototypeOf(v) !== Object.prototype &&
    Object.getPrototypeOf(v) !== null
  )
    fail("INVALID_JSON");
  if (Object.getOwnPropertySymbols(v).length) fail("INVALID_JSON");
  if (Array.isArray(v) && Object.keys(v).length !== v.length)
    fail("INVALID_JSON");
  for (const k of Object.getOwnPropertyNames(v)) {
    if (Array.isArray(v) && k === "length") continue;
    if (
      Array.isArray(v) &&
      (!/^(0|[1-9][0-9]*)$/.test(k) ||
        Number(k) >= v.length ||
        k !== String(Number(k)))
    )
      fail("INVALID_JSON");
    const d = Object.getOwnPropertyDescriptor(v, k)!;
    if (!("value" in d) || !d.enumerable || !validUnicode(k))
      fail("INVALID_JSON");
    assertJSON(d.value, depth);
  }
}
// Parse before ordinary JSON.parse can erase duplicate property names.
export function parseJSON(raw: string | Uint8Array): JSONValue {
  let s: string;
  try {
    s =
      typeof raw === "string"
        ? raw
        : new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
            raw,
          );
  } catch {
    return fail("INVALID_JSON");
  }
  let i = 0;
  const ws = () => {
    while (/[\x20\t\r\n]/.test(s[i] ?? "\0")) i++;
  };
  const str = (): string => {
    const start = i++;
    while (i < s.length) {
      if (s[i] === "\\") {
        i += 2;
        continue;
      }
      if (s[i++] === '"') {
        try {
          const v: unknown = JSON.parse(s.slice(start, i));
          assertJSON(v);
          return v as string;
        } catch {
          return fail("INVALID_JSON");
        }
      }
    }
    return fail("INVALID_JSON");
  };
  const value = (depth: number): JSONValue => {
    ws();
    const c = s[i];
    if (c === '"') return str();
    if (c === "{" || c === "[") {
      if (depth >= 32) fail("INVALID_JSON");
      const object = c === "{",
        end = object ? "}" : "]";
      i++;
      ws();
      const out: Record<string, JSONValue> = Object.create(null) as Record<
        string,
        JSONValue
      >;
      const arr: JSONValue[] = [];
      const seen = new Set<string>();
      if (s[i] === end) {
        i++;
        return object ? out : arr;
      }
      while (i < s.length) {
        ws();
        let key = "";
        if (object) {
          if (s[i] !== '"') fail("INVALID_JSON");
          key = str();
          if (seen.has(key)) fail("INVALID_JSON");
          seen.add(key);
          ws();
          if (s[i++] !== ":") fail("INVALID_JSON");
        }
        const v = value(depth + 1);
        if (object) out[key] = v;
        else arr.push(v);
        ws();
        if (s[i] === end) {
          i++;
          return object ? out : arr;
        }
        if (s[i++] !== ",") fail("INVALID_JSON");
      }
      return fail("INVALID_JSON");
    }
    const m =
      /^(?:true|false|null|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)/.exec(
        s.slice(i),
      );
    if (!m) fail("INVALID_JSON");
    i += m[0].length;
    const v: unknown = JSON.parse(m[0]);
    assertJSON(v);
    return v;
  };
  const out = value(0);
  ws();
  if (i !== s.length) fail("INVALID_JSON");
  return out;
}
