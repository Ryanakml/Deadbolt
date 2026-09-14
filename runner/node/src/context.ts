import type { TaskContext, TaskLogger } from "./protocol.js";

const SENSITIVE_KEY_PATTERN =
  /^(password|secret|token|apikey|api_key|authorization|bearer|cookie|credential|private_key)$/i;
const SENSITIVE_VALUE_PATTERN =
  /(bearer\s+[a-zA-Z0-9_\-\.]+)|(ey[a-zA-Z0-9_\-]{10,}\.[a-zA-Z0-9_\-]{10,}\.[a-zA-Z0-9_\-]+)|([0-9a-f]{32,64})/i;

export function redactSensitiveData(
  value: unknown,
  seen = new WeakSet(),
): unknown {
  if (value === null || value === undefined) return value;
  if (typeof value === "string") {
    if (SENSITIVE_VALUE_PATTERN.test(value)) {
      return value.replace(SENSITIVE_VALUE_PATTERN, (match, p1, p2, p3) => {
        if (p1) return "Bearer [REDACTED]";
        if (p2) return "[REDACTED_JWT]";
        if (p3 && p3.length >= 32) return "[REDACTED_HASH]";
        return "[REDACTED]";
      });
    }
    return value;
  }
  if (typeof value !== "object") return value;
  if (seen.has(value)) return "[Circular]";
  seen.add(value);
  if (Array.isArray(value)) {
    return value.map((item) => redactSensitiveData(item, seen));
  }
  const result: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value)) {
    if (SENSITIVE_KEY_PATTERN.test(k)) {
      result[k] = "[REDACTED]";
    } else {
      result[k] = redactSensitiveData(v, seen);
    }
  }
  return result;
}

export function createTaskLogger(attemptId: string): TaskLogger {
  const format = (level: string, message: string, meta: unknown[]) => {
    const timestamp = new Date().toISOString();
    const safeMessage = redactSensitiveData(message) as string;
    const safeMeta = meta.map((m) => redactSensitiveData(m));
    const entry = {
      timestamp,
      level,
      attemptId,
      message: safeMessage,
      ...(safeMeta.length > 0 ? { meta: safeMeta } : {}),
    };
    // Logs are emitted to stderr so that stdout remains clean for stream diagnostics
    // and neither stream ever pollutes the structured result channel (FD 3).
    process.stderr.write(JSON.stringify(entry) + "\n");
  };

  return {
    info: (msg, ...meta) => format("INFO", msg, meta),
    warn: (msg, ...meta) => format("WARN", msg, meta),
    error: (msg, ...meta) => format("ERROR", msg, meta),
    debug: (msg, ...meta) => format("DEBUG", msg, meta),
  };
}

export function createTaskContext(
  attemptId: string,
  operationId: string,
  signal: AbortSignal,
  stepId?: string,
  env?: Record<string, string>,
): TaskContext {
  const logger = createTaskLogger(attemptId);
  return {
    attemptId,
    operationId,
    signal,
    stepId,
    logger,
    log: logger,
    env: Object.freeze({ ...(env ?? {}) }),
  };
}
