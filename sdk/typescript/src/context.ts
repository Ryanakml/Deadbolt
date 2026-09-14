export interface TaskLogger {
  info(message: string, ...meta: unknown[]): void;
  warn(message: string, ...meta: unknown[]): void;
  error(message: string, ...meta: unknown[]): void;
  debug(message: string, ...meta: unknown[]): void;
}

export interface TaskContext {
  readonly stepId: string;
  readonly attemptId: string;
  readonly operationId: string;
  readonly signal: AbortSignal;
  readonly logger: TaskLogger;
  readonly log: TaskLogger; // Alias for logger per Blueprint §12.3
  readonly env: Readonly<Record<string, string>>;
}

export interface CreateTaskContextOptions {
  stepId?: string;
  attemptId: string;
  operationId: string;
  signal?: AbortSignal;
  logger?: TaskLogger;
  env?: Record<string, string>;
}

const SENSITIVE_KEY_PATTERN =
  /^(password|secret|token|apikey|api_key|authorization|bearer|cookie|credential|private_key)$/i;
const SENSITIVE_VALUE_PATTERN =
  /(bearer\s+[a-zA-Z0-9_\-\.]+)|(ey[a-zA-Z0-9_\-]{10,}\.[a-zA-Z0-9_\-]{10,}\.[a-zA-Z0-9_\-]+)|([0-9a-f]{32,64})/i;

export function redactSensitiveData(
  value: unknown,
  seen = new WeakSet(),
): unknown {
  if (value === null || value === undefined) {
    return value;
  }

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

  if (typeof value !== "object") {
    return value;
  }

  if (seen.has(value)) {
    return "[Circular]";
  }
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

export function createRedactionAwareLogger(
  prefix: string,
  sink: (level: string, message: string, meta: unknown[]) => void = (
    level,
    message,
    meta,
  ) => {
    const formatted = `[${level.toUpperCase()}] [${prefix}] ${message}`;
    if (meta.length > 0) {
      // eslint-disable-next-line no-console
      console.log(formatted, ...meta);
    } else {
      // eslint-disable-next-line no-console
      console.log(formatted);
    }
  },
): TaskLogger {
  const logWithLevel = (level: string, message: string, ...meta: unknown[]) => {
    const safeMessage = redactSensitiveData(message) as string;
    const safeMeta = meta.map((m) => redactSensitiveData(m));
    sink(level, safeMessage, safeMeta);
  };

  return {
    info: (msg, ...meta) => logWithLevel("info", msg, ...meta),
    warn: (msg, ...meta) => logWithLevel("warn", msg, ...meta),
    error: (msg, ...meta) => logWithLevel("error", msg, ...meta),
    debug: (msg, ...meta) => logWithLevel("debug", msg, ...meta),
  };
}

export function createTaskContext(
  options: CreateTaskContextOptions,
): TaskContext {
  const stepId = options.stepId ?? options.operationId;
  const attemptId = options.attemptId;
  const operationId = options.operationId;
  const signal = options.signal ?? new AbortController().signal;
  const logger =
    options.logger ?? createRedactionAwareLogger(`${stepId}:${attemptId}`);
  const env = Object.freeze({ ...(options.env ?? {}) });

  return {
    stepId,
    attemptId,
    operationId,
    signal,
    logger,
    log: logger,
    env,
  };
}
