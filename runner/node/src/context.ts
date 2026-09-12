import type { TaskContext, TaskLogger } from "./protocol.js";

export function createTaskLogger(attemptId: string): TaskLogger {
  const format = (level: string, message: string, meta: unknown[]) => {
    const timestamp = new Date().toISOString();
    const entry = {
      timestamp,
      level,
      attemptId,
      message,
      ...(meta.length > 0 ? { meta } : {}),
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
): TaskContext {
  return {
    attemptId,
    operationId,
    signal,
    log: createTaskLogger(attemptId),
  };
}
