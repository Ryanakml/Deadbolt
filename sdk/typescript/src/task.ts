import type { TaskContext } from "./context.js";
import type { JSONValue } from "./json.js";
import { type ObjectValue } from "./schema.js";
import { normalizeTask, validateTask } from "./validator.js";
import type { RecoveryMode } from "./enums.js";

export type TaskRecovery = RecoveryMode;

export interface TaskRetry {
  maxAttempts?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
}

export type TaskHandler<TInput = unknown, TOutput = unknown> = (
  input: TInput,
  ctx: TaskContext,
) => Promise<TOutput> | TOutput;

export interface TaskConfig<TInput = unknown, TOutput = unknown> {
  name: string;
  inputSchema: JSONValue;
  outputSchema: JSONValue;
  recovery: TaskRecovery;
  retry?: TaskRetry;
  timeoutMs?: number;
  idempotencyWindowMs?: number;
  entrypoint?: string;
  handler?: TaskHandler<TInput, TOutput>;
}

export interface TaskDefinition<TInput = unknown, TOutput = unknown> {
  readonly name: string;
  readonly inputSchema: JSONValue;
  readonly outputSchema: JSONValue;
  readonly recovery: TaskRecovery;
  readonly retry: {
    maxAttempts: number;
    initialDelayMs: number;
    maxDelayMs: number;
  };
  readonly timeoutMs: number;
  readonly idempotencyWindowMs?: number;
  readonly entrypoint: string;
  readonly handler?: TaskHandler<TInput, TOutput>;
  toManifest(entrypointOverride?: string): ObjectValue;
}

export function defineTask<TInput = unknown, TOutput = unknown>(
  config: TaskConfig<TInput, TOutput>,
): TaskDefinition<TInput, TOutput> {
  const rawTask: Record<string, unknown> = {
    name: config.name,
    inputSchema: config.inputSchema,
    outputSchema: config.outputSchema,
    recovery: config.recovery,
  };

  if (config.retry !== undefined) {
    rawTask.retry = config.retry;
  }
  if (config.timeoutMs !== undefined) {
    rawTask.timeoutMs = config.timeoutMs;
  }
  if (config.idempotencyWindowMs !== undefined) {
    rawTask.idempotencyWindowMs = config.idempotencyWindowMs;
  }
  if (config.entrypoint !== undefined) {
    rawTask.entrypoint = config.entrypoint;
  }

  // Validate task schema and invariants (recovery, idempotencyWindowMs >= 5000 + timeoutMs, etc.)
  validateTask(rawTask as unknown as JSONValue);

  // Normalize defaults (timeoutMs=300000, retry={maxAttempts:3, initialDelayMs:1000, maxDelayMs:30000})
  const normalized = normalizeTask(rawTask as unknown as JSONValue);
  const entrypoint = config.entrypoint ?? `./tasks/${config.name}.js`;

  const taskDef: TaskDefinition<TInput, TOutput> = {
    name: config.name,
    inputSchema: config.inputSchema,
    outputSchema: config.outputSchema,
    recovery: config.recovery,
    retry: normalized.retry as {
      maxAttempts: number;
      initialDelayMs: number;
      maxDelayMs: number;
    },
    timeoutMs: Number(normalized.timeoutMs),
    idempotencyWindowMs:
      normalized.idempotencyWindowMs !== undefined
        ? Number(normalized.idempotencyWindowMs)
        : undefined,
    entrypoint,
    handler: config.handler,
    toManifest(entrypointOverride?: string): ObjectValue {
      const manifest: ObjectValue = {
        name: taskDef.name,
        inputSchema: taskDef.inputSchema,
        outputSchema: taskDef.outputSchema,
        recovery: taskDef.recovery,
        retry: taskDef.retry,
        timeoutMs: taskDef.timeoutMs,
        entrypoint: entrypointOverride ?? taskDef.entrypoint,
      };
      if (taskDef.idempotencyWindowMs !== undefined) {
        manifest.idempotencyWindowMs = taskDef.idempotencyWindowMs;
      }
      return manifest;
    },
  };

  return taskDef;
}
