export interface TaskError {
  code: string;
  message: string;
  retryable: boolean;
  details?: unknown;
}

export interface TaskInput {
  attemptId: string;
  operationId: string;
  taskName: string;
  entrypoint?: string;
  input: unknown;
  timeoutMs?: number;
  env?: Record<string, string>;
}

export interface TaskMetrics {
  durationMs: number;
}

export interface TaskCompletion {
  attemptId: string;
  status: "SUCCEEDED" | "FAILED";
  output?: unknown;
  error?: TaskError;
  metrics: TaskMetrics;
}

export type TaskHandler = (input: any, ctx: TaskContext) => Promise<any> | any;

export interface TaskLogger {
  info(message: string, ...meta: unknown[]): void;
  warn(message: string, ...meta: unknown[]): void;
  error(message: string, ...meta: unknown[]): void;
  debug(message: string, ...meta: unknown[]): void;
}

export interface TaskContext {
  operationId: string;
  attemptId: string;
  signal: AbortSignal;
  log: TaskLogger;
}
