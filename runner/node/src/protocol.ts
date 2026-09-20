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
  stepId: string;
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

export interface TaskArtifactsClient {
  /**
   * Builds an upload marker for raw result bytes. The runner has no network
   * access; the worker agent publishes marked bytes through the scoped
   * artifact APIs and completes with the typed reference. Marker shape is
   * canonical with the TypeScript SDK (`artifacts.ts`).
   */
  upload(data: Uint8Array, contentType?: string): unknown;
}

export interface TaskContext {
  readonly stepId: string;
  readonly attemptId: string;
  readonly operationId: string;
  readonly signal: AbortSignal;
  readonly logger: TaskLogger;
  readonly log: TaskLogger;
  readonly env: Readonly<Record<string, string>>;
  readonly artifacts: TaskArtifactsClient;
}
