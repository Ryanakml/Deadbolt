import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createTaskContext } from "./context.js";
import type { TaskCompletion, TaskHandler, TaskInput } from "./protocol.js";

function getResultWriter(): (data: string) => void {
  // 1. Result file (highest portability across Windows/Linux)
  const resultFilePath = process.env.DEADBOLT_RESULT_FILE;
  if (resultFilePath) {
    return (data: string) => {
      fs.writeFileSync(resultFilePath, data, "utf8");
    };
  }

  // 2. Dedicated FD (FD 3 on POSIX via Go ExtraFiles)
  const resultFdStr = process.env.DEADBOLT_RESULT_FD ?? "3";
  const resultFd = parseInt(resultFdStr, 10);
  try {
    // Check if fd is valid and writable
    fs.fstatSync(resultFd);
    return (data: string) => {
      fs.writeSync(resultFd, Buffer.from(data, "utf8"));
    };
  } catch {
    // If FD 3 is not open (e.g. standalone test or non-POSIX without ExtraFiles),
    // fallback to a dedicated result stream file or process.send if Node IPC is present
    if (typeof process.send === "function") {
      return (data: string) => {
        process.send?.(JSON.parse(data));
      };
    }
    // Fallback: write to a well-known temporary location
    const fallbackPath = path.resolve(process.cwd(), ".deadbolt_result.json");
    return (data: string) => {
      fs.writeFileSync(fallbackPath, data, "utf8");
    };
  }
}

export async function resolveHandler(
  entrypoint?: string,
  taskName?: string,
): Promise<TaskHandler> {
  if (!entrypoint) {
    throw new Error("ENTRYPOINT_REQUIRED: No task entrypoint provided");
  }

  const resolvedPath = path.isAbsolute(entrypoint)
    ? entrypoint
    : path.resolve(process.cwd(), entrypoint);

  if (!fs.existsSync(resolvedPath)) {
    throw new Error(
      `ENTRYPOINT_NOT_FOUND: Cannot find module at ${resolvedPath}`,
    );
  }

  const fileUrl = pathToFileURL(resolvedPath).href;
  const mod = await import(fileUrl);

  if (taskName && typeof mod[taskName] === "function") {
    return mod[taskName];
  }
  if (typeof mod.task === "function") {
    return mod.task;
  }
  if (typeof mod.default === "function") {
    return mod.default;
  }
  if (typeof mod.handler === "function") {
    return mod.handler;
  }

  throw new Error(
    `HANDLER_NOT_FOUND: No executable task function found in ${entrypoint} for task '${taskName}'`,
  );
}

export async function executeTask(
  taskInput: TaskInput,
  writeResult: (data: string) => void,
): Promise<TaskCompletion> {
  const startTime = Date.now();
  const abortController = new AbortController();

  // Handle termination signals gracefully
  const onSignal = (sig: string) => {
    abortController.abort(new Error(`TASK_ABORTED_BY_${sig}`));
  };
  process.on("SIGTERM", () => onSignal("SIGTERM"));
  process.on("SIGINT", () => onSignal("SIGINT"));

  let timeoutTimer: NodeJS.Timeout | undefined;
  if (taskInput.timeoutMs && taskInput.timeoutMs > 0) {
    timeoutTimer = setTimeout(() => {
      abortController.abort(new Error("TASK_TIMEOUT_EXCEEDED"));
    }, taskInput.timeoutMs);
  }

  const ctx = createTaskContext(
    taskInput.attemptId,
    taskInput.operationId,
    abortController.signal,
  );

  let completion: TaskCompletion;

  try {
    ctx.log.info("Starting task execution", {
      taskName: taskInput.taskName,
      attemptId: taskInput.attemptId,
    });

    const handler = await resolveHandler(
      taskInput.entrypoint,
      taskInput.taskName,
    );
    const output = await handler(taskInput.input, ctx);

    completion = {
      attemptId: taskInput.attemptId,
      status: "SUCCEEDED",
      output: output ?? null,
      metrics: {
        durationMs: Date.now() - startTime,
      },
    };
    ctx.log.info("Task completed successfully", {
      durationMs: completion.metrics.durationMs,
    });
  } catch (err: any) {
    const isAborted = abortController.signal.aborted;
    const errorCode = isAborted
      ? "ABORTED"
      : err?.code || err?.name || "TASK_EXECUTION_ERROR";
    const errorMessage = err?.message || String(err);
    const retryable = Boolean(err?.retryable);

    completion = {
      attemptId: taskInput.attemptId,
      status: "FAILED",
      error: {
        code: errorCode,
        message: errorMessage,
        retryable,
        details: err?.stack,
      },
      metrics: {
        durationMs: Date.now() - startTime,
      },
    };
    ctx.log.error("Task execution failed", {
      code: errorCode,
      message: errorMessage,
      retryable,
    });
  } finally {
    if (timeoutTimer) clearTimeout(timeoutTimer);
  }

  // Deliver completion result through structured channel only.
  // Never write to stdout/stderr!
  writeResult(JSON.stringify(completion) + "\n");
  return completion;
}

export async function runMain(): Promise<void> {
  const writeResult = getResultWriter();

  // Read task input from stdin
  let inputChunks: Buffer[] = [];
  for await (const chunk of process.stdin) {
    inputChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  const rawInput = Buffer.concat(inputChunks).toString("utf8").trim();
  if (!rawInput) {
    const errorCompletion: TaskCompletion = {
      attemptId: "unknown",
      status: "FAILED",
      error: {
        code: "INVALID_INPUT",
        message: "No input payload received on stdin",
        retryable: false,
      },
      metrics: { durationMs: 0 },
    };
    writeResult(JSON.stringify(errorCompletion) + "\n");
    process.exit(1);
  }

  let taskInput: TaskInput;
  try {
    taskInput = JSON.parse(rawInput);
  } catch (err: any) {
    const errorCompletion: TaskCompletion = {
      attemptId: "unknown",
      status: "FAILED",
      error: {
        code: "MALFORMED_JSON_INPUT",
        message: `Failed to parse JSON input from stdin: ${err.message}`,
        retryable: false,
      },
      metrics: { durationMs: 0 },
    };
    writeResult(JSON.stringify(errorCompletion) + "\n");
    process.exit(1);
  }

  const completion = await executeTask(taskInput, writeResult);
  process.exit(completion.status === "SUCCEEDED" ? 0 : 1);
}
