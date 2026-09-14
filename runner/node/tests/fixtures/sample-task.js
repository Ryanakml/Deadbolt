export async function sampleTask(input, ctx) {
  ctx.log.info("sampleTask invoked", { input });
  return { sum: (input.x || 0) + (input.y || 0) };
}

export async function failingTask(input, ctx) {
  const err = new Error("This task intentionally failed");
  err.code = "INTENTIONAL_FAILURE";
  err.retryable = true;
  throw err;
}

export async function sensitiveFailingTask(input, ctx) {
  const err = new Error(
    "Failed with Bearer supersecrettoken12345678901234567890",
  );
  err.code = "SENSITIVE_FAILURE";
  err.retryable = false;
  throw err;
}

export async function noisyTask(input, ctx) {
  // Emit arbitrary noise to stdout and stderr
  console.log("NOISY_STDOUT: Some unformatted user log string");
  console.log('{"fake": "json", "status": "FAILED"}');
  console.error("NOISY_STDERR: User warning or stack trace");
  process.stdout.write("Raw stdout buffer without newline");
  return { clean: true };
}

export async function slowTask(input, ctx) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      resolve({ completed: true });
    }, 5000);

    ctx.signal.addEventListener("abort", () => {
      clearTimeout(timer);
      const abortErr = new Error("TASK_ABORTED");
      abortErr.code = "ABORTED";
      reject(abortErr);
    });
  });
}

export const sampleTaskDef = {
  name: "sample-task-def",
  recovery: "safe",
  handler: async (input, ctx) => {
    ctx.logger.info("sampleTaskDef invoked", { input });
    return {
      message: `hello ${input.name || "world"}`,
      stepId: ctx.stepId,
      allowedEnv: ctx.env.TEST_ENV || null,
    };
  },
};
