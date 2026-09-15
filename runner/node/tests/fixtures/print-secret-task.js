export async function printSecretTask() {
  console.log(`customer secret: ${process.env.DEMO_TASK_SECRET}`);
  console.error(`customer secret on stderr: ${process.env.DEMO_TASK_SECRET}`);
  return { ok: true };
}
