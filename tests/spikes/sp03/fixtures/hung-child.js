// Task that ignores graceful termination signals to verify SIGKILL after grace period
process.on("SIGTERM", () => {
  // Deliberately ignore SIGTERM
  console.log("IGNORING_SIGTERM: Task refuses to terminate gracefully");
});

export default async function hungTask(input, ctx) {
  console.log("hungTask started, will loop indefinitely");
  return new Promise(() => {
    // Never resolves
    setInterval(() => {}, 1000);
  });
}
