import { spawn } from "node:child_process";

export default async function spawningTask(input, ctx) {
  // Spawn a detached or background grandchild process to test process group termination
  const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
    detached: true,
    stdio: "ignore",
  });
  child.unref();

  return new Promise(() => {
    // Hangs while holding child process open
    setInterval(() => {}, 1000);
  });
}
