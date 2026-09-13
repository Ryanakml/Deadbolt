import { spawn } from "node:child_process";

export default async function spawningTask(input, ctx) {
  // Same process group by design; detached hostile children are out of SP-03 scope.
	const child = spawn(process.execPath, ["-e", "process.on('SIGTERM',()=>{}); setInterval(() => {}, 1000)"], {
    detached: false,
	stdio: "ignore",
  });
  if (process.env.CHILD_PID_FILE) {
    require("node:fs").writeFileSync(process.env.CHILD_PID_FILE, String(child.pid));
  }

  return new Promise(() => {
    // Hangs while holding child process open
    setInterval(() => {}, 1000);
  });
}
