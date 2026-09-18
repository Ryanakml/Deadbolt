import { realpathSync } from "node:fs";
import { fileURLToPath, pathToFileURL } from "node:url";
import { runMain } from "./runner.js";

export * from "./protocol.js";
export * from "./context.js";
export * from "./runner.js";

// Execute automatically if invoked directly as CLI entrypoint
if (process.argv[1]) {
  let isMain = false;
  try {
    const realArgv = realpathSync(process.argv[1]);
    const realMeta = realpathSync(fileURLToPath(import.meta.url));
    isMain = realArgv === realMeta;
  } catch {
    const currentUrl = pathToFileURL(process.argv[1]).href;
    isMain = import.meta.url === currentUrl;
  }

  if (isMain) {
    runMain().catch((err) => {
      process.stderr.write(`FATAL_RUNNER_ERROR: ${err.message}\n`);
      process.exit(1);
    });
  }
}
