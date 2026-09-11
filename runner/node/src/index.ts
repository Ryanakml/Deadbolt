import { pathToFileURL } from "node:url";
import { runMain } from "./runner.js";

export * from "./protocol.js";
export * from "./context.js";
export * from "./runner.js";

// Execute automatically if invoked directly as CLI entrypoint
if (process.argv[1]) {
  const currentUrl = pathToFileURL(process.argv[1]).href;
  if (import.meta.url === currentUrl) {
    runMain().catch((err) => {
      process.stderr.write(`FATAL_RUNNER_ERROR: ${err.message}\n`);
      process.exit(1);
    });
  }
}
