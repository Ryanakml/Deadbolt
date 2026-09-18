import test from "node:test";
import assert from "node:assert/strict";
import {
  clearStreamErrorOnLive,
  createStreamErrorBanner,
  markGlobalError,
  markStreamError,
} from "../dist/stream.js";
import {
  shouldShowWorkerWait,
  terminalStepEmptyText,
} from "../dist/inspector.js";

// A: transient stream error is cleared on LIVE recovery.
test("stream error -> LIVE clears the stale network banner state", () => {
  const banner = createStreamErrorBanner();
  markStreamError(banner, "network error");
  assert.equal(banner.message, "network error");
  assert.equal(banner.isStreamError, true);

  // RECONNECTING keeps the banner visible.
  assert.equal(
    clearStreamErrorOnLive({
      ...banner,
      isStreamError: false,
      message: banner.message,
    }),
    false,
  );

  const cleared = clearStreamErrorOnLive(banner);
  assert.equal(cleared, true);
  assert.equal(banner.message, null);
  assert.equal(banner.isStreamError, false);
});

// A: unrelated bootstrap/API errors survive reconnect.
test("unrelated global errors are not cleared on LIVE", () => {
  const banner = createStreamErrorBanner();
  markGlobalError(banner, "Failed to list projects (HTTP 500)");
  assert.equal(clearStreamErrorOnLive(banner), false);
  assert.equal(banner.message, "Failed to list projects (HTTP 500)");

  // A global error that overwrites a prior stream error also survives.
  markStreamError(banner, "network error");
  markGlobalError(banner, "Failed to get session (HTTP 401)");
  assert.equal(clearStreamErrorOnLive(banner), false);
  assert.equal(banner.message, "Failed to get session (HTTP 401)");
});

// B: terminal steps never show the worker-wait hint.
test("CANCELLED step with zero workers does not show worker wait", () => {
  assert.equal(
    shouldShowWorkerWait("CANCELLED", "NO_COMPATIBLE_WORKERS", 0),
    false,
  );
  assert.equal(shouldShowWorkerWait("CANCELLED", null, 0), false);
  assert.equal(
    shouldShowWorkerWait("FAILED", "NO_COMPATIBLE_WORKERS", 0),
    false,
  );
  assert.equal(
    shouldShowWorkerWait("SUCCEEDED", "NO_COMPATIBLE_WORKERS", 0),
    false,
  );
  assert.equal(
    shouldShowWorkerWait("BLOCKED", "NO_COMPATIBLE_WORKERS", 0),
    false,
  );
  assert.equal(
    shouldShowWorkerWait("RUNNING", "NO_COMPATIBLE_WORKERS", 0),
    false,
  );
});

// B: neutral terminal text carries no recovery hint.
test("terminal empty text is neutral, not a worker recovery hint", () => {
  const text = terminalStepEmptyText("CANCELLED");
  assert.ok(text.length > 0);
  assert.ok(!text.includes("No compatible workers"));
  assert.ok(!text.includes("Waiting for active worker"));
});

// B: genuine waiting states still show the warning.
test("READY/WAITING with NO_COMPATIBLE_WORKERS still shows the warning", () => {
  assert.equal(shouldShowWorkerWait("READY", "NO_COMPATIBLE_WORKERS", 2), true);
  assert.equal(
    shouldShowWorkerWait("WAITING", "NO_COMPATIBLE_WORKERS", 1),
    true,
  );
  assert.equal(shouldShowWorkerWait("READY", null, 0), true);
  assert.equal(shouldShowWorkerWait("WAITING", null, 0), true);
  // No waiting condition, workers available: no warning.
  assert.equal(shouldShowWorkerWait("READY", null, 2), false);
  assert.equal(shouldShowWorkerWait("WAITING", null, 3), false);
});
