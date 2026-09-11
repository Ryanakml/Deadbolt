#!/usr/bin/env node

/**
 * Real Headless Chrome Browser Smoke Test for Deadbolt BFF Auth
 *
 * Exercises the end-to-end browser authentication lifecycle:
 * 1. Navigates to /api/auth/login
 * 2. Follows full 302 redirect chain into OIDC authorize, callback, and SPA root
 * 3. Inspects real browser cookie storage (__Host-runtime_session & __Host-csrf_token)
 * 4. Confirms DOM cookie visibility (JS can read CSRF token, JS cannot read HttpOnly session)
 * 5. Executes browser fetch mutation with CSRF token and Origin
 * 6. Executes browser fetch logout and verifies session termination
 */

import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, existsSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

function findChrome() {
  const candidates = [
    process.env.CHROME_BIN,
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium-browser",
    "/usr/bin/chromium",
  ].filter(Boolean);

  for (const c of candidates) {
    if (existsSync(c)) {
      return c;
    }
  }
  return null;
}

class CDPClient {
  constructor(wsUrl) {
    this.ws = new WebSocket(wsUrl);
    this.id = 1;
    this.pending = new Map();
    this.events = [];
    this.ready = new Promise((resolve, reject) => {
      this.ws.onopen = resolve;
      this.ws.onerror = reject;
    });

    this.ws.onmessage = (msg) => {
      const data = JSON.parse(msg.data);
      if (data.id && this.pending.has(data.id)) {
        const { resolve, reject } = this.pending.get(data.id);
        this.pending.delete(data.id);
        if (data.error) {
          reject(new Error(data.error.message || JSON.stringify(data.error)));
        } else {
          resolve(data.result);
        }
      }
    };
  }

  async send(method, params = {}) {
    await this.ready;
    const msgId = this.id++;
    return new Promise((resolve, reject) => {
      this.pending.set(msgId, { resolve, reject });
      this.ws.send(JSON.stringify({ id: msgId, method, params }));
    });
  }

  close() {
    try {
      this.ws.close();
    } catch (_) {}
  }
}

async function runBrowserSmoke(targetBaseUrl) {
  const chromePath = findChrome();
  if (!chromePath) {
    console.log(
      "[BROWSER_SMOKE] Chrome/Chromium executable not found; skipping real browser test.",
    );
    process.exit(0);
  }

  console.log(
    `[BROWSER_SMOKE] Launching real Chrome browser from: ${chromePath}`,
  );
  const profileDir = mkdtempSync(join(tmpdir(), "deadbolt-chrome-smoke-"));
  const port = 9222 + Math.floor(Math.random() * 500);

  const chromeProc = spawn(
    chromePath,
    [
      "--headless=new",
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--disable-software-rasterizer",
      "--no-zygote",
      "--single-process",
      `--remote-debugging-port=${port}`,
      "--remote-debugging-address=127.0.0.1",
      `--user-data-dir=${profileDir}`,
      "--no-first-run",
      "--no-default-browser-check",
      "--ignore-certificate-errors",
      "about:blank",
    ],
    {
      stdio: ["ignore", "pipe", "pipe"],
    },
  );

  let chromeStderr = "";
  chromeProc.stderr?.on("data", (chunk) => {
    chromeStderr += chunk.toString();
  });

  const cleanup = () => {
    try {
      chromeProc.kill();
    } catch (_) {}
    try {
      rmSync(profileDir, { recursive: true, force: true });
    } catch (_) {}
  };

  process.on("exit", cleanup);
  process.on("SIGINT", () => {
    cleanup();
    process.exit(1);
  });

  // Wait for Chrome remote debugging to be ready
  let versionData = null;
  for (let i = 0; i < 30; i++) {
    await new Promise((r) => setTimeout(r, 200));
    try {
      const resp = await fetch(`http://127.0.0.1:${port}/json/version`);
      if (resp.ok) {
        versionData = await resp.json();
        break;
      }
    } catch (_) {}
  }

  if (!versionData || !versionData.webSocketDebuggerUrl) {
    cleanup();
    console.log(
      `[BROWSER_SMOKE] Chrome remote debugging unavailable in this environment (details: ${chromeStderr.trim() || "no response on DevTools port"}); skipping real browser smoke.`,
    );
    process.exit(0);
  }

  console.log(
    `[BROWSER_SMOKE] Connected to Chrome (${versionData["Browser"]}) CDP endpoint.`,
  );

  // Create a new target/page
  const newPageResp = await fetch(
    `http://127.0.0.1:${port}/json/new?about:blank`,
    { method: "PUT" },
  );
  const pageTarget = await newPageResp.json();
  const cdp = new CDPClient(pageTarget.webSocketDebuggerUrl);

  try {
    await cdp.send("Network.enable");
    await cdp.send("Page.enable");

    // Step 1: Navigate to login endpoint in real browser
    const loginUrl = `${targetBaseUrl}/api/auth/login`;
    console.log(`[BROWSER_SMOKE] Step 1: Navigating browser to: ${loginUrl}`);
    await cdp.send("Page.navigate", { url: loginUrl });

    // Wait for navigation and redirects to complete (reach app root)
    let finalUrl = "";
    for (let i = 0; i < 50; i++) {
      await new Promise((r) => setTimeout(r, 100));
      const evalResult = await cdp.send("Runtime.evaluate", {
        expression: "window.location.href",
        returnByValue: true,
      });
      finalUrl = evalResult.result.value;
      if (
        finalUrl &&
        !finalUrl.includes("/api/auth/login") &&
        !finalUrl.includes("/authorize") &&
        !finalUrl.includes("/callback")
      ) {
        break;
      }
    }

    console.log(
      `[BROWSER_SMOKE] Step 2: Browser reached final destination URL: ${finalUrl}`,
    );

    // Step 3: Inspect browser cookies via CDP Network domain
    const cookiesResp = await cdp.send("Network.getCookies");
    const cookies = cookiesResp.cookies || [];
    console.log(
      `[BROWSER_SMOKE] Step 3: Found ${cookies.length} cookies stored in browser:`,
      cookies.map(
        (c) => `${c.name} (httpOnly=${c.httpOnly}, secure=${c.secure})`,
      ),
    );

    const sessionCookie = cookies.find((c) => c.name.includes("session"));
    const csrfCookie = cookies.find((c) => c.name.includes("csrf"));

    if (!sessionCookie) {
      throw new Error(
        "BROWSER FAILURE: Session cookie was not set in browser storage",
      );
    }
    if (!csrfCookie) {
      throw new Error(
        "BROWSER FAILURE: CSRF bootstrap cookie was not set in browser storage",
      );
    }

    // Step 4: Verify DOM cookie visibility
    // The session cookie MUST be HttpOnly (invisible to document.cookie)
    // The CSRF cookie MUST be non-HttpOnly (visible to document.cookie for SPA bootstrap)
    const docCookieEval = await cdp.send("Runtime.evaluate", {
      expression: "document.cookie",
      returnByValue: true,
    });
    const domCookieStr = docCookieEval.result.value || "";
    const sessionCookieLeaked = domCookieStr.includes(sessionCookie.name);
    const csrfCookieReadable = domCookieStr.includes(csrfCookie.name);
    console.log(
      `[BROWSER_SMOKE] Step 4: DOM cookie visibility: csrf_cookie_readable=${csrfCookieReadable}, session_cookie_leaked=${sessionCookieLeaked}`,
    );

    if (sessionCookieLeaked) {
      throw new Error(
        `SECURITY VIOLATION: HttpOnly session cookie ${sessionCookie.name} leaked to document.cookie!`,
      );
    }
    if (!csrfCookieReadable) {
      throw new Error(
        `BOOTSTRAP VIOLATION: Readable CSRF cookie ${csrfCookie.name} not found in document.cookie!`,
      );
    }

    // Read the CSRF token from document.cookie inside the browser
    const jsExtractCSRF = await cdp.send("Runtime.evaluate", {
      expression: `(function() {
        const match = document.cookie.match(new RegExp('(^|; )' + '${csrfCookie.name}'.replace(/([.$?*|{}()[]\\\/+^])/g, '\\$1') + '=([^;]*)'));
        return match ? decodeURIComponent(match[2]) : '';
      })()`,
      returnByValue: true,
    });
    const csrfToken = jsExtractCSRF.result.value;
    if (!csrfToken) {
      throw new Error(
        "Failed to parse CSRF token from document.cookie in browser context",
      );
    }
    console.log(
      `[BROWSER_SMOKE] Successfully extracted CSRF token via browser JS: token_extracted=true, token_len=${csrfToken.length}`,
    );

    // Step 5: Execute an authenticated mutating fetch() inside the real browser
    console.log(
      "[BROWSER_SMOKE] Step 5: Executing authenticated POST mutation from browser window...",
    );
    const mutationResult = await cdp.send("Runtime.evaluate", {
      expression: `(async () => {
        const resp = await fetch('${targetBaseUrl}/api/mutation', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-CSRF-Token': '${csrfToken}',
          },
          body: JSON.stringify({ action: 'browser-smoke-mutation' }),
        });
        const data = await resp.json();
        return { status: resp.status, data };
      })()`,
      awaitPromise: true,
      returnByValue: true,
    });

    if (
      !mutationResult.result.value ||
      mutationResult.result.value.status !== 200
    ) {
      throw new Error(
        `BROWSER MUTATION FAILED: expected 200 OK, got: ${JSON.stringify(mutationResult.result.value)}`,
      );
    }
    console.log(
      `[BROWSER_SMOKE] Mutation executed successfully (Status 200 OK):`,
      mutationResult.result.value.data,
    );

    // Step 6: Execute authenticated logout fetch() inside the browser
    console.log(
      "[BROWSER_SMOKE] Step 6: Executing POST /api/auth/logout from browser window...",
    );
    const logoutResult = await cdp.send("Runtime.evaluate", {
      expression: `(async () => {
        const resp = await fetch('${targetBaseUrl}/api/auth/logout', {
          method: 'POST',
          headers: {
            'X-CSRF-Token': '${csrfToken}',
          },
        });
        return { status: resp.status };
      })()`,
      awaitPromise: true,
      returnByValue: true,
    });

    if (
      !logoutResult.result.value ||
      logoutResult.result.value.status !== 204
    ) {
      throw new Error(
        `BROWSER LOGOUT FAILED: expected 204 No Content, got: ${JSON.stringify(logoutResult.result.value)}`,
      );
    }
    console.log(
      `[BROWSER_SMOKE] Logout executed successfully (Status 204 No Content).`,
    );

    // Step 7: Verify subsequent authenticated request from browser fails with 401
    const postLogoutCheck = await cdp.send("Runtime.evaluate", {
      expression: `(async () => {
        const resp = await fetch('${targetBaseUrl}/api/auth/session');
        return { status: resp.status };
      })()`,
      awaitPromise: true,
      returnByValue: true,
    });

    if (
      !postLogoutCheck.result.value ||
      postLogoutCheck.result.value.status !== 401
    ) {
      throw new Error(
        `BROWSER SESSION REVOCATION FAILED: expected 401 after logout, got: ${JSON.stringify(postLogoutCheck.result.value)}`,
      );
    }
    console.log(
      `[BROWSER_SMOKE] Verified post-logout request returned 401 Unauthorized.`,
    );

    console.log(
      "[BROWSER_SMOKE] SUCCESS: Real Chrome browser smoke completed successfully!",
    );
  } finally {
    cdp.close();
    cleanup();
  }
}

const targetUrl = process.argv[2];
if (!targetUrl) {
  console.error("Usage: node scripts/browser-smoke.mjs <targetBaseUrl>");
  process.exit(1);
}

runBrowserSmoke(targetUrl).catch((err) => {
  console.error("[BROWSER_SMOKE_ERROR]", err);
  process.exit(1);
});
