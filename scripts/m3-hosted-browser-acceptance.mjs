#!/usr/bin/env node

import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const baseURL = process.argv[2];
const expectedCommit = process.argv[3];
const expectedDigest = process.argv[4];
const chromePath = process.env.CHROME_BIN;

if (!baseURL || !expectedCommit || !expectedDigest || !chromePath) {
  console.error(
    "Usage: CHROME_BIN=/path/to/chrome node --experimental-websocket scripts/m3-hosted-browser-acceptance.mjs <base-url> <commit> <digest>",
  );
  process.exit(2);
}

const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));
const profileDirectory = mkdtempSync(join(tmpdir(), "deadbolt-m3-hosted-"));
const devToolsActivePort = join(profileDirectory, "DevToolsActivePort");
let chromeStartupError;
let chromeStderr = "";
const chrome = spawn(
  chromePath,
  [
    "--headless=new",
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-gpu",
    "--disable-software-rasterizer",
    "--disable-dev-shm-usage",
    // Let Chrome allocate an available loopback port. A random fixed port can
    // collide with a concurrent runner process, making the acceptance check
    // flaky before it reaches the deployed application.
    "--remote-debugging-port=0",
    "--remote-debugging-address=127.0.0.1",
    `--user-data-dir=${profileDirectory}`,
    "--no-first-run",
    "--no-default-browser-check",
    "about:blank",
  ],
  { stdio: ["ignore", "ignore", "pipe"] },
);
chrome.stderr.on("data", (data) => {
  chromeStderr += data;
});
chrome.on("error", (error) => {
  chromeStartupError = error.message;
});
chrome.on("exit", (code, signal) => {
  if (code !== 0) {
    chromeStartupError = `Chrome exited with code ${code} (${signal ?? "no signal"})${chromeStderr ? `: ${chromeStderr}` : ""}`;
  }
});

function cleanup() {
  try {
    chrome.kill();
  } catch {}
  try {
    rmSync(profileDirectory, { recursive: true, force: true });
  } catch {}
}

class CDPClient {
  constructor(url) {
    this.socket = new WebSocket(url);
    this.nextID = 0;
    this.pending = new Map();
    this.ready = new Promise((resolve, reject) => {
      this.socket.onopen = resolve;
      this.socket.onerror = reject;
    });
    this.socket.onmessage = ({ data }) => {
      const message = JSON.parse(data);
      const pending = this.pending.get(message.id);
      if (!pending) return;
      this.pending.delete(message.id);
      message.error
        ? pending.reject(new Error(message.error.message))
        : pending.resolve(message.result);
    };
  }

  async send(method, params = {}) {
    await this.ready;
    const id = ++this.nextID;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.socket.send(JSON.stringify({ id, method, params }));
    });
  }
}

async function fetchJSON(url, options) {
  let lastError;
  // Fresh GitHub runners can take more than the usual six seconds to start
  // Chrome and its crashpad helper. Keep the readiness wait bounded but do
  // not misreport slow browser startup as a product acceptance failure.
  for (let attempt = 0; attempt < 90; attempt++) {
    try {
      const response = await fetch(url, options);
      if (response.ok) return response.json();
      lastError = new Error(`${response.status} ${response.statusText}`);
    } catch (error) {
      lastError = error;
    }
    await sleep(200);
  }
  throw lastError;
}

async function waitForChromeDebugger() {
  for (let attempt = 0; attempt < 90; attempt++) {
    if (existsSync(devToolsActivePort)) {
      const [port] = readFileSync(devToolsActivePort, "utf8")
        .trim()
        .split("\n");
      if (/^\d+$/.test(port)) {
        return fetchJSON(`http://127.0.0.1:${port}/json/version`);
      }
    }
    if (chromeStartupError) {
      throw new Error(
        `Chrome did not start its debugger: ${chromeStartupError}`,
      );
    }
    await sleep(200);
  }
  throw new Error(
    `Chrome did not create DevToolsActivePort${chromeStartupError ? `: ${chromeStartupError}` : chromeStderr ? `: ${chromeStderr}` : ""}`,
  );
}

async function evaluate(client, expression) {
  const result = await client.send("Runtime.evaluate", {
    expression,
    awaitPromise: true,
    returnByValue: true,
  });
  if (result.exceptionDetails) throw new Error(result.exceptionDetails.text);
  return result.result.value;
}

try {
  const browser = await waitForChromeDebugger();
  const debuggingPort = new URL(browser.webSocketDebuggerUrl).port;
  const target = await fetchJSON(
    `http://127.0.0.1:${debuggingPort}/json/new?about:blank`,
    { method: "PUT" },
  );
  const client = new CDPClient(target.webSocketDebuggerUrl);
  await client.send("Page.enable");
  await client.send("Runtime.enable");
  await client.send("Page.navigate", { url: `${baseURL}/dashboard/` });

  for (let attempt = 0; attempt < 60; attempt++) {
    if (await evaluate(client, "document.readyState === 'complete'")) break;
    await sleep(250);
  }

  const provenance = await evaluate(
    client,
    `(async () => {
    const response = await fetch('/version');
    return { status: response.status, body: await response.json() };
  })()`,
  );
  const page = await evaluate(
    client,
    `({
    url: location.href,
    title: document.title,
    heading: document.querySelector('h1')?.textContent?.trim(),
    navLabel: document.querySelector('nav')?.getAttribute('aria-label'),
    themeToggle: !!document.querySelector('#theme-toggle'),
    stylesheetLoaded: [...document.styleSheets].some((sheet) => sheet.href?.includes('/dashboard/styles.css'))
  })`,
  );

  const responsive = [];
  for (const viewport of [
    { name: "mobile", width: 375, height: 812 },
    { name: "tablet", width: 768, height: 1024 },
    { name: "desktop", width: 1440, height: 900 },
  ]) {
    await client.send("Emulation.setDeviceMetricsOverride", {
      width: viewport.width,
      height: viewport.height,
      deviceScaleFactor: 1,
      mobile: viewport.name === "mobile",
    });
    await sleep(200);
    responsive.push(
      await evaluate(
        client,
        `({
      name: ${JSON.stringify(viewport.name)},
      width: innerWidth,
      height: innerHeight,
      horizontalOverflow: document.documentElement.scrollWidth > innerWidth,
      headerVisible: getComputedStyle(document.querySelector('.app-header')).display !== 'none',
      mainVisible: getComputedStyle(document.querySelector('main')).display !== 'none'
    })`,
      ),
    );
  }

  await client.send("Emulation.setDeviceMetricsOverride", {
    width: 1440,
    height: 900,
    deviceScaleFactor: 1,
    mobile: false,
  });
  // Prove that an explicit light selection overrides a dark OS preference,
  // rather than only passing on runners whose default preference is light.
  await client.send("Emulation.setEmulatedMedia", {
    features: [{ name: "prefers-color-scheme", value: "dark" }],
  });
  const themes = await evaluate(
    client,
    `(() => {
    const toggle = document.querySelector('#theme-toggle');
    document.documentElement.classList.remove('dark', 'light');
    localStorage.removeItem('theme');
    toggle.click();
    const dark = {
      applied: document.documentElement.classList.contains('dark'),
      pressed: toggle.getAttribute('aria-pressed'),
      stored: localStorage.getItem('theme'),
      background: getComputedStyle(document.body).backgroundColor,
    };
    toggle.click();
    const light = {
      applied: !document.documentElement.classList.contains('dark'),
      pressed: toggle.getAttribute('aria-pressed'),
      stored: localStorage.getItem('theme'),
      background: getComputedStyle(document.body).backgroundColor,
    };
    return { dark, light };
  })()`,
  );

  await evaluate(client, "document.querySelector('#theme-toggle').focus()");
  // Space activates a focused native button on key-up. Using rawKeyDown and
  // keyUp exercises Chromium's native keyboard behavior instead of calling
  // click() from JavaScript.
  await client.send("Input.dispatchKeyEvent", {
    type: "rawKeyDown",
    key: " ",
    code: "Space",
    windowsVirtualKeyCode: 32,
    nativeVirtualKeyCode: 32,
  });
  await client.send("Input.dispatchKeyEvent", {
    type: "keyUp",
    key: " ",
    code: "Space",
    windowsVirtualKeyCode: 32,
    nativeVirtualKeyCode: 32,
  });
  const keyboard = await evaluate(
    client,
    `({
    focusedID: document.activeElement?.id,
    activatedByKeyboard: document.documentElement.classList.contains('dark'),
    pressed: document.querySelector('#theme-toggle')?.getAttribute('aria-pressed'),
    navButtons: [...document.querySelectorAll('nav button')].map((button) => button.textContent.trim()),
    mainLandmark: !!document.querySelector('main[role="main"]')
  })`,
  );

  const evidence = {
    browser: browser.Browser,
    provenance,
    page,
    responsive,
    themes,
    keyboard,
  };
  const failures = [];
  if (
    provenance.status !== 200 ||
    provenance.body.commit !== expectedCommit ||
    provenance.body.image_digest !== expectedDigest
  )
    failures.push("exact artifact provenance");
  if (
    !page.themeToggle ||
    !page.stylesheetLoaded ||
    page.navLabel !== "Main Navigation"
  )
    failures.push("dashboard assets/semantics");
  if (
    responsive.some(
      (result) =>
        result.horizontalOverflow ||
        !result.headerVisible ||
        !result.mainVisible,
    )
  )
    failures.push("responsive layout");
  if (
    !themes.dark.applied ||
    themes.dark.pressed !== "true" ||
    themes.dark.stored !== "dark"
  )
    failures.push("dark theme");
  if (
    !themes.light.applied ||
    themes.light.pressed !== "false" ||
    themes.light.stored !== "light" ||
    themes.light.background === themes.dark.background
  )
    failures.push("light theme");
  if (
    keyboard.focusedID !== "theme-toggle" ||
    !keyboard.activatedByKeyboard ||
    keyboard.pressed !== "true" ||
    !keyboard.mainLandmark
  )
    failures.push("keyboard focus/activation");

  console.log(
    JSON.stringify(
      { result: failures.length ? "FAIL" : "PASS", failures, evidence },
      null,
      2,
    ),
  );
  if (failures.length) process.exitCode = 1;
} finally {
  cleanup();
}
