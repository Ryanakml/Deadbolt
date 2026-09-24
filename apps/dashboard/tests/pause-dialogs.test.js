import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { pathToFileURL } from "node:url";

// Browser-behavior verification for pause/resume Inspector controls without
// a real browser: a minimal DOM stub drives the actual built dashboard
// (dist/index.js) through bootstrap → runs list → Inspector → pause/resume
// dialogs, proving permission-gated CTAs, stale-409 same-dialog UX with the
// latest revision/status visible, focus return, and Escape handling.
//
// The stub only implements what the dashboard touches: element registry,
// innerHTML capture with #id/.class lookup, listeners with manual dispatch,
// focus tracking, and a scripted fetch router. No parsing engine is needed
// because lookups are scoped to ids/classes the dashboard itself renders.

function escapeText(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function installDom() {
  const registry = new Map();
  const domListeners = {};

  function makeElement(tag, id) {
    const listeners = {};
    const el = {
      tagName: String(tag).toUpperCase(),
      id: id || "",
      children: [],
      _html: "",
      _text: "",
      textContent: "",
      value: "",
      disabled: false,
      dataset: {},
      style: {},
      attributes: {},
      focusCalled: false,
      removed: false,
      _bySel: new Map(),
      classList: {
        _s: new Set(),
        add(...c) {
          for (const x of c) this._s.add(x);
        },
        remove(...c) {
          for (const x of c) this._s.delete(x);
        },
        toggle(c, force) {
          const want = force === undefined ? !this._s.has(c) : !!force;
          if (want) this._s.add(c);
          else this._s.delete(c);
          return want;
        },
        contains(c) {
          return this._s.has(c);
        },
      },
      set innerHTML(v) {
        this._html = String(v);
        this._bySel.clear();
      },
      get innerHTML() {
        return this._html;
      },
      setAttribute(k, v) {
        this.attributes[String(k)] = String(v);
      },
      getAttribute(k) {
        return Object.hasOwn(this.attributes, String(k))
          ? this.attributes[String(k)]
          : null;
      },
      removeAttribute(k) {
        delete this.attributes[String(k)];
      },
      appendChild(c) {
        this.children.push(c);
        if (c && c.id) registry.set(c.id, c);
        return c;
      },
      remove() {
        this.removed = true;
        if (this.id) registry.delete(this.id);
      },
      addEventListener(t, fn) {
        if (!listeners[t]) listeners[t] = [];
        listeners[t].push(fn);
      },
      _dispatch(t, ev) {
        for (const fn of listeners[t] || []) {
          fn({
            currentTarget: el,
            target: (ev && ev.target) || el,
            ...(ev || {}),
          });
        }
      },
      click() {
        this._dispatch("click", {});
      },
      focus() {
        this.focusCalled = true;
      },
      _child(key, attrs) {
        if (!this._bySel.has(key)) {
          const child = makeElement("div", "");
          if (attrs) {
            for (const [k, v] of Object.entries(attrs))
              child.setAttribute(k, v);
          }
          this._bySel.set(key, child);
        }
        return this._bySel.get(key);
      },
      querySelector(sel) {
        const found = this.querySelectorAll(sel);
        return found.length > 0 ? found[0] : null;
      },
      querySelectorAll(sel) {
        const out = [];
        const visit = (node) => {
          if (sel.startsWith("#")) {
            const id = sel.slice(1);
            if (node._html.includes(`id="${id}"`)) {
              out.push(node._child(`#${id}`));
            }
          } else if (sel.startsWith(".")) {
            const cls = sel.slice(1);
            const re = new RegExp(
              `<[a-zA-Z][^>]*class="[^"]*\\b${cls}\\b[^"]*"[^>]*>`,
              "g",
            );
            for (const m of node._html.matchAll(re)) {
              const idMatch = m[0].match(/data-run-id="([^"]+)"/);
              // Stable cache key per matched tag so repeated lookups return
              // the same node identity (listeners survive across calls).
              const stub = node._child(`${cls}:${m[0]}`);
              if (idMatch) stub.setAttribute("data-run-id", idMatch[1]);
              out.push(stub);
            }
          }
          for (const c of node.children) visit(c);
        };
        visit(this);
        // Deduplicate cached stubs while preserving document order.
        return [...new Set(out)];
      },
    };
    Object.defineProperty(el, "textContent", {
      get() {
        return this._text;
      },
      set(v) {
        this._text = String(v);
        // Emulate the div.textContent→innerHTML escape trick used by the
        // dashboard's escapeHtml helper.
        this._html = escapeText(v);
      },
      configurable: true,
    });
    if (id) registry.set(id, el);
    return el;
  }

  const body = makeElement("body", "body");
  const documentStub = {
    cookie: "",
    body,
    documentElement: makeElement("html", "html"),
    createElement(tag) {
      return makeElement(tag, "");
    },
    getElementById(id) {
      if (!registry.has(id)) registry.set(id, makeElement("div", id));
      return registry.get(id);
    },
    querySelectorAll() {
      return [];
    },
    addEventListener(t, fn) {
      if (!domListeners[t]) domListeners[t] = [];
      domListeners[t].push(fn);
    },
  };

  globalThis.document = documentStub;
  globalThis.window = { location: { search: "" }, addEventListener() {} };

  return {
    registry,
    fireDomContentLoaded() {
      for (const fn of domListeners["DOMContentLoaded"] || []) fn();
    },
    overlayPresent(id) {
      return registry.has(id) && !registry.get(id).removed;
    },
  };
}

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj), { status });
}

function installFetch(server) {
  globalThis.fetch = async (input, init = {}) => {
    const url = String(input);
    const method = (init.method || "GET").toUpperCase();
    if (url.includes("/api/auth/session"))
      return jsonResponse(server.session());
    if (url.includes("/v1/projects/") && url.includes("/environments")) {
      return jsonResponse({ environments: [{ id: "env-1", name: "staging" }] });
    }
    if (url.includes("/v1/projects")) {
      return jsonResponse({ projects: [{ id: "p1", name: "P" }] });
    }
    if (url.includes("/v1/runs/run-1/pause") && method === "POST") {
      return server.pause();
    }
    if (url.includes("/v1/runs/run-1/resume") && method === "POST") {
      return server.resume();
    }
    if (url.includes("/v1/runs/run-1/stream")) {
      return new Promise(() => {});
    }
    if (url.includes("/v1/runs/run-1/events")) {
      return jsonResponse({ events: [], hasMore: false, nextCursor: null });
    }
    if (url.includes("/v1/runs/run-1/logs")) {
      return jsonResponse({ items: [], expired: false });
    }
    if (url.includes("/v1/runs/run-1")) return jsonResponse(server.snapshot());
    if (url.includes("/v1/runs?"))
      return jsonResponse({ items: [server.runRow()] });
    throw new Error(`unexpected fetch: ${method} ${url}`);
  };
}

async function settle(rounds = 12) {
  for (let i = 0; i < rounds; i++) {
    await new Promise((r) => setTimeout(r, 0));
  }
}

function developerSession() {
  return {
    user: { id: "u-1", email: "dev@example.com", name: "Dev" },
    active_organization_id: "org-1",
    memberships: [
      {
        OrganizationID: "org-1",
        OrganizationName: "Acme",
        Role: "developer",
        Status: "ACTIVE",
      },
    ],
  };
}

function viewerSession() {
  const s = developerSession();
  s.memberships[0].Role = "viewer";
  return s;
}

describe("Inspector pause/resume browser behavior", () => {
  test("authorized user: Pause CTA → stale 409 stays open with latest state → Escape returns focus", async () => {
    const dom = installDom();
    const state = {
      role: "developer",
      runStatus: "RUNNING",
      runRevision: 7,
      pauseCalls: 0,
    };
    const server = {
      session: () =>
        state.role === "developer" ? developerSession() : viewerSession(),
      runRow: () => ({
        id: "run-1",
        workflowName: "wf",
        status: state.runStatus,
        reasonCode: null,
        createdAt: new Date().toISOString(),
      }),
      snapshot: () => ({
        id: "run-1",
        workflowName: "wf",
        deploymentId: "dep-1",
        status: state.runStatus,
        revision: state.runRevision,
        lastEventSequence: 10,
        createdAt: new Date().toISOString(),
        steps: [],
      }),
      pause: () => {
        state.pauseCalls += 1;
        if (state.pauseCalls === 1) {
          // A concurrent commit wins first: the dialog must surface 409 and
          // then show this fresher state in the SAME dialog.
          state.runStatus = "PAUSING";
          state.runRevision = 8;
          return new Response("conflict", { status: 409 });
        }
        return jsonResponse(server.snapshot());
      },
      resume: () => jsonResponse(server.snapshot()),
    };
    installFetch(server);

    await import("../dist/index.js");
    dom.fireDomContentLoaded();
    await settle();

    // Open the Inspector through the runs list, like a real click path.
    const runsBody = globalThis.document.getElementById("runs-table-body");
    const links = runsBody.querySelectorAll(".inspect-link");
    assert.equal(links.length, 1);
    links[0].click();
    await settle();

    const container = globalThis.document.getElementById("inspector-content");
    assert.ok(container.innerHTML.includes('id="pause-run-btn"'));
    assert.ok(container.innerHTML.includes('id="cancel-run-btn"'));
    assert.ok(!container.innerHTML.includes('id="resume-run-btn"'));

    // Open the Pause dialog; confirm is focused for keyboard operation.
    const pauseBtn = container.querySelector("#pause-run-btn");
    assert.ok(pauseBtn);
    pauseBtn.click();
    await settle(4);
    assert.ok(dom.overlayPresent("pause-dialog-overlay"));
    const overlay = globalThis.document.getElementById("pause-dialog-overlay");
    const confirm = overlay.querySelector("#pause-confirm");
    assert.ok(confirm);
    assert.equal(confirm.focusCalled, true);

    // Stale revision → 409. The SAME dialog stays open with the error and
    // the latest revision/status; the action is disabled, not retried.
    confirm.click();
    await settle();
    assert.ok(
      dom.overlayPresent("pause-dialog-overlay"),
      "pause dialog must stay open on 409",
    );
    assert.equal(
      globalThis.document.getElementById("pause-dialog-overlay"),
      overlay,
      "same dialog instance must be reused, not recreated",
    );
    const errorBox = overlay.querySelector("#pause-error");
    assert.equal(errorBox.style.display, "block");
    // After the in-dialog refetch the generic 409 notice is replaced by the
    // status-specific guidance; either proves the conflict surfaced in-dialog.
    assert.match(errorBox.textContent, /409|already PAUSING/);
    const meta = overlay.querySelector(".hold-meta");
    assert.match(meta.textContent, /8/);
    assert.match(meta.textContent, /PAUSING/);
    assert.equal(confirm.getAttribute("disabled"), "true");

    // Snapshot push re-rendered the Inspector behind the dialog: Resume is
    // now offered, Pause is gone.
    assert.ok(container.innerHTML.includes('id="resume-run-btn"'));
    assert.ok(!container.innerHTML.includes('id="pause-run-btn"'));

    // Escape closes and returns focus to the invoking control.
    overlay._dispatch("keydown", { key: "Escape" });
    assert.ok(!dom.overlayPresent("pause-dialog-overlay"));
    assert.equal(pauseBtn.focusCalled, true);
  });

  test("authorized user: Resume dialog behaves the same on stale 409", async () => {
    const dom = installDom();
    const state = { runStatus: "PAUSED", runRevision: 9, resumeCalls: 0 };
    const server = {
      session: () => developerSession(),
      runRow: () => ({
        id: "run-1",
        workflowName: "wf",
        status: state.runStatus,
        reasonCode: null,
        createdAt: new Date().toISOString(),
      }),
      snapshot: () => ({
        id: "run-1",
        workflowName: "wf",
        deploymentId: "dep-1",
        status: state.runStatus,
        revision: state.runRevision,
        lastEventSequence: 12,
        createdAt: new Date().toISOString(),
        steps: [],
      }),
      pause: () => jsonResponse(server.snapshot()),
      resume: () => {
        state.resumeCalls += 1;
        if (state.resumeCalls === 1) {
          state.runStatus = "RUNNING";
          state.runRevision = 10;
          return new Response("conflict", { status: 409 });
        }
        return jsonResponse(server.snapshot());
      },
    };
    installFetch(server);

    await import("../dist/index.js?resume=1");
    dom.fireDomContentLoaded();
    await settle();

    globalThis.document
      .getElementById("runs-table-body")
      .querySelectorAll(".inspect-link")[0]
      .click();
    await settle();

    const container = globalThis.document.getElementById("inspector-content");
    assert.ok(container.innerHTML.includes('id="resume-run-btn"'));
    const resumeBtn = container.querySelector("#resume-run-btn");
    resumeBtn.click();
    await settle(4);
    assert.ok(dom.overlayPresent("resume-dialog-overlay"));
    const overlay = globalThis.document.getElementById("resume-dialog-overlay");
    const confirm = overlay.querySelector("#resume-confirm");
    assert.equal(confirm.focusCalled, true);

    confirm.click();
    await settle();
    assert.ok(dom.overlayPresent("resume-dialog-overlay"));
    const errorBox = overlay.querySelector("#resume-error");
    assert.equal(errorBox.style.display, "block");
    const meta = overlay.querySelector(".hold-meta");
    assert.match(meta.textContent, /10/);
    assert.match(meta.textContent, /RUNNING/);
    assert.equal(confirm.getAttribute("disabled"), "true");

    overlay._dispatch("keydown", { key: "Escape" });
    assert.ok(!dom.overlayPresent("resume-dialog-overlay"));
    assert.equal(resumeBtn.focusCalled, true);
  });

  test("read-only viewer: no Pause/Resume/Cancel affordance is rendered", async () => {
    const dom = installDom();
    const state = { runStatus: "RUNNING", runRevision: 7 };
    const server = {
      session: () => viewerSession(),
      runRow: () => ({
        id: "run-1",
        workflowName: "wf",
        status: state.runStatus,
        reasonCode: null,
        createdAt: new Date().toISOString(),
      }),
      snapshot: () => ({
        id: "run-1",
        workflowName: "wf",
        deploymentId: "dep-1",
        status: state.runStatus,
        revision: state.runRevision,
        lastEventSequence: 10,
        createdAt: new Date().toISOString(),
        steps: [],
      }),
      pause: () => new Response("forbidden", { status: 403 }),
      resume: () => new Response("forbidden", { status: 403 }),
    };
    installFetch(server);

    await import("../dist/index.js?viewer=1");
    dom.fireDomContentLoaded();
    await settle();

    globalThis.document
      .getElementById("runs-table-body")
      .querySelectorAll(".inspect-link")[0]
      .click();
    await settle();

    // The Inspector must not offer a fake mutation affordance: a read-only
    // identity discovers nothing to click, instead of a 403 after the fact.
    const container = globalThis.document.getElementById("inspector-content");
    assert.ok(!container.innerHTML.includes("pause-run-btn"));
    assert.ok(!container.innerHTML.includes("resume-run-btn"));
    assert.ok(!container.innerHTML.includes("cancel-run-btn"));
    assert.ok(dom);
  });
});
