import { test, describe } from "node:test";
import assert from "node:assert/strict";

// Browser-behavior verification for schedules without a real browser: a
// minimal DOM stub drives the actual built dashboard (dist/index.js) through
// bootstrap → schedules list → create/edit/pause dialogs + occurrence
// history, proving permission-gated CTAs, stale-409 same-dialog UX,
// focus return, Escape handling, and verbatim skipped-reason rendering.
//
// The stub follows pause-dialogs.test.js patterns: element registry,
// innerHTML capture with #id/.class lookup, listeners with manual dispatch,
// focus tracking, and a scripted fetch router.

function escapeText(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function parseDataAttrs(tag, el) {
  const re = /data-[a-zA-Z0-9_-]+="[^"]*"/g;
  for (const m of tag.matchAll(re)) {
    const eq = m[0].indexOf("=");
    const k = m[0].slice(0, eq);
    const v = m[0].slice(eq + 2, -1);
    el.setAttribute(k, v);
  }
  const idMatch = tag.match(/id="([^"]+)"/);
  if (idMatch && !el.id) el.id = idMatch[1];
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
      onclick: null,
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
        if (typeof this.onclick === "function") {
          this.onclick({ currentTarget: el, target: el });
        }
      },
      focus() {
        this.focusCalled = true;
      },
      _child(key, tag) {
        if (!this._bySel.has(key)) {
          const child = makeElement("div", "");
          if (tag) parseDataAttrs(tag, child);
          // Preserve the id selector key for later getAttribute checks.
          const idMatch = key.match(/^#(.+)$/);
          if (idMatch && !child.id) child.id = idMatch[1];
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
              const m = node._html.match(
                new RegExp(`<[^>]*id="${id}"[^>]*>`),
              );
              out.push(node._child(`#${id}`, m ? m[0] : undefined));
            }
          } else if (sel.startsWith(".")) {
            const cls = sel.slice(1);
            const re = new RegExp(
              `<[a-zA-Z][^>]*class="[^"]*\\b${cls}\\b[^"]*"[^>]*>`,
              "g",
            );
            for (const m of node._html.matchAll(re)) {
              const stub = node._child(`${cls}:${m[0]}`, m[0]);
              out.push(stub);
            }
          }
          for (const c of node.children) visit(c);
        };
        visit(this);
        return [...new Set(out)];
      },
    };
    Object.defineProperty(el, "textContent", {
      get() {
        return this._text;
      },
      set(v) {
        this._text = String(v);
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

function operatorSession() {
  return {
    user: { id: "u-1", email: "op@example.com", name: "Op" },
    active_organization_id: "org-1",
    memberships: [
      {
        OrganizationID: "org-1",
        OrganizationName: "Acme",
        Role: "operator",
        Status: "ACTIVE",
      },
    ],
  };
}

function viewerSession() {
  const s = operatorSession();
  s.memberships[0].Role = "viewer";
  return s;
}

function scheduleFixture(over = {}) {
  return {
    id: "sched-1",
    workflow: "nightly-report",
    environment: "env-1",
    cron: "*/5 * * * *",
    timezone: "UTC",
    deploymentId: null,
    overlapPolicy: "skip-overlap",
    misfirePolicy: "coalesce-one",
    paused: false,
    revision: 3,
    nextDueAt: new Date(Date.now() + 3600000).toISOString(),
    lastOccurrenceAt: null,
    ...over,
  };
}

function installFetch(server) {
  globalThis.fetch = async (input, init = {}) => {
    const url = String(input);
    const method = (init.method || "GET").toUpperCase();
    if (url.includes("/api/auth/session")) return jsonResponse(server.session());
    if (url.includes("/v1/projects/") && url.includes("/environments")) {
      return jsonResponse({ environments: [{ id: "env-1", name: "staging" }] });
    }
    if (url.includes("/v1/projects")) {
      return jsonResponse({ projects: [{ id: "p1", name: "P" }] });
    }
    if (url.includes("/v1/runs?")) {
      return jsonResponse({ items: [], nextCursor: null });
    }
    if (url.includes("/occurrences")) {
      return server.occurrences(url, init);
    }
    if (url.includes("/v1/schedules/") && url.includes("/pause")) {
      return server.pause(url, init);
    }
    if (url.includes("/v1/schedules/") && url.includes("/resume")) {
      return server.resume(url, init);
    }
    if (url.includes("/v1/schedules/") && method === "PATCH") {
      return server.update(url, init);
    }
    if (url.includes("/v1/schedules/") && method === "DELETE") {
      return server.remove(url, init);
    }
    if (url.includes("/v1/schedules") && method === "POST") {
      return server.create(url, init);
    }
    if (url.includes("/v1/schedules")) {
      return server.list(url, init);
    }
    throw new Error(`unexpected fetch: ${method} ${url}`);
  };
}

async function settle(rounds = 14) {
  for (let i = 0; i < rounds; i++) {
    await new Promise((r) => setTimeout(r, 0));
  }
}

async function openSchedulesView(dom, query) {
  await import(`../dist/index.js${query}`);
  dom.fireDomContentLoaded();
  await settle();
  globalThis.document.getElementById("nav-schedules").click();
  await settle();
}

describe("schedules browser behavior", () => {
  test("create dialog submit calls the client with CSRF+Idempotency and refreshes", async () => {
    const dom = installDom();
    const state = {
      schedules: [scheduleFixture()],
      createCalls: [],
      listCalls: 0,
    };
    const server = {
      session: () => operatorSession(),
      list: () => {
        state.listCalls += 1;
        return jsonResponse({
          items: state.schedules,
          nextCursor: null,
        });
      },
      create: (url, init) => {
        state.createCalls.push([url, init]);
        const body = JSON.parse(init.body);
        const created = scheduleFixture({
          id: "sched-2",
          workflow: body.workflow,
          cron: body.cron,
          timezone: body.timezone,
          revision: 1,
        });
        state.schedules.push(created);
        return jsonResponse(created);
      },
      update: () => jsonResponse(scheduleFixture()),
      remove: () => jsonResponse({ deleted: true, id: "sched-1" }),
      pause: () => jsonResponse(scheduleFixture({ paused: true })),
      resume: () => jsonResponse(scheduleFixture()),
      occurrences: () => jsonResponse({ items: [], nextCursor: null }),
    };
    installFetch(server);
    await openSchedulesView(dom, "?sched-create=1");

    const list = globalThis.document.getElementById("schedules-list");
    assert.ok(list.innerHTML.includes("nightly-report"));
    assert.ok(list.innerHTML.includes("*/5 * * * *"));
    assert.ok(list.innerHTML.includes("UTC"));
    assert.ok(list.innerHTML.includes("revision 3"));
    assert.ok(list.innerHTML.includes("active-at-fire"));
    assert.ok(list.innerHTML.includes("ACTIVE"));
    assert.ok(list.innerHTML.includes("skip-overlap"));
    assert.ok(list.innerHTML.includes("coalesce-one"));
    assert.ok(list.innerHTML.includes("next due"));
    assert.ok(!list.innerHTML.includes("paused") || list.innerHTML.includes("ACTIVE"));

    const newBtn = globalThis.document.getElementById("schedules-new-btn");
    assert.equal(newBtn.classList.contains("hidden"), false);
    newBtn.click();
    await settle(4);
    assert.ok(dom.overlayPresent("schedule-create-dialog-overlay"));
    const overlay = globalThis.document.getElementById(
      "schedule-create-dialog-overlay",
    );
    const workflow = overlay.querySelector("#schedule-create-workflow");
    const cron = overlay.querySelector("#schedule-create-cron");
    const tz = overlay.querySelector("#schedule-create-timezone");
    assert.ok(workflow && cron && tz);
    assert.equal(workflow.focusCalled, true);
    workflow.value = "hourly-sync";
    cron.value = "0 * * * *";
    tz.value = "UTC";
    overlay.querySelector("#schedule-create-submit").click();
    await settle();

    assert.equal(state.createCalls.length, 1);
    const [url, init] = state.createCalls[0];
    assert.ok(url.includes("/v1/schedules?"));
    assert.ok(url.includes("environment=env-1"));
    assert.equal(init.method, "POST");
    assert.ok("X-CSRF-Token" in init.headers);
    assert.ok(init.headers["Idempotency-Key"]);
    assert.deepEqual(JSON.parse(init.body), {
      workflow: "hourly-sync",
      cron: "0 * * * *",
      timezone: "UTC",
    });
    assert.ok(
      state.listCalls >= 2,
      "list must refresh after create",
    );
    assert.ok(
      !dom.overlayPresent("schedule-create-dialog-overlay"),
      "dialog closes on success",
    );
    assert.equal(newBtn.focusCalled, true);
  });

  test("edit 409 conflict updates revision in-dialog without blind retry", async () => {
    const dom = installDom();
    const state = {
      revision: 3,
      updateCalls: 0,
    };
    const server = {
      session: () => operatorSession(),
      list: () =>
        jsonResponse({
          items: [scheduleFixture({ revision: state.revision })],
          nextCursor: null,
        }),
      create: () => jsonResponse(scheduleFixture()),
      update: () => {
        state.updateCalls += 1;
        if (state.updateCalls === 1) {
          state.revision = 4;
          return new Response("conflict", { status: 409 });
        }
        return jsonResponse(scheduleFixture({ revision: 5 }));
      },
      remove: () => jsonResponse({ deleted: true, id: "sched-1" }),
      pause: () => jsonResponse(scheduleFixture()),
      resume: () => jsonResponse(scheduleFixture()),
      occurrences: () => jsonResponse({ items: [], nextCursor: null }),
    };
    installFetch(server);
    await openSchedulesView(dom, "?sched-edit409=1");

    const list = globalThis.document.getElementById("schedules-list");
    const editBtns = list.querySelectorAll(".schedule-edit-btn");
    assert.equal(editBtns.length, 1);
    editBtns[0].click();
    await settle(4);
    assert.ok(dom.overlayPresent("schedule-edit-dialog-overlay"));
    const overlay = globalThis.document.getElementById(
      "schedule-edit-dialog-overlay",
    );
    const submit = overlay.querySelector("#schedule-edit-submit");
    submit.click();
    await settle();

    assert.equal(state.updateCalls, 1, "must never blind retry");
    assert.ok(
      dom.overlayPresent("schedule-edit-dialog-overlay"),
      "same dialog stays open on 409",
    );
    const errorBox = overlay.querySelector("#schedule-edit-error");
    assert.equal(errorBox.style.display, "block");
    assert.match(errorBox.textContent, /409/);
    const meta = overlay.querySelector(".hold-meta");
    assert.match(meta.textContent, /4/);

    overlay._dispatch("keydown", { key: "Escape" });
    assert.ok(!dom.overlayPresent("schedule-edit-dialog-overlay"));
    assert.equal(editBtns[0].focusCalled, true);
  });

  test("pause flow: stale 409 stays open with latest paused state", async () => {
    const dom = installDom();
    const state = { paused: false, revision: 7, pauseCalls: 0 };
    const server = {
      session: () => operatorSession(),
      list: () =>
        jsonResponse({
          items: [
            scheduleFixture({
              paused: state.paused,
              revision: state.revision,
              nextDueAt: state.paused ? null : new Date(Date.now() + 60000).toISOString(),
            }),
          ],
          nextCursor: null,
        }),
      create: () => jsonResponse(scheduleFixture()),
      update: () => jsonResponse(scheduleFixture()),
      remove: () => jsonResponse({ deleted: true, id: "sched-1" }),
      pause: () => {
        state.pauseCalls += 1;
        if (state.pauseCalls === 1) {
          state.paused = true;
          state.revision = 8;
          return new Response("conflict", { status: 409 });
        }
        return jsonResponse(scheduleFixture({ paused: true, revision: 8 }));
      },
      resume: () => jsonResponse(scheduleFixture()),
      occurrences: () => jsonResponse({ items: [], nextCursor: null }),
    };
    installFetch(server);
    await openSchedulesView(dom, "?sched-pause=1");

    const list = globalThis.document.getElementById("schedules-list");
    assert.ok(list.innerHTML.includes("ACTIVE"));
    const pauseBtns = list.querySelectorAll(".schedule-pause-btn");
    assert.equal(pauseBtns.length, 1);
    pauseBtns[0].click();
    await settle(4);
    assert.ok(dom.overlayPresent("schedule-pause-dialog-overlay"));
    const overlay = globalThis.document.getElementById(
      "schedule-pause-dialog-overlay",
    );
    const confirm = overlay.querySelector("#schedule-pause-confirm");
    assert.equal(confirm.focusCalled, true);

    confirm.click();
    await settle();
    assert.ok(dom.overlayPresent("schedule-pause-dialog-overlay"));
    const errorBox = overlay.querySelector("#schedule-pause-error");
    assert.equal(errorBox.style.display, "block");
    assert.match(errorBox.textContent, /409|already paused/);
    const meta = overlay.querySelector(".hold-meta");
    assert.match(meta.textContent, /8/);
    assert.equal(confirm.getAttribute("disabled"), "true");

    overlay._dispatch("keydown", { key: "Escape" });
    assert.ok(!dom.overlayPresent("schedule-pause-dialog-overlay"));
    assert.equal(pauseBtns[0].focusCalled, true);
  });

  test("without capability: 403 notice with no mutation CTAs", async () => {
    const dom = installDom();
    const server = {
      session: () => viewerSession(),
      list: () => new Response("forbidden", { status: 403 }),
      create: () => new Response("forbidden", { status: 403 }),
      update: () => new Response("forbidden", { status: 403 }),
      remove: () => new Response("forbidden", { status: 403 }),
      pause: () => new Response("forbidden", { status: 403 }),
      resume: () => new Response("forbidden", { status: 403 }),
      occurrences: () => new Response("forbidden", { status: 403 }),
    };
    installFetch(server);
    await openSchedulesView(dom, "?sched-viewer=1");

    const list = globalThis.document.getElementById("schedules-list");
    assert.ok(
      list.innerHTML.includes(
        "Schedules require the schedules:write capability (operator role or above)",
      ),
    );
    assert.equal(list.querySelectorAll(".schedule-edit-btn").length, 0);
    assert.equal(list.querySelectorAll(".schedule-pause-btn").length, 0);
    assert.equal(list.querySelectorAll(".schedule-resume-btn").length, 0);
    assert.equal(list.querySelectorAll(".schedule-delete-btn").length, 0);
    assert.ok(!list.innerHTML.includes("schedule-edit-"));
    assert.ok(!list.innerHTML.includes("schedule-pause-"));
    assert.ok(!list.innerHTML.includes("schedule-delete-"));
    const newBtn = globalThis.document.getElementById("schedules-new-btn");
    assert.equal(newBtn.classList.contains("hidden"), true);
  });

  test("occurrence history renders skipped reasons verbatim with run links, no fake rows", async () => {
    const dom = installDom();
    const server = {
      session: () => operatorSession(),
      list: () =>
        jsonResponse({ items: [scheduleFixture()], nextCursor: null }),
      create: () => jsonResponse(scheduleFixture()),
      update: () => jsonResponse(scheduleFixture()),
      remove: () => jsonResponse({ deleted: true, id: "sched-1" }),
      pause: () => jsonResponse(scheduleFixture()),
      resume: () => jsonResponse(scheduleFixture()),
      occurrences: () =>
        jsonResponse({
          items: [
            {
              id: "occ-1",
              scheduleId: "sched-1",
              dueAt: "2026-01-01T00:00:00.000Z",
              revision: 3,
              status: "SKIPPED",
              skippedReason: "SKIPPED_QUOTA",
              skippedCount: 2,
              runId: null,
            },
            {
              id: "occ-2",
              scheduleId: "sched-1",
              dueAt: "2026-01-01T01:00:00.000Z",
              revision: 3,
              status: "SKIPPED",
              skippedReason: "SKIPPED_OVERLAP",
              skippedCount: 0,
              runId: null,
            },
            {
              id: "occ-3",
              scheduleId: "sched-1",
              dueAt: "2026-01-01T02:00:00.000Z",
              revision: 4,
              status: "STARTED",
              skippedReason: null,
              skippedCount: 1,
              runId: "run-abc",
            },
          ],
          nextCursor: null,
        }),
    };
    // Patch the third occurrence to a misfire reason to cover all three.
    const origOcc = server.occurrences;
    server.occurrences = () =>
      jsonResponse({
        items: [
          {
            id: "occ-1",
            scheduleId: "sched-1",
            dueAt: "2026-01-01T00:00:00.000Z",
            revision: 3,
            status: "SKIPPED",
            skippedReason: "SKIPPED_QUOTA",
            skippedCount: 2,
            runId: null,
          },
          {
            id: "occ-2",
            scheduleId: "sched-1",
            dueAt: "2026-01-01T01:00:00.000Z",
            revision: 3,
            status: "SKIPPED",
            skippedReason: "SKIPPED_OVERLAP",
            skippedCount: 0,
            runId: null,
          },
          {
            id: "occ-3",
            scheduleId: "sched-1",
            dueAt: "2026-01-01T02:00:00.000Z",
            revision: 4,
            status: "SKIPPED",
            skippedReason: "SKIPPED_MISFIRE",
            skippedCount: 0,
            runId: "run-abc",
          },
        ],
        nextCursor: null,
      });
    void origOcc;
    installFetch(server);
    await openSchedulesView(dom, "?sched-history=1");

    const list = globalThis.document.getElementById("schedules-list");
    const historyBtns = list.querySelectorAll(".schedule-history-btn");
    assert.equal(historyBtns.length, 1);
    historyBtns[0].click();
    await settle();

    const occBox = globalThis.document.getElementById(
      "schedule-occurrences-sched-1",
    );
    assert.ok(occBox.innerHTML.includes("SKIPPED_QUOTA"));
    assert.ok(occBox.innerHTML.includes("SKIPPED_OVERLAP"));
    assert.ok(occBox.innerHTML.includes("SKIPPED_MISFIRE"));
    assert.ok(occBox.innerHTML.includes("?runId=run-abc"));
    assert.ok(occBox.innerHTML.includes("revision 3"));
    assert.ok(occBox.innerHTML.includes("revision 4"));
    assert.ok(!occBox.innerHTML.includes("No occurrences recorded yet"));

    // No fake rows: an empty schedule list renders an honest empty state.
    const dom2 = installDom();
    const server2 = {
      session: () => operatorSession(),
      list: () => jsonResponse({ items: [], nextCursor: null }),
      create: () => jsonResponse(scheduleFixture()),
      update: () => jsonResponse(scheduleFixture()),
      remove: () => jsonResponse({ deleted: true, id: "sched-1" }),
      pause: () => jsonResponse(scheduleFixture()),
      resume: () => jsonResponse(scheduleFixture()),
      occurrences: () => jsonResponse({ items: [], nextCursor: null }),
    };
    installFetch(server2);
    await openSchedulesView(dom2, "?sched-empty=1");
    const list2 = globalThis.document.getElementById("schedules-list");
    assert.ok(list2.innerHTML.includes("No schedules found"));
    assert.equal(list2.querySelectorAll(".schedule-edit-btn").length, 0);
  });
});
