import { test, describe } from "node:test";
import assert from "node:assert/strict";

// Browser-behavior and DOM integration verification for the Deadbolt Inspector.
// Drives the actual production dashboard (dist/index.js) through a realistic DOM
// environment with persisted API state, verifying:
//   1. 1 node per logical step in DAG SVG; retry attempts contained in step tabs;
//      choice/merge and skipped branches rendered with appropriate wait reasons and styling.
//   2. Accessible list virtualization: bounded DOM rendering (<= 50 cards), physical
//      scroll offset preservation across rerender (no reset to 0), and Load More navigation.
//   3. Graph scroll position restoration across live SSE/snapshot rerenders and minimap sync.
//   4. Selected-step Logs tab bounded windowing, per-step cache coherence across multi-page
//      pagination, and stepId retention with zero cross-step row bleeding.
//   5. Keyboard navigation with roving tabindex and focus preservation across live rerenders.

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
      tabIndex: -1,
      scrollLeft: 0,
      scrollTop: 0,
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
        const fns = [...(listeners[t] || [])];
        for (const fn of fns) {
          fn({
            currentTarget: el,
            target: (ev && ev.target) || el,
            preventDefault() {},
            stopPropagation() {},
            ...(ev || {}),
          });
        }
      },
      click() {
        this._dispatch("click", {});
      },
      focus() {
        this.focusCalled = true;
        globalThis.document.activeElement = el;
      },
      contains(child) {
        if (child === el) return true;
        const search = (node) => {
          for (const c of node.children) {
            if (c === child || search(c)) return true;
          }
          for (const c of node._bySel.values()) {
            if (c === child || search(c)) return true;
          }
          return false;
        };
        return search(el);
      },
      _child(key, attrs) {
        if (!this._bySel.has(key)) {
          const child = makeElement("div", "");
          if (attrs) {
            for (const [k, v] of Object.entries(attrs)) {
              child.setAttribute(k, v);
            }
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
        // Handle descendant selector
        if (sel.includes(" ")) {
          const parts = sel.split(/\s+/).filter(Boolean);
          let current = [this];
          for (const p of parts) {
            const next = [];
            for (const c of current) {
              next.push(...c.querySelectorAll(p));
            }
            current = [...new Set(next)];
          }
          return current;
        }

        const out = [];
        const visit = (node) => {
          if (sel.startsWith("#")) {
            const id = sel.slice(1);
            if (node._html.includes(`id="${id}"`)) {
              const isNew = !node._bySel.has(`#${id}`);
              const stub = node._child(`#${id}`);
              stub.id = id;
              const openMatch = node._html.match(
                new RegExp(`<([a-zA-Z0-9_-]+)[^>]*id="${id}"[^>]*>`),
              );
              if (openMatch) {
                if (isNew) {
                  for (const am of openMatch[0].matchAll(
                    /([a-zA-Z0-9_-]+)="([^"]*)"/g,
                  )) {
                    stub.setAttribute(am[1], am[2]);
                    if (am[1] === "class") {
                      for (const c of am[2].split(/\s+/))
                        if (c) stub.classList.add(c);
                    }
                  }
                }
                const afterOpen = node._html.slice(
                  node._html.indexOf(openMatch[0]) + openMatch[0].length,
                );
                const nextClose = afterOpen.indexOf(`</${openMatch[1]}>`);
                if (nextClose !== -1) {
                  stub.innerHTML = afterOpen.slice(0, nextClose);
                }
              }
              out.push(stub);
            }
          } else if (sel.startsWith(".")) {
            // May include attribute filter like .dag-node[data-step-id="step-0"]
            let cls = sel.slice(1);
            let attrKey = null;
            let attrVal = null;
            if (cls.includes("[")) {
              const attrMatch = cls.match(/\[([a-zA-Z0-9_-]+)="([^"]+)"\]/);
              if (attrMatch) {
                attrKey = attrMatch[1];
                attrVal = attrMatch[2];
              }
              cls = cls.split("[")[0];
            }

            const re = new RegExp(
              `<[a-zA-Z0-9_-]+[^>]*class="[^"]*\\b${cls}\\b[^"]*"[^>]*>`,
              "g",
            );
            for (const m of node._html.matchAll(re)) {
              const tagStr = m[0];
              if (attrKey && !tagStr.includes(`${attrKey}="${attrVal}"`)) {
                continue;
              }

              const idMatch = tagStr.match(/id="([^"]+)"/);
              const runIdMatch = tagStr.match(/data-run-id="([^"]+)"/);
              const stepIdMatch = tagStr.match(/data-step-id="([^"]+)"/);
              const nodeIdMatch = tagStr.match(/data-node-id="([^"]+)"/);
              const offsetMatch = tagStr.match(/data-list-offset="([^"]+)"/);

              const cacheKey = idMatch
                ? `#${idMatch[1]}`
                : `${cls}:${stepIdMatch ? stepIdMatch[1] : m.index}`;
              const stub = node._child(cacheKey);
              if (idMatch) stub.id = idMatch[1];
              if (runIdMatch) stub.setAttribute("data-run-id", runIdMatch[1]);
              if (stepIdMatch)
                stub.setAttribute("data-step-id", stepIdMatch[1]);
              if (nodeIdMatch)
                stub.setAttribute("data-node-id", nodeIdMatch[1]);
              if (offsetMatch)
                stub.setAttribute("data-list-offset", offsetMatch[1]);
              for (const c of cls.split(" ")) stub.classList.add(c);
              out.push(stub);
            }
          } else if (sel.startsWith("[")) {
            const attrMatch = sel.match(/\[([a-zA-Z0-9_-]+)="([^"]+)"\]/);
            if (attrMatch) {
              const [_, k, v] = attrMatch;
              const re = new RegExp(
                `<[a-zA-Z0-9_-]+[^>]*\\b${k}="${v}"[^>]*>`,
                "g",
              );
              for (const m of node._html.matchAll(re)) {
                const stub = node._child(`${k}:${v}:${m[0]}`);
                stub.setAttribute(k, v);
                out.push(stub);
              }
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
    activeElement: body,
    createElement(tag) {
      return makeElement(tag, "");
    },
    getElementById(id) {
      for (const el of registry.values()) {
        const found = el.querySelector("#" + id);
        if (found) return found;
      }
      if (!registry.has(id)) registry.set(id, makeElement("div", id));
      return registry.get(id);
    },
    querySelector(sel) {
      return body.querySelector(sel);
    },
    querySelectorAll(sel) {
      return body.querySelectorAll(sel);
    },
    addEventListener(t, fn) {
      if (!domListeners[t]) domListeners[t] = [];
      domListeners[t].push(fn);
    },
  };

  globalThis.document = documentStub;
  globalThis.window = {
    location: { search: "" },
    addEventListener() {},
  };

  return {
    registry,
    fireDomContentLoaded() {
      for (const fn of domListeners["DOMContentLoaded"] || []) fn();
    },
  };
}

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

async function settle(rounds = 15) {
  for (let i = 0; i < rounds; i++) {
    await new Promise((r) => setTimeout(r, 10));
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

// Build a real 200-step execution graph matching persisted DB state:
// - Step 0: root task with 2 attempts (retried)
// - Step 1: choice node (evaluates condition)
// - Step 2: auto-approve (selected branch, SUCCEEDED)
// - Step 3: manual-review (unselected branch, SKIPPED, BRANCH_NOT_SELECTED)
// - Step 4: compliance-notify (dependent of skipped, SKIPPED, DEPENDENCY_SKIPPED)
// - Step 5..198: parallel fan-out tasks
// - Step 199: merge join task
function build200StepPersistedSnapshot() {
  const steps = [
    {
      id: "step-0",
      nodeId: "fetch-account",
      kind: "task",
      status: "SUCCEEDED",
      currentEpoch: 2,
      attempts: [
        {
          id: "att-0-1",
          attemptNumber: 1,
          status: "FAILED",
          error: "Connection timeout",
          workerSessionId: "sess-abc-1",
          startedAt: "2026-09-24T10:00:00Z",
          completedAt: "2026-09-24T10:00:02Z",
        },
        {
          id: "att-0-2",
          attemptNumber: 2,
          status: "SUCCEEDED",
          workerSessionId: "sess-abc-2",
          startedAt: "2026-09-24T10:00:05Z",
          completedAt: "2026-09-24T10:00:07Z",
        },
      ],
      output: { accountTier: "enterprise", score: 98 },
    },
    {
      id: "step-1",
      nodeId: "check-score",
      kind: "choice",
      status: "SUCCEEDED",
      currentEpoch: 1,
      after: ["fetch-account"],
      attempts: [],
    },
    {
      id: "step-2",
      nodeId: "auto-approve",
      kind: "task",
      status: "SUCCEEDED",
      currentEpoch: 1,
      after: ["check-score"],
      attempts: [
        {
          id: "att-2-1",
          attemptNumber: 1,
          status: "SUCCEEDED",
          startedAt: "2026-09-24T10:00:10Z",
          completedAt: "2026-09-24T10:00:12Z",
        },
      ],
    },
    {
      id: "step-3",
      nodeId: "manual-review",
      kind: "approval",
      status: "SKIPPED",
      waitReason: "BRANCH_NOT_SELECTED",
      currentEpoch: 0,
      after: ["check-score"],
      attempts: [],
    },
    {
      id: "step-4",
      nodeId: "compliance-hold",
      kind: "task",
      status: "SKIPPED",
      waitReason: "DEPENDENCY_SKIPPED",
      currentEpoch: 0,
      after: ["manual-review"],
      attempts: [],
    },
  ];

  for (let i = 5; i <= 198; i++) {
    steps.push({
      id: `step-${i}`,
      nodeId: `fanout-task-${i}`,
      kind: "task",
      status: i % 15 === 0 ? "SKIPPED" : "SUCCEEDED",
      waitReason: i % 15 === 0 ? "BRANCH_NOT_SELECTED" : undefined,
      currentEpoch: 1,
      after: ["auto-approve"],
      attempts: [
        {
          id: `att-${i}-1`,
          attemptNumber: 1,
          status: i % 15 === 0 ? "CANCELLED" : "SUCCEEDED",
          startedAt: "2026-09-24T10:00:15Z",
          completedAt: "2026-09-24T10:00:17Z",
        },
      ],
    });
  }

  steps.push({
    id: "step-199",
    nodeId: "merge-join",
    kind: "merge",
    status: "WAITING",
    currentEpoch: 0,
    after: steps.slice(5, 199).map((s) => s.nodeId),
    attempts: [],
  });

  return {
    id: "run-persisted-200",
    workflowName: "enterprise-approval-pipeline",
    deploymentId: "dep-prod-v1",
    status: "RUNNING",
    revision: 12,
    lastEventSequence: 204,
    createdAt: "2026-09-24T10:00:00Z",
    deadlineAt: "2026-09-24T11:00:00Z",
    steps,
  };
}

// Generate realistic log items for step-0 across multiple pages
function generateStepLogs(stepId, cursor) {
  const logsPerPage = 50;
  let pageIndex = 0;
  if (cursor === "cursor-page-2") pageIndex = 1;
  else if (cursor === "cursor-page-3") pageIndex = 2;

  const startSeq = pageIndex * logsPerPage;
  const items = Array.from({ length: logsPerPage }, (_, i) => ({
    timestamp: new Date(1774432800000 + (startSeq + i) * 1000).toISOString(),
    level: i % 10 === 0 ? "warn" : "info",
    message: `[${stepId}] Execution log line #${startSeq + i + 1} processing payload token`,
  }));

  const nextCursor = pageIndex < 2 ? `cursor-page-${pageIndex + 2}` : null;

  return {
    items,
    nextCursor,
    expired: false,
    budgetExhausted: false,
  };
}

describe("Production Inspector Browser DOM Integration (Issue #29)", () => {
  test("persisted 200-step DAG renders 1 node per step, bounded list, graph scroll restoration, bounded logs, and keyboard focus preservation", async () => {
    const dom = installDom();
    const snapshot = build200StepPersistedSnapshot();
    const logCalls = [];

    let streamController = null;
    function emitSseEvent(eventObj) {
      if (!streamController) return;
      const sseText = `event: event\ndata: ${JSON.stringify(eventObj)}\n\n`;
      streamController.enqueue(new TextEncoder().encode(sseText));
    }

    globalThis.fetch = async (input, init = {}) => {
      const url = String(input);
      const method = (init.method || "GET").toUpperCase();

      if (url.includes("/api/auth/session")) {
        return jsonResponse(developerSession());
      }
      if (url.includes("/v1/projects/") && url.includes("/environments")) {
        return jsonResponse({
          environments: [{ id: "env-1", name: "staging" }],
        });
      }
      if (url.includes("/v1/projects")) {
        return jsonResponse({
          projects: [{ id: "p1", name: "Enterprise Proj" }],
        });
      }
      if (url.includes("/v1/runs?")) {
        return jsonResponse({
          items: [
            {
              id: snapshot.id,
              workflowName: snapshot.workflowName,
              status: snapshot.status,
              createdAt: snapshot.createdAt,
            },
          ],
        });
      }
      if (url.includes(`/v1/runs/${snapshot.id}/events`)) {
        return jsonResponse({
          events: [
            {
              id: "ev-1",
              type: "run.started",
              sequence: 1,
              committedAt: "2026-09-24T10:00:00Z",
              payload: {},
            },
            {
              id: "ev-2",
              type: "step.succeeded",
              sequence: 2,
              committedAt: "2026-09-24T10:00:07Z",
              payload: { stepId: "step-0", nodeId: "fetch-account" },
            },
          ],
          hasMore: false,
          nextCursor: null,
        });
      }
      if (url.includes(`/v1/runs/${snapshot.id}/logs`)) {
        const u = new URL(url, "http://localhost");
        const stepId = u.searchParams.get("stepId");
        const cursor = u.searchParams.get("cursor");
        logCalls.push({ stepId, cursor });
        return jsonResponse(generateStepLogs(stepId || "global", cursor));
      }
      if (url.includes(`/v1/runs/${snapshot.id}/stream`)) {
        const stream = new ReadableStream({
          start(controller) {
            streamController = controller;
          },
        });
        return new Response(stream, {
          headers: {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
          },
        });
      }
      if (url.includes(`/v1/runs/${snapshot.id}`)) {
        return jsonResponse(snapshot);
      }
      throw new Error(`unexpected fetch: ${method} ${url}`);
    };

    // Load compiled production dashboard
    await import("../dist/index.js?t=" + Date.now());
    dom.fireDomContentLoaded();
    await settle();

    // 1. Enter the Inspector through the real runs list link
    const runsTable = globalThis.document.getElementById("runs-table-body");
    assert.ok(runsTable, "Runs table body must exist");
    const inspectLinks = runsTable.querySelectorAll(".inspect-link");
    assert.equal(
      inspectLinks.length,
      1,
      "Inspect link must be rendered in runs list",
    );
    inspectLinks[0].click();
    await settle();

    const inspectorContent =
      globalThis.document.getElementById("inspector-content");
    assert.ok(inspectorContent, "Inspector content must be mounted");

    // --- Invariant 1: 1 Node per Logical Step & Choice/Merge Graph Integrity ---
    const dagNodes = inspectorContent.querySelectorAll(".dag-node");
    assert.equal(
      dagNodes.length,
      200,
      "DAG must render exactly 1 node per logical step for 200 steps",
    );

    // Verify choice node and skipped branches
    const skippedEdges = inspectorContent.querySelectorAll(".edge-skipped");
    assert.ok(
      skippedEdges.length > 0,
      "Skipped branches must render with dashed .edge-skipped class",
    );

    // Verify retry attempts stay contained in step detail
    const step0Node = inspectorContent.querySelector(
      '.dag-node[data-step-id="step-0"]',
    );
    assert.ok(step0Node, "step-0 node must be in graph");
    step0Node.click();
    await settle();

    // Verify step detail Attempts tab button shows (2)
    const attemptsTabBtn = inspectorContent.querySelector("#step-tab-attempts");
    assert.ok(attemptsTabBtn, "Attempts tab must exist");
    assert.ok(
      attemptsTabBtn.innerHTML.includes("(2)"),
      "Attempts tab must indicate 2 attempts without spawning new graph nodes",
    );

    // --- Invariant 2: Graph Scroll & Minimap Sync Across Live Rerender ---
    const initialGraphScroll =
      inspectorContent.querySelector("#graph-scroll-area");
    assert.ok(initialGraphScroll, "Graph scroll area must exist");

    // Simulate scrolling the graph
    initialGraphScroll.scrollLeft = 140;
    initialGraphScroll.scrollTop = 90;
    initialGraphScroll._dispatch("scroll", {});

    const initialMinimapVp =
      inspectorContent.querySelector("#minimap-viewport");
    assert.ok(initialMinimapVp, "Minimap viewport must exist");
    const initialVpX = initialMinimapVp.getAttribute("x");
    const initialVpY = initialMinimapVp.getAttribute("y");
    assert.ok(
      Number(initialVpX) > 0,
      "Minimap viewport X must be > 0 after scrolling",
    );
    assert.ok(
      Number(initialVpY) > 0,
      "Minimap viewport Y must be > 0 after scrolling",
    );

    // Trigger a live SSE event rerender while scrolled
    emitSseEvent({
      id: "ev-205",
      type: "step.ready",
      sequence: 205,
      committedAt: "2026-09-24T10:00:10Z",
      payload: { stepId: "step-199", nodeId: "merge-join" },
    });
    await settle();

    // Query fresh graph scroll area and minimap viewport in the newly rendered DOM
    const rerenderedGraphScroll =
      inspectorContent.querySelector("#graph-scroll-area");
    assert.ok(rerenderedGraphScroll, "Graph scroll area must exist in new DOM");
    assert.notEqual(
      rerenderedGraphScroll,
      initialGraphScroll,
      "Live rerender must mount a fresh graph scroll container in the new DOM",
    );
    assert.equal(
      rerenderedGraphScroll.scrollLeft,
      140,
      "Graph scrollLeft must be preserved and restored across live rerender",
    );
    assert.equal(
      rerenderedGraphScroll.scrollTop,
      90,
      "Graph scrollTop must be preserved and restored across live rerender",
    );

    const rerenderedMinimapVp =
      inspectorContent.querySelector("#minimap-viewport");
    assert.ok(rerenderedMinimapVp, "Minimap viewport must exist in new DOM");
    assert.notEqual(
      rerenderedMinimapVp,
      initialMinimapVp,
      "Live rerender must mount a fresh minimap viewport rect",
    );
    assert.equal(
      rerenderedMinimapVp.getAttribute("x"),
      initialVpX,
      "Minimap viewport X must remain synchronized after live rerender",
    );
    assert.equal(
      rerenderedMinimapVp.getAttribute("y"),
      initialVpY,
      "Minimap viewport Y must remain synchronized after live rerender",
    );

    // --- Invariant 3: Accessible List Virtualization & Physical Scroll Preservation ---
    const listBtn = inspectorContent.querySelector("#view-mode-list-btn");
    assert.ok(listBtn, "Accessible list view button must exist");
    listBtn.click();
    await settle();

    const initialLsa = inspectorContent.querySelector("#list-scroll-area");
    assert.ok(initialLsa, "List scroll area must exist for 200 steps");

    // Only bounded cards are mounted in the DOM (<= 50)
    const mountedStepCards = initialLsa.querySelectorAll(
      ".accessible-step-card",
    );
    assert.ok(
      mountedStepCards.length <= 50,
      `DOM must mount at most 50 step cards, got ${mountedStepCards.length}`,
    );

    // Simulate scrolling list to step 150 (physical scroll position 150 * 140 = 21000px)
    initialLsa.scrollTop = 21000;
    initialLsa._dispatch("scroll", {});
    await settle(60); // wait for debounce

    // Query the FRESH list scroll area mounted after the scroll rerender
    const scrolledLsa = inspectorContent.querySelector("#list-scroll-area");
    assert.ok(
      scrolledLsa,
      "Fresh list scroll area must exist after scroll rerender",
    );
    assert.notEqual(
      scrolledLsa,
      initialLsa,
      "Scroll virtualization rerender must mount a new DOM container",
    );

    // Verify physical scroll offset on the NEW DOM element was NOT reset to 0!
    assert.equal(
      scrolledLsa.scrollTop,
      21000,
      "Newly mounted virtual list container must preserve physical scrollTop from listScrollTop, not reset to 0",
    );

    // Verify step 150 is now in the rendered DOM window
    assert.ok(
      inspectorContent.innerHTML.includes("fanout-task-150"),
      "Step 150 must be mounted after scrolling to offset 150",
    );

    // Verify still at most 50 cards mounted
    const mountedAfterScroll = scrolledLsa.querySelectorAll(
      ".accessible-step-card",
    );
    assert.ok(
      mountedAfterScroll.length <= 50,
      `DOM must maintain <= 50 cards mounted after scrolling, got ${mountedAfterScroll.length}`,
    );

    // Test "Load More Steps" button on the FRESH container
    const loadMoreStepsBtn = scrolledLsa.querySelector("#load-more-steps-btn");
    if (loadMoreStepsBtn) {
      loadMoreStepsBtn.click();
      await settle();
      const lsaAfterLoadMore =
        inspectorContent.querySelector("#list-scroll-area");
      assert.ok(
        lsaAfterLoadMore,
        "List scroll area must exist after Load More",
      );
      assert.notEqual(
        lsaAfterLoadMore,
        scrolledLsa,
        "Load More must mount a fresh container in the new DOM",
      );
      assert.ok(
        lsaAfterLoadMore.scrollTop > 0,
        "Load More Steps must preserve physical scroll offset on the fresh element, not reset to 0",
      );
    }

    // --- Invariant 4: Selected-Step Bounded Logs & Coherent Multi-Page Pagination ---
    const logsTabBtn = inspectorContent.querySelector("#step-tab-logs");
    assert.ok(logsTabBtn, "Step Logs tab button must exist");
    logsTabBtn.click();
    await settle();

    const stepLogsTerminal = inspectorContent.querySelector(".log-terminal");
    assert.ok(stepLogsTerminal, "Step task logs terminal must be mounted");

    const initialLogLinesElements =
      inspectorContent.querySelectorAll(".log-line");
    const initialLogLines = initialLogLinesElements.length;
    assert.equal(
      initialLogLines,
      50,
      `Selected step logs must be bounded to 50 lines, got ${initialLogLines}`,
    );

    // Verify Load More Step Logs button is present
    const loadMoreStepLogsBtn = inspectorContent.querySelector(
      "#load-more-step-logs-btn",
    );
    assert.ok(
      loadMoreStepLogsBtn,
      "Load More Step Logs button must exist for paginated logs",
    );

    // Click Load More Step Logs to advance to page 2
    loadMoreStepLogsBtn.click();
    await settle();

    // Verify the second log fetch request retained the exact same stepId
    assert.ok(logCalls.length >= 3, "Must issue second log request for page 2");
    assert.equal(
      logCalls[logCalls.length - 1].stepId,
      "step-0",
      "Page 2 request must retain the exact same stepId",
    );
    assert.equal(
      logCalls[logCalls.length - 1].cursor,
      "cursor-page-2",
      "Page 2 request must send the server nextCursor",
    );

    // Verify rendered logs window remains bounded to 50 lines (not unbounded 100 lines!)
    const pagedLogLines = inspectorContent.querySelectorAll(".log-line").length;
    assert.equal(
      pagedLogLines,
      50,
      `Step logs must remain bounded to 50 items across pagination, got ${pagedLogLines}`,
    );

    // --- Invariant 5: Keyboard Navigation & Live-Rerender Focus Preservation ---
    const graphModeBtn = inspectorContent.querySelector("#view-mode-graph-btn");
    const listModeBtn = inspectorContent.querySelector("#view-mode-list-btn");
    assert.ok(graphModeBtn && listModeBtn, "View buttons must exist");

    // Focus graph mode button
    graphModeBtn.focus();
    assert.equal(
      globalThis.document.activeElement,
      graphModeBtn,
      "Graph mode button must have focus",
    );

    // Press ArrowRight to navigate roving tabindex to List button
    graphModeBtn._dispatch("keydown", { key: "ArrowRight" });
    await settle();

    // The newly rendered list button must have received focus in the NEW DOM
    const activeBtn = globalThis.document.activeElement;
    assert.equal(
      activeBtn.id,
      "view-mode-list-btn",
      "ArrowRight must move focus to view-mode-list-btn in the new DOM",
    );
    assert.equal(
      activeBtn.tabIndex,
      0,
      "Active tab in roving tabindex must have tabIndex = 0",
    );

    // Exercise live SSE stream rerender focus preservation:
    // Focus a step tab button (e.g. Attempts tab)
    const stepTabAttempts =
      inspectorContent.querySelector("#step-tab-attempts");
    assert.ok(stepTabAttempts, "step-tab-attempts must exist");
    stepTabAttempts.focus();
    assert.equal(
      globalThis.document.activeElement,
      stepTabAttempts,
      "step-tab-attempts must have active focus prior to SSE rerender",
    );
    const focusedBeforeSse = globalThis.document.activeElement;

    // Emit live SSE event that updates execution status (step.succeeded)
    emitSseEvent({
      id: "ev-206",
      type: "step.succeeded",
      sequence: 206,
      committedAt: "2026-09-24T10:00:25Z",
      payload: { stepId: "step-199", nodeId: "merge-join" },
    });
    await settle();

    // The live SSE update triggers renderSnapshot, destroying old DOM elements.
    // Verify focus was preserved on the matching element in the FRESH DOM:
    const focusedAfterSse = globalThis.document.activeElement;
    assert.ok(
      focusedAfterSse,
      "An element must retain focus after live SSE rerender",
    );
    assert.notEqual(
      focusedAfterSse,
      focusedBeforeSse,
      "Focus must NOT remain on old detached DOM node; must be restored to fresh DOM element",
    );
    assert.equal(
      focusedAfterSse.id,
      "step-tab-attempts",
      "Active element ID must match the element focused prior to SSE update",
    );
  });
});
