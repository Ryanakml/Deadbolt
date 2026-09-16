export * from "./types.js";
export * from "./stream.js";
export * from "./inspector.js";
export * from "./api.js";

import { DashboardApiClient } from "./api.js";
import { RunInspector } from "./inspector.js";
import { RunSnapshot, StreamFreshness, TaskLogsResponse } from "./types.js";

// DOM Bootstrap for browser runtime
if (typeof document !== "undefined") {
  document.addEventListener("DOMContentLoaded", () => {
    initDashboard();
  });
}

function initDashboard(): void {
  const api = new DashboardApiClient();
  let currentEnv = "staging";
  let activeInspector: RunInspector | null = null;

  const envSelect = document.getElementById(
    "env-select",
  ) as HTMLSelectElement | null;
  const themeToggle = document.getElementById(
    "theme-toggle",
  ) as HTMLButtonElement | null;
  const runsNavBtn = document.getElementById(
    "nav-runs",
  ) as HTMLButtonElement | null;
  const workersNavBtn = document.getElementById(
    "nav-workers",
  ) as HTMLButtonElement | null;

  if (envSelect) {
    currentEnv = envSelect.value || "staging";
    envSelect.addEventListener("change", () => {
      currentEnv = envSelect.value;
      loadRunsList(api, currentEnv);
    });
  }

  if (themeToggle) {
    themeToggle.addEventListener("click", () => {
      const isDark = document.documentElement.classList.toggle("dark");
      localStorage.setItem("theme", isDark ? "dark" : "light");
      themeToggle.setAttribute("aria-pressed", isDark ? "true" : "false");
    });
  }

  if (runsNavBtn) {
    runsNavBtn.addEventListener("click", () => {
      showView("runs-view");
      loadRunsList(api, currentEnv);
    });
  }

  if (workersNavBtn) {
    workersNavBtn.addEventListener("click", () => {
      showView("workers-view");
      loadWorkersList(api, currentEnv);
    });
  }

  // Check URL params for deep linking to /runs/:id
  const urlParams = new URLSearchParams(window.location.search);
  const runIdParam = urlParams.get("runId");
  if (runIdParam) {
    inspectRun(runIdParam);
  } else {
    loadRunsList(api, currentEnv);
  }

  function showView(viewId: string): void {
    if (activeInspector) {
      activeInspector.destroy();
      activeInspector = null;
    }
    const views = document.querySelectorAll(".view-panel");
    views.forEach((v) => v.classList.add("hidden"));
    const target = document.getElementById(viewId);
    if (target) {
      target.classList.remove("hidden");
    }
  }

  async function loadRunsList(
    client: DashboardApiClient,
    env: string,
  ): Promise<void> {
    const listContainer = document.getElementById("runs-table-body");
    if (!listContainer) return;
    listContainer.innerHTML =
      '<tr><td colspan="5" class="loading">Loading workflow runs...</td></tr>';

    try {
      const resp = await client.listRuns(env);
      if (resp.items.length === 0) {
        listContainer.innerHTML =
          '<tr><td colspan="5" class="empty-state">No workflow runs found in this environment.</td></tr>';
        return;
      }

      listContainer.innerHTML = "";
      for (const run of resp.items) {
        const row = document.createElement("tr");
        row.innerHTML = `
          <td><button class="link-btn inspect-link" data-run-id="${run.id}">${run.id}</button></td>
          <td>${escapeHtml(run.workflowName)}</td>
          <td><span class="badge status-${run.status.toLowerCase()}">${run.status}</span></td>
          <td>${run.reasonCode ? escapeHtml(run.reasonCode) : "-"}</td>
          <td>${new Date(run.createdAt).toLocaleString()}</td>
        `;
        listContainer.appendChild(row);
      }

      listContainer.querySelectorAll(".inspect-link").forEach((btn) => {
        btn.addEventListener("click", (e) => {
          const id = (e.currentTarget as HTMLElement).getAttribute(
            "data-run-id",
          );
          if (id) inspectRun(id);
        });
      });
    } catch (err: unknown) {
      listContainer.innerHTML = `<tr><td colspan="5" class="error-state">Failed to load runs: ${escapeHtml(err instanceof Error ? err.message : String(err))}</td></tr>`;
    }
  }

  async function loadWorkersList(
    client: DashboardApiClient,
    env: string,
  ): Promise<void> {
    const listContainer = document.getElementById("workers-table-body");
    if (!listContainer) return;
    listContainer.innerHTML =
      '<tr><td colspan="4" class="loading">Loading workers...</td></tr>';

    try {
      const resp = await client.listWorkers(env);
      if (resp.items.length === 0) {
        listContainer.innerHTML =
          '<tr><td colspan="4" class="empty-state">No workers registered for this environment.</td></tr>';
        return;
      }

      listContainer.innerHTML = "";
      for (const worker of resp.items) {
        const row = document.createElement("tr");
        const digests =
          worker.deploymentDigests.length > 0
            ? worker.deploymentDigests
                .map(
                  (d) =>
                    `<code class="digest-tag">${escapeHtml(d.slice(0, 16))}...</code>`,
                )
                .join(" ")
            : '<span class="text-muted">(none)</span>';
        row.innerHTML = `
          <td><code>${worker.id}</code></td>
          <td>${escapeHtml(worker.pool)}</td>
          <td><span class="badge status-${worker.status.toLowerCase()}">${worker.status}</span></td>
          <td>${digests}</td>
        `;
        listContainer.appendChild(row);
      }
    } catch (err: unknown) {
      listContainer.innerHTML = `<tr><td colspan="4" class="error-state">Failed to load workers: ${escapeHtml(err instanceof Error ? err.message : String(err))}</td></tr>`;
    }
  }

  function inspectRun(runId: string): void {
    showView("inspector-view");
    const container = document.getElementById("inspector-content");
    if (!container) return;

    container.innerHTML = '<div class="loading">Loading Run Inspector...</div>';

    if (activeInspector) {
      activeInspector.destroy();
    }

    activeInspector = new RunInspector(runId);
    activeInspector.subscribe({
      onSnapshotUpdated: (snapshot) => renderSnapshot(snapshot),
      onFreshnessChanged: (freshness) => renderFreshness(freshness),
      onLogsUpdated: (logs, err) => renderLogs(logs, err),
      onError: (err) => renderError(err),
    });

    activeInspector.load();
  }

  function renderSnapshot(snap: RunSnapshot): void {
    const container = document.getElementById("inspector-content");
    if (!container) return;

    const stepsHtml = snap.steps
      .map((st) => {
        const attemptsHtml = st.attempts
          .map((att) => {
            const started = att.startedAt
              ? new Date(att.startedAt).toLocaleTimeString()
              : "-";
            return `
          <div class="attempt-card status-${att.status.toLowerCase()}">
            <div class="attempt-header">
              <span class="attempt-title">Attempt #${att.attemptNumber}</span>
              <span class="badge status-${att.status.toLowerCase()}">${att.status}</span>
            </div>
            <div class="attempt-details">
              <span>Session: <code>${att.workerSessionId ? att.workerSessionId.slice(0, 8) + "..." : "-"}</code></span>
              <span>Started: ${started}</span>
              <span>Epoch: ${att.ownershipEpoch ?? "-"}</span>
            </div>
          </div>
        `;
          })
          .join("");

        return `
        <div class="step-card" data-step-id="${st.id}">
          <div class="step-header">
            <h4>${escapeHtml(st.nodeId)}</h4>
            <span class="badge status-${st.status.toLowerCase()}">${st.status}</span>
          </div>
          <div class="attempts-container">
            ${attemptsHtml || '<div class="no-attempts text-muted">No attempts claimed yet</div>'}
          </div>
        </div>
      `;
      })
      .join("");

    container.innerHTML = `
      <div class="inspector-header">
        <div class="run-title-group">
          <h2>${escapeHtml(snap.workflowName)}</h2>
          <span class="run-id-label">ID: <code>${snap.id}</code></span>
        </div>
        <div class="run-badges">
          <span class="badge status-${snap.status.toLowerCase()}">${snap.status}</span>
          <span id="stream-freshness-badge" class="badge freshness-badge freshness-disconnected">DISCONNECTED</span>
        </div>
      </div>

      <div class="meta-grid">
        <div class="meta-item"><label>Revision</label><div>${snap.revision}</div></div>
        <div class="meta-item"><label>Last Event Seq</label><div>${snap.lastEventSequence}</div></div>
        <div class="meta-item"><label>Deployment ID</label><div><code>${snap.deploymentId.slice(0, 8)}...</code></div></div>
        <div class="meta-item"><label>Created At</label><div>${new Date(snap.createdAt).toLocaleString()}</div></div>
        ${snap.reasonCode ? `<div class="meta-item"><label>Reason</label><div>${escapeHtml(snap.reasonCode)}</div></div>` : ""}
      </div>

      <section class="steps-section">
        <h3>Execution Graph & Attempts</h3>
        <div class="steps-grid">${stepsHtml}</div>
      </section>

      <section class="logs-section">
        <h3>Task Diagnostic Logs</h3>
        <div id="logs-container" class="logs-container">
          <div class="loading">Loading logs...</div>
        </div>
      </section>

      ${
        snap.output
          ? `
        <section class="output-section">
          <h3>Workflow Output</h3>
          <pre class="code-block">${escapeHtml(JSON.stringify(snap.output, null, 2))}</pre>
        </section>
      `
          : ""
      }

      ${
        snap.error
          ? `
        <section class="error-section">
          <h3>Workflow Error</h3>
          <pre class="code-block error-text">${escapeHtml(JSON.stringify(snap.error, null, 2))}</pre>
        </section>
      `
          : ""
      }
    `;

    // Re-render freshness if already set
    if (activeInspector) {
      renderFreshness(activeInspector.getSnapshot() ? "LIVE" : "DISCONNECTED");
    }
  }

  function renderFreshness(f: StreamFreshness): void {
    const badge = document.getElementById("stream-freshness-badge");
    if (!badge) return;

    badge.className = `badge freshness-badge freshness-${f.toLowerCase()}`;
    badge.textContent = f;
    badge.setAttribute("aria-label", `Stream status: ${f}`);
  }

  function renderLogs(logs: TaskLogsResponse | null, error?: string): void {
    const container = document.getElementById("logs-container");
    if (!container) return;

    if (error) {
      container.innerHTML = `<div class="logs-notice logs-restricted">${escapeHtml(error)}</div>`;
      return;
    }

    if (!logs) {
      container.innerHTML = '<div class="text-muted">No logs available.</div>';
      return;
    }

    if (logs.expired) {
      container.innerHTML = `<div class="logs-notice logs-expired">${escapeHtml(logs.message ?? "Logs expired due to retention policy.")}</div>`;
      return;
    }

    if (logs.items.length === 0) {
      container.innerHTML =
        '<div class="text-muted">No logs recorded for this execution.</div>';
      return;
    }

    const logLines = logs.items
      .map((item) => {
        const time = new Date(item.timestamp).toLocaleTimeString();
        return `<div class="log-line log-${item.level}"><span class="log-time">${time}</span> <span class="log-level">[${item.level.toUpperCase()}]</span> <span class="log-msg">${escapeHtml(item.message)}</span></div>`;
      })
      .join("");

    container.innerHTML = `<div class="log-terminal">${logLines}</div>`;
  }

  function renderError(err: Error): void {
    const banner = document.getElementById("global-error-banner");
    if (banner) {
      banner.textContent = err.message;
      banner.classList.remove("hidden");
    }
  }

  function escapeHtml(str: string): string {
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
  }
}
