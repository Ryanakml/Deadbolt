export * from "./types.js";
export * from "./stream.js";
export * from "./inspector.js";
export * from "./api.js";
export * from "./permissions.js";
import { DashboardApiClient, isUnauthorized, isConflict, newIdempotencyKey, } from "./api.js";
import { createEnvironmentSelection, formatEnvironmentLabel, getSelectedEnvironmentId, selectEnvironment, setSelectionCatalog, setSelectionOrganization, } from "./api.js";
import { resolveOrgState } from "./auth.js";
import { sessionCanControlRuns, sessionCanDecideApprovals, sessionCanManageSchedules, visibleRunControls, } from "./permissions.js";
import { RunInspector } from "./inspector.js";
import { shouldShowWorkerWait, terminalStepEmptyText, openCaseForStep, reconciliationHoldText, terminationBannerText, RESOLVE_ACTIONS, getStatusPresentation, computeGraphLayout, computeMinimap, filterEventsForStep, virtualizeItems, getBoundedEvents, } from "./inspector.js";
import { clearStreamErrorOnLive, createStreamErrorBanner, markGlobalError, markStreamError, } from "./stream.js";
import { applyDashboardTheme, resolveInitialTheme } from "./theme.js";
// DOM Bootstrap for browser runtime
if (typeof document !== "undefined") {
    document.addEventListener("DOMContentLoaded", () => {
        initDashboard();
    });
}
function initDashboard() {
    const api = new DashboardApiClient();
    // Canonical environment selection for the active organization. The
    // selector value is always the environment UUID, never a bare name.
    const envSelection = createEnvironmentSelection();
    let activeInspector = null;
    // Whether the active session may invoke run mutations (pause/resume/
    // cancel, all runs:control). Derived once from the BFF session membership
    // in enterApp; the backend remains the security authority. Mutation CTAs
    // render only when state AND this flag both allow them (§23.2).
    let canControlRuns = false;
    let canDecideApprovals = false;
    // Whether the active session may manage schedules (schedules:write).
    // Derived once from the BFF session membership in enterApp; the backend
    // remains the security authority. Schedule mutation CTAs render only when
    // this flag allows them. It must not be inferred from runs:control:
    // developer holds runs:control but never schedules:write.
    let canManageSchedules = false;
    // Active view id so the environment selector can refresh the schedules
    // list only when the schedules view is showing. Unconditional refreshes
    // would issue protected requests the current view never asked for.
    let activeViewId = "runs-view";
    // Scoped banner state: only a transient stream error may be cleared on
    // SSE reconnect. Bootstrap/API errors stay visible.
    const streamBanner = createStreamErrorBanner();
    let currentStreamFreshness = "DISCONNECTED";
    let currentViewMode = "graph";
    let selectedStepId = null;
    let selectedStepTab = "summary";
    const collapsedClusters = new Set();
    const cachedStepLogs = new Map();
    let lastSnap = null;
    let listScrollIndex = 0;
    const LIST_PAGE_SIZE = 50;
    let eventsWindowStart = 0;
    const EVENTS_PAGE_SIZE = 50;
    let logsWindowStart = 0;
    const LOGS_PAGE_SIZE = 50;
    const envSelect = document.getElementById("env-select");
    const themeToggle = document.getElementById("theme-toggle");
    const runsNavBtn = document.getElementById("nav-runs");
    const workersNavBtn = document.getElementById("nav-workers");
    const schedulesNavBtn = document.getElementById("nav-schedules");
    if (envSelect) {
        envSelect.addEventListener("change", () => {
            selectEnvironment(envSelection, envSelect.value || null);
            loadRunsList(api, getSelectedEnvironmentId(envSelection));
            if (activeViewId === "schedules-view") {
                loadSchedulesList(api, getSelectedEnvironmentId(envSelection));
            }
        });
    }
    if (themeToggle) {
        const themeStorage = typeof localStorage !== "undefined" ? localStorage : null;
        const storedTheme = themeStorage?.getItem("theme") ?? null;
        const prefersDark = typeof window.matchMedia === "function" &&
            window.matchMedia("(prefers-color-scheme: dark)").matches;
        applyDashboardTheme(document.documentElement, themeToggle, resolveInitialTheme(storedTheme, prefersDark));
        themeToggle.addEventListener("click", () => {
            const theme = document.documentElement.classList.contains("dark")
                ? "light"
                : "dark";
            applyDashboardTheme(document.documentElement, themeToggle, theme);
            themeStorage?.setItem("theme", theme);
        });
    }
    if (runsNavBtn) {
        runsNavBtn.addEventListener("click", () => {
            showView("runs-view");
            loadRunsList(api, getSelectedEnvironmentId(envSelection));
        });
    }
    if (workersNavBtn) {
        workersNavBtn.addEventListener("click", () => {
            showView("workers-view");
            loadWorkersList(api, getSelectedEnvironmentId(envSelection));
        });
    }
    if (schedulesNavBtn) {
        schedulesNavBtn.addEventListener("click", () => {
            showView("schedules-view");
            loadSchedulesList(api, getSelectedEnvironmentId(envSelection));
        });
    }
    const schedulesNewBtn = document.getElementById("schedules-new-btn");
    if (schedulesNewBtn) {
        schedulesNewBtn.addEventListener("click", (e) => {
            const envId = getSelectedEnvironmentId(envSelection);
            if (!envId)
                return;
            openScheduleCreateDialog(api, envId, e.currentTarget);
        });
    }
    // Check URL params for deep linking to /runs/:id
    const urlParams = new URLSearchParams(window.location.search);
    const runIdParam = urlParams.get("runId");
    // Auth bootstrap gate: Issue #14 data loading starts only after the BFF
    // session (and a valid organization context) is established. While
    // unauthenticated, no protected API is called.
    void bootstrap();
    async function bootstrap() {
        let session;
        try {
            session = await api.getSession();
        }
        catch (err) {
            renderError(err instanceof Error ? err : new Error(String(err)));
            return;
        }
        await enterWithSession(session);
    }
    async function enterWithSession(session) {
        if (!session) {
            clearEnvironmentState();
            showAuthRequired("Sign in to view workflow runs.");
            return;
        }
        const state = resolveOrgState(session);
        if (state.kind === "ready") {
            await enterApp(session);
            return;
        }
        if (state.kind === "empty") {
            clearEnvironmentState();
            showAuthRequired("This identity has no organization yet. Create one with `runtime bootstrap`, then reload.");
            return;
        }
        if (state.kind === "select") {
            clearEnvironmentState();
            showOrgSelect(state.orgs);
            return;
        }
        // Exactly one membership: establish it deterministically, then enter.
        try {
            clearEnvironmentState();
            await api.switchOrganization(state.orgId);
            await enterWithSession(await api.getSession());
        }
        catch (err) {
            renderError(err instanceof Error ? err : new Error(String(err)));
        }
    }
    async function enterApp(session) {
        hideAuthView();
        const label = document.getElementById("session-label");
        if (label) {
            label.textContent = session.user?.email ?? "";
            label.classList.remove("hidden");
        }
        // Bind the catalog to the newly established organization. Any selection
        // from a previous org is cleared first so it can never be reused.
        const orgId = session.active_organization_id ?? null;
        // Authoritative frontend permission data for mutation CTAs: the BFF
        // session membership decides runs:control, never button clicks or HTTP
        // failures.
        canControlRuns = sessionCanControlRuns(session, orgId);
        // Approvals are a separate capability: §24.2 withholds approvals:decide
        // from developer, so it must not be inferred from runs:control.
        canDecideApprovals = sessionCanDecideApprovals(session, orgId);
        // Schedules are a separate capability: schedules:write is granted to
        // operator, admin, and owner and withheld from developer, so it must not
        // be inferred from runs:control either.
        canManageSchedules = sessionCanManageSchedules(session, orgId);
        await loadEnvironmentCatalog(orgId);
        if (runIdParam) {
            inspectRun(runIdParam);
        }
        else {
            showView("runs-view");
            loadRunsList(api, getSelectedEnvironmentId(envSelection));
        }
    }
    function clearEnvironmentState() {
        setSelectionOrganization(envSelection, null);
        const sel = document.getElementById("env-select");
        if (sel) {
            sel.innerHTML = "";
            const opt = document.createElement("option");
            opt.value = "";
            opt.textContent = "Loading environments…";
            sel.appendChild(opt);
            sel.disabled = true;
        }
    }
    function populateEnvironmentSelector(catalog) {
        const sel = document.getElementById("env-select");
        if (!sel)
            return;
        sel.innerHTML = "";
        if (catalog.length === 0) {
            const opt = document.createElement("option");
            opt.value = "";
            opt.textContent = "No environments yet";
            sel.appendChild(opt);
            sel.disabled = true;
            return;
        }
        for (const env of catalog) {
            const opt = document.createElement("option");
            // Canonical environment UUID is the only value runs/workers use.
            opt.value = env.environmentId;
            opt.textContent = formatEnvironmentLabel(env);
            sel.appendChild(opt);
        }
        sel.disabled = false;
        const selected = getSelectedEnvironmentId(envSelection);
        if (selected)
            sel.value = selected;
    }
    function renderEnvironmentEmptyState() {
        const runsBody = document.getElementById("runs-table-body");
        if (runsBody) {
            runsBody.innerHTML =
                '<tr><td colspan="5" class="empty-state">No environments yet. Create a project environment with `runtime bootstrap`, then reload.</td></tr>';
        }
        const workersBody = document.getElementById("workers-table-body");
        if (workersBody) {
            workersBody.innerHTML =
                '<tr><td colspan="4" class="empty-state">No environments yet. Create a project environment with `runtime bootstrap`, then reload.</td></tr>';
        }
        const schedulesList = document.getElementById("schedules-list");
        if (schedulesList) {
            schedulesList.innerHTML =
                '<div class="empty-state">No environments yet. Create a project environment with `runtime bootstrap`, then reload.</div>';
        }
    }
    async function loadEnvironmentCatalog(orgId) {
        // Switching organizations clears stale catalog/selection first.
        setSelectionOrganization(envSelection, orgId);
        const loading = document.getElementById("env-select");
        if (loading) {
            loading.innerHTML = "";
            const opt = document.createElement("option");
            opt.value = "";
            opt.textContent = "Loading environments…";
            loading.appendChild(opt);
            loading.disabled = true;
        }
        let catalog;
        try {
            catalog = await api.loadEnvironmentCatalog();
        }
        catch (err) {
            const sel = document.getElementById("env-select");
            if (sel) {
                sel.innerHTML = "";
                const opt = document.createElement("option");
                opt.value = "";
                opt.textContent = "Failed to load environments";
                sel.appendChild(opt);
                sel.disabled = true;
            }
            renderError(err instanceof Error ? err : new Error(String(err)));
            return;
        }
        setSelectionCatalog(envSelection, catalog);
        populateEnvironmentSelector(catalog);
        if (catalog.length === 0) {
            renderEnvironmentEmptyState();
        }
    }
    function showAuthRequired(message) {
        teardownAppViews();
        const msg = document.getElementById("auth-message");
        if (msg)
            msg.textContent = message;
        const login = document.getElementById("login-link");
        if (login)
            login.classList.remove("hidden");
        const orgWrap = document.getElementById("org-select-wrap");
        if (orgWrap)
            orgWrap.classList.add("hidden");
        showView("auth-view");
    }
    function hideAuthView() {
        const login = document.getElementById("login-link");
        if (login)
            login.classList.add("hidden");
        const orgWrap = document.getElementById("org-select-wrap");
        if (orgWrap)
            orgWrap.classList.add("hidden");
    }
    function showOrgSelect(orgs) {
        teardownAppViews();
        const msg = document.getElementById("auth-message");
        if (msg)
            msg.textContent = "Select an organization to continue.";
        const login = document.getElementById("login-link");
        if (login)
            login.classList.add("hidden");
        const orgWrap = document.getElementById("org-select-wrap");
        const select = document.getElementById("org-select");
        const cont = document.getElementById("org-continue-btn");
        if (orgWrap && select && cont) {
            select.innerHTML = "";
            for (const o of orgs) {
                const opt = document.createElement("option");
                opt.value = o.OrganizationID;
                opt.textContent = `${o.OrganizationName} (${o.Role})`;
                select.appendChild(opt);
            }
            cont.onclick = () => {
                if (!select.value)
                    return;
                cont.textContent = "Switching...";
                // Drop previous-org environment state before establishing the new
                // org so its IDs can never be reused.
                clearEnvironmentState();
                teardownAppViews();
                api
                    .switchOrganization(select.value)
                    .then(() => api.getSession())
                    .then((s) => enterWithSession(s))
                    .catch((err) => renderError(err instanceof Error ? err : new Error(String(err))))
                    .finally(() => {
                    cont.textContent = "Continue";
                });
            };
            orgWrap.classList.remove("hidden");
        }
        showView("auth-view");
    }
    // teardownAppViews stops live streams and clears protected data so an
    // expired session neither leaks data nor invents workflow failure.
    function teardownAppViews() {
        if (activeInspector) {
            activeInspector.destroy();
            activeInspector = null;
        }
        // Drop mutation authority with the session so a signed-out or expired
        // identity can never retain visible mutation affordances.
        canControlRuns = false;
        canDecideApprovals = false;
        canManageSchedules = false;
        activeViewId = "runs-view";
        clearEnvironmentState();
        for (const id of ["runs-table-body", "workers-table-body"]) {
            const el = document.getElementById(id);
            if (el)
                el.innerHTML = "";
        }
        const schedList = document.getElementById("schedules-list");
        if (schedList)
            schedList.innerHTML = "";
        const insp = document.getElementById("inspector-content");
        if (insp)
            insp.innerHTML = "";
        const label = document.getElementById("session-label");
        if (label) {
            label.textContent = "";
            label.classList.add("hidden");
        }
    }
    function handleUnauthorized() {
        teardownAppViews();
        showAuthRequired("Session expired. Sign in again to continue.");
    }
    function showView(viewId) {
        if (activeInspector) {
            activeInspector.destroy();
            activeInspector = null;
        }
        activeViewId = viewId;
        const views = document.querySelectorAll(".view-panel");
        views.forEach((v) => v.classList.add("hidden"));
        const target = document.getElementById(viewId);
        if (target) {
            target.classList.remove("hidden");
        }
    }
    async function loadRunsList(client, envId) {
        const listContainer = document.getElementById("runs-table-body");
        if (!listContainer)
            return;
        // Zero-environment (or not-yet-loaded) state is honest and issues no
        // protected request with an ambiguous/empty environment value.
        if (!envId) {
            renderEnvironmentEmptyState();
            return;
        }
        listContainer.innerHTML =
            '<tr><td colspan="5" class="loading">Loading workflow runs...</td></tr>';
        try {
            const resp = await client.listRuns(envId);
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
                    const id = e.currentTarget.getAttribute("data-run-id");
                    if (id)
                        inspectRun(id);
                });
            });
        }
        catch (err) {
            if (isUnauthorized(err)) {
                handleUnauthorized();
                return;
            }
            listContainer.innerHTML = `<tr><td colspan="5" class="error-state">Failed to load runs: ${escapeHtml(err instanceof Error ? err.message : String(err))}</td></tr>`;
        }
    }
    async function loadWorkersList(client, envId) {
        const listContainer = document.getElementById("workers-table-body");
        if (!listContainer)
            return;
        if (!envId) {
            renderEnvironmentEmptyState();
            return;
        }
        listContainer.innerHTML =
            '<tr><td colspan="4" class="loading">Loading workers...</td></tr>';
        try {
            const resp = await client.listWorkers(envId);
            if (resp.items.length === 0) {
                listContainer.innerHTML =
                    '<tr><td colspan="4" class="empty-state">No workers registered for this environment.</td></tr>';
                return;
            }
            listContainer.innerHTML = "";
            for (const worker of resp.items) {
                const row = document.createElement("tr");
                const digests = worker.deploymentDigests.length > 0
                    ? worker.deploymentDigests
                        .map((d) => `<code class="digest-tag">${escapeHtml(d.slice(0, 16))}...</code>`)
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
        }
        catch (err) {
            if (isUnauthorized(err)) {
                handleUnauthorized();
                return;
            }
            listContainer.innerHTML = `<tr><td colspan="4" class="error-state">Failed to load workers: ${escapeHtml(err instanceof Error ? err.message : String(err))}</td></tr>`;
        }
    }
    async function loadSchedulesList(client, envId) {
        const listContainer = document.getElementById("schedules-list");
        const newBtn = document.getElementById("schedules-new-btn");
        if (newBtn) {
            if (canManageSchedules) {
                newBtn.classList.remove("hidden");
            }
            else {
                newBtn.classList.add("hidden");
            }
        }
        if (!listContainer)
            return;
        if (!envId) {
            renderEnvironmentEmptyState();
            return;
        }
        listContainer.innerHTML =
            '<div class="loading">Loading schedules...</div>';
        try {
            const items = await client.listSchedules(envId);
            if (items.length === 0) {
                listContainer.innerHTML =
                    '<div class="empty-state">No schedules found in this environment.</div>';
                return;
            }
            const cards = items
                .map((schedule) => {
                const pin = schedule.deploymentId && schedule.deploymentId.length > 0
                    ? `<code>${escapeHtml(schedule.deploymentId.slice(0, 8))}...</code>`
                    : "active-at-fire";
                const nextDue = formatScheduleNextDue(schedule);
                const pausedBadge = schedule.paused
                    ? '<span class="badge status-paused">PAUSED</span>'
                    : '<span class="badge status-active">ACTIVE</span>';
                const actions = canManageSchedules
                    ? `<div class="schedule-actions">
                <button id="schedule-edit-${escapeHtml(schedule.id)}" class="secondary-btn schedule-edit-btn" data-schedule-edit="${escapeHtml(schedule.id)}" data-schedule-id="${escapeHtml(schedule.id)}">Edit</button>
                ${schedule.paused ? `<button id="schedule-resume-${escapeHtml(schedule.id)}" class="primary-btn schedule-resume-btn" data-schedule-resume="${escapeHtml(schedule.id)}" data-schedule-id="${escapeHtml(schedule.id)}">Resume</button>` : `<button id="schedule-pause-${escapeHtml(schedule.id)}" class="secondary-btn schedule-pause-btn" data-schedule-pause="${escapeHtml(schedule.id)}" data-schedule-id="${escapeHtml(schedule.id)}">Pause</button>`}
                <button id="schedule-delete-${escapeHtml(schedule.id)}" class="danger-btn schedule-delete-btn" data-schedule-delete="${escapeHtml(schedule.id)}" data-schedule-id="${escapeHtml(schedule.id)}">Delete</button>
              </div>`
                    : "";
                return `
          <div class="hold-item schedule-item" data-schedule-item="${escapeHtml(schedule.id)}">
            <div class="schedule-main">
              <strong>${escapeHtml(schedule.workflow)}</strong>
              ${pausedBadge}
              <div class="text-muted schedule-meta">cron ${escapeHtml(schedule.cron)} · ${escapeHtml(schedule.timezone)} · revision ${schedule.revision}</div>
              <div class="text-muted schedule-meta">pin ${pin} · ${escapeHtml(schedule.overlapPolicy)} · ${escapeHtml(schedule.misfirePolicy)}</div>
              <div class="text-muted schedule-next-due">next due ${escapeHtml(nextDue)}</div>
              <div id="schedule-occurrences-${escapeHtml(schedule.id)}" class="schedule-occurrences" style="display:none"></div>
            </div>
            <div class="schedule-cta-row">
              <button id="schedule-history-${escapeHtml(schedule.id)}" class="link-btn schedule-history-btn" data-schedule-history="${escapeHtml(schedule.id)}" data-schedule-id="${escapeHtml(schedule.id)}">History</button>
              ${actions}
            </div>
          </div>`;
            })
                .join("");
            listContainer.innerHTML = cards;
            wireScheduleItemButtons(client, envId, listContainer, items);
        }
        catch (err) {
            if (isUnauthorized(err)) {
                handleUnauthorized();
                return;
            }
            const msg = err instanceof Error ? err.message : String(err);
            if (msg.includes("HTTP 403")) {
                listContainer.innerHTML =
                    '<div class="empty-state">Schedules require the schedules:write capability (operator role or above)</div>';
                if (newBtn)
                    newBtn.classList.add("hidden");
                return;
            }
            listContainer.innerHTML = `<div class="error-state">Failed to load schedules: ${escapeHtml(msg)}</div>`;
            if (newBtn)
                newBtn.classList.add("hidden");
        }
    }
    // formatScheduleNextDue renders the persisted pending slot as a countdown
    // at render time. A null slot (paused) renders as "paused" without
    // recomputing cron client-side.
    function formatScheduleNextDue(schedule) {
        if (schedule.paused || !schedule.nextDueAt)
            return "paused";
        const dueMs = new Date(schedule.nextDueAt).getTime();
        if (!Number.isFinite(dueMs))
            return "paused";
        const diffMs = dueMs - Date.now();
        const abs = new Date(schedule.nextDueAt).toLocaleString();
        if (diffMs <= 0)
            return `due now (${abs})`;
        const totalS = Math.floor(diffMs / 1000);
        const days = Math.floor(totalS / 86400);
        const hours = Math.floor((totalS % 86400) / 3600);
        const mins = Math.floor((totalS % 3600) / 60);
        const secs = totalS % 60;
        const parts = [];
        if (days > 0)
            parts.push(`${days}d`);
        if (hours > 0 || days > 0)
            parts.push(`${hours}h`);
        if (mins > 0 || hours > 0 || days > 0)
            parts.push(`${mins}m`);
        parts.push(`${secs}s`);
        return `in ${parts.join(" ")} (${abs})`;
    }
    function wireScheduleItemButtons(client, envId, root, items) {
        const byId = new Map(items.map((s) => [s.id, s]));
        const findSchedule = (el) => {
            if (!el)
                return undefined;
            const id = el.getAttribute?.("data-schedule-id") ??
                el.getAttribute?.("data-schedule-edit") ??
                el.getAttribute?.("data-schedule-pause") ??
                el.getAttribute?.("data-schedule-resume") ??
                el.getAttribute?.("data-schedule-delete") ??
                el.getAttribute?.("data-schedule-history");
            if (!id)
                return undefined;
            return byId.get(id);
        };
        for (const btn of Array.from(root.querySelectorAll?.(".schedule-edit-btn") ??
            [])) {
            btn.addEventListener("click", (e) => {
                const s = findSchedule(e.currentTarget);
                if (s)
                    openScheduleEditDialog(client, envId, s, e.currentTarget);
            });
        }
        for (const btn of Array.from(root.querySelectorAll?.(".schedule-pause-btn") ??
            [])) {
            btn.addEventListener("click", (e) => {
                const s = findSchedule(e.currentTarget);
                if (s)
                    openSchedulePauseDialog(client, envId, s, e.currentTarget);
            });
        }
        for (const btn of Array.from(root.querySelectorAll?.(".schedule-resume-btn") ??
            [])) {
            btn.addEventListener("click", (e) => {
                const s = findSchedule(e.currentTarget);
                if (s)
                    openScheduleResumeDialog(client, envId, s, e.currentTarget);
            });
        }
        for (const btn of Array.from(root.querySelectorAll?.(".schedule-delete-btn") ??
            [])) {
            btn.addEventListener("click", (e) => {
                const s = findSchedule(e.currentTarget);
                if (s)
                    openScheduleDeleteDialog(client, envId, s, e.currentTarget);
            });
        }
        for (const btn of Array.from(root.querySelectorAll?.(".schedule-history-btn") ??
            [])) {
            btn.addEventListener("click", (e) => {
                const el = e.currentTarget;
                const s = findSchedule(el);
                if (!s)
                    return;
                void toggleScheduleOccurrences(client, envId, s, el);
            });
        }
    }
    async function toggleScheduleOccurrences(client, envId, schedule, button) {
        const container = document.getElementById(`schedule-occurrences-${schedule.id}`);
        if (!container)
            return;
        const expanded = container.style.display !== "none" && container.innerHTML.length > 0;
        if (expanded) {
            container.innerHTML = "";
            container.style.display = "none";
            button.textContent = "History";
            return;
        }
        container.style.display = "block";
        container.innerHTML = '<div class="loading">Loading history...</div>';
        button.textContent = "Hide history";
        try {
            const resp = await client.listScheduleOccurrences(schedule.id, envId, 25);
            if (resp.items.length === 0) {
                container.innerHTML =
                    '<div class="text-muted">No occurrences recorded yet.</div>';
                return;
            }
            const rows = resp.items
                .map((o) => {
                const reason = o.skippedReason
                    ? `<div class="text-muted schedule-skipped-reason">${escapeHtml(o.skippedReason)}</div>`
                    : "";
                const run = o.runId && o.runId.length > 0
                    ? `<a class="link-btn schedule-run-link" href="?runId=${escapeHtml(o.runId)}">${escapeHtml(o.runId)}</a>`
                    : '<span class="text-muted">-</span>';
                return `
          <div class="schedule-occurrence" data-occurrence-item="${escapeHtml(o.id)}">
            <div>${escapeHtml(o.dueAt)} · revision ${o.revision}</div>
            <div><span class="badge status-${o.status.toLowerCase()}">${escapeHtml(o.status)}</span>${o.skippedCount > 0 ? ` <span class="text-muted">skipped ${o.skippedCount}</span>` : ""}</div>
            ${reason}
            <div>run ${run}</div>
          </div>`;
            })
                .join("");
            container.innerHTML = rows;
        }
        catch (err) {
            if (isUnauthorized(err)) {
                handleUnauthorized();
                return;
            }
            container.innerHTML = `<div class="error-state">Failed to load history: ${escapeHtml(err instanceof Error ? err.message : String(err))}</div>`;
        }
    }
    function refreshSchedules(envId) {
        if (!envId)
            return;
        void loadSchedulesList(api, envId);
    }
    // openScheduleCreateDialog creates one schedule definition. The dialog
    // mints one Idempotency-Key for its lifetime so an ambiguous resubmit
    // reuses the identity instead of forking definitions.
    function openScheduleCreateDialog(client, envId, invoker) {
        document.getElementById("schedule-create-dialog-overlay")?.remove();
        const overlay = document.createElement("div");
        overlay.className = "dialog-overlay";
        overlay.id = "schedule-create-dialog-overlay";
        overlay.innerHTML = `
      <div class="dialog" role="dialog" aria-modal="true" aria-labelledby="schedule-create-dialog-title">
        <h3 id="schedule-create-dialog-title">New schedule</h3>
        <p class="text-muted">Schedules create runs without an operator present. Overlap is skip-overlap and misfire is coalesce-one.</p>
        <label>Workflow (required)
          <input id="schedule-create-workflow" type="text" autocomplete="off" />
        </label>
        <label>Cron (required)
          <input id="schedule-create-cron" type="text" placeholder="*/5 * * * *" autocomplete="off" />
        </label>
        <label>Timezone (required)
          <input id="schedule-create-timezone" type="text" placeholder="UTC" autocomplete="off" />
        </label>
        <label>Deployment ID (optional, blank means active-at-fire)
          <input id="schedule-create-deployment" type="text" autocomplete="off" />
        </label>
        <div id="schedule-create-error" class="dialog-error" role="alert" style="display:none"></div>
        <div class="dialog-actions">
          <button id="schedule-create-dismiss">Cancel</button>
          <button id="schedule-create-submit">Create schedule</button>
        </div>
      </div>
    `;
        document.body.appendChild(overlay);
        const idempotencyKey = newIdempotencyKey();
        const errorBox = overlay.querySelector("#schedule-create-error");
        const workflowInput = overlay.querySelector("#schedule-create-workflow");
        const cronInput = overlay.querySelector("#schedule-create-cron");
        const timezoneInput = overlay.querySelector("#schedule-create-timezone");
        const deploymentInput = overlay.querySelector("#schedule-create-deployment");
        const submitBtn = overlay.querySelector("#schedule-create-submit");
        const showError = (msg) => {
            errorBox.textContent = msg;
            errorBox.style.display = "block";
        };
        const close = () => {
            document.getElementById("schedule-create-dialog-overlay")?.remove();
            invoker?.focus();
        };
        overlay.querySelector("#schedule-create-dismiss").addEventListener("click", close);
        overlay.addEventListener("keydown", (e) => {
            if (e.key === "Escape")
                close();
        });
        overlay.addEventListener("mousedown", (e) => {
            if (e.target === overlay)
                close();
        });
        workflowInput.focus();
        submitBtn.addEventListener("click", () => {
            const workflow = workflowInput.value.trim();
            const cron = cronInput.value.trim();
            const timezone = timezoneInput.value.trim();
            if (!workflow) {
                showError("Workflow is required.");
                return;
            }
            if (!cron) {
                showError("Cron expression is required.");
                return;
            }
            if (!timezone) {
                showError("Timezone is required.");
                return;
            }
            const deployment = deploymentInput.value.trim();
            submitBtn.setAttribute("disabled", "true");
            const body = { workflow, cron, timezone };
            if (deployment)
                body.deploymentId = deployment;
            client
                .createSchedule(envId, body, idempotencyKey)
                .then(() => {
                close();
                refreshSchedules(envId);
            })
                .catch((err) => {
                submitBtn.removeAttribute("disabled");
                if (isConflict(err)) {
                    showError("This schedule changed since you opened it (409). The latest state was reloaded — review it before acting.");
                    refreshSchedules(envId);
                    return;
                }
                showError(err instanceof Error ? err.message : String(err));
            });
        });
    }
    // openScheduleEditDialog replaces the configuration of one schedule. The
    // caller binds expectedRevision; a 409 updates the revision in-dialog and
    // refreshes instead of retrying blindly.
    function openScheduleEditDialog(client, envId, schedule, invoker) {
        document.getElementById("schedule-edit-dialog-overlay")?.remove();
        const overlay = document.createElement("div");
        overlay.className = "dialog-overlay";
        overlay.id = "schedule-edit-dialog-overlay";
        let currentRevision = schedule.revision;
        overlay.innerHTML = `
      <div class="dialog" role="dialog" aria-modal="true" aria-labelledby="schedule-edit-dialog-title">
        <h3 id="schedule-edit-dialog-title">Edit schedule — ${escapeHtml(schedule.workflow)}</h3>
        <div class="hold-meta">Schedule revision ${currentRevision}</div>
        <label>Workflow (required)
          <input id="schedule-edit-workflow" type="text" autocomplete="off" />
        </label>
        <label>Cron (required)
          <input id="schedule-edit-cron" type="text" autocomplete="off" />
        </label>
        <label>Timezone (required)
          <input id="schedule-edit-timezone" type="text" autocomplete="off" />
        </label>
        <label>Deployment ID (optional, blank means active-at-fire)
          <input id="schedule-edit-deployment" type="text" autocomplete="off" />
        </label>
        <div id="schedule-edit-error" class="dialog-error" role="alert" style="display:none"></div>
        <div class="dialog-actions">
          <button id="schedule-edit-dismiss">Cancel</button>
          <button id="schedule-edit-submit">Save changes</button>
        </div>
      </div>
    `;
        document.body.appendChild(overlay);
        const idempotencyKey = newIdempotencyKey();
        const errorBox = overlay.querySelector("#schedule-edit-error");
        const metaBox = overlay.querySelector(".hold-meta");
        const workflowInput = overlay.querySelector("#schedule-edit-workflow");
        const cronInput = overlay.querySelector("#schedule-edit-cron");
        const timezoneInput = overlay.querySelector("#schedule-edit-timezone");
        const deploymentInput = overlay.querySelector("#schedule-edit-deployment");
        workflowInput.value = schedule.workflow;
        cronInput.value = schedule.cron;
        timezoneInput.value = schedule.timezone;
        deploymentInput.value = schedule.deploymentId ?? "";
        const submitBtn = overlay.querySelector("#schedule-edit-submit");
        const showError = (msg) => {
            errorBox.textContent = msg;
            errorBox.style.display = "block";
        };
        const close = () => {
            document.getElementById("schedule-edit-dialog-overlay")?.remove();
            invoker?.focus();
        };
        overlay.querySelector("#schedule-edit-dismiss").addEventListener("click", close);
        overlay.addEventListener("keydown", (e) => {
            if (e.key === "Escape")
                close();
        });
        overlay.addEventListener("mousedown", (e) => {
            if (e.target === overlay)
                close();
        });
        workflowInput.focus();
        submitBtn.addEventListener("click", () => {
            const workflow = workflowInput.value.trim();
            const cron = cronInput.value.trim();
            const timezone = timezoneInput.value.trim();
            if (!workflow) {
                showError("Workflow is required.");
                return;
            }
            if (!cron) {
                showError("Cron expression is required.");
                return;
            }
            if (!timezone) {
                showError("Timezone is required.");
                return;
            }
            const deployment = deploymentInput.value.trim();
            submitBtn.setAttribute("disabled", "true");
            const configuration = { workflow, cron, timezone };
            if (deployment)
                configuration.deploymentId = deployment;
            client
                .updateSchedule(schedule.id, envId, currentRevision, configuration, idempotencyKey)
                .then(() => {
                close();
                refreshSchedules(envId);
            })
                .catch((err) => {
                submitBtn.removeAttribute("disabled");
                if (isConflict(err)) {
                    showError("This schedule changed since you opened it (409). The latest state was reloaded — review it before acting.");
                    void client
                        .listSchedules(envId)
                        .then((items) => {
                        const fresh = items.find((s) => s.id === schedule.id);
                        if (fresh) {
                            currentRevision = fresh.revision;
                            metaBox.textContent = `Schedule revision ${currentRevision}`;
                        }
                    })
                        .catch(() => undefined);
                    refreshSchedules(envId);
                    return;
                }
                showError(err instanceof Error ? err.message : String(err));
            });
        });
    }
    // openSchedulePauseDialog confirms pausing one schedule. A 409 with
    // INVALID_SCHEDULE_STATE means it is already paused; the dialog stays open
    // with the latest revision instead of retrying blindly.
    function openSchedulePauseDialog(client, envId, schedule, invoker) {
        document.getElementById("schedule-pause-dialog-overlay")?.remove();
        const overlay = document.createElement("div");
        overlay.className = "dialog-overlay";
        overlay.id = "schedule-pause-dialog-overlay";
        let currentRevision = schedule.revision;
        overlay.innerHTML = `
      <div class="dialog" role="dialog" aria-modal="true" aria-labelledby="schedule-pause-dialog-title">
        <h3 id="schedule-pause-dialog-title">Pause schedule — ${escapeHtml(schedule.workflow)}</h3>
        <p class="text-muted">Pausing stops future occurrences from starting. Already-started runs are unaffected.</p>
        <div class="hold-meta">Schedule revision ${currentRevision}</div>
        <div id="schedule-pause-error" class="dialog-error" role="alert" style="display:none"></div>
        <div class="dialog-actions">
          <button id="schedule-pause-dismiss">Keep active</button>
          <button id="schedule-pause-confirm">Confirm pause</button>
        </div>
      </div>
    `;
        document.body.appendChild(overlay);
        const idempotencyKey = newIdempotencyKey();
        const errorBox = overlay.querySelector("#schedule-pause-error");
        const metaBox = overlay.querySelector(".hold-meta");
        const confirmBtn = overlay.querySelector("#schedule-pause-confirm");
        const showError = (msg) => {
            errorBox.textContent = msg;
            errorBox.style.display = "block";
        };
        const close = () => {
            document.getElementById("schedule-pause-dialog-overlay")?.remove();
            invoker?.focus();
        };
        overlay.querySelector("#schedule-pause-dismiss").addEventListener("click", close);
        overlay.addEventListener("keydown", (e) => {
            if (e.key === "Escape")
                close();
        });
        overlay.addEventListener("mousedown", (e) => {
            if (e.target === overlay)
                close();
        });
        confirmBtn.focus();
        confirmBtn.addEventListener("click", () => {
            confirmBtn.setAttribute("disabled", "true");
            client
                .pauseSchedule(schedule.id, envId, currentRevision, idempotencyKey)
                .then(() => {
                close();
                refreshSchedules(envId);
            })
                .catch((err) => {
                confirmBtn.removeAttribute("disabled");
                if (isConflict(err)) {
                    showError("This schedule changed since you opened it (409). The latest state was reloaded — review it before acting.");
                    void client
                        .listSchedules(envId)
                        .then((items) => {
                        const fresh = items.find((s) => s.id === schedule.id);
                        if (fresh) {
                            currentRevision = fresh.revision;
                            metaBox.textContent = `Schedule revision ${currentRevision}${fresh.paused ? " (paused)" : ""}`;
                            if (fresh.paused) {
                                confirmBtn.setAttribute("disabled", "true");
                                errorBox.textContent = `Schedule is already paused (revision ${currentRevision}).`;
                            }
                        }
                    })
                        .catch(() => undefined);
                    refreshSchedules(envId);
                    return;
                }
                showError(err instanceof Error ? err.message : String(err));
            });
        });
    }
    // openScheduleResumeDialog confirms resuming a paused schedule.
    function openScheduleResumeDialog(client, envId, schedule, invoker) {
        document.getElementById("schedule-resume-dialog-overlay")?.remove();
        const overlay = document.createElement("div");
        overlay.className = "dialog-overlay";
        overlay.id = "schedule-resume-dialog-overlay";
        let currentRevision = schedule.revision;
        overlay.innerHTML = `
      <div class="dialog" role="dialog" aria-modal="true" aria-labelledby="schedule-resume-dialog-title">
        <h3 id="schedule-resume-dialog-title">Resume schedule — ${escapeHtml(schedule.workflow)}</h3>
        <p class="text-muted">Resuming recomputes the next due slot from durable state. Missed slots coalesce into one.</p>
        <div class="hold-meta">Schedule revision ${currentRevision}</div>
        <div id="schedule-resume-error" class="dialog-error" role="alert" style="display:none"></div>
        <div class="dialog-actions">
          <button id="schedule-resume-dismiss">Cancel</button>
          <button id="schedule-resume-confirm">Confirm resume</button>
        </div>
      </div>
    `;
        document.body.appendChild(overlay);
        const idempotencyKey = newIdempotencyKey();
        const errorBox = overlay.querySelector("#schedule-resume-error");
        const metaBox = overlay.querySelector(".hold-meta");
        const confirmBtn = overlay.querySelector("#schedule-resume-confirm");
        const showError = (msg) => {
            errorBox.textContent = msg;
            errorBox.style.display = "block";
        };
        const close = () => {
            document.getElementById("schedule-resume-dialog-overlay")?.remove();
            invoker?.focus();
        };
        overlay.querySelector("#schedule-resume-dismiss").addEventListener("click", close);
        overlay.addEventListener("keydown", (e) => {
            if (e.key === "Escape")
                close();
        });
        overlay.addEventListener("mousedown", (e) => {
            if (e.target === overlay)
                close();
        });
        confirmBtn.focus();
        confirmBtn.addEventListener("click", () => {
            confirmBtn.setAttribute("disabled", "true");
            client
                .resumeSchedule(schedule.id, envId, currentRevision, idempotencyKey)
                .then(() => {
                close();
                refreshSchedules(envId);
            })
                .catch((err) => {
                confirmBtn.removeAttribute("disabled");
                if (isConflict(err)) {
                    showError("This schedule changed since you opened it (409). The latest state was reloaded — review it before acting.");
                    void client
                        .listSchedules(envId)
                        .then((items) => {
                        const fresh = items.find((s) => s.id === schedule.id);
                        if (fresh) {
                            currentRevision = fresh.revision;
                            metaBox.textContent = `Schedule revision ${currentRevision}${fresh.paused ? " (paused)" : ""}`;
                            if (!fresh.paused) {
                                confirmBtn.setAttribute("disabled", "true");
                                errorBox.textContent = `Schedule is already active (revision ${currentRevision}).`;
                            }
                        }
                    })
                        .catch(() => undefined);
                    refreshSchedules(envId);
                    return;
                }
                showError(err instanceof Error ? err.message : String(err));
            });
        });
    }
    // openScheduleDeleteDialog confirms deleting one schedule definition.
    function openScheduleDeleteDialog(client, envId, schedule, invoker) {
        document.getElementById("schedule-delete-dialog-overlay")?.remove();
        const overlay = document.createElement("div");
        overlay.className = "dialog-overlay";
        overlay.id = "schedule-delete-dialog-overlay";
        overlay.innerHTML = `
      <div class="dialog" role="dialog" aria-modal="true" aria-labelledby="schedule-delete-dialog-title">
        <h3 id="schedule-delete-dialog-title">Delete schedule — ${escapeHtml(schedule.workflow)}</h3>
        <p class="text-muted">Deleting stops all future occurrences. Already-started runs are unaffected. This cannot be undone.</p>
        <div class="hold-meta">Schedule revision ${schedule.revision}</div>
        <div id="schedule-delete-error" class="dialog-error" role="alert" style="display:none"></div>
        <div class="dialog-actions">
          <button id="schedule-delete-dismiss">Keep schedule</button>
          <button id="schedule-delete-confirm">Confirm delete</button>
        </div>
      </div>
    `;
        document.body.appendChild(overlay);
        const idempotencyKey = newIdempotencyKey();
        const errorBox = overlay.querySelector("#schedule-delete-error");
        const confirmBtn = overlay.querySelector("#schedule-delete-confirm");
        const showError = (msg) => {
            errorBox.textContent = msg;
            errorBox.style.display = "block";
        };
        const close = () => {
            document.getElementById("schedule-delete-dialog-overlay")?.remove();
            invoker?.focus();
        };
        overlay.querySelector("#schedule-delete-dismiss").addEventListener("click", close);
        overlay.addEventListener("keydown", (e) => {
            if (e.key === "Escape")
                close();
        });
        overlay.addEventListener("mousedown", (e) => {
            if (e.target === overlay)
                close();
        });
        confirmBtn.focus();
        confirmBtn.addEventListener("click", () => {
            confirmBtn.setAttribute("disabled", "true");
            client
                .deleteSchedule(schedule.id, envId, idempotencyKey)
                .then(() => {
                close();
                refreshSchedules(envId);
            })
                .catch((err) => {
                confirmBtn.removeAttribute("disabled");
                showError(err instanceof Error ? err.message : String(err));
            });
        });
    }
    function inspectRun(runId) {
        showView("inspector-view");
        const container = document.getElementById("inspector-content");
        if (!container)
            return;
        container.innerHTML = '<div class="loading">Loading Run Inspector...</div>';
        if (activeInspector) {
            activeInspector.destroy();
        }
        currentStreamFreshness = "DISCONNECTED";
        currentViewMode = "graph";
        selectedStepId = null;
        selectedStepTab = "summary";
        collapsedClusters.clear();
        cachedStepLogs.clear();
        lastSnap = null;
        let graphScrollLeft = 0;
        let graphScrollTop = 0;
        let listScrollTop = 0;
        let stepLogsWindowStart = 0;
        function captureFocusDescriptor(root) {
            const active = document.activeElement;
            if (!active || !root.contains(active))
                return null;
            if (active.id) {
                return `#${active.id}`;
            }
            const stepId = active.getAttribute("data-step-id");
            if (stepId) {
                if (active.classList.contains("dag-node")) {
                    return `.dag-node[data-step-id="${stepId}"]`;
                }
                if (active.classList.contains("select-step-btn")) {
                    return `.select-step-btn[data-step-id="${stepId}"]`;
                }
                if (active.classList.contains("accessible-step-card")) {
                    return `.accessible-step-card[data-step-id="${stepId}"]`;
                }
                return `[data-step-id="${stepId}"]`;
            }
            const role = active.getAttribute("role");
            const ariaLabel = active.getAttribute("aria-label");
            if (role && ariaLabel) {
                return `[role="${role}"][aria-label="${ariaLabel}"]`;
            }
            return null;
        }
        function restoreFocus(root, descriptor) {
            if (!descriptor)
                return;
            try {
                const el = root.querySelector(descriptor);
                if (el && typeof el.focus === "function") {
                    el.focus();
                }
            }
            catch {
                // Ignore invalid selector
            }
        }
        activeInspector = new RunInspector(runId);
        activeInspector.subscribe({
            onSnapshotUpdated: (snapshot) => renderSnapshot(snapshot),
            onFreshnessChanged: (freshness) => {
                currentStreamFreshness = freshness;
                renderFreshness(freshness);
                if (freshness === "LIVE" && clearStreamErrorOnLive(streamBanner)) {
                    clearBanner();
                }
            },
            onEventsUpdated: (events, hasMore, nextCursor) => renderEvents(events, hasMore, nextCursor),
            onLogsUpdated: (logs, err, stepId) => {
                if (stepId && logs) {
                    cachedStepLogs.set(stepId, logs);
                    if (selectedStepId === stepId) {
                        renderSnapshot(lastSnap);
                    }
                }
                renderLogs(logs, err, stepId ?? selectedStepId ?? undefined);
            },
            onError: (err) => {
                if (isUnauthorized(err)) {
                    handleUnauthorized();
                    return;
                }
                renderStreamError(err);
            },
        });
        activeInspector.load();
        function renderSnapshot(snap) {
            lastSnap = snap;
            const container = document.getElementById("inspector-content");
            if (!container)
                return;
            // Capture focus and scroll state before replacing innerHTML
            const focusDescriptor = captureFocusDescriptor(container);
            const existingGraphScroll = document.getElementById("graph-scroll-area");
            if (existingGraphScroll) {
                graphScrollLeft = existingGraphScroll.scrollLeft;
                graphScrollTop = existingGraphScroll.scrollTop;
            }
            const existingListScroll = document.getElementById("list-scroll-area");
            if (existingListScroll) {
                listScrollTop = existingListScroll.scrollTop;
            }
            // Select default step if not selected or no longer valid
            if (!selectedStepId || !snap.steps.some((s) => s.id === selectedStepId)) {
                const priorityStep = snap.steps.find((s) => s.status === "FAILED") ||
                    snap.steps.find((s) => s.status === "WAITING") ||
                    snap.steps.find((s) => s.status === "RUNNING") ||
                    snap.steps.find((s) => s.status === "READY") ||
                    snap.steps[0];
                selectedStepId = priorityStep ? priorityStep.id : null;
            }
            const selectedStep = snap.steps.find((s) => s.id === selectedStepId) ||
                snap.steps[0] ||
                null;
            // Blueprint §23.2: Pause/Resume/Cancel render only when the run state
            // permits the action AND the active identity holds runs:control.
            const controls = visibleRunControls(snap.status, canControlRuns);
            // Compute graph layout (1 node per logical step; attempts stay in step detail)
            const layout = computeGraphLayout(snap.steps, collapsedClusters);
            const minimap = computeMinimap(layout, 800, 450, graphScrollLeft, graphScrollTop, 160, 100);
            // Render SVG Graph Edges and Nodes
            const svgEdgesHtml = layout.edges
                .map((e) => {
                const midX = (e.fromX + e.toX) / 2;
                const d = `M ${e.fromX} ${e.fromY} C ${midX} ${e.fromY}, ${midX} ${e.toY}, ${e.toX} ${e.toY}`;
                const cls = e.isSkipped ? "dag-edge edge-skipped" : "dag-edge";
                const marker = e.isSkipped
                    ? "url(#arrow-skipped)"
                    : "url(#arrow-default)";
                return `<path class="${cls}" d="${d}" marker-end="${marker}" data-from="${escapeHtml(e.fromNodeId)}" data-to="${escapeHtml(e.toNodeId)}" />`;
            })
                .join("");
            const svgNodesHtml = layout.nodes
                .map((n) => {
                const pres = getStatusPresentation(n.status);
                const isSelected = selectedStepId === n.id;
                const nodeClass = `dag-node status-${n.status.toLowerCase()}${isSelected ? " node-selected" : ""}${n.isCollapsedPlaceholder ? " node-collapsed" : ""}`;
                const ariaLabel = n.isCollapsedPlaceholder
                    ? `Collapsed group of ${n.collapsedCount} parallel steps, click to expand`
                    : `Step ${n.nodeId}: ${pres.label}, ${n.attemptsCount} attempts`;
                return `
            <g class="${nodeClass}" tabindex="0" role="button" data-step-id="${escapeHtml(n.id)}" data-node-id="${escapeHtml(n.nodeId)}" data-cluster-id="${escapeHtml(n.clusterId || "")}" aria-label="${escapeHtml(ariaLabel)}" aria-pressed="${isSelected}">
              <rect class="node-bg" x="${n.x}" y="${n.y}" width="${n.width}" height="${n.height}" rx="6" ry="6" />
              <g class="node-badge status-${n.status.toLowerCase()}">
                <rect class="badge-bg" x="${n.x + 8}" y="${n.y + 8}" width="88" height="20" rx="4" />
                <text class="badge-text" x="${n.x + 12}" y="${n.y + 22}">${pres.symbol} ${escapeHtml(pres.label)}</text>
              </g>
              <text class="node-kind" x="${n.x + n.width - 10}" y="${n.y + 22}" text-anchor="end">${escapeHtml(n.kind)}</text>
              <text class="node-title" x="${n.x + 10}" y="${n.y + 44}">${escapeHtml(n.nodeId)}</text>
              <text class="node-meta" x="${n.x + 10}" y="${n.y + 60}">${n.isCollapsedPlaceholder
                    ? `[+ Expand ${n.collapsedCount} steps]`
                    : `${n.attemptsCount} attempt${n.attemptsCount === 1 ? "" : "s"}${n.waitReason ? ` · ${escapeHtml(n.waitReason)}` : ""}`}</text>
            </g>
          `;
            })
                .join("");
            // Minimap SVG Nodes
            const minimapNodesHtml = minimap.nodes
                .map((mn) => {
                return `<rect class="mini-node status-${mn.status.toLowerCase()}" x="${mn.x}" y="${mn.y}" width="${mn.width}" height="${mn.height}" rx="1" />`;
            })
                .join("");
            // Virtualized Accessible List rendering with spacers for true virtual scroll
            const virtualized = virtualizeItems(snap.steps, listScrollIndex, LIST_PAGE_SIZE);
            const visibleSteps = virtualized.items;
            const totalSteps = snap.steps.length;
            const ITEM_HEIGHT = 140;
            const LIST_CONTAINER_HEIGHT = 600;
            const topSpacerHeight = virtualized.offset * ITEM_HEIGHT;
            const bottomSpacerHeight = Math.max(0, totalSteps - virtualized.offset - virtualized.items.length) * ITEM_HEIGHT;
            const listItemsHtml = visibleSteps
                .map((st, vi) => {
                const pres = getStatusPresentation(st.status);
                const isSelected = selectedStepId === st.id;
                const deps = st.after && st.after.length > 0
                    ? st.after.map(escapeHtml).join(", ")
                    : "None (Root)";
                const openCase = openCaseForStep(snap, st.id);
                let holdHtml = "";
                if (st.status === "WAITING" && openCase) {
                    const evidenceRef = evidenceReference(openCase);
                    holdHtml = `
              <div class="hold-banner" role="status">
                <strong>Waiting for reconciliation.</strong>
                <div class="recovery-hint">${escapeHtml(reconciliationHoldText(openCase.reason))}</div>
                ${evidenceRef ? `<div class="hold-evidence">Reference: <code>${escapeHtml(evidenceRef)}</code></div>` : ""}
                <div class="hold-meta">Case <code>${escapeHtml(openCase.id.slice(0, 8))}…</code> · revision ${openCase.revision}</div>
                <button class="resolve-link" data-case-id="${escapeHtml(openCase.id)}" data-revision="${openCase.revision}" data-step-id="${escapeHtml(st.id)}">Resolve</button>
              </div>
            `;
                }
                return `
            <li class="step-card accessible-step-card${isSelected ? " selected" : ""}" data-step-id="${escapeHtml(st.id)}" data-node-id="${escapeHtml(st.nodeId)}" role="listitem" aria-posinset="${virtualized.offset + vi + 1}" aria-setsize="${virtualized.total}" style="height:${ITEM_HEIGHT}px;">
              <div class="step-header">
                <h4>${escapeHtml(st.nodeId)}</h4>
                <div class="step-badges">
                  <span class="kind-tag">${escapeHtml(st.kind || "task")}</span>
                  <span class="badge status-${st.status.toLowerCase()}">${pres.symbol} ${st.status}</span>
                  ${st.completionSource ? `<span class="badge source-${st.completionSource.toLowerCase()}">${escapeHtml(st.completionSource)}</span>` : ""}
                </div>
              </div>
              <div class="step-summary-meta">
                <div><strong>Dependencies:</strong> ${deps}</div>
                ${st.waitReason ? `<div><strong>Wait Reason:</strong> <code class="wait-reason-tag">${escapeHtml(st.waitReason)}</code></div>` : ""}
                <div><strong>Attempts:</strong> ${st.attempts.length}</div>
              </div>
              ${holdHtml}
              <div class="step-actions">
                <button class="secondary-btn select-step-btn" data-step-id="${escapeHtml(st.id)}" aria-label="Inspect ${escapeHtml(st.nodeId)} details" aria-pressed="${isSelected}">
                  ${isSelected ? "Inspecting" : "Inspect Step"}
                </button>
              </div>
            </li>
          `;
            })
                .join("");
            const listVirtualizationHtml = totalSteps > LIST_PAGE_SIZE
                ? `
        <div class="list-virtualization-info" role="status" aria-live="polite">
          Showing steps ${virtualized.offset + 1}–${Math.min(virtualized.offset + virtualized.items.length, virtualized.total)} of ${virtualized.total}.
          ${virtualized.hasMore ? '<button id="load-more-steps-btn" class="load-more-btn" data-list-offset="${virtualized.offset + virtualized.items.length}">Load More Steps</button>' : ""}
        </div>
      `
                : "";
            const listHtml = totalSteps > LIST_PAGE_SIZE
                ? `
        <div id="list-scroll-area" class="list-scroll-area" style="height:${LIST_CONTAINER_HEIGHT}px;overflow-y:auto;" aria-label="Accessible step list, scroll to navigate">
          <div style="height:${topSpacerHeight}px;" aria-hidden="true"></div>
          <ul class="accessible-steps-list" role="list" aria-label="Execution steps" style="height:${visibleSteps.length * ITEM_HEIGHT}px;">
            ${listItemsHtml}
          </ul>
          <div style="height:${bottomSpacerHeight}px;" aria-hidden="true"></div>
          ${listVirtualizationHtml}
        </div>
      `
                : `
        <ul class="accessible-steps-list" role="list" aria-label="Execution steps">
          ${listItemsHtml}
        </ul>
      `;
            // Render Step Tabs Details Panel (Summary, Attempts, Events, Logs, Input, Output, Trace)
            let stepDetailHtml = "";
            if (selectedStep) {
                const pres = getStatusPresentation(selectedStep.status);
                const openCase = openCaseForStep(snap, selectedStep.id);
                let holdHtml = "";
                if (selectedStep.status === "WAITING" && openCase) {
                    const evidenceRef = evidenceReference(openCase);
                    holdHtml = `
            <div class="hold-banner" role="status">
              <strong>Waiting for reconciliation.</strong>
              <div class="recovery-hint">${escapeHtml(reconciliationHoldText(openCase.reason))}</div>
              ${evidenceRef ? `<div class="hold-evidence">Reference: <code>${escapeHtml(evidenceRef)}</code></div>` : ""}
              <div class="hold-meta">Case <code>${escapeHtml(openCase.id.slice(0, 8))}…</code> · revision ${openCase.revision}</div>
              <button class="resolve-link" data-case-id="${escapeHtml(openCase.id)}" data-revision="${openCase.revision}" data-step-id="${escapeHtml(selectedStep.id)}">Resolve</button>
            </div>
          `;
                }
                // Summary Tab Content
                let summaryContent = `
          <div class="step-meta-grid">
            <div class="meta-item"><label>Node ID</label><div><code>${escapeHtml(selectedStep.nodeId)}</code></div></div>
            <div class="meta-item"><label>Kind</label><div>${escapeHtml(selectedStep.kind || "task")}</div></div>
            <div class="meta-item"><label>Status</label><div><span class="badge status-${selectedStep.status.toLowerCase()}">${pres.symbol} ${selectedStep.status}</span></div></div>
            <div class="meta-item"><label>Epoch</label><div>${selectedStep.currentEpoch}</div></div>
            <div class="meta-item"><label>Dependencies</label><div>${selectedStep.after && selectedStep.after.length > 0 ? selectedStep.after.map(escapeHtml).join(", ") : "None (Root)"}</div></div>
            <div class="meta-item"><label>Attempts</label><div>${selectedStep.attempts.length}</div></div>
                ${selectedStep.waitReason ? `<div class="meta-item"><label>Wait Reason</label><div><code class="wait-reason-tag">${escapeHtml(selectedStep.waitReason)}</code></div></div>` : ""}
            ${selectedStep.dueAt ? `<div class="meta-item"><label>Due At</label><div>${new Date(selectedStep.dueAt).toLocaleString()}</div></div>` : ""}
            ${selectedStep.completionSource ? `<div class="meta-item"><label>Completion Source</label><div>${escapeHtml(selectedStep.completionSource)}</div></div>` : ""}
          </div>
        `;
                if (shouldShowWorkerWait(selectedStep.status, snap.waitingReason, snap.activeCompatibleWorkers)) {
                    summaryContent += `
            <div class="waiting-warning mt-2" role="status">
              <strong>No compatible workers available.</strong>
              <div class="recovery-hint">
                Waiting for active worker advertising deployment <code>${escapeHtml(snap.deploymentId.slice(0, 8))}...</code>. Ensure an enrolled worker is running.
              </div>
            </div>
          `;
                }
                else if (selectedStep.attempts.length === 0) {
                    summaryContent += `<div class="no-attempts text-muted mt-2">${escapeHtml(terminalStepEmptyText(selectedStep.status))}</div>`;
                }
                // Attempts Tab Content
                const attemptsHtml = selectedStep.attempts.length > 0
                    ? selectedStep.attempts
                        .map((att) => {
                        const started = att.startedAt
                            ? new Date(att.startedAt).toLocaleTimeString()
                            : "-";
                        const completed = att.completedAt
                            ? new Date(att.completedAt).toLocaleTimeString()
                            : "-";
                        const attPres = getStatusPresentation(att.status);
                        return `
                    <div class="attempt-card status-${att.status.toLowerCase()}">
                      <div class="attempt-header">
                        <span class="attempt-title">Attempt #${att.attemptNumber}</span>
                        <span class="badge status-${att.status.toLowerCase()}">${attPres.symbol} ${att.status}</span>
                      </div>
                      <div class="attempt-details">
                        <span>Session: <code>${att.workerSessionId ? att.workerSessionId.slice(0, 8) + "..." : "-"}</code></span>
                        <span>Started: ${started}</span>
                        <span>Completed: ${completed}</span>
                        <span>Epoch: ${att.ownershipEpoch ?? "-"}</span>
                        ${att.error !== undefined ? `<div class="attempt-error mt-1"><label>Error:</label><pre class="code-block error-text">${escapeHtml(JSON.stringify(att.error, null, 2))}</pre></div>` : ""}
                      </div>
                    </div>
                  `;
                    })
                        .join("")
                    : `<div class="no-attempts text-muted">${escapeHtml(terminalStepEmptyText(selectedStep.status))}</div>`;
                // Events Tab Content (Filtered for step) - bounded rendering
                const allEvs = activeInspector ? activeInspector.getEvents() : [];
                const stepEventsAll = filterEventsForStep(allEvs, selectedStep);
                const stepEventsBounded = getBoundedEvents(stepEventsAll, eventsWindowStart, EVENTS_PAGE_SIZE);
                const stepEventsHtml = stepEventsBounded.events.length > 0
                    ? stepEventsBounded.events
                        .map((ev) => {
                        const time = new Date(ev.committedAt).toLocaleTimeString();
                        return `
                    <div class="event-card" data-sequence="${ev.sequence}">
                      <div class="event-header">
                        <span class="event-type">${escapeHtml(ev.type)}</span>
                        <span class="event-seq">#${ev.sequence}</span>
                      </div>
                      <div class="event-time">${time}</div>
                      <pre class="event-payload">${escapeHtml(JSON.stringify(ev.payload, null, 2))}</pre>
                    </div>
                  `;
                    })
                        .join("")
                    : `<div class="text-muted">No execution events recorded for this step yet.</div>`;
                const stepEventsLoadMore = stepEventsBounded.hasMore
                    ? `<button id="load-more-step-events-btn" class="load-more-btn">Load More Step Events</button>`
                    : "";
                const stepEventsWindowInfo = stepEventsAll.length > EVENTS_PAGE_SIZE
                    ? `<div class="events-window-info" role="status" aria-live="polite">Showing events ${stepEventsBounded.offset + 1}–${Math.min(stepEventsBounded.offset + stepEventsBounded.events.length, stepEventsBounded.total)} of ${stepEventsBounded.total}.</div>`
                    : "";
                // Logs Tab Content - bounded rendering
                let logsContent = "";
                const stepLogs = cachedStepLogs.get(selectedStep.id);
                if (stepLogs && stepLogs.items.length > 0) {
                    const stepLogsBounded = virtualizeItems(stepLogs.items, stepLogsWindowStart, LOGS_PAGE_SIZE);
                    const visibleStepLogItems = stepLogsBounded.items;
                    const stepLogsHasMore = stepLogs.nextCursor != null || stepLogsBounded.hasMore;
                    const logLines = visibleStepLogItems
                        .map((line) => {
                        const time = new Date(line.timestamp).toLocaleTimeString();
                        return `<div class="log-line log-${line.level}"><span class="log-time">${time}</span> <span class="log-level">[${line.level.toUpperCase()}]</span> <span class="log-msg">${escapeHtml(line.message)}</span></div>`;
                    })
                        .join("");
                    const stepLogWindowInfo = stepLogs.items.length > LOGS_PAGE_SIZE
                        ? `<div class="logs-window-info" role="status" aria-live="polite">Showing logs ${stepLogsBounded.offset + 1}–${Math.min(stepLogsBounded.offset + visibleStepLogItems.length, stepLogsBounded.total)} of ${stepLogsBounded.total}.</div>`
                        : "";
                    let stepLoadMoreHtml = "";
                    if (stepLogsHasMore) {
                        stepLoadMoreHtml = `<button id="load-more-step-logs-btn" class="load-more-btn" data-step-id="${escapeHtml(selectedStep.id)}">${stepLogs.nextCursor ? "Load More Step Logs" : "Load More Step Logs (local)"}</button>`;
                    }
                    logsContent = `
            ${stepLogWindowInfo}
            <div class="log-terminal" role="region" aria-label="Step Task Logs">
              ${logLines}
            </div>
            ${stepLoadMoreHtml}
          `;
                }
                else if (stepLogs?.expired) {
                    logsContent = `<div class="logs-notice logs-expired">Logs have expired due to the 7-day retention policy.</div>`;
                }
                else {
                    logsContent = `<div class="text-muted">No logs recorded for this step yet (or click Logs to load).</div>`;
                }
                // Input Tab Content
                const stepInput = selectedStep.input;
                const inputContent = stepInput !== undefined
                    ? `<pre class="code-block">${escapeHtml(JSON.stringify(stepInput, null, 2))}</pre>`
                    : `<div class="text-muted">No step input recorded or redacted by tenant policy (<code>payload:read</code> required).</div>`;
                // Output Tab Content
                const outputContent = selectedStep.output !== undefined
                    ? `<pre class="code-block">${escapeHtml(JSON.stringify(selectedStep.output, null, 2))}</pre>`
                    : `<div class="text-muted">${selectedStep.status === "SUCCEEDED" || selectedStep.status === "SKIPPED" ? "No output payload or redacted by tenant policy (<code>payload:read</code> required)." : "Step is not complete; no output produced yet."}</div>`;
                // Trace Tab Content
                const traceContent = `
          <div class="trace-summary">
            <div><strong>Logical Step:</strong> <code>${escapeHtml(selectedStep.nodeId)}</code> (ID: <code>${escapeHtml(selectedStep.id)}</code>)</div>
            <div><strong>Current Epoch:</strong> ${selectedStep.currentEpoch}</div>
            <div><strong>Total Attempts:</strong> ${selectedStep.attempts.length}</div>
            ${selectedStep.completionSource ? `<div><strong>Completion Source:</strong> ${escapeHtml(selectedStep.completionSource)}</div>` : ""}
            <div class="trace-attempts-list mt-2">
              ${selectedStep.attempts
                    .map((a) => `<div>Attempt #${a.attemptNumber}: status <strong>${a.status}</strong>, session <code>${a.workerSessionId ? a.workerSessionId.slice(0, 8) + "..." : "-"}</code>, epoch ${a.ownershipEpoch ?? "-"}</div>`)
                    .join("")}
            </div>
          </div>
        `;
                stepDetailHtml = `
          <div class="step-detail-card" role="region" aria-labelledby="step-detail-heading">
            <div class="step-detail-header">
              <h4 id="step-detail-heading">Step Inspector: <code>${escapeHtml(selectedStep.nodeId)}</code></h4>
              <div class="step-badges">
                <span class="kind-tag">${escapeHtml(selectedStep.kind || "task")}</span>
                <span class="badge status-${selectedStep.status.toLowerCase()}">${pres.symbol} ${selectedStep.status}</span>
              </div>
            </div>
            ${holdHtml}
            <div class="step-tabs-nav" role="tablist" aria-label="Step Detail Tabs">
              <button role="tab" id="step-tab-summary" class="step-tab-btn ${selectedStepTab === "summary" ? "active" : ""}" aria-selected="${selectedStepTab === "summary"}" aria-controls="step-panel-summary" tabindex="${selectedStepTab === "summary" ? "0" : "-1"}">Summary</button>
              <button role="tab" id="step-tab-attempts" class="step-tab-btn ${selectedStepTab === "attempts" ? "active" : ""}" aria-selected="${selectedStepTab === "attempts"}" aria-controls="step-panel-attempts" tabindex="${selectedStepTab === "attempts" ? "0" : "-1"}">Attempts (${selectedStep.attempts.length})</button>
              <button role="tab" id="step-tab-events" class="step-tab-btn ${selectedStepTab === "events" ? "active" : ""}" aria-selected="${selectedStepTab === "events"}" aria-controls="step-panel-events" tabindex="${selectedStepTab === "events" ? "0" : "-1"}">Events (${stepEventsAll.length})</button>
              <button role="tab" id="step-tab-logs" class="step-tab-btn ${selectedStepTab === "logs" ? "active" : ""}" aria-selected="${selectedStepTab === "logs"}" aria-controls="step-panel-logs" tabindex="${selectedStepTab === "logs" ? "0" : "-1"}">Logs</button>
              <button role="tab" id="step-tab-input" class="step-tab-btn ${selectedStepTab === "input" ? "active" : ""}" aria-selected="${selectedStepTab === "input"}" aria-controls="step-panel-input" tabindex="${selectedStepTab === "input" ? "0" : "-1"}">Input</button>
              <button role="tab" id="step-tab-output" class="step-tab-btn ${selectedStepTab === "output" ? "active" : ""}" aria-selected="${selectedStepTab === "output"}" aria-controls="step-panel-output" tabindex="${selectedStepTab === "output" ? "0" : "-1"}">Output</button>
              <button role="tab" id="step-tab-trace" class="step-tab-btn ${selectedStepTab === "trace" ? "active" : ""}" aria-selected="${selectedStepTab === "trace"}" aria-controls="step-panel-trace" tabindex="${selectedStepTab === "trace" ? "0" : "-1"}">Trace</button>
            </div>
            <div class="step-tab-content">
              <div id="step-panel-summary" role="tabpanel" class="tab-panel ${selectedStepTab === "summary" ? "" : "hidden"}" aria-labelledby="step-tab-summary">${summaryContent}</div>
              <div id="step-panel-attempts" role="tabpanel" class="tab-panel ${selectedStepTab === "attempts" ? "" : "hidden"}" aria-labelledby="step-tab-attempts">${attemptsHtml}</div>
              <div id="step-panel-events" role="tabpanel" class="tab-panel ${selectedStepTab === "events" ? "" : "hidden"}" aria-labelledby="step-tab-events">
                  <div class="step-events-timeline">
                    ${stepEventsWindowInfo}${stepEventsHtml}${stepEventsLoadMore}
                  </div>
                </div>
              <div id="step-panel-logs" role="tabpanel" class="tab-panel ${selectedStepTab === "logs" ? "" : "hidden"}" aria-labelledby="step-tab-logs">${logsContent}</div>
              <div id="step-panel-input" role="tabpanel" class="tab-panel ${selectedStepTab === "input" ? "" : "hidden"}" aria-labelledby="step-tab-input">${inputContent}</div>
              <div id="step-panel-output" role="tabpanel" class="tab-panel ${selectedStepTab === "output" ? "" : "hidden"}" aria-labelledby="step-tab-output">${outputContent}</div>
              <div id="step-panel-trace" role="tabpanel" class="tab-panel ${selectedStepTab === "trace" ? "" : "hidden"}" aria-labelledby="step-tab-trace">${traceContent}</div>
            </div>
          </div>
        `;
            }
            container.innerHTML = `
        <div class="inspector-header">
          <div class="run-title-group">
            <h2>${escapeHtml(snap.workflowName)}</h2>
            <span class="run-id-label">ID: <code>${snap.id}</code></span>
          </div>
          <div class="run-badges">
            <span class="badge status-${snap.status.toLowerCase()}">${snap.status}</span>
            <span id="stream-freshness-badge" class="badge freshness-badge freshness-${currentStreamFreshness.toLowerCase()}">${currentStreamFreshness}</span>
            ${controls.pause ? `<button id="pause-run-btn" class="secondary-btn">Pause run</button>` : ""}
            ${controls.resume ? `<button id="resume-run-btn" class="primary-btn">Resume run</button>` : ""}
            ${controls.cancel ? `<button id="cancel-run-btn" class="danger-btn">Cancel run</button>` : ""}
          </div>
        </div>

        ${(() => {
                const banner = terminationBannerText(snap.status, snap.terminationConfirmed);
                return banner
                    ? `<div class="termination-banner" role="status">${escapeHtml(banner)}</div>`
                    : "";
            })()}

        ${renderApprovalsPanel(api, snap)}

        <div class="meta-grid">
          <div class="meta-item"><label>Revision</label><div>${snap.revision}</div></div>
          <div class="meta-item"><label>Last Event Seq</label><div>${snap.lastEventSequence}</div></div>
          <div class="meta-item"><label>Deployment ID</label><div><code>${snap.deploymentId.slice(0, 8)}...</code></div></div>
          <div class="meta-item"><label>Deadline</label><div>${snap.deadlineAt ? new Date(snap.deadlineAt).toLocaleString() : "None"}</div></div>
          <div class="meta-item"><label>Created At</label><div>${new Date(snap.createdAt).toLocaleString()}</div></div>
          ${snap.reasonCode ? `<div class="meta-item"><label>Reason</label><div>${escapeHtml(snap.reasonCode)}</div></div>` : ""}
        </div>

        <section class="steps-section" aria-labelledby="graph-steps-heading">
          <div class="steps-section-header">
            <h3 id="graph-steps-heading">Execution Graph & Steps</h3>
            <div class="view-controls" role="tablist" aria-label="View Mode">
              <button id="view-mode-graph-btn" class="toggle-btn ${currentViewMode === "graph" ? "active" : ""}" role="tab" aria-selected="${currentViewMode === "graph"}" aria-controls="graph-view-wrapper">Graph View</button>
              <button id="view-mode-list-btn" class="toggle-btn ${currentViewMode === "list" ? "active" : ""}" role="tab" aria-selected="${currentViewMode === "list"}" aria-controls="list-view-wrapper">Accessible List</button>
              <button id="toggle-collapse-btn" class="secondary-btn" aria-label="Toggle collapse of parallel groups">
                ${collapsedClusters.size > 0 ? "Expand Groups" : "Collapse Groups"}
              </button>
            </div>
          </div>

          <div class="graph-inspector-layout">
            <div class="graph-main-pane">
              <div id="graph-view-wrapper" class="graph-view-container ${currentViewMode === "graph" ? "" : "hidden"}" role="tabpanel" aria-labelledby="view-mode-graph-btn">
                <div id="minimap-container" class="minimap-panel" aria-label="Execution Graph Minimap" role="region">
                  <div class="minimap-title">Minimap</div>
                  <svg id="minimap-svg" class="minimap-svg" width="160" height="100" viewBox="0 0 160 100">
                    ${minimapNodesHtml}
                    <rect id="minimap-viewport" class="minimap-vp" x="${minimap.viewport.x}" y="${minimap.viewport.y}" width="${minimap.viewport.width}" height="${minimap.viewport.height}" />
                  </svg>
                </div>
                <div id="graph-scroll-area" class="graph-scroll-area" tabindex="0" aria-label="Workflow execution graph canvas, click steps to inspect">
                  <svg id="graph-svg" class="dag-svg" width="${layout.width}" height="${layout.height}" viewBox="0 0 ${layout.width} ${layout.height}" role="graphics-document" aria-label="Workflow execution DAG">
                    <defs>
                      <marker id="arrow-default" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                        <path d="M 0 1 L 10 5 L 0 9 z" fill="var(--text-secondary)" />
                      </marker>
                      <marker id="arrow-skipped" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                        <path d="M 0 1 L 10 5 L 0 9 z" fill="var(--text-muted)" stroke-dasharray="2,2" />
                      </marker>
                    </defs>
                    <g class="dag-edges">${svgEdgesHtml}</g>
                    <g class="dag-nodes">${svgNodesHtml}</g>
                  </svg>
                </div>
              </div>

              <div id="list-view-wrapper" class="list-view-container ${currentViewMode === "list" ? "" : "hidden"}" role="tabpanel" aria-labelledby="view-mode-list-btn">
                ${listHtml}
              </div>
            </div>

            <div class="graph-side-pane">
              ${stepDetailHtml}
            </div>
          </div>
        </section>

        <section class="events-section">
          <h3>Execution Event History</h3>
          <div id="events-container" class="events-timeline">
            <div class="loading">Loading event history...</div>
          </div>
        </section>

        <section class="logs-section">
          <h3>Task Diagnostic Logs</h3>
          <div id="logs-container" class="logs-container">
            <div class="loading">Loading logs...</div>
          </div>
        </section>

        ${snap.output
                ? `
          <section class="output-section">
            <h3>Workflow Output</h3>
            <pre class="code-block">${escapeHtml(JSON.stringify(snap.output, null, 2))}</pre>
          </section>
        `
                : ""}

        ${snap.error
                ? `
          <section class="error-section">
            <h3>Workflow Error</h3>
            <pre class="code-block error-text">${escapeHtml(JSON.stringify(snap.error, null, 2))}</pre>
          </section>
        `
                : ""}
      `;
            // Re-apply current transport freshness
            renderFreshness(currentStreamFreshness);
            // Wire view mode buttons with roving tabindex and keyboard nav
            const viewTabOrder = ["graph", "list"];
            const graphBtn = container.querySelector("#view-mode-graph-btn");
            const listBtn = container.querySelector("#view-mode-list-btn");
            const viewButtons = [graphBtn, listBtn].filter(Boolean);
            function updateViewRovingTabindex(activeIndex) {
                viewButtons.forEach((btn, i) => {
                    if (btn) {
                        btn.tabIndex = i === activeIndex ? 0 : -1;
                        btn.setAttribute("aria-selected", i === activeIndex ? "true" : "false");
                    }
                });
            }
            updateViewRovingTabindex(currentViewMode === "graph" ? 0 : 1);
            viewButtons.forEach((btn, idx) => {
                btn.addEventListener("click", () => {
                    currentViewMode = viewTabOrder[idx];
                    updateViewRovingTabindex(idx);
                    renderSnapshot(lastSnap);
                });
                btn.addEventListener("keydown", (e) => {
                    let newIdx = -1;
                    if (e.key === "ArrowRight") {
                        e.preventDefault();
                        newIdx = (idx + 1) % viewButtons.length;
                    }
                    else if (e.key === "ArrowLeft") {
                        e.preventDefault();
                        newIdx = (idx - 1 + viewButtons.length) % viewButtons.length;
                    }
                    else if (e.key === "Home") {
                        e.preventDefault();
                        newIdx = 0;
                    }
                    else if (e.key === "End") {
                        e.preventDefault();
                        newIdx = viewButtons.length - 1;
                    }
                    if (newIdx >= 0) {
                        currentViewMode = viewTabOrder[newIdx];
                        renderSnapshot(lastSnap);
                        const targetId = viewTabOrder[newIdx] === "graph"
                            ? "view-mode-graph-btn"
                            : "view-mode-list-btn";
                        const newBtn = container.querySelector(`#${targetId}`);
                        newBtn?.focus();
                    }
                });
            });
            // Wire collapse/expand groups button
            const collapseBtn = container.querySelector("#toggle-collapse-btn");
            if (collapseBtn) {
                collapseBtn.addEventListener("click", () => {
                    if (collapsedClusters.size > 0) {
                        collapsedClusters.clear();
                    }
                    else {
                        const testLayout = computeGraphLayout(snap.steps);
                        for (const n of testLayout.nodes) {
                            if (n.clusterId)
                                collapsedClusters.add(n.clusterId);
                        }
                    }
                    renderSnapshot(lastSnap);
                });
            }
            // Wire minimap scroll binding to graph scroll container
            const scrollAreaEl = document.getElementById("graph-scroll-area");
            if (scrollAreaEl) {
                scrollAreaEl.addEventListener("scroll", () => {
                    graphScrollLeft = scrollAreaEl.scrollLeft;
                    graphScrollTop = scrollAreaEl.scrollTop;
                    if (lastSnap) {
                        const layout = computeGraphLayout(lastSnap.steps, collapsedClusters);
                        const newMinimap = computeMinimap(layout, 800, 450, graphScrollLeft, graphScrollTop, 160, 100);
                        const vp = document.getElementById("minimap-viewport");
                        if (vp) {
                            vp.setAttribute("x", String(newMinimap.viewport.x));
                            vp.setAttribute("y", String(newMinimap.viewport.y));
                            vp.setAttribute("width", String(newMinimap.viewport.width));
                            vp.setAttribute("height", String(newMinimap.viewport.height));
                        }
                    }
                });
            }
            // Wire list scroll handler for virtualization (debounced)
            const listScrollArea = document.getElementById("list-scroll-area");
            let listScrollTimer = null;
            if (listScrollArea) {
                listScrollArea.addEventListener("scroll", () => {
                    listScrollTop = listScrollArea.scrollTop;
                    if (listScrollTimer)
                        clearTimeout(listScrollTimer);
                    listScrollTimer = setTimeout(() => {
                        const scrollTop = listScrollArea.scrollTop;
                        listScrollTop = scrollTop;
                        const itemHeight = 140;
                        const newIndex = Math.floor(scrollTop / itemHeight);
                        const clampedIndex = Math.max(0, Math.min(newIndex, Math.max(0, snap.steps.length - 1)));
                        if (clampedIndex !== listScrollIndex) {
                            listScrollIndex = clampedIndex;
                            renderSnapshot(lastSnap);
                        }
                    }, 50);
                });
            }
            // Wire DAG SVG node selection
            container.querySelectorAll(".dag-node").forEach((nodeEl) => {
                const handleSelect = () => {
                    const clusterId = nodeEl.getAttribute("data-cluster-id");
                    const isCollapsed = nodeEl.classList.contains("node-collapsed");
                    if (isCollapsed && clusterId) {
                        collapsedClusters.delete(clusterId);
                        renderSnapshot(lastSnap);
                        return;
                    }
                    const stepId = nodeEl.getAttribute("data-step-id");
                    if (stepId && stepId !== selectedStepId) {
                        selectedStepId = stepId;
                        stepLogsWindowStart = 0;
                        renderSnapshot(lastSnap);
                    }
                };
                nodeEl.addEventListener("click", handleSelect);
                nodeEl.addEventListener("keydown", (e) => {
                    if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        handleSelect();
                    }
                });
            });
            // Wire Accessible List item selection
            container.querySelectorAll(".select-step-btn").forEach((btn) => {
                btn.addEventListener("click", (e) => {
                    const stepId = e.currentTarget.getAttribute("data-step-id");
                    if (stepId) {
                        selectedStepId = stepId;
                        stepLogsWindowStart = 0;
                        renderSnapshot(lastSnap);
                    }
                });
            });
            // Wire Step Tabs navigation
            const tabOrder = [
                "summary",
                "attempts",
                "events",
                "logs",
                "input",
                "output",
                "trace",
            ];
            container.querySelectorAll(".step-tab-btn").forEach((tabBtn) => {
                const tabId = tabBtn.id.replace("step-tab-", "");
                const selectTab = (t) => {
                    selectedStepTab = t;
                    if (t === "logs" &&
                        selectedStep &&
                        !cachedStepLogs.has(selectedStep.id)) {
                        activeInspector?.fetchLogs(selectedStep.id).then((l) => {
                            if (l)
                                cachedStepLogs.set(selectedStep.id, l);
                            renderSnapshot(lastSnap);
                        });
                    }
                    renderSnapshot(lastSnap);
                };
                tabBtn.addEventListener("click", () => selectTab(tabId));
                tabBtn.addEventListener("keydown", (e) => {
                    const idx = tabOrder.indexOf(tabId);
                    if (e.key === "ArrowRight") {
                        e.preventDefault();
                        const nextTab = tabOrder[(idx + 1) % tabOrder.length];
                        selectTab(nextTab);
                        const nextEl = container.querySelector(`#step-tab-${nextTab}`);
                        nextEl?.focus();
                    }
                    else if (e.key === "ArrowLeft") {
                        e.preventDefault();
                        const prevTab = tabOrder[(idx - 1 + tabOrder.length) % tabOrder.length];
                        selectTab(prevTab);
                        const prevEl = container.querySelector(`#step-tab-${prevTab}`);
                        prevEl?.focus();
                    }
                });
            });
            // Wire Load More Step Events button
            const loadMoreStepEventsBtn = document.getElementById("load-more-step-events-btn");
            if (loadMoreStepEventsBtn) {
                loadMoreStepEventsBtn.addEventListener("click", () => {
                    eventsWindowStart += EVENTS_PAGE_SIZE;
                    renderSnapshot(lastSnap);
                });
            }
            // Wire Load More Step Logs button
            const loadMoreStepLogsBtn = document.getElementById("load-more-step-logs-btn");
            if (loadMoreStepLogsBtn && selectedStep) {
                loadMoreStepLogsBtn.addEventListener("click", () => {
                    const currentLogs = cachedStepLogs.get(selectedStep.id);
                    stepLogsWindowStart += LOGS_PAGE_SIZE;
                    if (currentLogs?.nextCursor) {
                        loadMoreStepLogsBtn.textContent = "Loading...";
                        loadMoreStepLogsBtn.setAttribute("disabled", "true");
                        activeInspector?.fetchLogs(selectedStep.id, undefined, currentLogs.nextCursor, true);
                    }
                    else {
                        renderSnapshot(lastSnap);
                    }
                });
            }
            // Wire Load More Steps button
            const loadMoreStepsBtn = document.getElementById("load-more-steps-btn");
            if (loadMoreStepsBtn) {
                loadMoreStepsBtn.addEventListener("click", () => {
                    const offset = parseInt(loadMoreStepsBtn.getAttribute("data-list-offset") ?? "0");
                    listScrollIndex = offset;
                    listScrollTop = offset * 140;
                    renderSnapshot(lastSnap);
                });
            }
            // Restore physical scroll positions immediately on the new DOM elements
            const newGraphScrollArea = document.getElementById("graph-scroll-area");
            if (newGraphScrollArea) {
                newGraphScrollArea.scrollLeft = graphScrollLeft;
                newGraphScrollArea.scrollTop = graphScrollTop;
            }
            const newLsa = document.getElementById("list-scroll-area");
            if (newLsa) {
                newLsa.scrollTop = listScrollTop;
            }
            // Restore focus to active element if still present in new DOM
            restoreFocus(container, focusDescriptor);
            // Wire human approval decisions. The CTA renders only for a PENDING
            // approval when this identity holds approvals:decide; the backend stays
            // the authority and 409s refresh in-dialog.
            wireApprovalActions(api, snap, container);
            // Wire durable pause/resume controls. The backend stays authoritative:
            // expectedRevision is captured at open time and 409s refresh in-dialog.
            const pauseBtn = container.querySelector("#pause-run-btn");
            if (pauseBtn) {
                pauseBtn.addEventListener("click", (e) => {
                    openPauseDialog(api, snap, e.currentTarget);
                });
            }
            const resumeBtn = container.querySelector("#resume-run-btn");
            if (resumeBtn) {
                resumeBtn.addEventListener("click", (e) => {
                    openResumeDialog(api, snap, e.currentTarget);
                });
            }
            // Wire durable cancellation. The backend stays authoritative:
            // expectedRevision is captured at open time and 409s refresh in-dialog.
            const cancelBtn = container.querySelector("#cancel-run-btn");
            if (cancelBtn) {
                cancelBtn.addEventListener("click", (e) => {
                    openCancelDialog(api, snap, e.currentTarget);
                });
            }
            // Wire audited resolution dialogs. The backend stays authoritative:
            // expectedRevision is captured at open time and 409s refresh in-dialog.
            container.querySelectorAll(".resolve-link").forEach((btn) => {
                btn.addEventListener("click", (e) => {
                    const el = e.currentTarget;
                    const caseId = el.getAttribute("data-case-id");
                    const stepId = el.getAttribute("data-step-id");
                    const revision = Number(el.getAttribute("data-revision"));
                    if (caseId && stepId && Number.isFinite(revision)) {
                        openResolveDialog(api, snap, stepId, caseId, revision, el);
                    }
                });
            });
            // Re-render cached events and logs if activeInspector already has them
            if (activeInspector) {
                const evs = activeInspector.getEvents();
                if (evs.length > 0) {
                    renderEvents(evs, false, null);
                }
            }
        }
    }
    function renderFreshness(f) {
        const badge = document.getElementById("stream-freshness-badge");
        if (!badge)
            return;
        badge.className = `badge freshness-badge freshness-${f.toLowerCase()}`;
        badge.textContent = f;
        badge.setAttribute("aria-label", `Stream status: ${f}`);
    }
    function renderEvents(events, hasMore, nextCursor) {
        const container = document.getElementById("events-container");
        if (!container)
            return;
        if (events.length === 0) {
            container.innerHTML =
                '<div class="text-muted">No events recorded yet.</div>';
            return;
        }
        const bounded = getBoundedEvents(events, eventsWindowStart, EVENTS_PAGE_SIZE);
        const visibleEvents = bounded.events;
        const cardsHtml = visibleEvents
            .map((ev) => {
            const time = new Date(ev.committedAt).toLocaleTimeString();
            const payloadStr = JSON.stringify(ev.payload, null, 2);
            return `
          <div class="event-card" data-sequence="${ev.sequence}">
            <div class="event-header">
              <span class="event-type">${escapeHtml(ev.type)}</span>
              <span class="event-seq">#${ev.sequence}</span>
            </div>
            <div class="event-time">${time}</div>
            <pre class="event-payload">${escapeHtml(payloadStr)}</pre>
          </div>
        `;
        })
            .join("");
        let loadMoreHtml = "";
        if (bounded.hasMore && nextCursor !== null) {
            loadMoreHtml = `<button id="load-more-events-btn" class="load-more-btn">Load Earlier Events (${events.length - visibleEvents.length} more in history)</button>`;
        }
        else if (hasMore && nextCursor !== null) {
            loadMoreHtml = `<button id="load-more-events-btn" class="load-more-btn">Load Earlier Events</button>`;
        }
        const windowInfo = events.length > EVENTS_PAGE_SIZE
            ? `
      <div class="events-window-info" role="status" aria-live="polite">
        Showing events ${bounded.offset + 1}–${Math.min(bounded.offset + visibleEvents.length, bounded.total)} of ${bounded.total}.
      </div>
    `
            : "";
        container.innerHTML = windowInfo + cardsHtml + loadMoreHtml;
        if (bounded.hasMore && nextCursor !== null) {
            const btn = document.getElementById("load-more-events-btn");
            if (btn) {
                btn.addEventListener("click", () => {
                    btn.textContent = "Loading...";
                    btn.setAttribute("disabled", "true");
                    eventsWindowStart += EVENTS_PAGE_SIZE;
                    activeInspector?.fetchEvents(nextCursor, true);
                });
            }
        }
    }
    function renderLogs(logs, error, stepId) {
        const container = document.getElementById("logs-container");
        if (!container)
            return;
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
        let warningNotice = "";
        if (logs.budgetExhausted) {
            warningNotice = `<div class="logs-notice logs-restricted">Log budget exhausted (1 MiB per attempt limit reached). ${logs.droppedCount ? logs.droppedCount + " records dropped." : ""}</div>`;
        }
        else if (logs.droppedCount) {
            warningNotice = `<div class="logs-notice logs-restricted">${logs.droppedCount} log record(s) dropped (exceeded 16 KiB per-line limit).</div>`;
        }
        const logBounded = virtualizeItems(logs.items, logsWindowStart, LOGS_PAGE_SIZE);
        const visibleLogItems = logBounded.items;
        // hasMore is true when server has more pages (nextCursor) OR local window has more items
        const logsHasMore = logs.nextCursor != null || logBounded.hasMore;
        const logLines = visibleLogItems
            .map((item) => {
            const time = new Date(item.timestamp).toLocaleTimeString();
            return `<div class="log-line log-${item.level}"><span class="log-time">${time}</span> <span class="log-level">[${item.level.toUpperCase()}]</span> <span class="log-msg">${escapeHtml(item.message)}</span></div>`;
        })
            .join("");
        let loadMoreHtml = "";
        if (logsHasMore) {
            loadMoreHtml = `<button id="load-more-logs-btn" class="load-more-btn">${logs.nextCursor ? "Load More Logs" : "Load More Logs (local)"}</button>`;
        }
        const logWindowInfo = logs.items.length > LOGS_PAGE_SIZE
            ? `
      <div class="logs-window-info" role="status" aria-live="polite">
        Showing logs ${logBounded.offset + 1}–${Math.min(logBounded.offset + visibleLogItems.length, logBounded.total)} of ${logBounded.total}.
      </div>
    `
            : "";
        container.innerHTML = `${warningNotice}${logWindowInfo}<div class="log-terminal">${logLines}</div>${loadMoreHtml}`;
        if (logsHasMore) {
            const btn = document.getElementById("load-more-logs-btn");
            if (btn) {
                btn.addEventListener("click", () => {
                    btn.textContent = "Loading...";
                    btn.setAttribute("disabled", "true");
                    logsWindowStart += LOGS_PAGE_SIZE;
                    activeInspector?.fetchLogs(stepId ?? selectedStepId ?? undefined, undefined, logs.nextCursor, true);
                });
            }
        }
    }
    function renderError(err) {
        // Bootstrap/API errors are unrelated to transient stream recovery and
        // must never be cleared implicitly on reconnect.
        markGlobalError(streamBanner, err.message);
        const banner = document.getElementById("global-error-banner");
        if (banner) {
            banner.textContent = err.message;
            banner.classList.remove("hidden");
            banner.dataset.errorKind = "global";
        }
    }
    function renderStreamError(err) {
        markStreamError(streamBanner, err.message);
        const banner = document.getElementById("global-error-banner");
        if (banner) {
            banner.textContent = err.message;
            banner.classList.remove("hidden");
            banner.dataset.errorKind = "stream";
        }
    }
    function clearBanner() {
        const banner = document.getElementById("global-error-banner");
        if (banner) {
            banner.textContent = "";
            banner.classList.add("hidden");
            delete banner.dataset.errorKind;
        }
    }
    function escapeHtml(str) {
        const div = document.createElement("div");
        div.textContent = str;
        return div.innerHTML;
    }
    // evidenceReference surfaces the hold's recorded reference (operation ID
    // or provider reference) without ever implying the outcome is known.
    function evidenceReference(c) {
        const ev = c.evidence;
        if (ev && typeof ev === "object") {
            for (const key of ["reference", "operationId", "externalRef"]) {
                if (typeof ev[key] === "string" && ev[key].length > 0) {
                    return ev[key];
                }
            }
        }
        return "";
    }
    // openResolveDialog offers only the three audited resolutions for an
    // unknown outcome. There is deliberately no blind "retry anyway": retries
    // go through confirm_not_executed_retry within budget, and every decision
    // binds the revision read at open time.
    function openResolveDialog(api, snap, stepId, caseId, revision, invoker) {
        closeResolveDialog();
        const step = snap.steps.find((s) => s.id === stepId);
        const overlay = document.createElement("div");
        overlay.className = "dialog-overlay";
        overlay.id = "resolve-dialog-overlay";
        const optionsHtml = RESOLVE_ACTIONS.map((opt, i) => `
        <label class="resolve-option">
          <input type="radio" name="resolve-action" value="${opt.action}" ${i === 0 ? "checked" : ""} />
          <span><strong>${escapeHtml(opt.label)}</strong><br />
          <span class="text-muted">${escapeHtml(opt.hint)}</span></span>
        </label>
      `).join("");
        overlay.innerHTML = `
      <div class="dialog" role="dialog" aria-modal="true" aria-labelledby="resolve-dialog-title">
        <h3 id="resolve-dialog-title">Resolve unknown outcome — ${escapeHtml(step?.nodeId ?? stepId)}</h3>
        <p class="text-muted">The provider may already have received this operation. Your decision is audited with your identity.</p>
        <fieldset>
          <legend>Decision</legend>
          ${optionsHtml}
        </fieldset>
        <label>Evidence reference (required)
          <input id="resolve-evidence" type="text" placeholder="e.g. provider payment ID, message ID" autocomplete="off" />
        </label>
        <label>Decision reason (required, max 280 characters)
          <input id="resolve-reason" type="text" placeholder="Why is this decision correct?" maxlength="280" autocomplete="off" />
        </label>
        <label id="resolve-result-label" style="display:none">Result JSON (required for confirm succeeded)
          <textarea id="resolve-result" rows="4" placeholder='{"key": "value"}'></textarea>
        </label>
        <div id="resolve-error" class="dialog-error" role="alert" style="display:none"></div>
        <div class="dialog-actions">
          <button id="resolve-cancel">Cancel</button>
          <button id="resolve-submit">Submit decision</button>
        </div>
      </div>
    `;
        document.body.appendChild(overlay);
        // One command identity for this decision: retries of the same ambiguous
        // submit reuse it, so the server can dedupe instead of double-deciding.
        const idempotencyKey = newIdempotencyKey();
        const errorBox = overlay.querySelector("#resolve-error");
        const evidenceInput = overlay.querySelector("#resolve-evidence");
        const reasonInput = overlay.querySelector("#resolve-reason");
        const resultLabel = overlay.querySelector("#resolve-result-label");
        const resultInput = overlay.querySelector("#resolve-result");
        const submitBtn = overlay.querySelector("#resolve-submit");
        const showError = (msg) => {
            errorBox.textContent = msg;
            errorBox.style.display = "block";
        };
        const syncResultVisibility = () => {
            const checked = overlay.querySelector('input[name="resolve-action"]:checked');
            resultLabel.style.display =
                checked?.value === "confirm_succeeded" ? "block" : "none";
        };
        overlay
            .querySelectorAll('input[name="resolve-action"]')
            .forEach((r) => r.addEventListener("change", syncResultVisibility));
        syncResultVisibility();
        const close = () => {
            closeResolveDialog();
            invoker?.focus();
        };
        overlay.querySelector("#resolve-cancel").addEventListener("click", close);
        overlay.addEventListener("keydown", (e) => {
            if (e.key === "Escape")
                close();
        });
        overlay.addEventListener("mousedown", (e) => {
            if (e.target === overlay)
                close();
        });
        evidenceInput.focus();
        submitBtn.addEventListener("click", () => {
            const checked = overlay.querySelector('input[name="resolve-action"]:checked');
            const action = (checked?.value ?? "confirm_succeeded");
            const evidence = evidenceInput.value.trim();
            if (!evidence) {
                showError("Evidence reference is required.");
                return;
            }
            const reason = reasonInput.value.trim();
            if (!reason) {
                showError("Decision reason is required.");
                return;
            }
            if (reason.length > 280) {
                showError("Decision reason must be at most 280 characters.");
                return;
            }
            let result;
            if (action === "confirm_succeeded") {
                if (!resultInput.value.trim()) {
                    showError("Result JSON is required to confirm success.");
                    return;
                }
                try {
                    result = JSON.parse(resultInput.value);
                }
                catch {
                    showError("Result must be valid JSON.");
                    return;
                }
            }
            submitBtn.setAttribute("disabled", "true");
            const body = { action, evidence, reason, expectedRevision: revision };
            if (action === "confirm_succeeded")
                body.result = result;
            api
                .resolveReconciliationCase(caseId, body, idempotencyKey)
                .then(() => {
                close();
                void activeInspector
                    ?.fetchSnapshot()
                    .catch((err) => renderError(err instanceof Error ? err : new Error(String(err))));
            })
                .catch((err) => {
                submitBtn.removeAttribute("disabled");
                if (isConflict(err)) {
                    showError("This case changed since you opened it (409). The latest state was reloaded — review it before acting.");
                    void activeInspector?.fetchSnapshot().catch(() => undefined);
                    return;
                }
                showError(err instanceof Error ? err.message : String(err));
            });
        });
    }
    function closeResolveDialog() {
        document.getElementById("resolve-dialog-overlay")?.remove();
    }
    // openApprovalDialog records one human approve/reject decision for a
    // durable approval (Blueprint §16.3, §23.2).
    //
    // Two things the wording must not blur:
    //   - Approve and reject are both successful outcomes. The node collected a
    //     valid decision; whether that decision is good for the business is a
    //     later choice's job, so rejecting does not fail the run.
    //   - An approval can expire. The deadline is database time, so the dialog
    //     states it rather than implying the operator can decide at will.
    function openApprovalDialog(api, approval, invoker) {
        document.getElementById("approval-dialog-overlay")?.remove();
        const overlay = document.createElement("div");
        overlay.className = "dialog-overlay";
        overlay.id = "approval-dialog-overlay";
        let currentRevision = approval.revision;
        const payloadText = approval.payload === undefined || approval.payload === null
            ? ""
            : escapeHtml(JSON.stringify(approval.payload, null, 2));
        overlay.innerHTML = `
      <div class="dialog" role="dialog" aria-modal="true" aria-labelledby="approval-dialog-title">
        <h3 id="approval-dialog-title">${approval.status === "PENDING" ? "Decide approval" : "Approval already decided"} — ${escapeHtml(approval.workflowName)}</h3>
        <p class="text-muted">Node <strong>${escapeHtml(approval.nodeId)}</strong> is waiting on this decision.
        Approving and rejecting are both successful step outcomes: the workflow continues either way
        and a following branch decides what the answer means.</p>
        ${payloadText ? `<pre class="hold-payload">${payloadText}</pre>` : ""}
        <div class="hold-meta">
          Approval status ${escapeHtml(approval.status)} · revision ${currentRevision}
          ${approval.expiresAt ? `· expires ${escapeHtml(approval.expiresAt)}` : ""}
        </div>
        <label class="dialog-label" for="approval-comment">Comment (optional, recorded with the decision)</label>
        <textarea id="approval-comment" rows="3" aria-describedby="approval-dialog-title"></textarea>
        <div id="approval-error" class="dialog-error" role="alert" style="display:none"></div>
        <div class="dialog-actions">
          <button id="approval-dismiss">Close</button>
          <button id="approval-reject">Reject</button>
          <button id="approval-approve">Approve</button>
        </div>
      </div>
    `;
        document.body.appendChild(overlay);
        const errorBox = overlay.querySelector("#approval-error");
        const commentBox = overlay.querySelector("#approval-comment");
        const approveBtn = overlay.querySelector("#approval-approve");
        const rejectBtn = overlay.querySelector("#approval-reject");
        const close = () => {
            document.getElementById("approval-dialog-overlay")?.remove();
            invoker?.focus();
        };
        overlay.querySelector("#approval-dismiss").addEventListener("click", close);
        overlay.addEventListener("keydown", (e) => {
            if (e.key === "Escape")
                close();
        });
        overlay.addEventListener("mousedown", (e) => {
            if (e.target === overlay)
                close();
        });
        approveBtn.focus();
        // A decided approval is terminal. Render the state honestly instead of
        // offering an action that the backend will reject.
        if (approval.status !== "PENDING") {
            approveBtn.setAttribute("disabled", "true");
            rejectBtn.setAttribute("disabled", "true");
            errorBox.textContent = `This approval is already ${approval.status}. Decisions are final.`;
            errorBox.style.display = "block";
        }
        const submit = (decision) => {
            approveBtn.setAttribute("disabled", "true");
            rejectBtn.setAttribute("disabled", "true");
            // One command identity per decision attempt, reused on ambiguous retry.
            const idempotencyKey = newIdempotencyKey();
            api
                .decideApproval(approval.id, decision, currentRevision, commentBox.value.trim() || undefined, idempotencyKey)
                .then(() => {
                close();
                void activeInspector
                    ?.fetchSnapshot()
                    .catch((err) => renderError(err instanceof Error ? err : new Error(String(err))));
            })
                .catch((err) => {
                approveBtn.removeAttribute("disabled");
                rejectBtn.removeAttribute("disabled");
                if (isConflict(err)) {
                    errorBox.textContent =
                        "Someone else already acted on this approval, or it expired (409). The run was reloaded — review the current state before deciding again.";
                    errorBox.style.display = "block";
                    void activeInspector
                        ?.fetchSnapshot()
                        .then((latest) => {
                        const current = latest?.approvals?.find((a) => a.id === approval.id);
                        if (current) {
                            currentRevision = current.revision;
                            if (current.status !== "PENDING") {
                                approveBtn.setAttribute("disabled", "true");
                                rejectBtn.setAttribute("disabled", "true");
                                errorBox.textContent = `This approval is now ${current.status}. Decisions are final.`;
                            }
                        }
                    })
                        .catch(() => undefined);
                    return;
                }
                errorBox.textContent =
                    err instanceof Error ? err.message : String(err);
                errorBox.style.display = "block";
            });
        };
        approveBtn.addEventListener("click", () => submit("approved"));
        rejectBtn.addEventListener("click", () => submit("rejected"));
    }
    // renderApprovalsPanel lists the run's approvals with the decision CTA.
    // The CTA renders only for a PENDING approval when the active identity holds
    // approvals:decide; the backend remains the authority.
    function renderApprovalsPanel(api, snap) {
        const approvals = snap.approvals ?? [];
        if (approvals.length === 0)
            return "";
        const rows = approvals
            .map((a) => {
            const decidable = a.status === "PENDING" && canDecideApprovals;
            const detail = a.decidedAt
                ? `decided ${a.decidedAt}${a.actorId ? ` by ${escapeHtml(a.actorId)}` : ""}${a.decisionComment ? ` — ${escapeHtml(a.decisionComment)}` : ""}`
                : a.expiresAt
                    ? `expires ${escapeHtml(a.expiresAt)}`
                    : "no expiry";
            const action = decidable
                ? `<button class="secondary" data-approval-open="${escapeHtml(a.id)}">Decide</button>`
                : "";
            return `
        <div class="hold-item" data-approval-item="${escapeHtml(a.id)}">
          <div>
            <strong>${escapeHtml(a.nodeId)}</strong>
            <span class="badge">${escapeHtml(a.status)}</span>
            <div class="text-muted">${detail}</div>
          </div>
          ${action}
        </div>`;
        })
            .join("");
        return `
      <section class="hold-panel" aria-labelledby="approvals-panel-title">
        <h3 id="approvals-panel-title">Approvals</h3>
        <p class="text-muted">Human decisions this run is waiting on or has recorded.
        An approval is a control node: it holds no worker, lease, or attempt.</p>
        ${rows}
      </section>`;
    }
    // wireApprovalActions binds the Decide buttons produced by the panel.
    function wireApprovalActions(api, snap, root) {
        for (const btn of Array.from(root.querySelectorAll("[data-approval-open]"))) {
            btn.addEventListener("click", (e) => {
                const id = e.currentTarget.getAttribute("data-approval-open");
                const approval = (snap.approvals ?? []).find((a) => a.id === id);
                if (!approval)
                    return;
                openApprovalDialog(api, approval, e.currentTarget);
            });
        }
    }
    // openCancelDialog confirms durable cancellation. Cancelling stops
    // platform execution and revokes worker ownership; it never rolls back
    // external side effects already performed.
    function openCancelDialog(api, snap, invoker) {
        closeCancelDialog();
        const overlay = document.createElement("div");
        overlay.className = "dialog-overlay";
        overlay.id = "cancel-dialog-overlay";
        let currentRevision = snap.revision;
        let currentStatus = snap.status;
        overlay.innerHTML = `
      <div class="dialog" role="dialog" aria-modal="true" aria-labelledby="cancel-dialog-title">
        <h3 id="cancel-dialog-title">Cancel run — ${escapeHtml(snap.workflowName)}</h3>
        <p class="text-muted">This revokes worker ownership and stops nonterminal work immediately.
        Already-succeeded steps are preserved. External effects already performed are
        <strong>not</strong> rolled back — verify provider state afterwards.</p>
        <div class="hold-meta">Run revision ${currentRevision} (${currentStatus})</div>
        <div id="cancel-error" class="dialog-error" role="alert" style="display:none"></div>
        <div class="dialog-actions">
          <button id="cancel-dismiss">Keep running</button>
          <button id="cancel-confirm">Confirm cancel</button>
        </div>
      </div>
    `;
        document.body.appendChild(overlay);
        // One command identity for this decision, reused if the submit is
        // ambiguously delivered.
        const idempotencyKey = newIdempotencyKey();
        const errorBox = overlay.querySelector("#cancel-error");
        const metaBox = overlay.querySelector(".hold-meta");
        const confirmBtn = overlay.querySelector("#cancel-confirm");
        const close = () => {
            closeCancelDialog();
            invoker?.focus();
        };
        overlay.querySelector("#cancel-dismiss").addEventListener("click", close);
        overlay.addEventListener("keydown", (e) => {
            if (e.key === "Escape")
                close();
        });
        overlay.addEventListener("mousedown", (e) => {
            if (e.target === overlay)
                close();
        });
        confirmBtn.focus();
        confirmBtn.addEventListener("click", () => {
            confirmBtn.setAttribute("disabled", "true");
            api
                .cancelRun(snap.id, currentRevision, idempotencyKey)
                .then(() => {
                close();
                void activeInspector
                    ?.fetchSnapshot()
                    .catch((err) => renderError(err instanceof Error ? err : new Error(String(err))));
            })
                .catch((err) => {
                confirmBtn.removeAttribute("disabled");
                if (isConflict(err)) {
                    errorBox.textContent =
                        "This run changed since you opened it (409). The latest state was reloaded — review it before acting.";
                    errorBox.style.display = "block";
                    void activeInspector
                        ?.fetchSnapshot()
                        .then((latest) => {
                        if (latest) {
                            currentRevision = latest.revision;
                            currentStatus = latest.status;
                            metaBox.textContent = `Run revision ${currentRevision} (${currentStatus})`;
                            if (currentStatus === "CANCELLED" ||
                                currentStatus === "SUCCEEDED" ||
                                currentStatus === "FAILED") {
                                confirmBtn.setAttribute("disabled", "true");
                                errorBox.textContent = `Run is now ${currentStatus}. No further actions can be taken.`;
                            }
                        }
                    })
                        .catch(() => undefined);
                    return;
                }
                errorBox.textContent =
                    err instanceof Error ? err.message : String(err);
                errorBox.style.display = "block";
            });
        });
    }
    function closeCancelDialog() {
        document.getElementById("cancel-dialog-overlay")?.remove();
    }
    // openPauseDialog confirms durable pause. Pausing stops new task claims
    // while in-flight claims finish or drain.
    function openPauseDialog(api, snap, invoker) {
        closePauseDialog();
        const overlay = document.createElement("div");
        overlay.className = "dialog-overlay";
        overlay.id = "pause-dialog-overlay";
        let currentRevision = snap.revision;
        let currentStatus = snap.status;
        overlay.innerHTML = `
      <div class="dialog" role="dialog" aria-modal="true" aria-labelledby="pause-dialog-title">
        <h3 id="pause-dialog-title">Pause run — ${escapeHtml(snap.workflowName)}</h3>
        <p class="text-muted">Pausing blocks new task claims immediately.
        In-flight attempts remain active and may start or finish normally.
        The run drains to <code>PAUSED</code> once in-flight tasks finish.</p>
        <div class="hold-meta">Run revision ${currentRevision} (${currentStatus})</div>
        <div id="pause-error" class="dialog-error" role="alert" style="display:none"></div>
        <div class="dialog-actions">
          <button id="pause-dismiss">Keep running</button>
          <button id="pause-confirm">Confirm pause</button>
        </div>
      </div>
    `;
        document.body.appendChild(overlay);
        const idempotencyKey = newIdempotencyKey();
        const errorBox = overlay.querySelector("#pause-error");
        const metaBox = overlay.querySelector(".hold-meta");
        const confirmBtn = overlay.querySelector("#pause-confirm");
        const close = () => {
            closePauseDialog();
            invoker?.focus();
        };
        overlay.querySelector("#pause-dismiss").addEventListener("click", close);
        overlay.addEventListener("keydown", (e) => {
            if (e.key === "Escape")
                close();
        });
        overlay.addEventListener("mousedown", (e) => {
            if (e.target === overlay)
                close();
        });
        confirmBtn.focus();
        confirmBtn.addEventListener("click", () => {
            confirmBtn.setAttribute("disabled", "true");
            api
                .pauseRun(snap.id, currentRevision, idempotencyKey)
                .then(() => {
                close();
                void activeInspector
                    ?.fetchSnapshot()
                    .catch((err) => renderError(err instanceof Error ? err : new Error(String(err))));
            })
                .catch((err) => {
                confirmBtn.removeAttribute("disabled");
                if (isConflict(err)) {
                    errorBox.textContent =
                        "This run changed since you opened it (409). The latest state was reloaded — review it before acting.";
                    errorBox.style.display = "block";
                    void activeInspector
                        ?.fetchSnapshot()
                        .then((latest) => {
                        if (latest) {
                            currentRevision = latest.revision;
                            currentStatus = latest.status;
                            metaBox.textContent = `Run revision ${currentRevision} (${currentStatus})`;
                            if (currentStatus === "PAUSED" ||
                                currentStatus === "PAUSING") {
                                confirmBtn.setAttribute("disabled", "true");
                                errorBox.textContent = `Run is already ${currentStatus}.`;
                            }
                            else if (currentStatus === "CANCELLED" ||
                                currentStatus === "SUCCEEDED" ||
                                currentStatus === "FAILED" ||
                                currentStatus === "CANCELLING") {
                                confirmBtn.setAttribute("disabled", "true");
                                errorBox.textContent = `Run is now ${currentStatus}; cannot pause.`;
                            }
                        }
                    })
                        .catch(() => undefined);
                    return;
                }
                errorBox.textContent =
                    err instanceof Error ? err.message : String(err);
                errorBox.style.display = "block";
            });
        });
    }
    function closePauseDialog() {
        document.getElementById("pause-dialog-overlay")?.remove();
    }
    // openResumeDialog confirms resuming a paused or pausing run.
    function openResumeDialog(api, snap, invoker) {
        closeResumeDialog();
        const overlay = document.createElement("div");
        overlay.className = "dialog-overlay";
        overlay.id = "resume-dialog-overlay";
        let currentRevision = snap.revision;
        let currentStatus = snap.status;
        overlay.innerHTML = `
      <div class="dialog" role="dialog" aria-modal="true" aria-labelledby="resume-dialog-title">
        <h3 id="resume-dialog-title">Resume run — ${escapeHtml(snap.workflowName)}</h3>
        <p class="text-muted">Resuming clears the pause flag and recomputes eligibility from durable state.
        Eligible tasks can be claimed immediately.</p>
        <div class="hold-meta">Run revision ${currentRevision} (${currentStatus})</div>
        <div id="resume-error" class="dialog-error" role="alert" style="display:none"></div>
        <div class="dialog-actions">
          <button id="resume-dismiss">Cancel</button>
          <button id="resume-confirm">Confirm resume</button>
        </div>
      </div>
    `;
        document.body.appendChild(overlay);
        const idempotencyKey = newIdempotencyKey();
        const errorBox = overlay.querySelector("#resume-error");
        const metaBox = overlay.querySelector(".hold-meta");
        const confirmBtn = overlay.querySelector("#resume-confirm");
        const close = () => {
            closeResumeDialog();
            invoker?.focus();
        };
        overlay.querySelector("#resume-dismiss").addEventListener("click", close);
        overlay.addEventListener("keydown", (e) => {
            if (e.key === "Escape")
                close();
        });
        overlay.addEventListener("mousedown", (e) => {
            if (e.target === overlay)
                close();
        });
        confirmBtn.focus();
        confirmBtn.addEventListener("click", () => {
            confirmBtn.setAttribute("disabled", "true");
            api
                .resumeRun(snap.id, currentRevision, idempotencyKey)
                .then(() => {
                close();
                void activeInspector
                    ?.fetchSnapshot()
                    .catch((err) => renderError(err instanceof Error ? err : new Error(String(err))));
            })
                .catch((err) => {
                confirmBtn.removeAttribute("disabled");
                if (isConflict(err)) {
                    errorBox.textContent =
                        "This run changed since you opened it (409). The latest state was reloaded — review it before acting.";
                    errorBox.style.display = "block";
                    void activeInspector
                        ?.fetchSnapshot()
                        .then((latest) => {
                        if (latest) {
                            currentRevision = latest.revision;
                            currentStatus = latest.status;
                            metaBox.textContent = `Run revision ${currentRevision} (${currentStatus})`;
                            if (currentStatus !== "PAUSED" &&
                                currentStatus !== "PAUSING") {
                                confirmBtn.setAttribute("disabled", "true");
                                errorBox.textContent = `Run is now ${currentStatus}; cannot resume.`;
                            }
                        }
                    })
                        .catch(() => undefined);
                    return;
                }
                errorBox.textContent =
                    err instanceof Error ? err.message : String(err);
                errorBox.style.display = "block";
            });
        });
    }
    function closeResumeDialog() {
        document.getElementById("resume-dialog-overlay")?.remove();
    }
}
//# sourceMappingURL=index.js.map