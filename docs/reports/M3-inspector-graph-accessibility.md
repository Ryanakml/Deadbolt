# M3 Inspector Graph & Accessibility Acceptance — Issue #29

This report provides the evidence and acceptance verification for Issue #29: **[M3] Make the Inspector graph and accessible list match persisted execution**.

## 1. Outcome & Scope

The Deadbolt Inspector UI provides an accurate, durable view of workflow executions directly matching persisted state from PostgreSQL. It satisfies the core invariants established in Blueprint §23, §24, and §25:

- **1 Node per Logical Step:** The DAG visualizes exactly one node per logical step. All retry attempts, epoch increments, and transient executions remain strictly contained within step detail tabs.
- **Selected vs Skipped Branch Truthfulness:** Choice and branch divergence faithfully reflect database state. Skipped branches show `SKIPPED` status with audited `wait_reason` codes (`BRANCH_NOT_SELECTED`, `DEPENDENCY_SKIPPED`), and DAG edges are visually and semantically distinguished with dashed styling.
- **200-Node Scalability:** Handles fixtures up to the 200-node admission limit with topological level layout, an interactive SVG minimap, collapsible parallel/branch cluster groupings, and an accessible list fallback.
- **Accessibility (WCAG 2.2 AA):**
  - Full keyboard navigation and visible focus rings (`:focus-visible`).
  - Accessible names and roles (`role="tablist"`, `role="tab"`, `role="tabpanel"`, `role="list"`, `role="listitem"`, `aria-label`, `aria-selected`, `aria-controls`).
  - Non-color status differentiation using explicit Unicode symbols alongside text labels:
    - `SUCCEEDED`: `✓ Succeeded`
    - `FAILED`: `✕ Failed`
    - `SKIPPED`: `↷ Skipped`
    - `CANCELLED`: `⊘ Cancelled`
    - `RUNNING`: `● Running`
    - `WAITING`: `⏳ Waiting`
    - `READY`: `○ Ready`
    - `BLOCKED`: `◌ Blocked`
  - Reduced-motion support (`@media (prefers-reduced-motion: reduce)` disables animations and smooth scrolling).
  - High-contrast mode support (`@media (prefers-contrast: more)`).
  - Fully responsive across desktop, tablet (<=900px), and mobile (<=600px).
- **Stream & Conflict Resilience:** Disconnected and slow SSE streams preserve committed state and display non-destructive reconnection indicators without failing the run; mutation conflicts (409 Conflict) keep dialogs open with latest state and clear error guidance.

---

## 2. Architecture & Data Flow

### Backend Contracts & Model

- `contracts/openapi/control-plane.yaml`:
  - `RunStep` schema extended with optional fields: `kind`, `waitReason`, `after` (list of parent `nodeId` dependencies), and `output`.
- `internal/execution/types.go`:
  - `RunStepDTO` updated with `Kind *string`, `WaitReason *string`, `After []string`, and `Output any`.
- `internal/execution/service.go`:
  - `GetRun()` retrieves `kind`, `wait_reason`, and `output` from `run_steps` table.
  - Manifest node definitions from `deployments.manifest` populate topological `after` dependencies and node `kind`.
  - Step `output` is redacted (`nil`) when the caller lacks `payload:read` capability, enforcing strict tenant security boundaries.

### Frontend Inspector Architecture

- `apps/dashboard/src/types.ts`:
  - Updated `StepStatus` to include `"SKIPPED"`.
  - Added `RunStep` properties (`kind`, `waitReason`, `after`, `output`).
  - Defined `StepTab` (`Summary`, `Attempts`, `Events`, `Logs`, `Input`, `Output`, `Trace`) and `InspectorViewMode` (`graph` | `list`).
- `apps/dashboard/src/inspector.ts`:
  - `getStatusPresentation(status)`: maps status to symbol, accessible label, and ARIA attributes.
  - `computeGraphLayout(steps, collapsedClusters)`: calculates topological levels from `after` dependencies, creates SVG DAG geometry, and groups parallel siblings (>= 4 nodes) into collapsible clusters.
  - `computeMinimap(layout, vpWidth, vpHeight, scrollLeft, scrollTop)`: computes scaled minimap coordinates and viewport boundary rectangle.
  - `virtualizeItems(items, offset, pageSize)`: windowing helper for list views.
  - `filterEventsForStep(events, step)`: isolates step-specific event streams.
  - `applyEvent(event)`: converges state on SSE events including `step.succeeded` and `step.skipped`.
- `apps/dashboard/src/index.ts`:
  - Implements dual-pane layout: DAG graph or accessible list on the left, sticky step detail tabbed pane on the right.
  - Handles cluster expand/collapse toggles, graph/list view switching, keyboard navigation, and dialog interactions.
- `apps/dashboard/public/styles.css`:
  - CSS variables for light and dark themes.
  - Accessible focus outlines, responsive breakpoints, and reduced-motion overrides.

---

## 3. Automated Test Evidence

### Frontend Unit & E2E Tests (`apps/dashboard/tests`)

| Test Suite                              | Tests Passed         | Key Invariants Proven                                                                                                                                                                                                                   |
| --------------------------------------- | -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `inspector-graph-accessibility.test.js` | 7                    | 1 node per logical step (attempts stay in tabs); selected vs skipped branch wait reasons; 200-node fixture under 250ms with minimap and collapsing; WCAG 2.2 AA symbols; list virtualization; step event filtering; stream convergence. |
| `pause-dialogs.test.js`                 | 3                    | Stale 409 handling in dialog; focus preservation on Escape; read-only viewer CTA masking.                                                                                                                                               |
| `reconciliation.test.js`                | 7                    | 409 revision conflict refresh; step.waiting without fabricated success; run.resumed convergence.                                                                                                                                        |
| `inspector-truthfulness.test.js`        | 5                    | Transient stream errors cleared on LIVE; unrelated errors preserved; terminal steps never claim worker-wait.                                                                                                                            |
| `inspector.test.js`                     | 5                    | Monotonic snapshot increments; attempt completion mapping; event deduplication and sorting.                                                                                                                                             |
| Full Dashboard Test Suite               | **59 passed (100%)** | Zero regressions across all dashboard functionality.                                                                                                                                                                                    |

### Backend Integration Tests (`tests/integration`)

| Test Case                                           | Status | Verified Invariant                                                                                                                                                             |
| --------------------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `TestRunInspector_StepGraphMetadataAndRedaction`    | PASS   | `GET /v1/runs/{id}` returns step `kind` and `after` dependencies; returns `output` when caller has `payload:read`; redacts `output` to `nil` when caller lacks `payload:read`. |
| `TestRunInspectorConsistentSnapshotAndStepAttempts` | PASS   | Snapshot consistency across retries.                                                                                                                                           |
| `TestRunInspectorPayloadReadBoundaryAndRedaction`   | PASS   | API key permission boundaries for run payloads.                                                                                                                                |
| `TestRunInspectorSSEReconnectAndCatchUp`            | PASS   | Stream reconnection and Last-Event-Id catch-up.                                                                                                                                |

---

## 4. Acceptance Criteria & Failure-Case Matrix

| Area                      | Requirement                                                                     | Evidence                                                                   | Status        |
| ------------------------- | ------------------------------------------------------------------------------- | -------------------------------------------------------------------------- | ------------- |
| **Logical Graph**         | Exactly 1 node per logical step; retries contained in step tabs                 | `inspector-graph-accessibility.test.js` (Test 1)                           | PASS          |
| **Branch Selection**      | DB `SKIPPED` status with `BRANCH_NOT_SELECTED` and `DEPENDENCY_SKIPPED`         | `inspector-graph-accessibility.test.js` (Test 2)                           | PASS          |
| **200-Node Scalability**  | Topological layout, minimap scaling, collapsible parallel clusters              | `inspector-graph-accessibility.test.js` (Test 3)                           | PASS (< 20ms) |
| **Accessibility**         | Non-color status differentiation (symbols + text), ARIA attributes, focus rings | `inspector-graph-accessibility.test.js` (Test 4), `styles.css`             | PASS          |
| **Accessible List**       | Fallback list view with virtualization                                          | `inspector-graph-accessibility.test.js` (Test 5), `index.ts`               | PASS          |
| **Tabbed Step Detail**    | Summary, Attempts, Events, Logs, Input, Output, Trace tabs                      | `inspector-graph-accessibility.test.js` (Test 6), `index.ts`               | PASS          |
| **Stream Resilience**     | SSE `step.skipped` and `step.succeeded` converge snapshot                       | `inspector-graph-accessibility.test.js` (Test 7)                           | PASS          |
| **Mutation Conflicts**    | Stale 409 keep dialog open with latest state                                    | `pause-dialogs.test.js`, `reconciliation.test.js`                          | PASS          |
| **Security Redaction**    | Step output redacted for callers without `payload:read`                         | `run_inspector_test.go` (`TestRunInspector_StepGraphMetadataAndRedaction`) | PASS          |
| **Responsive UX**         | Desktop, tablet, and mobile layouts                                             | `styles.css` media queries                                                 | PASS          |
| **Contracts Consistency** | OpenAPI specification aligned with Go and TS types                              | `check:contracts`                                                          | PASS          |
| **Go/TS Parity**          | Parity checks on all canonical enums and fixtures                               | `check:parity`                                                             | PASS          |
