# Quickstart: Deadbolt Developer CLI Journey

This guide walks you through the end-to-end Deadbolt developer journey using exclusively official, public command-line and API interfaces:

```text
clean project ↓ runtime init ↓ runtime dev ↓ runtime build ↓ runtime doctor ↓ runtime login
      ↓ runtime deploy ↓ runtime worker enroll/start ↓ runtime deployments activate
      ↓ runtime runs create ↓ runtime runs inspect & logs
```

Every step adheres to **Blueprint §4, §6, §12, §14, §20, §22, §24** and **ADR-01, ADR-02, ADR-08, ADR-10**.

> [!IMPORTANT]
> **No Database Backdoors:** Product operations never use direct PostgreSQL modifications or internal shortcuts. All orchestration is managed through official authenticated HTTP APIs and Ed25519 worker protocols.
> **Zero-Leak Secret Protection:** Customer secret values are never printed in diagnostic reports, logs, or command output.

---

## Prerequisites

- **Go 1.24+**
- **Node.js 24.x** (`v24.21.0` pinned in runtime contracts)
- **Docker & Docker Compose** (for running local control-plane and broker services)

Install the Deadbolt CLI from the packaged distribution (binary plus
required companion assets). A bare `go build` alone is only a repository
development build: without `share/deadbolt/` assets, `runtime dev` and
`runtime worker start` fail fast instead of working.

```bash
go build -o bin/runtime ./cmd/runtime
./scripts/package-cli-distribution.sh bin/runtime "<control-plane-image@sha256:...>" <install-prefix>
export PATH="<install-prefix>/bin:$PATH"
```

Verify the installation:

```bash
runtime --help
```

### Packaged Distribution & Companion Assets

The install prefix layout is the supported standalone contract
(`runtime dev` and `runtime worker start` resolve assets relative to the
executable, never from a source checkout):

- `<install-prefix>/bin/runtime`
- `<install-prefix>/share/deadbolt/compose.yaml`
- `<install-prefix>/share/deadbolt/release.json`
- `<install-prefix>/share/deadbolt/runner/index.js`

> [!NOTE]
> **Repository development vs packaged use:** inside a source checkout you
> may run an uninstalled binary with `DEADBOLT_DEV_ASSETS=1` (Compose) and
> `--runner-path`/`DEADBOLT_RUNNER_PATH` (runner). Outside a checkout, only
> the packaged layout above is supported.

---

## 1. Project Initialization (`runtime init`)

Scaffold a clean workflow project containing valid DAG definitions, task handlers, and schema declarations:

```bash
runtime init --name order-service --dir ./order-service
cd ./order-service
```

This creates:

- `deadbolt.config.json`: Non-secret project configuration (name, default workflow, required secret names).
- `package.json` and `package-lock.json`: Project manifest and lock material.
- `workflow.json`: Declarative DAG workflow specification (`validate` → `provision` → `notify`) with explicit JSON Pointer input/output mappings.
- `tasks.json`: Task contract definitions with JSON schemas and explicit recovery policies (`safe` or `idempotent`).
- `tasks/validate.js`, `tasks/provision.js`, `tasks/notify.js`: Pure JavaScript task implementations.
- `.env.example`: Secret name placeholders (never commit actual secrets).
- `.gitignore`: Ensures secrets and compiled bundles are never committed.

---

## 2. Local Development Stack (`runtime dev`)

Start the local orchestration stack (Control Plane, PostgreSQL, NATS, MinIO) and two local worker processes in one command:

```bash
runtime dev
```

Key local mode characteristics:

- **Account-free & loopback-restricted:** Runs strictly on `127.0.0.1:8080`.
- **Zero cloud telemetry:** Leaks no customer data or telemetry outside the local host.
- **Two local worker processes:** Automatically enrolls and runs `worker-a` and `worker-b` as separate processes with distinct PIDs, satisfying local HA activation preflight requirements out of the box.
- **Auto-terminating:** Press `Ctrl+C` to gracefully drain running tasks and stop all containers.

### Local canonical context

Inside a Deadbolt workspace, `runtime dev` uses the workspace project
identity from `DEADBOLT_PROJECT` first, otherwise `deadbolt.config.json`
(`project`). Local bootstrap exact-selects that project by name or creates
it when missing; it never picks `projects[0]`. The `development`
environment (or an explicit custom local env) is selected or created inside
that exact project.

Bootstrap then stores the canonical local CLI context:

- `org_id`, `project_id`, `project`, `env_id`, `env`

together with the generated local API credential. Automatic worker
enrollment during `runtime dev` consumes the stored canonical `EnvID` and
deliberately does not pass `--env development`, so name re-resolution
cannot diverge from the already-established workspace context.
`runtime login --local` outside a workspace falls back to project
`"default"`. Explicit user-supplied `--env` behavior is unchanged. Local
bootstrap uses only public HTTP APIs and is idempotent: rerunning selects
the same project and environment instead of duplicating them.

---

## 3. Deterministic Bundle Assembly (`runtime build`)

Compile your workflow code into an immutable `.tar` bundle archive and a validated deployment manifest:

```bash
runtime build --dir . --os linux
```

When `--arch` is omitted, the build targets the current host architecture (or the project config `targetArch`); pass `--arch amd64|arm64` only to cross-compile for an explicit deployment target. Hosted/CI callers that need `linux/amd64` select it explicitly. The target platform is embedded in the bundle (`.deadbolt/platform.json`) and participates in the bundle digest, so different targets always produce different digests.

Output:

- `dist/manifest.json`: Strictly validated against `contracts/manifest/deployment.schema.json`.
- `bundles/<bundleDigest>.tar`: Deterministic archive with sorted tar headers and clamped timestamps.

Key outputs:

- **Bundle Digest (SHA-256):** `sha256(bundle.tar)` pinned identity.
- **Dependency Lock Digest (SHA-256):** Deterministic lock hash.

---

## 4. Environment & Health Diagnostics (`runtime doctor`)

Run actionable diagnostic checks before deploying or activating workloads:

```bash
runtime doctor --manifest ./dist/manifest.json --bundle-dir ./bundles
```

The doctor command verifies:

- Docker daemon availability.
- Node.js runtime toolchain conformance (Node 24.x).
- Control plane `/livez` and `/readyz` health endpoints.
- Deployment manifest schema validation.
- Bundle archive integrity and digest matching.
- Host architecture compatibility (`darwin/arm64`, `linux/amd64`, etc.).
- Required task secret presence in worker environment (**masked as `Present (value masked)`**).

---

## 5. Hosted Authentication (`runtime login`)

Authenticate the CLI against your Deadbolt control plane:

### Interactive Browser Login (PKCE OAuth)

```bash
runtime login --control-plane-url https://api.deadbolt.cloud
```

### Headless / API Key Authentication

```bash
runtime login --control-plane-url https://api.deadbolt.cloud --api-key <YOUR_ADMIN_KEY> --org <ORG_ID> --env staging
```

Credentials are automatically stored in the secure OS Keychain:

1. **macOS Keychain:** Managed securely via macOS Keychain Services (`security`).
2. **Linux Secret Service:** Managed via FreeDesktop Secret Service (`secret-tool`).
3. **Headless / CI Automation:** Explicit credentials directory configured via `DEADBOLT_CREDENTIALS_DIR`.

> [!IMPORTANT]
> **No Insecure Storage:** Deadbolt requires a functional OS Keychain or explicit test credentials directory (`DEADBOLT_CREDENTIALS_DIR`). Plaintext fallback storage is strictly rejected.

---

## 5b. Hosted Context Bootstrap (`runtime bootstrap`)

A fresh hosted identity starts with zero organizations. Establish organization, project, and environment context using only supported CLI commands — no database writes, no raw curl, no manual UUID copying:

```bash
runtime bootstrap --org-name "acme" --project "order-service" --env staging --control-plane-url https://api.deadbolt.cloud
```

Behavior:

- **Zero organizations:** creates the named organization and selects it.
- **Exactly one organization:** selects it deterministically.
- **Several organizations:** fails with the list; rerun with `--org <id>`.
- **Project/environment:** selected by name when present, created otherwise. Rerunning is safe and creates no duplicates.
- Project name defaults to `deadbolt.config.json`; environment defaults to stored context, then `staging`.

The selected organization, project, and environment — including the canonical project/environment IDs — are stored in the OS credential store and become the defaults for `deploy`, `activate`, and `runs` commands. The canonical environment UUID is stored alongside the human-readable names, so later commands target exactly the bootstrapped environment: explicit `--env <UUID>` always resolves exactly, and an environment name shared by several projects fails with an explicit ambiguity error instead of silently picking one.

---

## 6. Manifest Registration (`runtime deploy`)

Register your immutable deployment manifest on the control plane:

```bash
runtime deploy --env staging --manifest ./dist/manifest.json
```

> [!NOTE]
> **Manifest-Only Registration:** Deadbolt's control plane never accepts or stores customer source code. Only the metadata manifest and cryptographic digests are registered. Worker nodes load bundles directly from local storage or private artifact repositories.

---

## 7. Self-Hosted Worker Enrollment & Startup (`runtime worker`)

Deadbolt uses Ed25519 cryptographic challenge-response nonces for mutual authentication.

### Step 7a: Enroll Worker 1 & Worker 2

```bash
# Enroll Worker 1
runtime worker enroll --control-plane-url https://api.deadbolt.cloud --key-path ~/.deadbolt/worker1.key --env staging --create-token

# Enroll Worker 2
runtime worker enroll --control-plane-url https://api.deadbolt.cloud --key-path ~/.deadbolt/worker2.key --env staging --create-token
```

This generates an Ed25519 keypair, signs the single-use challenge nonce from `/worker/v1/challenge`, binds the public key to the environment pool, and writes key/identity files with `0600` permissions.

### Step 7b: Start Worker 1 & Worker 2 Agents

Worker processes automatically discover the Node runner companion asset located at `<install-prefix>/share/deadbolt/runner/index.js`. Alternatively, pass `--runner-path /path/to/runner/index.js` or set `DEADBOLT_RUNNER_PATH`.

> [!IMPORTANT]
> **Required worker secrets:** tasks declare required secret _names_ (e.g. `NOTIFICATION_API_KEY` in `deadbolt.config.json`). Each worker process must have those variables set in its own environment before starting — secret values are never embedded in manifests and never travel through the control plane. A worker missing a required secret fails its claimed attempt closed with `MISSING_REQUIRED_SECRET`.

```bash
# Terminal 1: Worker 1
runtime worker start --control-plane-url https://api.deadbolt.cloud --key-path ~/.deadbolt/worker1.key --bundle-dir ./bundles --slots 2

# Terminal 2: Worker 2
runtime worker start --control-plane-url https://api.deadbolt.cloud --key-path ~/.deadbolt/worker2.key --bundle-dir ./bundles --slots 2
```

### Step 7c: Verify Active Workers

```bash
runtime worker list --env staging
```

Expected output:

```text
WORKER ID                             POOL     STATUS  DEPLOYMENTS
baa5c105-b3fb-41d6-a0a0-e6ab3f67f9f3  default  ACTIVE  b08aa9dba0b518aafaeafcb8e47bf4a32014a005c072e823d83593e284473879
5c77f72f-465e-4585-a393-fb547e05ea8c  default  ACTIVE  b08aa9dba0b518aafaeafcb8e47bf4a32014a005c072e823d83593e284473879
```

---

## 8. Deployment Activation (`runtime deployments activate`)

Activate the deployment on the workflow channel:

```bash
runtime deployments activate <DEPLOYMENT_ID> --workflow customer-onboarding --env staging
```

### Activation Preflight Enforcement

- **Strict High-Availability Rule:** Activation requires at least **2 compatible online workers** advertising the deployment's bundle digest.
- **Development Exception:** In non-production environments with only 1 worker online, pass `--allow-single-worker` (a failover recovery warning will be displayed).
- If 0 compatible workers are online, activation fails closed with `409 WORKER_PREFLIGHT_FAILED`.

---

## 9. Workflow Run Creation (`runtime runs create`)

Submit a workflow execution run via the public control plane interface:

```bash
runtime runs create \
  --workflow customer-onboarding \
  --env staging \
  --input '{"email":"alice@example.com","name":"Alice User"}' \
  --idempotency-key "order-run-$(date +%s)"
```

The control plane persists the run, initializes DAG step states (`validate: READY`, `provision: BLOCKED`, `notify: BLOCKED`), and returns `HTTP 202 Accepted`.

> [!NOTE]
> **Operator surface:** `runtime runs create`, `runtime runs inspect`, and `runtime logs` are acceptance/operator surfaces for humans. A production SaaS/backend normally triggers workflow runs automatically through the API/SDK/event integration; humans do not manually invoke the CLI for every customer request.

---

## 10. Run Listing (`runtime runs list`)

List recent workflow runs in the environment:

```bash
runtime runs list --env staging --limit 10
```

Output:

```text
RUN ID                                WORKFLOW             STATUS     REASON  CREATED AT
4b51cf6b-9de6-4cce-ad27-c5a3d8c6bb2a  customer-onboarding  SUCCEEDED  -       2026-09-16T14:26:46Z
```

---

## 11. Run Inspection (`runtime runs inspect`)

Inspect the detailed snapshot of a run, including DAG step statuses, attempt histories, monotonic epochs, failure reasons, and committed outputs:

```bash
runtime runs inspect <RUN_ID>
```

Output:

```text
Run ID:              4b51cf6b-9de6-4cce-ad27-c5a3d8c6bb2a
Workflow:            customer-onboarding
Status:              SUCCEEDED
Revision:            1
Last Event Sequence: 13
Created At:          2026-09-16T14:26:46Z

Steps & Attempts:
  STEP ID                               NODE       STATUS     EPOCH  ATTEMPTS
  4e6f7e39-f8a1-4d15-a03a-64b94dbe597b  validate   SUCCEEDED  1      1 attempt(s)
    └── Attempt #1:                     [SUCCEEDED] Started: 2026-09-16T14:26:47Z  ID: 19627c3a-3c93-46c2-816b-04194b3844eb
  ce2d0906-d400-4bc7-8c41-56297cacb13d  provision  SUCCEEDED  1      1 attempt(s)
    └── Attempt #1:                     [SUCCEEDED] Started: 2026-09-16T14:26:47Z  ID: bfef7751-b7e6-46bb-a9b9-22b442a10c56
  bfb4aa7e-2c54-451b-90e9-d5ab6fa4cc77  notify     SUCCEEDED  1      1 attempt(s)
    └── Attempt #1:                     [SUCCEEDED] Started: 2026-09-16T14:26:47Z  ID: 75e87fd4-c389-4189-8d42-346d74fadd77

Output:
{
  "accountId": "acc_usr_4e6f7e39-f8a1-4d15-a03a-64b94dbe597b",
  "deliveryId": "del_op_6358025066a4d4614a38509156bef54136d3c10bf2588077456c3d3764c5065a"
}
```

---

## 12. Execution Logs Streaming (`runtime logs`)

Stream stdout/stderr logs from task executions across workers:

```bash
runtime logs <RUN_ID>
```

Filter logs by specific step or attempt:

```bash
runtime logs <RUN_ID> --step <STEP_ID>
runtime logs <RUN_ID> --attempt <ATTEMPT_ID>
```

Example output:

```text
[14:26:47.000] [INFO ] #1 {"timestamp":"...","level":"INFO","attemptId":"...","message":"Starting task execution","meta":[{"taskName":"tasks/validate.js"}]}
[14:26:47.000] [INFO ] #2 {"timestamp":"...","level":"INFO","attemptId":"...","message":"Validating signup request","meta":[{"email":"alice@example.com"}]}
[14:26:47.000] [INFO ] #3 {"timestamp":"...","level":"INFO","attemptId":"...","message":"Task completed successfully","meta":[{"durationMs":15}]}
[14:26:47.000] [INFO ] #1 {"timestamp":"...","level":"INFO","attemptId":"...","message":"Provisioning customer account","meta":[{"userId":"usr_4e6f7e39..."}]}
[14:26:47.000] [INFO ] #2 {"timestamp":"...","level":"INFO","attemptId":"...","message":"Dispatching welcome email with stable operationId","meta":[{"operationId":"op_[REDACTED_HASH]","email":"alice@example.com"}]}
```

---

## 13. Automated Two-Worker Linear Pipeline Script

To run this entire sequence automatically with a single script:

```bash
./sdk/typescript/examples/linear-pipeline/two-workers-run.sh
```

The script automatically:

1. Validates prerequisites with `runtime doctor`.
2. Assembles bundle and manifest with `runtime build`.
3. Registers manifest with `runtime deploy`.
4. Enrolls two workers with `runtime worker enroll`.
5. Starts Worker 1 & Worker 2 background agents.
6. Verifies HA preflight and activates with `runtime deployments activate`.
7. Submits workflow run with `runtime runs create`.
8. Polls until completion and inspects final snapshot with `runtime runs inspect`.
9. Displays task logs with `runtime logs`.
10. Traps script exit and cleanly terminates both worker processes.

---

## 14. Browser Dashboard (hosted)

Open the hosted dashboard (served by the control plane):

```text
https://<staging-host>/dashboard/
```

The dashboard authenticates through the existing browser session flow — CLI credentials are never shared with the browser:

1. The dashboard checks `GET /api/auth/session`. While unauthenticated it shows a **Sign in** call-to-action and issues no protected API requests.
2. Sign in navigates to `/api/auth/login` (hosted OIDC). After Auth0 approval the callback sets the HttpOnly session cookie and returns to `/dashboard/`.
3. If the session has no active organization, the dashboard establishes one deterministically (single membership) or offers an explicit organization selector — it never silently picks between several.
4. Runs, workers, inspector, logs, and the reconnect-safe event stream then load under the authenticated session. An expired session returns to the sign-in state without leaking data.
