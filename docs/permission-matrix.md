# Deadbolt Permission Matrix & Tenant Security Architecture

> **Blueprint References:** §18, §20, §24 (Authentication, authorization, and tenant isolation), §32 (ADRs)  
> **Invariants:** `INV-01`, `INV-14`, `REQ-SEC-01`, `REQ-PRODUCT-01`

---

## 1. Canonical Roles & Capability Matrix (Blueprint §24.2)

Deadbolt enforces role-based access control (RBAC) across five canonical organization roles. Permissions are evaluated against explicit capabilities before any control-plane or data-plane operation is executed.

| Role          | Read Status & History | Read Payload & Log               | Run & Control (`run:create`, `run:control`)    | Reconcile & Approve (`approval:decide`, `reconciliation:resolve`) | Deploy & Activate (`deployment:register`, `deployment:activate:*`) | Administration (`admin:member`, `admin:key`, `worker:drain`) |
| :------------ | :-------------------- | :------------------------------- | :--------------------------------------------- | :---------------------------------------------------------------- | :----------------------------------------------------------------- | :----------------------------------------------------------- |
| **Viewer**    | **Yes**               | **No** (requires `payload:read`) | **No**                                         | **No**                                                            | **No**                                                             | **No**                                                       |
| **Developer** | **Yes**               | **Yes**                          | **Yes** (Create, pause, resume, cancel, rerun) | **No**                                                            | **Register & Staging Only**                                        | **No**                                                       |
| **Operator**  | **Yes**               | **Yes**                          | **Yes**                                        | **Yes**                                                           | **Yes** (Staging & Production)                                     | **Drain Worker**                                             |
| **Admin**     | **Yes**               | **Yes**                          | **Yes**                                        | **Yes**                                                           | **Yes**                                                            | **Yes** (except removing last Owner)                         |
| **Owner**     | **Yes**               | **Yes**                          | **Yes**                                        | **Yes**                                                           | **Yes**                                                            | **Yes** (including Org Deletion)                             |

### 1.1 Canonical Capabilities

- `run:create`: Submit and trigger new workflow executions.
- `run:read`: Query workflow runs, step states, execution events, and metadata.
- `run:control`: Pause, resume, cancel, or rerun existing workflow executions.
- `payload:read`: Inspect workflow and step inputs, outputs, diagnostic logs, and artifacts. (Viewers see only metadata and sanitized errors unless explicitly granted this capability).
- `deployment:register`: Upload and register workflow manifests and bundle digests.
- `deployment:activate:staging`: Promote registered deployments to the `staging` environment.
- `deployment:activate:production`: Promote deployments to the `production` environment.
- `worker:drain`: Issue graceful drain signals to worker pools.
- `approval:decide`: Submit human decisions (approve / reject) on waiting approval steps.
- `reconciliation:resolve`: Mark ambiguous side effects as confirmed or failed during reconciliation.
- `admin:member`: Invite, update roles of, and remove team members.
- `admin:key`: Generate, inspect summaries of, and revoke environment-scoped API keys.
- `admin:project`: Create projects and configure environment admissions / concurrency limits.
- `org:update`: Modify organization settings.
- `org:delete`: Permanently delete an organization and cascade all contained resources (Owner only).

---

## 2. API Key Lifecycle & Machine Identity Contract (Blueprint §24.4)

API keys are machine credentials intended for headless CI/CD pipelines and self-hosted worker processes.

### 2.1 Cryptographic Entropy & Format

- **Entropy:** Secret keys are generated with $\ge 256$ bits (32 bytes) of cryptographic randomness from `crypto/rand`.
- **Key Format:** Plaintext keys follow the structure:
  $$\text{Plaintext Key} = \underbrace{\text{db\_}\langle\text{env}\rangle\text{\_}\langle\text{8-hex}\rangle}_{\text{Prefix}} \text{\_} \underbrace{\text{Base64RawURL}(\text{Secret})}_{\text{256-bit Secret}}$$
  _Example:_ `db_production_3f9a1b2c_s7K8_d...`
- **Prefix Disambiguation:** The prefix (`db_<env>_<8hex>`) is indexed with a global `UNIQUE` constraint for fast lookup without exposing the secret.
- **One-Time Display:** Plaintext keys are returned **exactly once** upon generation in the `GeneratedKey` response. Plaintext secrets are never stored, logged, or recoverable.
- **Persistence:** Keys are stored strictly as hex-encoded SHA-256 cryptographic hashes (`hashed_secret`). Key verification uses constant-time comparison (`subtle.ConstantTimeCompare`) to eliminate timing side-channel attacks.

### 2.2 Environment Scoping & Expiry

- **Single Environment Binding:** Each API key belongs to **exactly one** environment (e.g. `staging` or `production`). Keys cannot cross environment boundaries.
- **Default Expiry:** Keys default to a 90-day expiration window.
- **Revocation:** Keys can be immediately revoked via `DELETE /api/v1/api-keys/{id}`. Revocation is checked on every invocation.
- **Last-Used Auditing:** `last_used_at` timestamps are updated on successful authentication within the tenant's transaction-local context.

### 2.3 Machine Key Authorization Restrictions

- **No Human Approvals or Reconciliations:** In strict compliance with Blueprint §24.2, machine API keys cannot possess `approval:decide` or `reconciliation:resolve` capabilities. Attempts to create an API key with these capabilities are rejected with `MACHINE_KEY_UNAUTHORIZED`. Human decisions require an identifiable human user session.

---

## 3. Defense-in-Depth & Database Enforcement (Blueprint §24.3)

### 3.1 PostgreSQL Row Level Security (RLS)

- All tenant tables (`organizations`, `organization_members`, `projects`, `environments`, `environment_admissions`, `api_keys`) have `ENABLE ROW LEVEL SECURITY` and `FORCE ROW LEVEL SECURITY` applied.
- The platform runtime connects as `deadbolt_runtime` with `NOBYPASSRLS`.
- Every database transaction sets the transaction-local tenant context:
  ```sql
  SELECT set_config('app.current_organization_id', $1, true);
  ```
- Any query executed without this context evaluates to `NULL`, failing closed with zero rows returned.
- Composite foreign keys (e.g., `(organization_id, id)`) prevent resources owned by Organization A from referencing projects or environments belonging to Organization B.

### 3.2 Last Owner Defense

To prevent accidental tenant abandonment, Deadbolt strictly enforces the **Last Owner Defense**:

- Rejects any attempt to demote the sole remaining active `Owner` of an organization (`LAST_OWNER_DEMOTION_FORBIDDEN`).
- Rejects any attempt to remove the sole remaining active `Owner` (`LAST_OWNER_REMOVAL_FORBIDDEN`).
- An organization must always retain at least one active Owner identity.

### 3.3 Restricted Discovery Functions

- Discovery of user memberships and API key resolution execute narrowly scoped `SECURITY DEFINER` functions with locked-down `search_path = app, public, pg_temp`:
  - `app.discover_user_memberships(p_user_id UUID)`: Returns only organizations where the verified user is an active member.
  - `app.authenticate_api_key(p_prefix TEXT)`: Returns key metadata and hash for the unique prefix.
- Public execute privileges are revoked; only `deadbolt_runtime` possesses execute rights.
