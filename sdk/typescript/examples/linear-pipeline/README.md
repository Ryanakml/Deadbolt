# Linear Pipeline Example: Customer Onboarding ($A \rightarrow B \rightarrow C$)

This example demonstrates building and executing a durable linear workflow with Deadbolt and the `@runtime/sdk` TypeScript SDK, strictly adhering to **Blueprint §4, §6, §12, §14, §20, §22** and **ADR-01, ADR-02, ADR-08, ADR-10**.

## Architecture & Data Flow Boundaries

```text
[Client Application]
       │
       ▼ (1) POST /v1/workflows/customer-onboarding/runs (with Idempotency-Key)
[Control Plane API]
       │
       ▼ (2) Atomic DB Transaction: Commit Run + Steps + Event + Outbox (HTTP 202 Accepted)
[PostgreSQL Authority]
       │
       ▼ (3) Worker Long Poll Claims Step & Obtains Lease + Fencing Token
[Self-Hosted Worker] (Executes Task inside Pinned Immutable Bundle)
       │
       ├─► Node A: "validate"   (Recovery: safe, produces userId)
       │       │
       │       ▼ (Output mapping: validate -> provision)
       ├─► Node B: "provision"  (Recovery: safe, produces accountId)
       │       │
       │       ▼ (Output mapping: provision + validate -> send-email)
       └─► Node C: "send-email" (Recovery: idempotent, uses stable ctx.operationId)
```

### Responsibility Model
- **Client Application:** Submits create request with a unique `Idempotency-Key` and polls control-plane snapshots for terminal outcomes.
- **Control Plane:** Durably manages states, assigns step leases, validates fencing tokens, and coordinates recovery.
- **Worker Process:** Executes task code in isolated attempts using pinned SHA-256 local bundles. Does not make uncoordinated scheduling decisions.

## Files
- `tasks.ts`: Declarative task definitions with schemas and explicit recovery policies (`safe`, `idempotent`).
- `workflow.ts`: Declarative DAG definition with JSON Pointer reference mappings (`input()`, `output()`).
- `build.ts`: Builds immutable bundle, computes SHA-256 digests, and generates deployment manifest.
- `run.ts`: Uses `DeadboltClient` to create idempotent runs and poll for results.

## Recovery Policy Enforcement
1. **`recovery: "safe"` (Nodes A & B):** The developer explicitly declares the operation safe to retry without external duplicate side-effects.
2. **`recovery: "idempotent"` (Node C):** For operations communicating with third-party external services (e.g. email/SMS/payment), `idempotencyWindowMs` must be declared and validated (`idempotencyWindowMs >= 5000 + timeoutMs`). Downstream deduplication uses `ctx.operationId`, which remains stable across retries.

## Running the Example
Compile and execute the bundle builder:
```bash
npx tsx build.ts
```
The manifest output is strictly validated against `contracts/manifest/deployment.schema.json`.
