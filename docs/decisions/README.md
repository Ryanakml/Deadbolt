# Architectural Decision Records (ADRs)

This directory contains the immutable Architectural Decision Records baseline established in Blueprint §32.

| ADR | Title | Decision |
|---|---|---|
| [ADR-01](ADR-01.md) | Static Declarative DAG, Not Arbitrary Workflow Replay | Static declarative DAG represented as validated JSON, not arbitrary workflow-code replay. |
| [ADR-02](ADR-02.md) | Polyglot Architecture: TypeScript Tasks, Go Control Plane, Agent, and CLI | Control plane, worker agent, and CLI are built in Go. Task definitions, execution runner, and developer SDK are built in TypeScript/Node.js. |
| [ADR-03](ADR-03.md) | Hosted Control Plane with Customer-Hosted Compute | Control plane is centrally hosted; task execution runs exclusively on customer-managed workers via outbound HTTPS long-polling. |
| [ADR-04](ADR-04.md) | PostgreSQL as Authoritative State, Atomic Outbox, and Events | PostgreSQL is the single source of truth for runs, steps, attempts, leases, timers, history, and outbox intents committed in single ACID transactions. |
| [ADR-05](ADR-05.md) | NATS JetStream as Notification Transport Only, DB Reconciliation as Authority | NATS JetStream delivers internal wake-up hints to accelerate scheduling. PostgreSQL background reconcilers periodically sweep for ready work and expired leases. |
| [ADR-06](ADR-06.md) | HTTP Long Polling for Workers and SSE for Dashboard | Worker agents connect to Control Plane via outbound HTTPS JSON long-polling (20s). Web dashboard receives real-time updates via Server-Sent Events (SSE). |
| [ADR-07](ADR-07.md) | Renewable Leases, Epoch Fencing Tokens, and Session Binding | Step execution ownership uses a temporary lease (30s TTL, 5s heartbeat) with a strictly increasing ownership epoch (fencing token) and cryptographic session binding. |
| [ADR-08](ADR-08.md) | Mandatory Explicit Recovery Policy (Safe, Idempotent, Reconcile) | Every task definition must explicitly declare recovery mode: safe, idempotent, or reconcile. |
| [ADR-09](ADR-09.md) | Fail-Fast Workflow Behavior with No Implicit Rollback | Unrecoverable step failure triggers immediate fail-fast termination of the run, revoking remaining ownership and cancelling uncommitted sibling work. No automatic rollback/compensation of side effects is performed. |
| [ADR-10](ADR-10.md) | Immutable Deployments and Pinning Runs to Bundle Digests | Deployments are immutable manifests tied to SHA-256 bundle digests. Runs remain permanently pinned to their creation deployment. |
| [ADR-11](ADR-11.md) | Local Task Secrets on Workers and Platform Secrets via KMS | Task environment secrets remain strictly local on customer worker hosts. The platform stores only its own credentials and webhook signing secrets encrypted with KMS. |
| [ADR-12](ADR-12.md) | PostgreSQL Row-Level Security (RLS) and Application Scope Enforcement | All multi-tenant database tables enforce PostgreSQL Row-Level Security (FORCE RLS) with transaction-local tenant context (SET LOCAL), complemented by service-level RBAC. |
| [ADR-13](ADR-13.md) | Modular Monolith Architecture with Docker Compose Reference Deployment | Control plane modules (API, Gateway, Engine, Scheduler, Reconciler, Outbox, SSE) run in a single modular Go binary. Reference deployment uses Docker Compose. |
| [ADR-14](ADR-14.md) | Polling Result in MVP with Signed Webhooks in V1 | MVP client reads results via polling/SSE. V1 adds transactional, signed outbound webhooks (HMAC-SHA256) with SSRF defense and retry queues. |
| [ADR-15](ADR-15.md) | Best-Effort Diagnostic Logs and Durable Execution Events | Diagnostic console logs are bounded, sampled, and retained for 7 days. Execution state events are strictly durable, un-sampled, and retained with the run. |
| [ADR-16](ADR-16.md) | Cumulative Delivery, Testing, and Security Gates | Every milestone builds upon non-negotiable automated testing, failure matrix validation (F-01 to F-28), invariant enforcement, and reproducible CI builds. |
