# Contract Versioning & Target SDK Semantics

This document outlines the versioning contracts, schema boundaries, and target SDK developer experience defined in Blueprint §6, §12, and §20.

---

## 1. Contract Versioning Strategy

All interface contracts in Deadbolt follow strict independent versioning:

| Contract                    | Current Version      | Schema / Spec Location                    | Compatibility Rule                                                               |
| --------------------------- | -------------------- | ----------------------------------------- | -------------------------------------------------------------------------------- |
| **Workflow Manifest**       | `manifestVersion: 1` | `contracts/manifest/workflow.schema.json` | Pinned to immutable deployment SHA-256; major versions require engine migration. |
| **Task Definition**         | `v1`                 | `contracts/manifest/task.schema.json`     | Must declare explicit `recovery` (`safe`, `idempotent`, `reconcile`).            |
| **Worker Gateway Protocol** | `protocolVersion: 1` | `contracts/worker/protocol.schema.json`   | Protocol major must match between Control Plane and Worker Agent.                |
| **Control Plane API**       | `v1` (`/v1/...`)     | `contracts/openapi/control-plane.yaml`    | OpenAPI 3.1.0 standard with standard `ErrorEnvelope`.                            |

---

## 2. Target SDK Semantics

### Defining a Task

Every task must explicitly specify its schemas, timeout, retry budget, and recovery policy:

```typescript
import { defineTask } from "@runtime/sdk";

export const searchTask = defineTask({
  name: "search-web",
  inputSchema: {
    type: "object",
    properties: { query: { type: "string" } },
    required: ["query"],
    additionalProperties: false,
  },
  outputSchema: {
    type: "object",
    properties: { pages: { type: "array", items: { type: "string" } } },
    required: ["pages"],
    additionalProperties: false,
  },
  recovery: "safe", // Must be "safe" | "idempotent" | "reconcile"
  retry: {
    maxAttempts: 3,
    initialDelayMs: 1000,
    maxDelayMs: 30000,
  },
  timeoutMs: 60000,
  handler: async ({ query }, ctx) => {
    return { pages: await performSearch(query, { signal: ctx.signal }) };
  },
});
```

### Defining a Declarative Workflow (DAG)

Workflows are static DAG definitions that compile into the versioned manifest:

```typescript
import { defineWorkflow, input, output } from "@runtime/sdk";

export const researchWorkflow = defineWorkflow({
  name: "research-report",
  inputSchema: {
    type: "object",
    properties: { query: { type: "string" } },
    required: ["query"],
  },
  nodes: [
    {
      id: "search",
      type: "task",
      task: searchTask,
      input: { query: input("/query") },
    },
    {
      id: "analyze",
      type: "task",
      task: "analyze-pages",
      after: ["search"],
      input: { pages: output("search", "/pages") },
    },
    {
      id: "report",
      type: "task",
      task: "generate-report",
      after: ["analyze"],
      input: { analysis: output("analyze", "/analysis") },
    },
  ],
  output: {
    reportUrl: output("report", "/reportUrl"),
  },
  outputSchema: {
    type: "object",
    properties: { reportUrl: { type: "string" } },
    required: ["reportUrl"],
  },
});
```

---

## 3. Input & Output Reference Descriptors

- `input("/path/pointer")`: References a JSON pointer within the top-level run input (`$ref: "run.input"`).
- `output("stepId", "/path/pointer")`: References a committed step output (`$ref: "step.output"`).
- Missing fields fail during transition evaluation with `INPUT_MAPPING_ERROR`.

## Executable M0 contract details

The examples above are target SDK ergonomics; `defineTask`, `defineWorkflow`, CLI build/deploy and handlers are not implemented by this PR. The executable exports are JSON parsing/canonicalization, payload/schema validation, task normalization, mapping, choice-expression conformance and MVP graph/deployment validation.

- `parseJSON(raw)` / Go `ParseJSON` must run at the raw boundary before a normal decoder can erase duplicate keys or repair invalid Unicode. Container nesting counts objects/arrays, with a maximum of 32. Safe integers are bounded by ±(2^53−1). Schema size is measured as canonical UTF-8 JSON, capped at 65,536 bytes. No Unicode normalization occurs.
- `validateWorkflowManifest(workflow, tasks)` / Go `ValidateWorkflow` require the task registry. A missing task is an error; a task name alone does not prove its recovery policy exists. `validateDeployment` checks version/runtime/OS/architecture, lock/bundle digests, secret-name declarations and all referenced definitions. Registration/activation is not implemented here.
- `normalizeTask` materializes timeout 300,000 ms and retry defaults (3 attempts, 1,000 ms initial, 30,000 ms cap) without mutating its argument. For idempotent recovery the window must cover the 5,000 ms claim-to-start budget plus timeout. Actual deadline/retry dispatch belongs to the engine issue.
- Mapping descriptors are `{ "$ref": "run.input", "pointer": "...", "default": ... }` or `{ "$ref": "step.output", "stepId": "...", "pointer": "...", "default": ... }`. `default` is optional and used only when absent, never when present with null. An empty pointer references the entire JSON value; `~0` and `~1` escape tilde/slash. Array pointers use canonical nonnegative decimal indexes; `-` does not read an element. Only own object properties are visible.
- `{ "literal": value }` quotes literal data, including objects containing reserved `$ref` or `literal` keys. Other objects/arrays recursively map their members. A reference to a step must identify an ancestor for node input; a default cannot authorize a non-ancestor. Output references must be guaranteed by the source schema or provide an explicit default. Every terminal leaf feeds the workflow output or declares `sideEffect: true`.
- Choice conformance expressions use `{ "op": "eq", "args": [operand, operand] }`. `eq`/`neq` compare JSON values of the same type by canonical structural equality; ordering uses numbers; `in` compares against an array with no coercion. `exists` takes a reference. `and`/`or` take one or more expressions, `not` exactly one. Invalid operators, arity, or type mismatch fail validation, including inside otherwise short-circuited expressions. Operators never evaluate JavaScript.
- The payload schema meta-schema enumerates the supported subset. Tagged `oneOf` requires a common required discriminator with distinct `const` values. Annotation-only `title`/`description` are allowed. JSON Schema `$ref`, `format`, code hooks and keywords outside the subset are rejected recursively; mapping `$ref` is a separate descriptor syntax.
- `choice`, `merge`, `approval`, and `delay` are reserved node types/fields. Their execution remains unavailable and `validateWorkflowManifest` rejects them with `UNSUPPORTED_CAPABILITY`, as it rejects parallel graphs. The structural schema's V1 ceiling is 200 nodes; executable MVP admission remains 50. This is not V1 graph execution or structured-branch validation.
- Worker endpoint bodies validate the corresponding `$defs/*Request` schema. The worker schema root's `{operation,message}` wrapper is a **conformance catalogue envelope**, not an extra wire envelope. Every request body carries `protocolVersion` and `requestId`; session-bound operations include session/worker identity and still require authenticated HTTP scope. `CompleteRequest` requires a result digest and exactly one output/artifact for success, or a task error for failure. No schema authenticates a session or verifies ownership by itself.
- OpenAPI declares target endpoints, explicit security schemes and error envelopes. `x-implemented: false` and `x-minimum-capability: V1` prevent declarations being mistaken for implemented/available features. Cookie mutations require CSRF/Origin checks; reconciliation/approval permit human sessions only. Runtime service/authorization tests remain with their owners.

Schema changes are pre-release: no deployed data or published SDK users need migration. After release, a breaking change requires a new major or the blueprint's explicit compatibility/migration process. Unknown additive event fields remain tolerable; `schemaVersion` is mandatory. See the SP-01 report for exact commands and evidence boundaries.
