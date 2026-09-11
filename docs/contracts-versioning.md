# Contract Versioning & Target SDK Semantics

This document outlines the versioning contracts, schema boundaries, and target SDK developer experience defined in Blueprint §6, §12, and §20.

---

## 1. Contract Versioning Strategy

All interface contracts in Deadbolt follow strict independent versioning:

| Contract | Current Version | Schema / Spec Location | Compatibility Rule |
|---|---|---|---|
| **Workflow Manifest** | `manifestVersion: 1` | `contracts/manifest/workflow.schema.json` | Pinned to immutable deployment SHA-256; major versions require engine migration. |
| **Task Definition** | `v1` | `contracts/manifest/task.schema.json` | Must declare explicit `recovery` (`safe`, `idempotent`, `reconcile`). |
| **Worker Gateway Protocol** | `protocolVersion: 1` | `contracts/worker/protocol.schema.json` | Protocol major must match between Control Plane and Worker Agent. |
| **Control Plane API** | `v1` (`/v1/...`) | `contracts/openapi/control-plane.yaml` | OpenAPI 3.1.0 standard with standard `ErrorEnvelope`. |

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
