# Parallel task DAGs

Deadbolt task workflows may now contain multiple independent roots, fan-out,
and all-success joins. A node with `after: ["a", "b"]` becomes eligible only
after both dependencies are `SUCCEEDED`; capacity and worker slots determine
when eligible siblings actually start.

```ts
const workflow = defineWorkflow({
  name: "parallel-report",
  inputSchema,
  outputSchema,
  nodes: [
    { id: "load", type: "task", task: loadTask },
    { id: "risk", type: "task", task: riskTask, after: ["load"] },
    { id: "notify", type: "task", task: notifyTask, after: ["load"] },
    {
      id: "publish",
      type: "task",
      task: publishTask,
      after: ["risk", "notify"],
      input: {
        risk: output("risk", "/score"),
        notification: output("notify", "/id"),
      },
    },
  ],
  output: { report: output("publish", "/report") },
});
```

Mappings are evaluated from committed dependency outputs. A missing field is a
non-retryable `INPUT_MAPPING_ERROR`; a final output that does not match the
workflow schema is a non-retryable `OUTPUT_SCHEMA_VIOLATION`.

If a branch fails definitively, the run fails fast. Successful sibling outputs
remain durable, nonterminal siblings are cancelled, their leases are revoked,
and stop commands are persisted for workers that may still be running. These
actions do not imply rollback of external side effects.

