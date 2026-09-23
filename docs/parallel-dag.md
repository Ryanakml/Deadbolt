# Parallel task DAGs

Deadbolt task workflows may contain multiple independent roots, fan-out,
and all-success joins. A node with no `after` is a root and starts `READY`;
a node with `after: ["a", "b"]` becomes eligible only after **every**
dependency is terminally `SUCCEEDED` or `SKIPPED`. Capacity and worker slots
determine when eligible siblings actually start: parallelism is
capacity-dependent, never a simultaneous-start guarantee.

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

## Joins and skipped propagation

A join with all dependencies `SUCCEEDED` becomes `READY`. If any
dependency is `SKIPPED` (once every dependency is `SUCCEEDED` or `SKIPPED`),
the join becomes `SKIPPED` with reason `DEPENDENCY_SKIPPED` instead of
executing. Skipped propagation is transitive and re-evaluated to a fixed
point regardless of manifest node order: a skipped node skips its children,
which skip theirs, until stable. Skipped steps never generate worker
assignments. Normal completion advancement and restart/broker-loss
reconciliation (`ReconcileReadyWork`) share one graph-evaluation path, so
recovery can never execute a graph differently from normal execution.

## Committed output mapping

Mappings are evaluated from committed dependency outputs only. When a join
becomes eligible, its `input` is built with `MapInput` from the run input
plus committed step outputs, then validated against the target task input
schema. A missing field (`INPUT_MAPPING_ERROR`) or a mapped input that
violates the task schema is deterministic and non-retryable: the run fails
through canonical fail-fast handling, never as a transient Poll or
reconciler error. A final output that does not match the workflow schema is
a non-retryable `OUTPUT_SCHEMA_VIOLATION` / `OUTPUT_MAPPING_ERROR`.

## Fail-fast sibling settlement

If a branch fails definitively, the run fails fast in one atomic settlement:
the failing step becomes `FAILED`; successful terminal siblings stay exactly
`SUCCEEDED` with their committed outputs preserved; every other nonterminal
sibling or dependent step (`BLOCKED`, `READY`, `WAITING`, `RUNNING`) becomes
`CANCELLED`; every live sibling attempt (`CLAIMED`/`RUNNING`) becomes
`CANCELLED`; leases are revoked; stop commands persist for workers that may
still be executing customer code; pending retry timers are cancelled; the run
becomes `FAILED`. After fail-fast no nonterminal step remains.

Partial effects are not rolled back: `SUCCEEDED` outputs committed before
the failure stay durable, and the stop record is the durable hand-off for
late workers, not an implicit rollback claim.

## Late results are fenced

Ownership (session, epoch, lease) fences every completion. A stale or late
result from a cancelled sibling — same or different payload — is rejected
(`ErrStaleOwnership` / `ErrResultConflict`), the run stays `FAILED`, the
cancelled step never becomes `SUCCEEDED`, downstream work is not created,
and no terminal state reopens. Expired-lease recovery revalidates run,
attempt, and step state under canonical locks per candidate, so a stale
sweep snapshot can never resurrect a terminal run with new timers, holds, or
attempts.

## Retry and run state

One branch entering `RETRY_BACKOFF` never stalls independent siblings. Run
state follows priority: while any live attempt or `READY`/`RUNNING` sibling
work exists the run stays `RUNNING` and siblings remain claimable; the run
becomes `WAITING` only when all remaining work is durably waiting. When the
retry timer fires, normal DAG semantics resume without duplicating
steps or attempts.
