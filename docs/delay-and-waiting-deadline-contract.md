# Delay and waiting/deadline contract

Issue #33 adds the V1 user-delay node. A delay is a durable control step, not a
worker task: it creates one `DELAY` timer and keeps the run in `WAITING` while
the timer is pending. No worker lease or task attempt is created.

## Workflow contract

Use the TypeScript SDK helper:

```ts
const workflow = defineWorkflow({
  name: "onboarding",
  nodes: [
    delayNode("cool-off", 60_000),
    // downstream task nodes may depend on "cool-off"
  ],
});
```

`delayMs` is an integer from 1 millisecond through 30 days. Invalid values are
rejected consistently by the JSON manifest schema, Go validator, and TypeScript
SDK. The deployment and workflow contracts remain versioned; older manifests
without delay nodes remain valid.

## Persistence and restart behavior

When a delay first becomes eligible, the engine calculates and persists one
absolute `timers.due_at` value. Re-evaluation, process restart, scheduler
restart, and pause/resume do not calculate a new deadline. A unique pending
timer guard prevents duplicate activation. The timer transition and step/run
state transition are committed in one transaction using the normal run → step →
timer lock order.

When due, the scheduler changes the timer to `FIRED`, marks the delay step
`SUCCEEDED` with `completionSource=TIMER`, emits the step event, and advances
the DAG. A duplicate fire is a no-op. Cancellation cancels pending timers and a
terminal or cancelling run cannot be reopened by a late timer.

## Run lifetime and observability

The V1 default run lifetime is 7 days and the hard maximum is 30 days. The
deadline includes queueing, approvals, delays, holds, pauses, and execution;
pause does not freeze it. If the run deadline wins the race, reconciliation
fails the run as `RUN_DEADLINE_EXCEEDED`, cancels the pending delay timer, and
the late timer cannot resume it.

Run snapshots expose `steps[].waitReason` and `steps[].dueAt` for delayed
steps. The Inspector displays both fields, so an operator can distinguish a
durable user delay from worker unavailability or reconciliation.

## Validation evidence

The integration coverage exercises durable persistence, zero task attempts,
duplicate firing, deadline expiry, and the terminal-state guard against late
activation. Controlled-time tests move the persisted timer due timestamp in the
database; no process sleep is required.
