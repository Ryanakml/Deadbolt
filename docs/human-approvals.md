# Human Approvals (Blueprint §16.3)

An approval is a **control node**. It is the point in a workflow where the
platform stops and waits for a person, without pretending a machine is doing
the waiting.

This document is the developer-facing contract for approval nodes, the decide
API, and the boundary between a technical step outcome and a business outcome.

---

## 1. What an approval actually is

When an approval node's dependencies are satisfied, Deadbolt:

1. persists **one** `PENDING` approval record for that step,
2. records the request payload, the required permission, and an expiry,
3. parks the step at `WAITING` with reason `APPROVAL`,
4. writes an `approval.requested` execution event.

It does **not** claim the step, create an attempt, hold a lease, or start a
runner. There is no process anywhere holding this wait open.

```text
build ──▶ [ approval: WAITING / APPROVAL ] ──▶ publish
             ▲
             └── a person decides, later
```

Because the wait lives in PostgreSQL, the entire control plane — API, gateway,
scheduler, every worker — can be restarted and the approval is still decidable.

### Consequence: long waits are cheap

A run waiting three days for a human costs one row, not one process. See
[LONG-LIVED WORKFLOW ≠ LONG-LIVED COMPUTE](#9-what-this-does-not-buy-you).

---

## 2. Defining an approval node

```ts
import { approvalNode, defineWorkflow } from "@runtime/sdk";

export const publishReport = defineWorkflow({
  name: "publish-report",
  inputSchema: reportInput,
  outputSchema: reportOutput,
  nodes: [
    {
      id: "build",
      type: "task",
      task: buildReport,
      input: {
        /* ... */
      },
    },
    approvalNode(
      "gate",
      {
        payload: {
          question: "Publish the generated report?",
          documentId: input("/documentId"),
          risk: output("build", "/risk"),
        },
        outputSchema: {
          type: "object",
          properties: {
            decision: { type: "string" },
            actorId: { type: "string" },
          },
          required: ["decision"],
        },
      },
      ["build"], // after
    ),
    {
      id: "publish",
      type: "task",
      task: publishReport,
      input: {
        /* ... */
      },
    },
  ],
  output: { reportUrl: output("publish", "/reportUrl") },
});
```

### Configuration

| Field                | Required | Meaning                                                                  |
| -------------------- | -------- | ------------------------------------------------------------------------ |
| `payload`            | no       | The request shown to the approver. Stored verbatim.                      |
| `outputSchema`       | no       | Schema the committed decision output is validated against.               |
| `requiredPermission` | no       | Must be `approvals:decide` if present. Omitting it means the same thing. |
| `expiresInMs`        | no       | Shortens the 24h default wait. Always clamped to the run lifetime.       |

An approval with no config, or one that names a different permission, is
**rejected at registration**. It fails closed before a run ever exists rather
than becoming an undecidable wait later.

An approval node may not also declare `task`, `input`, `choice`, `merge`, or
`delayMs`. Control nodes are not dispatchable work.

---

## 3. Execution success is not business approval

This is the single most important idea in this document.

**Both `approved` and `rejected` make the approval step `SUCCEEDED`.**

The node's job was to _obtain a valid human decision_. It did. Whether that
decision is good for the business is a separate question, and the workflow
answers it with the next `choice`:

```ts
choiceNode(
  "what-next",
  {
    branches: [
      {
        name: "ship",
        condition: expr("eq", output("gate", "/decision"), "approved"),
      },
      { name: "hold" },
    ],
    default: "hold",
  },
  ["gate"],
);
```

```text
                 gate: SUCCEEDED  (decision = "rejected")
                   │
             what-next (choice)
              ╱            ╲
          ship               hold
     publish report      notify the reviewer
```

If rejection failed the step, you would lose the reason, you could not branch
on the answer, and a rejection would be indistinguishable from a technical
error. Instead:

| Outcome                 | Approval step | Run         | Why                                             |
| ----------------------- | ------------- | ----------- | ----------------------------------------------- |
| Human approved          | `SUCCEEDED`   | continues   | Valid decision collected                        |
| Human rejected          | `SUCCEEDED`   | continues   | Valid decision collected; branch decides        |
| Nobody answered in time | `FAILED`      | `FAILED`    | `APPROVAL_EXPIRED` — a real operational failure |
| Run cancelled           | `CANCELLED`   | `CANCELLED` | Terminal means terminal                         |

So this is wrong:

```ts
// Do not do this. It makes a business answer look like an infrastructure fault.
if (output("gate", "/decision") === "rejected") {
  throw new Error("rejected");
}
```

---

## 4. Deciding an approval

```http
POST /v1/approvals/{approvalId}/decision
Idempotency-Key: <unique per decision attempt>
Content-Type: application/json

{
  "decision": "approved",
  "expectedRevision": 1,
  "comment": "Reviewed the sample chapters"
}
```

```json
{ "id": "…", "status": "APPROVED", "revision": 2 }
```

`decision` is `approved` or `rejected`. `expectedRevision` is the approval
revision you last read. `comment` is optional and stored with the decision.

### Who may decide

An **identifiable human** holding `approvals:decide`. Per §24.2 that is
`operator`, `admin`, and `owner`.

- A developer can pause a run but **cannot** approve one. The two capabilities
  are separate and neither implies the other.
- **Machine keys are refused**, even one that somehow carries the capability.
  §24.2 withholds approval machine keys in V1.
- **Worker sessions are refused.**
- There is **no public approval link** and no anonymous action.

### Listing pending decisions

```http
GET /v1/approvals?environment={envId}&status=PENDING
```

`environment` must match the caller's verified key scope. A mismatch is
rejected; it never creates a new scope.

### Reading one

```http
GET /v1/approvals/{approvalId}
```

A foreign organization's approval is reported as `404`, not `403`, so the
endpoint cannot confirm that someone else's id exists.

---

## 5. Concurrency: exactly one decision (INV-10, F-14)

Two administrators with the same approval open is normal. The outcome is
always exactly one committed decision.

| Situation                                            | Result                                                           |
| ---------------------------------------------------- | ---------------------------------------------------------------- |
| Same decision submitted twice (double-click)         | **Idempotent.** Returns the committed decision, advances nothing |
| Opposite decision after one is committed             | `409 APPROVAL_CONFLICT`                                          |
| Stale `expectedRevision` on a still-pending approval | `409 REVISION_CONFLICT`                                          |
| Two operators decide simultaneously                  | One wins, one gets `409`                                         |
| Decision after the deadline                          | `409 APPROVAL_EXPIRED`, run fails                                |
| Decision after cancel                                | `409`, run stays `CANCELLED`                                     |

An identical replay deliberately ignores a **stale** `expectedRevision`: the
browser resent the revision it first read, and the intent it expressed is
already satisfied. Revision guards _new_ decisions, not replays of a committed
one.

Every decision writes an `approval.decide` audit record with the actor, role,
capabilities, comment, and correlation ID.

---

## 6. Expiry is database time

An approval carries an expiry: **24 hours by default**, and never longer than
the run's remaining lifetime. An expired approval fails the run with
`APPROVAL_EXPIRED`.

The check happens **inside the decision transaction, against the database
clock, before the decision is accepted**. A background sweeper also settles
overdue approvals, but the sweeper is _progression only_ — it is never what
makes a decision invalid.

```text
operator clicks Approve at 14:00:05
approval expired at 14:00:00
sweeper has not run yet

  → decision refused. The sweeper's timing is irrelevant.
```

This is the same discipline as every other deadline in Deadbolt: **the
database owns time**, not a worker, a browser, or an application process.

---

## 7. What an approval is not

An approval is **not**:

- a task — it has no attempt, no epoch, no lease, no operation ID
- a retry — there is nothing to retry
- a compensation — cancelling it does not undo a side effect
- a signature — it does not prove who read what before deciding
- a public link — it is never reachable unauthenticated
- a guarantee of a business outcome — see §3

It also does not survive the run. Cancelling a run cancels its pending
approvals, and a decision on that terminal record is refused rather than
reopening anything (§9 INV-09).

---

## 8. Observing approvals

Execution events:

| Event                | Meaning                                           |
| -------------------- | ------------------------------------------------- |
| `approval.requested` | A decision was requested; the step is now waiting |
| `approval.decided`   | A human committed approve or reject               |
| `approval.expired`   | The deadline passed with no decision              |

The run snapshot exposes `approvals[]` alongside `steps[]` and
`reconciliationCases[]`, read in the same consistent snapshot. The Inspector
renders one entry per approval with its status, actor, comment, and expiry —
there is no separate request, so an action can never be drawn against a stale
step.

```json
{
  "status": "WAITING",
  "reasonCode": "APPROVAL",
  "steps": [
    { "nodeId": "gate", "status": "WAITING", "waitReason": "APPROVAL" }
  ],
  "approvals": [
    {
      "id": "…",
      "nodeId": "gate",
      "status": "PENDING",
      "requiredPermission": "approvals:decide",
      "revision": 1,
      "expiresAt": "2026-09-27T14:00:00Z"
    }
  ]
}
```

---

## 9. What this does not buy you

Be clear about the limits:

- **A rejection is not a rollback.** Nothing undoes work that already ran
  before the approval.
- **An approval is not a signature.** It records _who decided_, not _what they
  read_ at the time. Use an audit trail if you need read-access evidence.
- **Approvals do not guarantee exactly-once side effects.** A task that charges
  a card and _then_ waits for approval can still leave the charge applied when
  the request is rejected. Model irreversible effects as `recovery: "reconcile"`
  tasks, not as approvals.
- **An approval cannot wait forever.** 24 hours is the default; plan downstream
  work accordingly.

---

## 10. Related

- [Blueprint §16.3](../non_pushable_docs/blueprint.md) — approval node semantics
- [Blueprint §15.2](../non_pushable_docs/blueprint.md) — approval wait bounds
- [Blueprint §24.2](../non_pushable_docs/blueprint.md) — role capability matrix
- [Permission Matrix](permission-matrix.md) — who holds what
- [Run Inspector and Diagnostics](run-inspector-and-diagnostics.md) — reading
  approvals in the UI
- [Choice and Merge](choice-merge.md) — branching on the decision
- [Timeout, Cancellation, and Stop](timeout-cancellation-and-stop-operations.md)
  — how cancellation interacts with pending waits
