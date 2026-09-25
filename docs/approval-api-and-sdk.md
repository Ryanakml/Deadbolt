# Human approvals

An approval is a durable control node. When its dependencies succeed, the
control plane creates one `PENDING` approval and parks the step in
`WAITING/APPROVAL`; no worker lease or process remains active. The default
expiry is 24 hours, bounded by the run deadline, and PostgreSQL time is the
authority.

```ts
const workflow = defineWorkflow({
  name: "publish-report",
  inputSchema,
  outputSchema,
  nodes: [
    { id: "draft", type: "task", task: draftTask },
    {
      id: "approval",
      type: "approval",
      after: ["draft"],
      approval: {
        payload: { title: "Publish report", risk: "external side effect" },
        decisionSchema: { type: "string", enum: ["approved", "rejected"] },
        requiredPermission: "approvals:decide",
      },
    },
  ],
  output: {},
});
```

The Inbox reads `GET /v1/approvals?environment=<id>` with a human session.
`GET /v1/approvals/{id}` returns the committed payload, status, expiry, and
revision. Decide with:

```http
POST /v1/approvals/{id}/decision
Content-Type: application/json

{"decision":"approved","expectedRevision":1,"comment":"Reviewed by release owner"}
```

Approve and reject are business outcomes. Both succeed the approval step and
record the actor, database decision time, and comment; a later choice node can
branch on the decision when choice support is enabled. Reject does not itself
fail the run. Repeating the same decision is idempotent. An opposing decision,
stale revision, expired approval, or cancelled approval returns `409` and does
not create a second outcome.

Only an identifiable human session with `approvals:decide` can decide. Machine
API keys, workers, anonymous links, and public links are denied. Run
cancellation changes pending approvals to `CANCELLED`; expiry changes the
step/run to `FAILED` with `APPROVAL_EXPIRED` before or independently of the
periodic sweeper.
