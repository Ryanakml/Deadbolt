# Choice and merge execution

Deadbolt workflows support deterministic branching and structured convergence through declarative `choice` and `merge` control nodes (Blueprint §6, §10, §16; INV-08, INV-09; F-16, F-28).

Branching is purely declarative: conditions are evaluated by the control plane without dynamic code execution, external I/O, or worker assignment. When a branch is chosen, unselected branches are immediately marked `SKIPPED` with reason `BRANCH_NOT_SELECTED`, and the convergence `merge` node waits strictly for the selected branch's terminal step.

```ts
import {
  defineWorkflow,
  choiceNode,
  mergeNode,
  expr,
  input,
  output,
  literal,
} from "@runtime/sdk";

const workflow = defineWorkflow({
  name: "conditional-approval",
  inputSchema: {
    type: "object",
    properties: { amount: { type: "number" } },
    required: ["amount"],
    additionalProperties: false,
  },
  outputSchema: {
    type: "object",
    properties: {
      branch: { type: "string" },
      value: { type: "object" },
    },
    required: ["branch", "value"],
    additionalProperties: false,
  },
  nodes: [
    choiceNode("evaluate-risk", {
      branches: [
        {
          name: "high-value",
          condition: expr("gt", input("/amount"), literal(10000)),
        },
        {
          name: "standard",
        },
      ],
      default: "standard",
    }),
    {
      id: "manual-review",
      type: "task",
      task: manualReviewTask,
      after: ["evaluate-risk"],
      input: { amount: input("/amount") },
    },
    {
      id: "auto-approve",
      type: "task",
      task: autoApproveTask,
      after: ["evaluate-risk"],
      input: { amount: input("/amount") },
    },
    mergeNode(
      "approval-outcome",
      {
        choice: "evaluate-risk",
        branches: [
          {
            branch: "high-value",
            terminal: "manual-review",
            value: output("manual-review", "/"),
          },
          {
            branch: "standard",
            terminal: "auto-approve",
            value: output("auto-approve", "/"),
          },
        ],
      },
      ["manual-review", "auto-approve"],
    ),
  ],
  output: {
    branch: output("approval-outcome", "/branch"),
    value: output("approval-outcome", "/value"),
  },
});
```

## Declarative choice expressions

Choice conditions are expressed using canonical JSON expression trees. Only the following 11 operators are permitted:

| Category       | Operators                | Semantics & Types                                                                                |
| :------------- | :----------------------- | :----------------------------------------------------------------------------------------------- |
| **Relational** | `gt`, `gte`, `lt`, `lte` | Strictly numbers (`float64`) or strings. Operands must have identical types.                     |
| **Equality**   | `eq`, `neq`              | Any JSON types with identical kinds.                                                             |
| **Membership** | `in`                     | First argument is any scalar/object; second argument must be an array of matching element types. |
| **Existence**  | `exists`                 | Unary operator checking if a reference exists and is not `null` or `undefined`.                  |
| **Logical**    | `and`, `or`              | Variable arguments of boolean sub-expressions. Short-circuiting evaluation.                      |
| **Negation**   | `not`                    | Unary operator negating a boolean sub-expression.                                                |

### Strict safety & rejection rules

1. **No dynamic execution**: Arbitrary JavaScript, `eval`, `Function`, filesystem access, timers, random numbers, or network requests are strictly prohibited and fail validation with `INVALID_EXPRESSION`.
2. **No type coercion**: Implicit coercion (e.g. `5 == "5"`, `null == 0`, `"" == false`) is prohibited. Mismatched operand types fail immediately with `INVALID_EXPRESSION`.
3. **Sequential evaluation**: Branches are evaluated in array declaration order. The first branch whose condition evaluates to `true` is selected.
4. **Default fallback**: If no branch condition evaluates to `true`, the declared `default` branch is selected. If no condition matches and no `default` is configured, execution fails fast with `INVALID_EXPRESSION`.

## Structured branch validation

To guarantee deterministic scheduling, bounded resource usage, and clean state recovery, workflows with choice and merge nodes must adhere to strict structural constraints:

1. **Declared merge convergence**: Every `choice` node must have a corresponding `merge` node referencing it.
2. **Disjoint branch subgraphs**: Branches originating from a choice node must be strictly non-overlapping until they converge at the declared `merge` node.
3. **No cross-branch dependencies**: A node in branch $A$ may never depend on (`after`) or reference (`$ref`) a node in branch $B$. Violations fail validation with `CROSS_BRANCH_DEPENDENCY`.
4. **No branch leaks**: Nodes outside the branch may not reference internal branch steps directly. References from outside must exit strictly through the `merge` node. Unmerged outside references fail validation with `INPUT_MAPPING_ERROR`.
5. **Nesting depth cap**: Choice nesting is strictly capped at a depth of 8. Exceeding this limit fails validation with `CHOICE_NESTING_EXCEEDED`.
6. **No irreducible graphs**: Arbitrary multi-exit graphs or unstructured loops fail validation with `IRREDUCIBLE_GRAPH`.

## Merge tagged output & skipped branch handling

When a choice node transitions to `SUCCEEDED`:

1. The selected branch name is durably recorded in `run_steps.output` as `{"selected": branchName, "branch": branchName}` and emitted in `STEP_SUCCEEDED`.
2. All nodes belonging to unselected branches are immediately marked `SKIPPED` in `run_steps` with `wait_reason = 'BRANCH_NOT_SELECTED'` and emit `STEP_SKIPPED`.

The corresponding `merge` node:

- Inspects the choice step's committed output to identify the selected branch.
- Waits strictly for the selected branch's terminal node to reach `SUCCEEDED`.
- **Never hangs on skipped branches**: Terminals of unselected branches are already `SKIPPED`; the merge node ignores their skipped status and does not propagate `DEPENDENCY_SKIPPED`.
- Produces a canonical schema-valid tagged object:
  ```json
  {
    "branch": "high-value",
    "value": { "status": "approved", "reviewer": "ops@example.com" }
  }
  ```
- If declared, validates the tagged output against `merge.outputSchema`, failing fast with `SCHEMA_VALIDATION_ERROR` on violation.

## Scheduler replay & reconciliation invariance

Deadbolt execution is resilient to process crashes, broker loss, and restart:

- Both normal completion advancement (`advanceAfterStepSuccessTx`) and control-plane reconciliation repair (`ReconcileReadyWork`) share the exact same `evaluateBlockedDAGTx` evaluation logic.
- During recovery or replay, the choice node remains `SUCCEEDED` with its original selected branch intact in Postgres.
- Unselected branch nodes remain `SKIPPED` with `BRANCH_NOT_SELECTED`.
- Terminal work is **never reopened**, preserving exact single-branch selection and preventing duplicate executions or side effects.
