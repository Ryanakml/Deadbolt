# Developer Execution Guide — Deadbolt

This document is the daily working guide for building Deadbolt with **two developers**.

The Blueprint remains the main source of truth for architecture, contracts, and invariants. GitHub Issues remain the main source of truth for dependencies, scope, and acceptance. This document focuses on the practical parts that were still unclear before: who works on what, when Developer A and B can work in parallel, when someone can move to another issue, when it is okay to “jump” issue numbers, when someone must stop and wait, and how review, merge, staging, and acceptance should work.

The goal is simple: when one developer finishes something, they should not need to ask _“what should I take next?”_. Just check the dependencies and the list of issues that are already **Ready**.

---

## 1. How the team works

We use one `main` branch, short-lived branches for each change, one owner per issue, and cross-review.

| Role | Responsibility |
|---|---|
| Developer A | Implements the issue they claimed, runs tests, keeps the PR up to date, and prepares evidence |
| Developer B | Same as A for their own issue, while also reviewing A's work |
| Reviewer | Checks behavior, contracts, tenant/permission boundaries, race/failure cases when relevant, migrations, config, rollback, and acceptance |
| Staging coordinator | The one person currently handling deployment/migration/rollback on shared staging |

A and B are **not permanent backend/frontend roles**. Ownership can change depending on the issue.

Each developer should have at most **one active implementation issue** at a time. If someone finishes early, they can take another Ready issue, review the other developer's work, help with acceptance, or help remove a blocker.

We do not create random work just to make sure both people always look busy.

---

## 2. The most important rule: issue number is not execution order

Deadbolt is not built based on issue number order.

The real execution order is decided by **dependencies**.

Meaning:

```text
a higher issue number
does not automatically mean
all lower-numbered issues must be finished first
```

If an issue belongs to a milestone that is already unlocked and all its dependencies have passed, it can be worked on immediately.

Simple example:

```text
#23 is still Blocked
#24 is already Ready
```

A free developer can directly take `#24`.

They do not need to wait for `#23` just because `#23` has a smaller number.

But there is one hard boundary:

> **You can jump inside an unlocked milestone. You cannot jump to the next milestone before the current milestone gate passes.**

Example:

```text
M2 is still in progress
#25 has not passed yet

→ M3 is still locked
```

Even if an M3 issue looks independent or easy, implementation should not start yet.

A free developer should instead help finish M2, review, prepare tests, fixtures, diagnose blockers, or read M3 issues without starting production code.

---

## 3. When can an issue be claimed?

An issue becomes **Ready** when:

1. all required dependency issues have passed;
2. the previous milestone gate has passed;
3. the required contract/schema/protocol is already available in `main`;
4. there is no conflicting foundation change happening in another active issue;
5. the required resources are available;
6. the issue can be tested with clear acceptance criteria.

We use this status flow:

```text
Backlog
  ↓
Blocked
  ↓
Ready
  ↓
In progress
  ↓
In review
  ↓
Awaiting acceptance
  ↓
Done
```

`Blocked` does not mean the developer has to do nothing. They can still help with the issue causing the blocker.

`Ready` means the issue is technically safe to claim based on dependencies and project state.

---

## 4. Main work split between A and B

This is the **default execution plan**, not a new dependency graph.

If an issue listed as “next” is not actually Ready according to the GitHub Issue, do not force it. Take another Ready issue from the same milestone.

| Milestone | Developer A | Developer B | Gate |
|---|---|---|---|
| M0 | `#1 → #3 → #4 → #5`, then run `#7` | review `#1` → `#2 → #6`, then reproduce/review `#7` | `#7` |
| M1 | `#8 → #10 → next Ready` | `#9 → #11 → next Ready` | `#16` |
| M2 | `#17 → #18 → #19 → #20 → #21`, then take whichever of `#23/#24` is Ready | review `#17` → `#22`, then take whichever of `#24/#23` is Ready | `#25` |
| M3 | `#26 → #27 → #28 → #29` when Ready | `#30`, then review/help the engine track | `#31` |
| M4 | `#32`, then `#35` when Ready | `#33 → #34`, then help with `#35` | `#36` |
| M5 | `#37 → #38`, then `#40` when Ready | `#39 → #41`, then help with `#40/#42` | `#43` |
| M6 | `#44 → #45` | `#46 → #47` | `#49` |

If one developer finishes earlier, ownership can move. The important part is that dependencies stay correct and both developers do not independently change the same foundation.

---

## 5. M0 workflow — foundation

M0 is stricter than the later milestones because almost everything else depends on this foundation.

Developer A starts with [#1 workspace and environment](https://github.com/Ryanakml/Deadbolt/issues/1). Developer B does not start another foundation task yet; B helps review decisions, inventory prerequisites, and make sure missing provisioning inputs stay clearly documented.

After `#1` passes, Developer B works on [#2 Go/TS contracts](https://github.com/Ryanakml/Deadbolt/issues/2), while Developer A reviews the Go/engine contracts and fixtures.

After `#2` passes, the schema and fixtures become the shared reference.

Only then do the first two real implementation tracks open:

```text
Developer A → #3 migration / RLS / claim spike

Developer B → #6 worker lifecycle spike
```

This is the first point where both developers can truly work in parallel.

Both use the result of `#1` and `#2`. If one of them finds that the shared contract needs to change, they should not create their own version inside a consumer.

```text
contract change needed
        ↓
stop affected consumer work
        ↓
agree on one contract owner
        ↓
change + review
        ↓
merge to main
        ↓
consumer work continues
```

After `#3` passes, Developer A continues with [#4 auth](https://github.com/Ryanakml/Deadbolt/issues/4).

Developer B finishes `#6`. If B finishes early, they review auth, negative tests, tenant/permission boundaries, or help prepare `#5`.

Once the requirements for `#5` are satisfied, Developer A takes [#5 Compose/CI/staging](https://github.com/Ryanakml/Deadbolt/issues/5), while Developer B helps with rollout, rollback, and evidence.

After `#2–#6` are complete, run [#7 M0 gate](https://github.com/Ryanakml/Deadbolt/issues/7). One developer runs the gate, while the other tries to reproduce the evidence independently.

M1 unlocks only after `#7` actually passes.

---

## 6. M1 to M6 workflow

### M1 — first production path

After `#7`:

```text
Developer A → #8 tenant/API key
Developer B → #9 SDK
```

After both are ready:

```text
Developer A → #10 deployment
Developer B → #11 worker
```

`#10` uses worker inventory, so the inventory contract must be agreed on first.

After that, the work moves toward:

```text
#12 → #13 → #14 → #15 → #16
```

There is no need to force an A-B-A-B pattern when only one issue is Ready. If only one implementation issue is open, one developer implements while the other reviews, tests, or helps with acceptance.

M2 unlocks only after `#16`.

### M2 — durability

After `#16`, work on `#17` as the first foundation for the milestone.

Then the work splits into two tracks:

```text
Track A
#18 retry
  ↓
#19 hold
  ↓
#20 cancel
  ↓
#21 sweep
```

and:

```text
Track B
#22 artifact
```

By default, Developer A owns the state/durability track and Developer B owns the artifact track.

Later, the dependencies converge:

```text
#23 waits for #21 + #22

#24 waits for #19 + #20 + #22
```

This is a good example of why **issue number is not execution order**.

For example:

```text
#20 finished
#22 finished
#21 still in progress
```

then:

```text
#24 = Ready
#23 = still Blocked
```

A free developer can immediately take `#24`.

There is no need to wait for `#23`.

What is not allowed is running destructive resource/fault/restore tests for `#23` and `#24` at the same time on the same staging host.

After all M2 work is complete, run `#25`.

M3 unlocks only after `#25`.

### M3 — DAG and scheduling

After `#25`:

```text
Developer A → #26 parallel DAG
Developer B → #30 cron spike
```

After `#26`, `#27 choice` and `#28 pause` become the next candidates.

Developer A may continue to `#27` or `#28` even while Developer B is still finishing `#30`.

But `#27` and `#28` touch the same engine area. Do not let two developers create different state-machine rules. If there is a shared foundation, merge that first, then split again.

After `#27` and `#28`, `#29` can move forward based on its dependencies.

The final gate is `#31`, and `#31` also waits for `#30`.

### M4 — approval and timers

After `#31`:

```text
Developer A → #32 approval
Developer B → #33 delay
```

After `#33`, Developer B can continue directly to `#34 schedule` even if `#32` is still in progress, as long as the real issue dependencies are already satisfied.

Then:

```text
#35 waits for #32 + #34
```

Then the gate:

```text
#36
```

### M5 — integration

After `#36`, available work includes:

```text
#37 → #38
#39 webhook
#41 lifecycle
```

With two developers, we still keep only two active implementation issues.

Default:

```text
Developer A → #37 → #38

Developer B → #39
```

The first developer who becomes free may take `#41` when it is Ready.

Then:

```text
#40 waits for #38 + #39
```

and:

```text
#42 waits for #37 + #38 + #40 + #41
```

The final M5 gate is:

```text
#43
```

### M6 — production hardening

After `#43`:

```text
Developer A → #44 observability
Developer B → #46 release/restore
```

After `#44`, `#45 capacity` and `#47 security` become the next candidates based on their dependencies.

Default:

```text
Developer A → #45
Developer B → #47
```

But ownership can change if one developer becomes free first.

Load tests, restore tests, security tests, and fault tests on shared staging should run in separate turns so the evidence from one test does not break another test.

After `#44–#47` are complete:

```text
#48
  ↓
#49 final gate
```

Deadbolt is only considered complete after the final gate `#49` passes.

---

## 7. What should someone take after finishing early?

Use this flow:

```text
Issue finished
    ↓
Is there a Ready issue in the current milestone?
    ├─ Yes → claim it
    │
    └─ No
        ↓
Does the other developer need PR review?
        ├─ Yes → review
        │
        └─ No
            ↓
Can you help with acceptance/test/evidence?
            ├─ Yes → help
            │
            └─ No
                ↓
Is there a milestone blocker?
                ├─ Yes → help remove the blocker
                │
                └─ No
                    ↓
Prepare for the next milestone,
but do not implement before its gate unlocks
```

Preparing future work can include reading the Blueprint, reading the issue, writing questions, preparing a test plan, or identifying contract areas that may be touched.

What is not allowed yet is writing production code for the next milestone, merging next-milestone contracts, creating hidden implementation branches, or changing shared architecture early.

---

## 8. Changes that must be coordinated

| Area | Rule |
|---|---|
| `contracts/`, enums, errors, events, Go/TS fixtures | One owner per contract change. Producer and consumer should be reviewed together |
| `internal/execution/`, claim, lease, fencing, idempotency | Transition/state/lock changes must be coordinated. Do not create two different rule sets |
| `migrations/`, RLS, shared SQL | Local parallel development is fine, but migration ID/order must be coordinated and merged one by one |
| `sdk/typescript/`, `runner/node/`, `cmd/worker/` | Can move in parallel after the contract is stable |
| `apps/dashboard/` | Can work against a stable contract. Mocks are useful for development, but they are not acceptance |
| root manifest, lockfile, toolchain, shared config | Announce dependency/pin changes. Do not resolve lockfile conflicts blindly |
| `.github/workflows/`, `deploy/compose/`, staging | One active rollout coordinator |
| docs/examples/tests | Usually safe in parallel as long as expected results are not changed just to hide a bug |

If a Git conflict happens, do not immediately choose `ours` or `theirs`.

Understand the intent of both changes, decide the correct combined result, then run the tests again.

---

## 9. PR, review, and merge

Each implementation issue uses a short-lived branch created from the latest `main`.

Example:

```text
m0/3-rls-foundation
m1/9-sdk
m2/18-retry
```

Each developer or coding agent should use their own checkout/worktree. Do not let two people write to the same checkout.

Open a draft PR early enough so the direction is visible.

At minimum, a PR should explain:

```text
Issue:
Related REQ / INV:
Behavior before → after:
Changed contract:
Tests run:
Test results:
Migration/config impact:
Rollback impact:
Acceptance still pending:
```

Use `Refs #N` when the PR can be merged but issue acceptance still needs deployment or other evidence.

Use `Closes #N` only when all acceptance requirements are already proven at merge time.

**Merged does not automatically mean Done.**

Cross-review is still required. AI can help with implementation and review, but the responsible developer still needs to inspect the diff and evidence.

---

## 10. Local and staging

Each developer can use their own local stack and test DB.

If two checkouts run on the same machine, use different Compose projects, ports, volumes, and configs. A test reset should only affect resources owned by that checkout.

Shared staging follows the planned foundation:

```text
GitHub Actions
      ↓
GHCR
      ↓
automated SSH
      ↓
existing EC2 x86_64
      ↓
existing Caddy
```

Deadbolt must stay isolated from FlowDesk.

Do not reuse FlowDesk networks/volumes/services, open a second proxy on ports `80/443`, run blind image pruning, or edit live containers instead of making the change through a PR.

Before a staging operation, record:

```text
coordinator
SHA / digest
target
operation
rollback plan
```

CI builds can run in parallel.

These operations must take turns:

```text
deployment
migration
rollback
destructive fault test
restore test
load test that can disturb the environment
```

If a health gate fails, promotion stops until rollback or diagnosis is finished.

---

## 11. When is an issue or milestone actually done?

Do not use one vague “done” state for everything.

Track these separately:

```text
implemented
automated tests passed
hosted CI passed
deployed
acceptance verified
```

If something has not been done yet, write:

```text
not yet verified
```

Example:

```text
Implemented: yes
Automated tests: passed
Hosted CI: passed
Deployed: no
Acceptance verified: not yet verified
```

If deployment or hosted acceptance is part of the requirement, the issue is **not Done yet**.

Mocks are not proof of recovery, tenant isolation, real-worker behavior, or real deployment behavior.

A milestone is complete only after its integration gate passes:

```text
M0 → #7
M1 → #16
M2 → #25
M3 → #31
M4 → #36
M5 → #43
M6 → #49
```

A lot of merged PRs do not replace integration testing.

---

## 12. Daily workflow for the team

At the start of a session, it is enough to answer:

```text
What issue am I owning?
What issues are Ready right now?
What dependency am I still waiting for?
Am I touching the same contract/migration/host as the other developer?
Who is reviewing my work?
If I finish early, what is the next Ready issue?
```

A simple board is enough:

| Issue | Milestone | Status | Owner | Dependency | Next |
|---|---|---|---|---|---|
| `#N` | Mx | Ready | — | Passed | Can be claimed |
| `#N` | Mx | In progress | A | Passed | B reviews |
| `#N` | Mx | Blocked | — | Waiting `#X` | Help remove blocker |
| `#N` | Mx | Awaiting acceptance | B | Code merged | Wait for staging turn |

We do not need to assign the entire backlog to A and B upfront.

Just maintain a **Ready Queue** for the currently active milestone.

---

## Summary

```text
Issue number is not execution order.
Dependencies decide the order.

Inside the same milestone:
if Ready → it can be taken immediately.

Next milestone:
wait for the current milestone gate.

If touching shared contract/state/migration:
coordinate first.

If using shared staging:
one turn at a time.

Merged does not always mean Done.
Acceptance must be proven.
```

The project gate sequence stays:

```text
#7 → #16 → #25 → #31 → #36 → #43 → #49
```

The source-of-truth hierarchy is:

```text
Blueprint
  ↓
architecture, contracts, invariants

GitHub Issues
  ↓
dependencies, scope, acceptance, evidence

Developer Execution Guide
  ↓
A/B work split, Ready Queue, review, and daily execution
```

If dependencies change, update the GitHub Issue first. If the change affects semantics or architecture, update the Blueprint/ADR too. After that, update this execution guide.
