# ADR-09: Fail-Fast Workflow Behavior with No Implicit Rollback

## Status
Accepted (Baseline Blueprint v1.0)

## Context
Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision
Unrecoverable step failure triggers immediate fail-fast termination of the run, revoking remaining ownership and cancelling uncommitted sibling work. No automatic rollback/compensation of side effects is performed.

## Rationale & Consequences
Side effects already committed to external systems cannot be magically undone. Partial progress remains visible in the Run Inspector, and compensation requires explicit workflow logic.

## Invariants Enforced
Referenced across Invariants INV-01 through INV-14.
