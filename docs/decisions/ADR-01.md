# ADR-01: Static Declarative DAG, Not Arbitrary Workflow Replay

## Status
Accepted (Baseline Blueprint v1.0)

## Context
Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision
Static declarative DAG represented as validated JSON, not arbitrary workflow-code replay.

## Rationale & Consequences
Engine reads persisted graph state from DB; restarted scheduler does not need to replay arbitrary JavaScript async functions. Replay nondeterminism is eliminated; graph shown in UI matches execution graph exactly.

## Invariants Enforced
Referenced across Invariants INV-01 through INV-14.
