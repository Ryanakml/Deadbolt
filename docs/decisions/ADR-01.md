# ADR-01: Static Declarative DAG, Not Arbitrary Workflow Replay

## Status

Accepted (Baseline Blueprint v1.0)

## Context

Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision

Static declarative DAG represented as validated JSON, not arbitrary workflow-code replay.

## Rationale & Consequences

Engine reads persisted graph state from DB; restarted scheduler does not need to replay arbitrary JavaScript async functions. Replay nondeterminism is eliminated; graph shown in UI matches execution graph exactly.

## Contract traceability

This records the fixed decision in blueprint §32. Related runtime invariants in §9 require evidence from their implementing issues; this ADR is not runtime acceptance evidence.
