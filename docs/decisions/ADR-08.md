# ADR-08: Mandatory Explicit Recovery Policy (Safe, Idempotent, Reconcile)

## Status
Accepted (Baseline Blueprint v1.0)

## Context
Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision
Every task definition must explicitly declare recovery mode: safe, idempotent, or reconcile.

## Rationale & Consequences
Tasks cannot be assumed safely repeatable. External side effects with unknown outcomes halt in WAITING/RECONCILIATION holds rather than causing duplicate charges or actions.

## Invariants Enforced
Referenced across Invariants INV-01 through INV-14.
