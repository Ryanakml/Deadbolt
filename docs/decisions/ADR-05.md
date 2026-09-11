# ADR-05: NATS JetStream as Notification Transport Only, DB Reconciliation as Authority

## Status
Accepted (Baseline Blueprint v1.0)

## Context
Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision
NATS JetStream delivers internal wake-up hints to accelerate scheduling. PostgreSQL background reconcilers periodically sweep for ready work and expired leases.

## Rationale & Consequences
If NATS crashes, drops messages, or experiences downtime, execution continues safely via database polling without losing runnable tasks.

## Invariants Enforced
Referenced across Invariants INV-01 through INV-14.
