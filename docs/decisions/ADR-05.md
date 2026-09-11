# ADR-05: NATS JetStream as Notification Transport Only, DB Reconciliation as Authority

## Status

Accepted (Baseline Blueprint v1.0)

## Context

Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision

NATS JetStream delivers internal wake-up hints to accelerate scheduling. PostgreSQL background reconcilers periodically sweep for ready work and expired leases.

## Rationale & Consequences

If NATS crashes, drops messages, or experiences downtime, execution continues safely via database polling without losing runnable tasks.

## Contract traceability

This records the fixed decision in blueprint §32. Related runtime invariants in §9 require evidence from their implementing issues; this ADR is not runtime acceptance evidence.
