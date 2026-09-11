# ADR-13: Modular Monolith Architecture with Docker Compose Reference Deployment

## Status

Accepted (Baseline Blueprint v1.0)

## Context

Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision

Control plane modules (API, Gateway, Engine, Scheduler, Reconciler, Outbox, SSE) run in a single modular Go binary. Reference deployment uses Docker Compose.

## Rationale & Consequences

Maximizes development velocity, testability, and operational simplicity for single-region MVP/V1 before considering microservice/Kubernetes overhead.

## Contract traceability

This records the fixed decision in blueprint §32. Related runtime invariants in §9 require evidence from their implementing issues; this ADR is not runtime acceptance evidence.
