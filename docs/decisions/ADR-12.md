# ADR-12: PostgreSQL Row-Level Security (RLS) and Application Scope Enforcement

## Status

Accepted (Baseline Blueprint v1.0)

## Context

Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision

All multi-tenant database tables enforce PostgreSQL Row-Level Security (FORCE RLS) with transaction-local tenant context (SET LOCAL), complemented by service-level RBAC.

## Rationale & Consequences

Defense-in-depth ensures that SQL bugs or connection pool reuse cannot leak data across tenant/organization boundaries.

## Contract traceability

This records the fixed decision in blueprint §32. Related runtime invariants in §9 require evidence from their implementing issues; this ADR is not runtime acceptance evidence.
