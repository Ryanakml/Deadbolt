# ADR-07: Renewable Leases, Epoch Fencing Tokens, and Session Binding

## Status

Accepted (Baseline Blueprint v1.0)

## Context

Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision

Step execution ownership uses a temporary lease (30s TTL, 5s heartbeat) with a strictly increasing ownership epoch (fencing token) and cryptographic session binding.

## Rationale & Consequences

Prevents zombie or partitioned workers from overwriting newer attempts or corrupting state. Stale results are rejected with 409 STALE_OWNERSHIP.

## Contract traceability

This records the fixed decision in blueprint §32. Related runtime invariants in §9 require evidence from their implementing issues; this ADR is not runtime acceptance evidence.
