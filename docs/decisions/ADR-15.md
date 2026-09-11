# ADR-15: Best-Effort Diagnostic Logs and Durable Execution Events

## Status

Accepted (Baseline Blueprint v1.0)

## Context

Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision

Diagnostic console logs are bounded, sampled, and retained for 7 days. Execution state events are strictly durable, un-sampled, and retained with the run.

## Rationale & Consequences

High-volume telemetry or log ingestion backlogs never compromise execution correctness or state machine guarantees.

## Contract traceability

This records the fixed decision in blueprint §32. Related runtime invariants in §9 require evidence from their implementing issues; this ADR is not runtime acceptance evidence.
