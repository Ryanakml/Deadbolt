# ADR-16: Cumulative Delivery, Testing, and Security Gates

## Status

Accepted (Baseline Blueprint v1.0)

## Context

Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision

Every milestone builds upon non-negotiable automated testing, failure matrix validation (F-01 to F-28), invariant enforcement, and reproducible CI builds.

## Rationale & Consequences

Features are not considered done by documentation alone. No milestone is marked complete without executable proof and automated regression evidence.

## Contract traceability

This records the fixed decision in blueprint §32. Related runtime invariants in §9 require evidence from their implementing issues; this ADR is not runtime acceptance evidence.
