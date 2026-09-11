# ADR-11: Local Task Secrets on Workers and Platform Secrets via KMS

## Status
Accepted (Baseline Blueprint v1.0)

## Context
Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision
Task environment secrets remain strictly local on customer worker hosts. The platform stores only its own credentials and webhook signing secrets encrypted with KMS.

## Rationale & Consequences
Eliminates customer secret leakage risks and simplifies compliance; the platform never holds or manages arbitrary customer API keys.

## Invariants Enforced
Referenced across Invariants INV-01 through INV-14.
