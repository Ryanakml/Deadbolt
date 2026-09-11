# ADR-03: Hosted Control Plane with Customer-Hosted Compute

## Status

Accepted (Baseline Blueprint v1.0)

## Context

Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision

Control plane is centrally hosted; task execution runs exclusively on customer-managed workers via outbound HTTPS long-polling.

## Rationale & Consequences

Customer task code and proprietary secrets stay on customer infrastructure. Avoids security risks and complexities of hostile multi-tenant code execution on platform servers in MVP/V1.

## Contract traceability

This records the fixed decision in blueprint §32. Related runtime invariants in §9 require evidence from their implementing issues; this ADR is not runtime acceptance evidence.
