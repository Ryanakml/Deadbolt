# ADR-06: HTTP Long Polling for Workers and SSE for Dashboard

## Status

Accepted (Baseline Blueprint v1.0)

## Context

Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision

Worker agents connect to Control Plane via outbound HTTPS JSON long-polling (20s). Web dashboard receives real-time updates via Server-Sent Events (SSE).

## Rationale & Consequences

Minimizes protocol complexity in MVP/V1, avoids firewall/inbound port requirements for workers, and bypasses gRPC/Redis infrastructure dependencies.

## Contract traceability

This records the fixed decision in blueprint §32. Related runtime invariants in §9 require evidence from their implementing issues; this ADR is not runtime acceptance evidence.
