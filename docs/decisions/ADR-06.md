# ADR-06: HTTP Long Polling for Workers and SSE for Dashboard

## Status
Accepted (Baseline Blueprint v1.0)

## Context
Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision
Worker agents connect to Control Plane via outbound HTTPS JSON long-polling (20s). Web dashboard receives real-time updates via Server-Sent Events (SSE).

## Rationale & Consequences
Minimizes protocol complexity in MVP/V1, avoids firewall/inbound port requirements for workers, and bypasses gRPC/Redis infrastructure dependencies.

## Invariants Enforced
Referenced across Invariants INV-01 through INV-14.
