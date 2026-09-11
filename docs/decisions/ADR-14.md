# ADR-14: Polling Result in MVP with Signed Webhooks in V1

## Status
Accepted (Baseline Blueprint v1.0)

## Context
Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision
MVP client reads results via polling/SSE. V1 adds transactional, signed outbound webhooks (HMAC-SHA256) with SSRF defense and retry queues.

## Rationale & Consequences
Keeps MVP focused on core execution durability while laying the groundwork for robust external integration in V1.

## Invariants Enforced
Referenced across Invariants INV-01 through INV-14.
