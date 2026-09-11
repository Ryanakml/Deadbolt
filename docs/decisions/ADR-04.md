# ADR-04: PostgreSQL as Authoritative State, Atomic Outbox, and Events

## Status
Accepted (Baseline Blueprint v1.0)

## Context
Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision
PostgreSQL is the single source of truth for runs, steps, attempts, leases, timers, history, and outbox intents committed in single ACID transactions.

## Rationale & Consequences
Prevents split-brain and state loss during crashes. Broker queues and memory state are not authoritative; state transitions, audit events, and notifications commit atomically.

## Invariants Enforced
Referenced across Invariants INV-01 through INV-14.
