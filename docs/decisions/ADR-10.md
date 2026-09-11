# ADR-10: Immutable Deployments and Pinning Runs to Bundle Digests

## Status
Accepted (Baseline Blueprint v1.0)

## Context
Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision
Deployments are immutable manifests tied to SHA-256 bundle digests. Runs remain permanently pinned to their creation deployment.

## Rationale & Consequences
New deployments affect only new runs. Old active runs continue on compatible workers without breaking due to code drift or schema changes.

## Invariants Enforced
Referenced across Invariants INV-01 through INV-14.
