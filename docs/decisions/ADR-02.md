# ADR-02: Polyglot Architecture: TypeScript Tasks, Go Control Plane, Agent, and CLI

## Status

Accepted (Baseline Blueprint v1.0)

## Context

Established by Blueprint v1.0 §32 as a fixed foundational architectural decision for Deadbolt / tf-low.

## Decision

Control plane, worker agent, and CLI are built in Go. Task definitions, execution runner, and developer SDK are built in TypeScript/Node.js.

## Rationale & Consequences

Go provides high concurrent I/O performance, lightweight single-binary distribution, and robust concurrency primitives for the coordination engine. TypeScript offers a natural developer experience for modern backend and AI workloads.

## Contract traceability

This records the fixed decision in blueprint §32. Related runtime invariants in §9 require evidence from their implementing issues; this ADR is not runtime acceptance evidence.
