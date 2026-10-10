# Architecture notes

## First milestone

RelayForge begins as a single-process execution service backed by a thread-safe in-memory repository. This is a deliberate boundary, not a durability claim: process restarts currently erase run history. The repository interface is the seam for the PostgreSQL implementation planned next.

- `internal/workflow`: workflow model and deterministic DAG validation.
- `internal/engine`: bounded run concurrency, dependency-aware step execution, step timeouts, retry policy, and recorded outcomes.
- `internal/store`: repository contract and in-memory implementation.
- `internal/api`: versioned HTTP API, JSON errors, and baseline browser security headers.

## Guarantees

- Unknown dependencies, duplicate IDs, repeated edges, unsupported step kinds, and dependency cycles are rejected before execution.
- A dependent step only runs after all dependencies succeeded.
- A failed dependency blocks downstream steps.
- Current `task` steps are local demonstrations, not arbitrary user code or a distributed worker fleet.
- Retry semantics can repeat side effects; real task handlers must be idempotent.
- Authentication, multi-tenant isolation, PostgreSQL durability, leases, and crash recovery are not implemented yet.

## API v1

- `GET /api/v1/health`
- `GET /api/v1/workflows`
- `GET /api/v1/workflows/{id}`
- `POST /api/v1/workflows/{id}/runs` (202 Accepted)
- `GET /api/v1/runs?limit=50` (limit 1–200)
- `GET /api/v1/runs/{id}`

Errors use `{ "error": { "code": "...", "message": "..." } }`.
