# Architecture notes

## Runtime modes

RelayForge can run in two modes:

- **PostgreSQL mode**: set `RELAYFORGE_DATABASE_URL`. Run history and step-result snapshots are stored as JSONB, indexed by creation time and status, and survive API process restarts.
- **Memory mode**: used when the database URL is unset. This is convenient for quick local development, but run history disappears when the process exits.

Workflow definitions are currently a built-in, versioned catalog. They are not dynamically authored or persisted yet.

## Components

- `internal/workflow`: workflow model and deterministic DAG validation.
- `internal/engine`: fixed worker pools, dependency-aware step execution, step timeouts, retry policy, lease checkpoints, and recorded outcomes.
- `internal/store`: repository interface, in-memory adapter, and PostgreSQL run-history adapter.
- `internal/api`: versioned HTTP API, JSON errors, and baseline browser security headers.
- `web`: React/TypeScript operator console.

## Guarantees and boundaries

- Unknown dependencies, duplicate IDs, repeated edges, unsupported step kinds, and dependency cycles are rejected before execution.
- Root steps are included in deterministic topological ordering.
- A dependent step only runs after all dependencies succeeded; downstream steps are marked cancelled when a dependency fails.
- PostgreSQL persists run snapshots and step outcomes across API restarts.
- Workers claim queued runs in a transaction using `FOR UPDATE SKIP LOCKED`; each claim has an expiring lease and a worker owner.
- Each checkpoint renews the lease. Updates from a worker whose lease expired or was reclaimed are rejected, preventing stale workers from overwriting a newer owner's state.
- Every step has a stable idempotency key derived from the run ID and step ID. Retries and crash recovery reuse the same key.
- A running checkpoint is persisted before invoking a handler; successful results are persisted after the handler returns. On recovery, succeeded steps are skipped and interrupted steps are retried.
- A crash after an external side effect but before the success checkpoint can still repeat that operation. External handlers should pass the stable key to downstream services that support deduplication.
- This is **at-least-once execution**, not exactly-once execution. RelayForge cannot guarantee exactly-once side effects when an external system does not participate in idempotency.
- Current `task` steps are local demonstrations, not arbitrary user code. Worker processes can coordinate through PostgreSQL, but this is not yet a hardened production worker fleet.
- Authentication, multi-tenant isolation, dynamic workflow definitions, operator-level authorization, and versioned schema migrations remain future work.

## Database schema

The `relayforge_schema_migrations` ledger records applied schema versions. Migrations run transactionally and use a PostgreSQL advisory transaction lock to serialize concurrent application startup. Version 1 creates the run-history table and indexes; version 2 adds lease-owner and lease-expiry columns and an index. The JSONB payload preserves the complete run snapshot, including step checkpoints and idempotency keys. Add a new numbered migration for future schema changes rather than editing already-applied migrations.

## Run locally

For the full stack with PostgreSQL:

```sh
docker compose up --build
```

The console is at `http://localhost:5173`; the API is at `http://localhost:8080`. Set `RELAYFORGE_DB_PASSWORD` to a strong value before using Compose beyond a local development machine.

For a memory-only API:

```sh
go mod tidy
go test -race ./...
go vet ./...
go run ./cmd/relayforge
```

## API v1

- `GET /api/v1/health`
- `GET /api/v1/workflows`
- `GET /api/v1/workflows/{id}`
- `POST /api/v1/workflows/{id}/runs` (202 Accepted)
- `GET /api/v1/runs?limit=50` (limit 1–200)
- `GET /api/v1/runs/{id}`

Errors use `{ "error": { "code": "...", "message": "..." } }`.
