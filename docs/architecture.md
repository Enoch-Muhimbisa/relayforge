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
- A process restart can reclaim queued work and running work whose lease expired. Recovery replays a run from its first step; completed external side effects may therefore be repeated.
- This is **at-least-once execution**, not exactly-once execution. Real task handlers must be idempotent or use their own deduplication keys.
- Current `task` steps are local demonstrations, not arbitrary user code. Worker processes can coordinate through PostgreSQL, but this is not yet a hardened production worker fleet.
- Authentication, multi-tenant isolation, dynamic workflow definitions, operator-level authorization, and versioned schema migrations remain future work.

## Database schema

Startup creates `relayforge_runs` and indexes for creation time, status, and active lease expiry. Relational `worker_id` and `lease_until` columns coordinate worker ownership while the JSONB payload preserves the complete run snapshot. Startup migrations add lease columns idempotently to existing installations; production deployment should evolve this into versioned migrations before making incompatible schema changes.

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
