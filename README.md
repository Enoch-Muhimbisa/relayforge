# RelayForge

**A workflow execution engine built around explainable state transitions, dependency-aware scheduling, and recovery-first engineering.**

RelayForge pairs a Go execution core with a purpose-built operator console. The interface is designed as an instrument panel for engineers: restrained color, dense operational information, explicit system status, and a dependency graph that helps operators reason about work.

## Current implementation

- Deterministic DAG validation with cycle, duplicate, and unknown-dependency rejection.
- Asynchronous workflow runs with bounded concurrent admission.
- Dependency-aware step execution, retries, step timeouts, and recorded outcomes.
- Versioned HTTP API for workflow definitions and run inspection.
- PostgreSQL-backed run history with transactional, versioned schema migrations.
- Durable per-step checkpoints, stable idempotency keys, expiring worker leases, stale-worker fencing, and automatic reclaim of abandoned runs.
- React + TypeScript control room with workflow inspection, execution ledger, API connection state, and per-run step trace.
- Go race tests, static analysis, and frontend build in GitHub Actions.

## Start the full stack

Requirements: Docker Compose.

```sh
# Optional: replace the local-development password before starting
export RELAYFORGE_DB_PASSWORD='use-a-strong-local-password'
docker compose up --build
```

Open http://localhost:5173 for the console and http://localhost:8080 for the API. PostgreSQL data is stored in the named `relayforge_postgres` volume.

For a memory-only API, unset `RELAYFORGE_DATABASE_URL` and run `go mod tidy && go test -race ./... && go vet ./... && go run ./cmd/relayforge`.

## API

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/api/v1/health` | Service health |
| GET | `/api/v1/workflows` | List workflow definitions |
| GET | `/api/v1/workflows/{id}` | Read a definition |
| POST | `/api/v1/workflows/{id}/runs` | Start a run (202 Accepted) |
| GET | `/api/v1/runs?limit=50` | List recent runs |
| GET | `/api/v1/runs/{id}` | Inspect one run |

## Engineering honesty

**RelayForge is an early-stage at-least-once workflow engine.** PostgreSQL-backed workers coordinate through atomic claims and expiring leases; queued work and runs with expired leases can be reclaimed after a restart. Recovery skips successfully checkpointed steps and retries interrupted steps with the same stable idempotency key. A crash after an external side effect but before its success checkpoint can still repeat that operation unless the downstream handler honors the key. The project does not yet have authentication, multi-tenant authorization, dynamic workflow definitions, or production-grade external task handlers. Workflow definitions remain a built-in catalog.

Worker claiming, lease expiry, stale-worker fencing, durable step checkpoints, stable per-step idempotency keys, transactional schema migrations, and failure/recovery tests are implemented. External handlers must pass the provided key to downstream systems that support deduplication; RelayForge cannot guarantee exactly-once side effects by itself. See [the architecture notes](docs/architecture.md) for guarantees and limitations.

## Engineering priorities

1. Make state transitions and failure semantics testable.
2. Separate durable orchestration state from worker execution.
3. Treat retries and side effects explicitly; task handlers must be idempotent where needed.
4. Keep the operator console grounded in API data rather than fabricated metrics.
5. Publish benchmarks only with reproducible workloads and documented test conditions.
