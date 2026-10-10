# RelayForge

**A workflow execution engine built around explainable state transitions, dependency-aware scheduling, and recovery-first engineering.**

RelayForge pairs a Go execution core with a purpose-built operator console. The interface is designed as an instrument panel for engineers: restrained color, dense operational information, explicit system status, and a dependency graph that helps operators reason about work.

## Current implementation

- Deterministic DAG validation with cycle, duplicate, and unknown-dependency rejection.
- Asynchronous workflow runs with bounded concurrent admission.
- Dependency-aware step execution, retries, step timeouts, and recorded outcomes.
- Versioned HTTP API for workflow definitions and run inspection.
- Optional PostgreSQL-backed run history and step-result snapshots, including startup schema creation and query indexes.
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

**RelayForge is still an early-stage, single-process executor.** PostgreSQL preserves run history and step snapshots, but active work is not yet resumed after a process crash. The project does not yet have a distributed worker fleet, durable leases, authentication, or multi-tenant authorization. Workflow definitions remain a built-in catalog.

The next milestone is durable worker claiming and recovery semantics, backed by integration tests against a real PostgreSQL service. See [the architecture notes](docs/architecture.md) for guarantees and limitations.

## Engineering priorities

1. Make state transitions and failure semantics testable.
2. Separate durable orchestration state from worker execution.
3. Treat retries and side effects explicitly; task handlers must be idempotent where needed.
4. Keep the operator console grounded in API data rather than fabricated metrics.
5. Publish benchmarks only with reproducible workloads and documented test conditions.
