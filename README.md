# RelayForge

**A workflow execution engine built around explainable state transitions, dependency-aware scheduling, and recovery-first engineering.**

RelayForge pairs a Go execution core with a purpose-built operator console. The interface is designed as an instrument panel for engineers: restrained color, dense operational information, explicit system status, and a dependency graph that helps operators reason about work.

## Current implementation

- Deterministic DAG validation with cycle, duplicate, and unknown-dependency rejection.
- Asynchronous workflow runs with bounded concurrent admission.
- Dependency-aware step execution, retries, step timeouts, and recorded outcomes.
- Versioned HTTP API for workflow definitions and run inspection.
- React + TypeScript control room with a workflow inspector, execution ledger, live API connection state, and per-run step trace.
- Go unit tests and a GitHub Actions CI workflow.

## Start locally

Requirements: Go 1.23+ and Node.js 20+.

Terminal 1:

```sh
go mod download
go test ./...
go run ./cmd/relayforge
```

Terminal 2:

```sh
cd web
npm install
npm run dev
```

Open http://localhost:5173. The API listens on http://localhost:8080 by default. To change the API URL for the console, set `VITE_API_URL` before starting Vite.

## API

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/api/v1/health` | Service health |
| GET | `/api/v1/workflows` | List workflow definitions |
| GET | `/api/v1/workflows/{id}` | Read a definition |
| POST | `/api/v1/workflows/{id}/runs` | Start a run (202 Accepted) |
| GET | `/api/v1/runs?limit=50` | List recent runs |
| GET | `/api/v1/runs/{id}` | Inspect one run |

## Architecture honesty

**This is an early milestone, not a production-ready distributed platform.** Execution currently happens in one process and run state lives in memory. Restarting the service loses run history. The step runner uses local demonstration tasks; there is no distributed worker fleet, PostgreSQL persistence, durable leases, authentication, or crash recovery yet.

See [the architecture notes](docs/architecture.md) for current guarantees, limitations, and the planned evolution toward PostgreSQL-backed state, lease-based workers, telemetry, and access controls.

## Engineering priorities

1. Make state transitions and failure semantics testable.
2. Add durable storage before claiming recovery guarantees.
3. Treat retries and side effects explicitly; task handlers must be idempotent where needed.
4. Keep the operator console grounded in API data rather than fabricated metrics.
5. Publish benchmarks only with reproducible workloads and documented test conditions.
