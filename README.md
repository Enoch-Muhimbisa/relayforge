# RelayForge

A durable, observable workflow engine for executing dependency graphs across workers.

RelayForge is being built around a clear engineering rule: execution state must be explainable, recoverable, and testable. The initial milestone focuses on a real Go execution core and an intentionally designed operator console—not a collection of mock dashboard cards.

## Current milestone

- [x] Repository initialized with project principles
- [ ] Deterministic DAG validation and scheduling core
- [ ] Durable execution records and worker leases
- [ ] HTTP API and contract tests
- [ ] Operator console connected to real API data
- [ ] Docker-based development environment and CI

## Design principles

1. **Durability over demos** — state transitions are explicit and persisted.
2. **At-least-once execution, honest semantics** — retries can duplicate side effects; handlers must be designed accordingly.
3. **Observable by default** — runs explain what happened, not just whether they failed.
4. **Small, testable boundaries** — scheduler, storage, workers, and HTTP API remain independently testable.
5. **No fake metrics** — the interface labels demo fixtures and live system data distinctly.

## Status

Early development. Interfaces and guarantees will be documented as they become implemented. No production-readiness claim is made yet.
