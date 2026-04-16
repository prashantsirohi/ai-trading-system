# AI Trading System

This repository runs a staged, local-first NSE workflow:
`ingest -> features -> rank -> execute -> publish`.

Current runtime foundation:
- stage orchestration and contracts in `run/stages/*`
- domain services in `services/ingest`, `services/features`, `services/rank`, `services/execute`, and `services/publish`
- market ingestion and repair utilities in `collectors/*` and `scripts/*`
- retry-safe and idempotent publish delivery via `run/publisher.py`
- control-plane and run tracking in `data/control_plane.duckdb`

## Documentation

Canonical docs live under [`docs/README.md`](docs/README.md).

Recommended starting points:
- system overview: [`docs/architecture/system-overview.md`](docs/architecture/system-overview.md)
- pipeline contracts: [`docs/architecture/pipeline.md`](docs/architecture/pipeline.md)
- module ownership: [`docs/architecture/module-map.md`](docs/architecture/module-map.md)
- commands: [`docs/reference/commands.md`](docs/reference/commands.md)
- operator runbook: [`docs/operations/runbook.md`](docs/operations/runbook.md)
- refactor end-state and migration notes: [`docs/refactor/final_architecture.md`](docs/refactor/final_architecture.md)
