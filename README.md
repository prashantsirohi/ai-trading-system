# AI Trading System

This repository runs a staged, local-first NSE workflow:
`ingest -> features -> rank -> execute -> publish`.

Current runtime foundation:
- stage orchestration and contracts in `src/ai_trading_system/pipeline`
- domain services in `src/ai_trading_system/domains`
- market ingestion and repair utilities in `src/ai_trading_system/domains/ingest`
- retry-safe and idempotent publish delivery via `src/ai_trading_system/domains/publish`
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

## Frontend Console

- React V2 operator dashboard: [`web/execution-console-v2/ai-trading-dashboard-starter`](web/execution-console-v2/ai-trading-dashboard-starter)

## Runtime Setup Notes

- Install dependencies and project package for `src` layout imports:
  - `python -m pip install -r requirements.txt`
  - `python -m pip install -e .`
- Execution API requires `EXECUTION_API_KEY` to be set; `/api/*` requests return `500` when the key is missing.
