# Commands

This file lists the current runnable commands used by the codebase. Examples use the `python -m ...` form because it works without relying on installed console-script aliases.

## Setup

Create a virtual environment:
```bash
python3 -m venv .venv
. .venv/bin/activate
```

Install dependencies and local entrypoints:
```bash
pip install -r requirements.txt
pip install -e .
```

Bootstrap master data:
```bash
python -m ai_trading_system.domains.ingest.masterdata
```

Bootstrap runtime directories (and optionally refresh seed masterdata):
```bash
python -m ai_trading_system.interfaces.cli.bootstrap_runtime_data
python -m ai_trading_system.interfaces.cli.bootstrap_runtime_data --refresh-masterdata
```

## Operational pipeline

CLI default pipeline:
```bash
python -m ai_trading_system.pipeline.orchestrator
```

Safe local operator verification run:
```bash
python -m ai_trading_system.pipeline.orchestrator --skip-preflight --stages ingest,features,rank,publish --local-publish
```

Daily wrapper:
```bash
python -m ai_trading_system.pipeline.daily_pipeline
```

Run preflight explicitly on the orchestrator CLI:
```bash
python -m ai_trading_system.pipeline.orchestrator --run-preflight --stages ingest,features,rank,publish --local-publish
```

## Stage-only runs

Ingest:
```bash
python -m ai_trading_system.pipeline.orchestrator --skip-preflight --stages ingest
```

Features:
```bash
python -m ai_trading_system.pipeline.orchestrator --skip-preflight --stages features
```

Rank:
```bash
python -m ai_trading_system.pipeline.orchestrator --stages rank
```

Execute for an existing run:
```bash
python -m ai_trading_system.pipeline.orchestrator --run-id <run_id> --stages execute
```

Publish for an existing run:
```bash
python -m ai_trading_system.pipeline.orchestrator --run-id <run_id> --stages publish
```

## Canary

Built-in reduced canary:
```bash
python -m ai_trading_system.pipeline.orchestrator --canary --skip-preflight
```

Canary plus local publish:
```bash
python -m ai_trading_system.pipeline.orchestrator --canary --skip-preflight --stages ingest,features,rank,publish --local-publish
```

## Publish and recovery

Publish target healthcheck:
```bash
python -m ai_trading_system.pipeline.publish_test
```

Repair a date window without applying changes:
```bash
python -m ai_trading_system.domains.ingest.reset_reingest_validate --from-date YYYY-MM-DD --to-date YYYY-MM-DD
```

Repair a date window and apply changes:
```bash
python -m ai_trading_system.domains.ingest.reset_reingest_validate --from-date YYYY-MM-DD --to-date YYYY-MM-DD --apply
```

## UI and API

FastAPI operator backend:
```bash
python -m ai_trading_system.ui.execution_api.app --port 8090
```

React V2 execution console:
```bash
cd web/execution-console-v2/ai-trading-dashboard-starter
npm install
npm run dev
```

## Research and ML

Run a named research recipe:
```bash
python -m ai_trading_system.research.run_recipe --recipe <recipe_name>
```

Run a recipe bundle:
```bash
python -m ai_trading_system.research.run_recipe --bundle <bundle_name>
```

Run the shadow monitor:
```bash
python -m ai_trading_system.research.shadow_monitor
```

Backfill recent shadow-monitor days:
```bash
python -m ai_trading_system.research.shadow_monitor --backfill-days 30
```

## Installed console-script aliases

After `pip install -e .`, current aliases include:
- `ai-trading-pipeline`
- `ai-trading-publish-test`
- `ai-trading-daily`
- `ai-trading-execution-api`
- `ai-trading-research-recipe`
- `ai-trading-bootstrap-data`
- `ai-trading-repair-ingest-schema`
