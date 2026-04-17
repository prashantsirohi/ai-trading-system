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
python -m collectors.masterdata
```

Bootstrap runtime directories (and optionally refresh seed masterdata):
```bash
python -m scripts.bootstrap_runtime_data
python -m scripts.bootstrap_runtime_data --refresh-masterdata
```

## Operational pipeline

CLI default pipeline:
```bash
python -m run.orchestrator
```

Safe local operator verification run:
```bash
python -m run.orchestrator --skip-preflight --stages ingest,features,rank,publish --local-publish
```

Daily wrapper:
```bash
python -m run.daily_pipeline
```

Run preflight explicitly on the orchestrator CLI:
```bash
python -m run.orchestrator --run-preflight --stages ingest,features,rank,publish --local-publish
```

## Stage-only runs

Ingest:
```bash
python -m run.orchestrator --skip-preflight --stages ingest
```

Features:
```bash
python -m run.orchestrator --skip-preflight --stages features
```

Rank:
```bash
python -m run.orchestrator --stages rank
```

Execute for an existing run:
```bash
python -m run.orchestrator --run-id <run_id> --stages execute
```

Publish for an existing run:
```bash
python -m run.orchestrator --run-id <run_id> --stages publish
```

## Canary

Built-in reduced canary:
```bash
python -m run.orchestrator --canary --skip-preflight
```

Canary plus local publish:
```bash
python -m run.orchestrator --canary --skip-preflight --stages ingest,features,rank,publish --local-publish
```

## Publish and recovery

Publish target healthcheck:
```bash
python -m run.publish_test
```

Repair a date window without applying changes:
```bash
python -m collectors.reset_reingest_validate --from-date YYYY-MM-DD --to-date YYYY-MM-DD
```

Repair a date window and apply changes:
```bash
python -m collectors.reset_reingest_validate --from-date YYYY-MM-DD --to-date YYYY-MM-DD --apply
```

## UI and API

NiceGUI operator console:
```bash
python -m ui.execution.app --port 8080
```

FastAPI operator backend:
```bash
python -m ui.execution_api.app --port 8090
```

Research Streamlit UI:
```bash
python -m streamlit run ui/research/app.py
```

ML Streamlit workbench:
```bash
python -m streamlit run ui/ml/app.py
```

React execution console:
```bash
cd web/execution-console
npm install
npm run dev
```

## Research and ML

Run a named research recipe:
```bash
python -m research.run_recipe --recipe <recipe_name>
```

Run a recipe bundle:
```bash
python -m research.run_recipe --bundle <bundle_name>
```

Run the shadow monitor:
```bash
python -m research.shadow_monitor
```

Backfill recent shadow-monitor days:
```bash
python -m research.shadow_monitor --backfill-days 30
```

## Installed console-script aliases

After `pip install -e .`, current aliases include:
- `ai-trading-pipeline`
- `ai-trading-publish-test`
- `ai-trading-daily`
- `ai-trading-execution-ui`
- `ai-trading-execution-api`
- `ai-trading-research-recipe`
