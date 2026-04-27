# Runbook

## Operator checklist before a run

Confirm:
1. the repo virtual environment is active
2. `data/masterdata.db` exists and contains current symbols
3. you know which surface you are using: CLI, daily wrapper, FastAPI, or React V2
4. you know whether `execute` should be included, because CLI defaults and UI defaults differ
5. publish credentials are present if you are not using `--local-publish`
6. you are selecting the correct data domain

## Common commands

### Safe first local run

```bash
python -m ai_trading_system.pipeline.orchestrator --skip-preflight --stages ingest,features,rank,publish --local-publish
```

### CLI default run

```bash
python -m ai_trading_system.pipeline.orchestrator
```

This includes `execute` unless you override `--stages`.

### Daily wrapper run

```bash
python -m ai_trading_system.pipeline.daily_pipeline
```

This also defaults to `execute`, and preflight runs unless `--skip-preflight` is passed.

## Stage-only runs

Ingest only:
```bash
python -m ai_trading_system.pipeline.orchestrator --skip-preflight --stages ingest
```

Features only:
```bash
python -m ai_trading_system.pipeline.orchestrator --skip-preflight --stages features
```

Rank only:
```bash
python -m ai_trading_system.pipeline.orchestrator --stages rank
```

Execute only for an existing run id:
```bash
python -m ai_trading_system.pipeline.orchestrator --run-id <run_id> --stages execute
```

Publish only for an existing run id:
```bash
python -m ai_trading_system.pipeline.orchestrator --run-id <run_id> --stages publish
```

## Canary runs

CLI canary using the built-in reduced stage set:
```bash
python -m ai_trading_system.pipeline.orchestrator --canary --skip-preflight
```

This becomes `ingest,features,rank` when the default stage string is untouched.

Canary plus publish:
```bash
python -m ai_trading_system.pipeline.orchestrator --canary --skip-preflight --stages ingest,features,rank,publish --local-publish
```

## Publish workflows

Local-only publish retry for the latest publishable run:
```bash
python -m ai_trading_system.pipeline.orchestrator --run-id <run_id> --stages publish --local-publish
```

Publish target healthcheck:
```bash
python -m ai_trading_system.pipeline.publish_test
```

Current publish retry behavior:
- rerun only `publish` with the same `run_id`
- previously delivered channels are deduped and marked `duplicate`
- publish failures do not erase successful upstream stage state

## Trust check workflow

When rank or execute is blocked by trust:
1. inspect the latest ingest artifact under `data/pipeline_runs/<run_id>/ingest/attempt_<n>/ingest_summary.json`
2. inspect trust state in `_catalog_quarantine` and the latest `data_trust` payload exposed by the operator API or UI
3. confirm whether the status is `degraded` or `blocked`
4. only use trust overrides intentionally; do not treat `allow_untrusted_*` as a routine fix

## Recovery flow for unresolved ingest dates

Dry-run reset and validation:
```bash
python -m ai_trading_system.domains.ingest.reset_reingest_validate --from-date YYYY-MM-DD --to-date YYYY-MM-DD
```

Apply repair and validation:
```bash
python -m ai_trading_system.domains.ingest.reset_reingest_validate --from-date YYYY-MM-DD --to-date YYYY-MM-DD --apply
```

After repair:
1. rerun `ingest`
2. rerun `features`
3. rerun `rank`
4. rerun `publish` if the prior run was otherwise publishable

## Recovery flow for publish failures

Use the same `run_id` and rerun `publish` only:
```bash
python -m ai_trading_system.pipeline.orchestrator --run-id <run_id> --stages publish
```

Add `--local-publish` if you want to verify artifact assembly without network delivery.

## UI startup

FastAPI backend:
```bash
python -m ai_trading_system.ui.execution_api.app --port 8090
```

React V2 execution console:
```bash
cd web/execution-console-v2/ai-trading-dashboard-starter
npm install
npm run dev
```

## Operator checklist after a run

Confirm:
1. final run status in `pipeline_run`
2. each expected stage attempt status in `pipeline_stage_run`
3. expected artifacts exist under `data/pipeline_runs/<run_id>/...`
4. publish logs reflect `delivered`, `duplicate`, `retrying`, or `failed` as expected
5. trust state matches the run outcome
6. if `execute` was included, `data/execution.duckdb` contains the expected order and fill rows
