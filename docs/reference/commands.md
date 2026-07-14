# Commands

- **Purpose:** Authoritative runnable command and console-entrypoint reference.
- **Audience:** Operators and developers.
- **Last verified:** 2026-07-14
- **Source of truth:** `pyproject.toml [project.scripts]` and the referenced CLI parsers.

---

Start with the common workflows in the [System Guide](../SYSTEM_GUIDE.md). Commands below are run from the repository root unless they explicitly change directories.

## Environment

```bash
set -a
source .env
set +a
```

Use the virtual-environment interpreter. `PYTHONPATH=src` permits module execution without relying on an editable installation:

```bash
python3 -m venv .venv
./.venv/bin/python -m pip install -r requirements.txt
./.venv/bin/python -m pip install -e .
```

## Bootstrap and health

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.bootstrap_runtime_data
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.bootstrap_runtime_data --refresh-masterdata
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.healthcheck
```

## Operational pipeline

Default operational run:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator --data-domain operational
```

Run readiness checks before stages:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator --run-preflight
```

Reduced real-data canary with local publishing:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --canary --symbol-limit 25 --local-publish
```

Daily wrapper:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.daily_pipeline
```

## Stage selection and retry

The `features` alias expands to all feature substages. Explicit stage lists do not automatically add omitted upstream dependencies.

```bash
# One new ingest attempt.
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator --stages ingest

# Full feature expansion.
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator --stages features

# Retry publish against registered artifacts for an existing run.
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --run-id <run_id> --stages publish

# Force a new attempt for an already completed requested stage.
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --run-id <run_id> --stages rank --force-rerun

# Bypass same-date auto-resume and create a fresh run.
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator --new-run
```

The default CLI stage string includes `fundamentals` and `candidate_tracker` but omits `opportunities` and `narrative`. `--opportunity-registry-mode shadow` inserts the non-authoritative opportunity stage after Investigator; add `--opportunity-registry-dry-run` to suppress registry writes. `--no-enable-candidate-tracker` removes the tracker from the default list. Although `--no-enable-fundamentals` exists, the current default stage string explicitly names `fundamentals`; omit it with an explicit `--stages` list. Run `--help` for the full current flag set.

Opportunity shadow run and isolated retry:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --opportunity-registry-mode shadow

PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --run-id <run_id> --stages opportunities \
  --opportunity-registry-mode shadow --opportunity-registry-dry-run
```

## Publish and recovery

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.publish_test

# Dry-run ingest repair.
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.domains.ingest.reset_reingest_validate \
  --from-date YYYY-MM-DD --to-date YYYY-MM-DD

# Apply only after backup and explicit approval.
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.domains.ingest.reset_reingest_validate \
  --from-date YYYY-MM-DD --to-date YYYY-MM-DD --apply
```

See [data repair](../runbooks/data_repair.md), [publish retry](../runbooks/publish_retry.md), and [backup and restore](../runbooks/backup_and_restore.md).

## API and operator console

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.ui.execution_api.app --port 8090
```

```bash
cd web/execution-console-v2/ai-trading-dashboard-starter
npm install
npm run dev
```

```bash
curl http://localhost:8090/api/execution/health
```

## Research and optimization

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.research.run_recipe --recipe <recipe_name>
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.research.run_recipe --bundle <bundle_name>
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.research.shadow_monitor
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.research.shadow_monitor --backfill-days 30
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.research.optimization.cli --help
```

Research commands must preserve `DATA_DOMAIN=research` isolation where required by their contracts.

## Installed console scripts

After `pip install -e .`, these aliases are defined by `pyproject.toml`:

| Alias | Entrypoint |
|---|---|
| `ai-trading-pipeline` | Canonical pipeline orchestrator |
| `ai-trading-daily` | Daily pipeline wrapper |
| `ai-trading-healthcheck` | Operator health probe |
| `ai-trading-publish-test` | Publish-channel health check |
| `ai-trading-execution-api` | FastAPI backend |
| `ai-trading-bootstrap-data` | Runtime-data bootstrap |
| `ai-trading-repair-ingest-schema` | Ingest schema repair |
| `ai-trading-repair-control-plane-timestamps` | Control-plane timestamp repair |
| `ai-trading-research-recipe` | Research recipe runner |
| `ai-trading-optimize` | Optimization runner |
| `ai-trading-optimize-promote` | Optimization promotion workflow |
| `ai-trading-fundamentals-sync` | Screener fundamentals sync |
| `ai-trading-fundamentals-refresh-readmodels` | Fundamentals read-model refresh |
| `ai-trading-fundamentals-validate-exports` | Fundamentals export validation |
| `ai-trading-valuation-refresh` | Valuation feature refresh |
| `ai-trading-sector-earnings-refresh` | Sector earnings refresh |
| `ai-trading-backfill-operational-valuation` | Operational valuation backfill |
| `ai-trading-daily-gainers-report` | Daily gainers report |
| `ai-trading-fundamental-opportunity-report` | Fundamental opportunities report |
| `ai-trading-winner-validation-report` | Winner validation report |
| `ai-trading-early-accumulation-validate` | Early accumulation validation |
| `ai-trading-symbol-report` | Symbol research report |

For any mutating repair, migration, backfill, promotion, or live execution command, inspect `--help`, confirm the target data domain, and take the required backup first.
