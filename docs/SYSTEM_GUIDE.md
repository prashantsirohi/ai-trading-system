# AI Trading System Guide

- **Purpose:** Canonical orientation and operating contract for the current AI Trading System.
- **Audience:** Operators, developers, reviewers, and coding agents.
- **Last verified:** 2026-07-14
- **Source of truth:** Current code, primarily `src/ai_trading_system/pipeline/orchestrator.py`, `src/ai_trading_system/platform/db/paths.py`, `src/ai_trading_system/pipeline/registry.py`, `src/ai_trading_system/domains/execution/store.py`, and `pyproject.toml`.

---

This is the single starting point for understanding the system. Code is authoritative for runtime behavior; this guide is the canonical human-readable summary. Follow its links instead of searching the repository or relying on older summaries.

## System purpose and boundaries

The repository contains a single-operator, NSE-focused trading and research system. The operational domain ingests trusted market data, computes features, ranks opportunities, prepares and tracks candidates, optionally enriches them, dispatches paper or explicitly authorized live orders, and publishes operator views. The research domain runs isolated backtests, optimizations, model training, and performance tracking.

The main surfaces are:

- The Python pipeline and domain packages under `src/ai_trading_system/`.
- The FastAPI operator backend under `src/ai_trading_system/ui/execution_api/`.
- The React operator console under `web/execution-console-v2/ai-trading-dashboard-starter/`.
- External runtime storage resolved from `.env`, normally through `DATA_ROOT`.

## Safety and operating invariants

- Resolve live data through the existing path helpers and `$DATA_ROOT`; never hardcode a repo-local `data/...` path in application code.
- The local operator setting is `DATA_ROOT=/Volumes/MacData/Trading/data`. If `DATA_ROOT` is unset, code retains a legacy repo-local fallback; operational work must load `.env` and use the configured external root.
- NSE bhavcopy is the operational OHLC source of record. Provider fallback and quarantine behavior are defined in [data sources](reference/data_sources.md) and [trust and DQ](architecture/data_trust_and_dq.md).
- Synthetic smoke data is disabled. Canary runs use a reduced real symbol universe.
- Critical trust or DQ failures block downstream execution.
- Historical ranking is point-in-time: market, return, volume, delivery, sector,
  stage, benchmark, and persisted feature inputs cannot read observations after
  the requested run date.
- Default artifact resolution promotes only outputs whose exact producing stage
  attempt completed. Failed-attempt files remain immutable forensic evidence but
  cannot feed retries, execution, or publishing.
- Paper execution is the safe default. Do not enable live broker placement without explicit operator authorization, and do not describe the live path as production-certified.
- Preview, diagnostics, documentation checks, and tests must not mutate broker state or live DuckDB files.

## Operational design and stages

<!-- system-guide-logical-stages: ingest,features,rank,investigator,fundamentals,candidates,candidate_tracker,events,execute,insight,narrative,publish,perf_tracker -->

```text
ingest -> features -> rank -> investigator -> fundamentals* -> candidates
       -> candidate_tracker -> events -> execute -> insight -> narrative
       -> publish -> perf_tracker
```

`PIPELINE_ORDER` contains all 13 logical stages above. The current CLI default omits `narrative`, so its normal stage list is `ingest,features,rank,investigator,fundamentals,candidates,candidate_tracker,events,execute,insight,publish,perf_tracker`. Canary mode replaces that default with `ingest,features,rank`.

`fundamentals` is optional in the orchestrator's implicit-stage contract, but the CLI's default stage string names it explicitly. To omit it from a CLI run, pass an explicit `--stages` list without `fundamentals`; the current `--no-enable-fundamentals` flag does not remove it from that default string. `candidate_tracker` is enabled by default and `--no-enable-candidate-tracker` removes it from the default CLI list. Any other explicit `--stages` list runs only the requested stages after expanding the `features` alias.

| Stage | Responsibility | Primary handoff | Detailed contract |
|---|---|---|---|
| `ingest` | Refresh, validate, provenance-tag, and quarantine operational OHLCV/delivery data. | Trusted catalog and ingest artifacts | [ingest](stages/ingest.md) |
| `features` | Compute technical, sector, valuation, earnings, and derived feature snapshots. | Feature Parquet and snapshot metadata | [features](stages/features.md) |
| `rank` | Score the universe and materialize ranking, breakout, pattern, stock, sector, and Stage 1 evidence. | Rank artifact family | [rank](stages/rank.md) |
| `investigator` | Build a non-executable operator investigation queue from post-rank evidence. | Investigator artifacts and control-plane history | [investigator](stages/investigator.md) |
| `fundamentals` | Optionally import and score fundamental evidence. | Fundamental scores and watchlists | [fundamentals](stages/fundamentals.md) |
| `candidates` | Deterministically select the operator/execution shortlist. | `final_candidates.csv` | [candidates](stages/candidates.md) |
| `candidate_tracker` | Maintain durable lifecycle episodes, reviews, alerts, and current candidate state. | Tracker DB and tracker artifacts | [candidate tracker](stages/candidate_tracker.md) |
| `events` | Collect and enrich catalyst/event evidence. | Event packet and enriched rank data | [events](stages/events.md) |
| `execute` | Apply trust, policy, portfolio, and risk gates before paper or authorized live dispatch. | Actions, orders, fills, positions | [execute](stages/execute.md) |
| `insight` | Build the structured analyst brief from upstream evidence. | `market_insight.json` | [insight](stages/insight.md) |
| `narrative` | Render the configured market narrative. | `market_report.json` | [narrative](stages/narrative.md) |
| `publish` | Deliver already-materialized outputs to configured channels. | Delivery records and publish summary | [publish](stages/publish.md) |
| `perf_tracker` | Mature forward-return cohorts in the research domain. | Research performance rows | [performance tracker](stages/perf_tracker.md) |

### Feature substages

The `features` CLI alias expands in this exact order:

<!-- system-guide-feature-substages: features_technical,features_sector_rs,features_valuation,features_stock_valuation_bands,features_sector_earnings,features_phase1,features_snapshot -->

```text
features_technical
-> features_sector_rs
-> features_valuation
-> features_stock_valuation_bands
-> features_sector_earnings
-> features_phase1
-> features_snapshot
```

Each substage receives its own run/stage/attempt record. See [operational data flow](architecture/operational_data_flow.md) for inputs, artifacts, preflight, DQ, failure, and retry behavior.

## Persistence and lineage

Load `.env` before operating the system:

```bash
set -a
source .env
set +a
```

Canonical operational paths are resolved beneath `$DATA_ROOT`:

| Store or tree | Responsibility |
|---|---|
| `$DATA_ROOT/ohlcv.duckdb` | Operational OHLCV, delivery, trust/provenance, quarantine, registries, and feature metadata. |
| `$DATA_ROOT/control_plane.duckdb` | Pipeline runs, stage attempts, artifacts, DQ, alerts, models, operator state, and durable decision history. |
| `$DATA_ROOT/execution.duckdb` | Orders, fills, positions, and execution ledger state. |
| `$DATA_ROOT/candidate_tracker.duckdb` | Candidate episodes, snapshots, reviews, alerts, and current lifecycle state. |
| `$DATA_ROOT/masterdata.db` | Shared instrument/master data. |
| `$DATA_ROOT/fundamentals/` | Fundamental snapshots and stores. |
| `$DATA_ROOT/raw/` | Provider-native raw inputs. |
| `$DATA_ROOT/feature_store/<symbol_id>/` | Per-symbol feature Parquet snapshots. |
| `$DATA_ROOT/stage_store/` | Stage-owned durable materializations. |
| `$DATA_ROOT/pipeline_runs/<run_id>/<stage>/attempt_<n>/` | Immutable-attempt CSV/JSON/HTML artifacts. |
| `$DATA_ROOT/cache/` and `$DATA_ROOT/exports/` | Runtime caches and explicit exports. |

For `DATA_DOMAIN=research`, domain-owned stores are re-rooted under `$DATA_ROOT/research/`; master data remains shared. `MODELS_ROOT`, `REPORTS_ROOT`, and `LOGS_ROOT` can independently relocate their trees.

The control plane records one `pipeline_run`, one `pipeline_stage_run` per stage attempt, and a content-hashed `pipeline_artifact` row per registered output. Discover lineage through the registry rather than assuming a filesystem listing is complete. See [storage and lineage](architecture/storage_and_lineage.md), [database schema](reference/database_schema.md), and [artifacts](reference/artifacts.md).

## Operator quick start

Run commands from the repository root after loading `.env`. Use `PYTHONPATH=src` when the package has not been installed editable.

Bootstrap runtime directories:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.bootstrap_runtime_data
```

Run the current default operational pipeline:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator --data-domain operational
```

Run a reduced real-data canary without network publishing:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --canary --symbol-limit 25 --local-publish
```

The command above uses the configured runtime stores. When validation must not mutate live stores, follow the [copied-data canary](runbooks/copied_data_canary.md) maintenance-window procedure instead.

Retry one stage for an existing run:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --run-id <run_id> --stages publish
```

Start the API and React console in separate terminals:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.ui.execution_api.app --port 8090
```

```bash
cd web/execution-console-v2/ai-trading-dashboard-starter
npm install
npm run dev
```

Run safe diagnostics:

```bash
curl http://localhost:8090/api/execution/health
duckdb "$DATA_ROOT/control_plane.duckdb" -cmd \
  "SELECT run_id, status, started_at FROM pipeline_run ORDER BY started_at DESC LIMIT 1"
duckdb "$DATA_ROOT/ohlcv.duckdb" -cmd \
  "SELECT MIN(date), MAX(date), COUNT(*) FROM _catalog"
```

Before a repair or migration, follow [backup and restore](runbooks/backup_and_restore.md). The exhaustive command and flag inventory is [commands](reference/commands.md); isolated production-shaped validation is in [copied-data canary](runbooks/copied_data_canary.md), and recovery starts with [troubleshooting](runbooks/troubleshooting.md).

## Where to go deeper

| Question | Read next |
|---|---|
| How does a complete run move data? | [Operational data flow](architecture/operational_data_flow.md) |
| Where is data persisted and how is lineage resolved? | [Storage and lineage](architecture/storage_and_lineage.md) |
| Why was a run degraded or blocked? | [Data trust and DQ](architecture/data_trust_and_dq.md) and [DQ response](runbooks/dq_failure_response.md) |
| What does one stage read, write, and retry? | The relevant document under [stages](INDEX.md#stages-13) |
| Which configuration, schema, artifact, or CLI contract applies? | [Reference documents](INDEX.md#reference) |
| How does the operator UI work? | [UI architecture](architecture/ui_architecture.md) and [API reference](reference/api_reference.md) |
| How is research isolated? | [Research domain](domains/research_domain.md) |
| What is planned rather than implemented? | [Target architecture](architecture/target_architecture.md) |
| Why was a major design chosen? | [Architecture decisions](INDEX.md#decisions-adrs) |

## Maintenance contract

Update this guide in the same commit whenever a change affects its system-level contract. Update the linked detailed document at the same time.

| Change | Code authority | Required detailed update |
|---|---|---|
| Pipeline order, aliases, optional/default stages, retry semantics | `pipeline/orchestrator.py`, `pipeline/stages/` | `architecture/operational_data_flow.md` and affected stage docs |
| Runtime roots, stores, lineage, or migrations | `platform/db/paths.py`, `pipeline/registry.py`, execution/tracker stores, `pipeline/migrations/` | `architecture/storage_and_lineage.md` and schema/artifact references as applicable |
| Trust, DQ, execution safety, or broker defaults | Ingest trust, DQ engine, execution policy/adapters | Trust/DQ or execution-policy reference and affected stage doc |
| Console scripts, common flags, or operator startup | `pyproject.toml` and CLI parsers | `reference/commands.md` and configuration references |
| API/UI system boundaries | FastAPI app/routers and React application | UI architecture and API reference |

After documentation changes, run:

```bash
PYTHONPATH=src ./.venv/bin/python scripts/check_docs.py
```
