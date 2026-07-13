# Stage: features

- **Purpose:** Describe the seven feature substages that compute technical, sector, valuation, earnings, and Phase 1 feature materializations and then register the feature snapshot.
- **Audience:** Operators, developers, and reviewers diagnosing feature attempts.
- **Last verified:** 2026-07-13
- **Source of truth:** `src/ai_trading_system/pipeline/orchestrator.py`, `src/ai_trading_system/pipeline/stages/features.py`, `src/ai_trading_system/domains/features/service.py`, and `src/ai_trading_system/pipeline/dq/engine.py`.

---

## Purpose

`features` is the operator-facing logical stage immediately after `ingest`. The orchestrator expands that alias into seven independently attempted runtime substages:

| Order | Runtime stage | Responsibility | Primary attempt artifact |
|---:|---|---|---|
| 1 | `features_technical` | Incremental/full technical feature computation and feature registry writes | `features_technical.json` |
| 2 | `features_sector_rs` | Sector and benchmark relative-strength materialization | `features_sector_rs.json` |
| 3 | `features_valuation` | Point-in-time universe valuation features when enabled | `features_valuation.json` |
| 4 | `features_stock_valuation_bands` | Stock own-history PE/PS/PB valuation bands when enabled | `features_stock_valuation_bands.json` |
| 5 | `features_sector_earnings` | Sector earnings-leadership refresh when enabled | `features_sector_earnings.json` plus registered CSV when emitted |
| 6 | `features_phase1` | Phase 1 derived feature tables | `features_phase1.json` |
| 7 | `features_snapshot` | Snapshot row, trust envelope, final feature metadata, and DQ boundary | `feature_snapshot.json` |

Each substage has its own `pipeline_stage_run` row, attempt number, output directory, status, and retry boundary. The orchestrator accepts `--stages features` as the alias for all seven; an explicit substage name selects only that runtime node.

## Entrypoints

- Canonical CLI: `ai-trading-pipeline --stages features`.
- Orchestrator mapping: `PipelineOrchestrator.stages` assigns one `FeaturesStage` wrapper to each runtime substage.
- Service dispatch: `FeaturesOrchestrationService.run_substage` selects the operation from `context.stage_name`.

## Input data

`FeaturesStage.run` delegates to `FeaturesOrchestrationService.run`. The service dispatches by `context.stage_name`; the legacy unsplit `features` path remains as a compatibility call but is not part of current `PIPELINE_ORDER`.

All runtime locations resolve through `get_domain_paths()`:

- input OHLCV and feature metadata: `$DATA_ROOT/ohlcv.duckdb`;
- incremental feature partitions: `$DATA_ROOT/feature_store/`;
- master/sector identity: `$DATA_ROOT/masterdata.db`;
- attempt artifacts: `$DATA_ROOT/pipeline_runs/<run_id>/<feature_substage>/attempt_<n>/`;
- research runs: the corresponding paths below `$DATA_ROOT/research/` when `DATA_DOMAIN=research`.

The service reads `ingest_summary.json` to obtain `downstream_changed_symbols` or `updated_symbols`. Missing/unreadable ingest metadata falls back to the full catalog, which can make a retry materially larger. Operational technical computation is incremental unless `full_rebuild` is set; research defaults to a full rebuild.

## Output artifacts

The feature domain owns:

- `_feature_registry` and `_snapshots` in the domain OHLCV DuckDB;
- feature Parquet partitions below the resolved feature-store root;
- universe-index, sector-RS, valuation, sector-earnings, and Phase 1 materializations created by their respective substage services;
- substage JSON/CSV attempt artifacts registered by the orchestrator.

`features_snapshot` records the completed feature-row total, registry-entry count, OHLCV date range/symbol count, feature mode, enabled enhancement summaries, trust-confidence envelope, and completion time. `rank` consumes that registered snapshot plus the feature materializations.

## Main modules

- `pipeline/orchestrator.py` owns alias expansion, ordering, attempts, retries, and progress labels.
- `pipeline/stages/features.py` is the shared thin stage wrapper.
- `domains/features/service.py` dispatches each substage and registers its JSON/CSV artifacts.
- `domains/features/feature_store.py` and `compute_features_batch.py` own technical feature persistence.
- valuation, sector earnings, sector RS, and Phase 1 modules own their corresponding materializations.

## Process flow

1. The orchestrator expands `features` into the ordered seven-substage list.
2. Each substage receives its own `StageContext`, attempt number, and output directory.
3. Computational substages update their owned DuckDB/Parquet materializations and emit metadata artifacts.
4. `features_snapshot` reads the resulting registry/catalog state, appends `_snapshots`, writes `feature_snapshot.json`, and supplies the DQ metadata.
5. After DQ succeeds, `rank` resolves the feature snapshot and feature materializations.

## DQ and trust boundary

Final feature DQ runs on `features_snapshot`, not on each computational substage:

- `features_snapshot_created` is a hard floor requiring a snapshot identifier;
- `features_registry_not_empty` is a hard floor requiring completed feature rows;
- `features_trust_quarantine_clear` checks the current quarantine/trust state and is relaxable under the configured DQ policy.

Hard-floor failures block downstream stages. Computational substage failures stop before `features_snapshot`; their prior successful attempts remain separate registry history.

## Failure modes

- Missing/unreadable `ingest_summary` expands technical work to the full catalog.
- DuckDB or Parquet schema/write failures fail the owning computational substage.
- Disabled optional valuation or earnings inputs yield an explicit skipped/disabled metadata result rather than synthetic data.
- Missing snapshot identifiers or zero completed feature rows fail the final hard floors.
- Active quarantine/trust failures can block or relax according to the configured DQ mode, except hard floors are never relaxed.

## Retry behavior

- Retry the failed runtime substage by name when its upstream substages already completed and its persisted inputs remain valid.
- Retry `features_snapshot` alone only when all six computational substages have valid durable outputs for the run.
- Use `--stages features --force-rerun` when the complete feature chain must be recomputed.
- Use `--full-rebuild` for a verified corrupted/incompatible feature store or an explicit research rebuild; it is not the default operational repair.
- Do not delete prior attempt directories or mutate registered artifacts during recovery.
- Smoke/synthetic feature data is disabled and cannot be used to bypass trust checks.

## Downstream consumers

`rank` is the direct downstream consumer of the completed feature snapshot and feature surface. Fundamentals, candidates, candidate tracking, events, execution, insight, narrative, publish, and performance tracking consume rank-derived outputs and therefore depend indirectly on successful feature completion.

## Commands

Run all feature substages through the alias:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --stages features --data-domain operational
```

Retry one substage for an existing run:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --run-id <run_id> --stages features_sector_rs --force-rerun
```

Run a complete research rebuild without touching operational feature stores:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --stages features --data-domain research --full-rebuild
```

Relevant parameters include `batch_size`, `bulk`, `symbol_limit`, `data_domain`, `full_rebuild`, `feature_tail_bars`, `benchmark_symbol`, and the valuation/sector-earnings enablement settings. See [commands](../reference/commands.md), [environment variables](../reference/environment_variables.md), and [storage and lineage](../architecture/storage_and_lineage.md) for the current operator contracts.
