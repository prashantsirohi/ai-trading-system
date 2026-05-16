# Stage: features

- **Purpose:** Compute and persist per-symbol technical features (indicators, returns, sector RS, pattern preconditions) from the operational OHLCV catalog, register them in the feature store, and emit a snapshot record that downstream stages can consume.
- **Audience:** Operator, developer, debugging
- **Last verified:** 2026-05-16
- **Source of truth:** `src/ai_trading_system/pipeline/stages/features.py`, `src/ai_trading_system/domains/features/service.py`, `src/ai_trading_system/domains/features/{feature_store.py,compute_features_batch.py,indicators.py,sector_rs.py,universe_index.py,pattern_features.py}`, `src/ai_trading_system/domains/ingest/daily_update_runner.py` (features path), `src/ai_trading_system/pipeline/dq/engine.py`

---

## Purpose

`features` is the second stage of the pipeline. It runs immediately after `ingest` and turns the refreshed OHLCV catalog into a wide feature surface — momentum, trend, volatility, volume, multi-timeframe returns, liquidity, cross-sectional rank features, sector relative-strength, and pattern preconditions — that the rank/breakout/candidates stages consume. It also writes a `_snapshots` row that the rest of the pipeline uses to track feature coverage.

## Entrypoints

- **Stage wrapper:** `src/ai_trading_system/pipeline/stages/features.py` — class `FeaturesStage` (`name = "features"`); `FeaturesStage.run` (`features.py:20`) delegates to `FeaturesOrchestrationService.run`, passing `_record_snapshot` as the snapshot callback. Smoke mode is explicitly disabled at `features.py:21-22`.
- **Service class:** `src/ai_trading_system/domains/features/service.py::FeaturesOrchestrationService` (`service.py:16`).

## Input data

- **`_catalog` in `data/ohlcv.duckdb`** — the OHLCV catalog written by the ingest stage. Read via `daily_update_runner` and via `FeatureStore.compute_and_store_features` (`feature_store.py:1828`). Symbol list is selected with `SELECT DISTINCT symbol_id FROM _catalog WHERE exchange = 'NSE'` (`daily_update_runner.py:1383-1385`).
- **Prior feature parquet partitions under `data/feature_store/`** — read by `FeatureStore.get_last_feature_date` (`feature_store.py:367`) and `_get_incremental_window` (`feature_store.py:556`) to compute incremental windows when `incremental=True`.
- **Ingest stage artifact** — `feature_snapshot` consumer side reads `ingest_summary.json` via `context.artifact_for("ingest", "ingest_summary")` to pull `updated_symbols` and limit feature compute to the subset of symbols actually refreshed (`service.py:50-58`).
- **Masterdata DB (`paths.master_db_path`)** — sector / liquidity metadata used by sector-RS routines (`sector_rs.py:59`, `:77`, `:208`).
- **Trust summary** — `load_data_trust_summary(db_path, run_date)` (`service.py:129`) is read to build the `TrustConfidenceEnvelope` recorded in stage metadata.

## Output artifacts

Per stage attempt, under `data/pipeline_runs/<run_id>/features/attempt_<n>/`:

- `feature_snapshot.json` — stage metadata payload (snapshot id, computed-row count, registry-entry count, feature mode, multi-timeframe lookbacks, trust-confidence envelope, completion timestamp). Written by `FeaturesOrchestrationService.run` at `service.py:29`.

Feature-store parquet layout (written by `FeatureStore`):

- `data/feature_store/<feature_table>/year=YYYY/month=MM/data.parquet` — partitioned by year/month per feature group via `FeatureStore._get_partition_path` (`feature_store.py:281`) and `store_partitioned` / `_append_to_parquet` / `_overwrite_parquet` / `_replace_tail_in_parquet` (`feature_store.py:290-554`). Feature groups currently computed: `rsi`, `adx`, `sma`, `ema`, `macd`, `atr`, `bb`, `roc`, `supertrend` (set at `daily_update_runner.py:1401-1411`) plus swing-low and benchmark-relative additions.

DuckDB tables written / mutated (in `data/ohlcv.duckdb`):

- `_snapshots` — created if missing and appended with one row per attempt (`service.py:159-218`). Columns: `snapshot_id`, `snapshot_ts`, `symbols_processed`, `rows_written`, `from_date`, `to_date`, `status`, `note`.
- `_feature_registry` — written by `compute_features_batch.register_features` (`compute_features_batch.py:444`) and read by `record_snapshot` (`service.py:178-189`) to aggregate `rows_computed`.
- Universe-index support tables created via `ensure_index_catalog_tables` (`universe_index.py:294`); membership and bar rows upserted via `upsert_membership` / `upsert_index_bar` (`universe_index.py:397`, `:443`).
- Sector-RS outputs persisted by `compute_all_symbols_rs` (`sector_rs.py:208`).

## Main modules

- `src/ai_trading_system/pipeline/stages/features.py` — thin stage wrapper.
- `src/ai_trading_system/domains/features/service.py` — `FeaturesOrchestrationService`: ingest-artifact lookup, progress reporting, snapshot recording, trust envelope.
- `src/ai_trading_system/domains/features/feature_store.py` — `FeatureStore` (`feature_store.py:125`): partitioned parquet IO, per-feature compute methods (`compute_rsi`, `compute_adx`, `compute_sma`, `compute_ema`, `compute_macd`, `compute_swing_low`, …), incremental window logic, and `compute_and_store_features` (`feature_store.py:1828`) orchestrator. Also contains feature-augmentation helpers: `add_feature_readiness` (`:34`), `add_feature_confidence` (`:49`), `add_liquidity_features` (`:71`), `add_cross_sectional_features` (`:92`).
- `src/ai_trading_system/domains/features/compute_features_batch.py` — set-based DuckDB SQL implementations: `batch_rsi`, `batch_sma`, `batch_ema`, `batch_macd`, `batch_atr`, `batch_adx`, `batch_bollinger_bands`, `batch_roc`, `batch_supertrend`, plus `register_features`.
- `src/ai_trading_system/domains/features/indicators.py` — pandas-side helpers: `add_multi_timeframe_returns` (`:16`), `add_volume_zscore_features` (`:58`), `add_stage2_features` (`:96`), and the `FeatureEngine` class (`:266`) for ad-hoc compute.
- `src/ai_trading_system/domains/features/sector_rs.py` — sector relative-strength: `compute_all_symbols_rs` (`:208`), `add_benchmark_relative_features` (`:9`), lookback resolution and liquidity-filtered universe helpers.
- `src/ai_trading_system/domains/features/universe_index.py` — internal universe index: membership computation, rebalance schedule, index-bar synthesis, catalog tables.
- `src/ai_trading_system/domains/features/pattern_features.py` — pattern precondition flags (small module, 52 lines).
- `src/ai_trading_system/domains/ingest/daily_update_runner.py` — `run(features_only=True, ...)` (`daily_update_runner.py:1317`, features branch at `:1373-1434`) is the actual workhorse the service calls.

## Process flow

1. `FeaturesStage.run` rejects `smoke=True` and calls `FeaturesOrchestrationService.run` with `record_snapshot=self._record_snapshot` (`features.py:20-23`).
2. The service calls `run_default`, then persists the returned dict as `feature_snapshot.json` (`service.py:22-37`).
3. `run_default` reads the upstream `ingest_summary` artifact and extracts `updated_symbols` (or `None` for "all symbols") (`service.py:50-58`).
4. `full_rebuild` is set when explicitly requested *or* when `data_domain == "research"` (`service.py:60-63`).
5. A `_feature_progress` callback is wired up to emit `report_task("feature_progress", …)` updates to the run console (`service.py:65-110`).
6. The service calls `daily_update_runner.run(symbols_only=False, features_only=True, ...)` (`service.py:112-123`). The runner instantiates `FeatureStore` and invokes `compute_and_store_features` (`daily_update_runner.py:1393-1416`) with the fixed feature set listed in §"Output artifacts". `incremental` defaults to `True` for operational domain unless `full_rebuild` is set (`daily_update_runner.py:1338`, `feature_store.py:1879`).
7. The runner then calls `sector_rs.compute_all_symbols_rs(...)` (`daily_update_runner.py:1422-1426`) to compute sector / benchmark relative-strength features.
8. Back in the service, `record_snapshot` queries `_feature_registry` for the completed `rows_computed` sum and entry count, reads `_catalog` min/max date and distinct symbol count, then inserts a new row into `_snapshots` (`service.py:155-221`).
9. `load_data_trust_summary` is called and a `TrustConfidenceEnvelope` is built; `feature_confidence` is `1.0` if any feature rows were written, else `0.0` (`service.py:128-134`).
10. The returned metadata payload (`service.py:136-153`) includes: `snapshot_id`, `feature_rows`, `feature_registry_entries`, `feature_mode` (`"full_rebuild"` or `"incremental"`), `target_symbol_count`, the `feature_enhancements` capability block (multi-timeframe returns at lookbacks `[5, 20, 60, 120, 252]`, benchmark relative with `benchmark_symbol` defaulting to `"NIFTY_500"`), the `trust_confidence` dict, and `completed_at`.
11. `FeaturesOrchestrationService.run` wraps that payload in a `StageArtifact.from_file(...)` with `row_count=metadata["feature_rows"]` (`service.py:30-36`).

## DQ / trust gates

Rules evaluated by `src/ai_trading_system/pipeline/dq/engine.py::DataQualityEngine` for `stage_name == "features"`:

- `features_snapshot_created` — hard-floor (never relaxed); fails if `result.metadata["snapshot_id"]` is missing (`engine.py:217-229`, `:27-38`).
- `features_registry_not_empty` — hard-floor; fails if `result.metadata["feature_rows"] == 0` (`engine.py:231-243`).
- `features_trust_quarantine_clear` — non-hard-floor; reads trust state to confirm no quarantined symbols are blocking the latest trade date (`engine.py:534`).

Hard-floor failures raise `DataQualityCriticalError` and block the pipeline; other criticals raise `DataQualityRepairableError`, which `dq_mode=relaxed` (default) downgrades to amber and lets the pipeline proceed (`engine.py:89-119`).

The `TrustConfidenceEnvelope` written into `feature_snapshot.json` (`service.py:131-134`, `:151`) is not a gate by itself but is consumed downstream as a confidence signal.

## Failure modes

- **`DataQualityCriticalError: features_snapshot_created`** — the service did not write a `_snapshots` row; usually means `record_snapshot` threw before reaching the INSERT, or `_catalog` was empty so `min_date`/`max_date` came back NULL and the INSERT failed.
- **`DataQualityCriticalError: features_registry_not_empty`** — `_feature_registry` had no `status='completed'` rows; usually means the SQL feature batch failed silently (check `compute_features_batch` logs) or `_catalog` had no NSE rows.
- **Missing upstream ingest artifact** — `context.artifact_for("ingest", "ingest_summary")` returns `None`; the service swallows the error and proceeds with `updated_symbols=None`, meaning "compute for the full catalog" (`service.py:50-58`). This is intentional but can lead to large recompute on the first run after a reset.
- **Parquet write failures** — `_append_to_parquet` / `_overwrite_parquet` raise on disk / schema-drift errors; the calling feature method will mark its entry as failed in `_feature_registry`.
- **Smoke mode requested** — explicit `RuntimeError` (`features.py:21-22`).
- **Schema drift in `_catalog`** — `record_snapshot` assumes columns `timestamp` and `symbol_id` (`service.py:195-200`); if `repair_ingest_schema` has not been run, the query fails.

## Retry behavior

- Each attempt writes its own `feature_snapshot.json` under a new `attempt_<n>/` directory; `StageArtifact.from_file(..., attempt_number=context.attempt_number)` (`service.py:35`).
- Feature compute is **incrementally idempotent** in operational mode: `FeatureStore.compute_and_store_features` resolves a compute window via `_get_incremental_window` (`feature_store.py:556`) and rewrites the parquet tail starting from `replace_from_ts` via `_replace_tail_in_parquet` (`feature_store.py:521`). Re-running an attempt on the same data does not duplicate rows.
- `full_rebuild=True` forces a full recompute and is the default when `data_domain == "research"` (`service.py:60-63`); operators can set it explicitly via stage params to recover from corrupted parquet files.
- `_snapshots` is append-only; each attempt inserts a new `snapshot_id = MAX(snapshot_id) + 1` (`service.py:201-203`), so retry history is preserved.
- The features stage relies on the ingest stage's `downstream_input_fingerprint` upstream for short-circuiting decisions; the features stage itself does not currently publish its own skip-eligibility flag. Current code status: confirmed by reading `service.py:136-153` — no `downstream_skip_eligible` key is emitted.

## Downstream consumers

- **`rank`** stage — `domains/ranking/input_loader.py` and friends read the feature-store parquet files plus the `_snapshots` and `_feature_registry` tables to assemble the ranking input frame. The `feature_rows` metric on `feature_snapshot.json` drives the `features_registry_not_empty` precondition that rank depends on.
- **`fundamentals`** (optional) — reads rank output, which in turn reads features.
- **`candidates`** — reads rank artifacts that depend on the feature surface.
- **`events`**, **`insight`**, **`narrative`**, **`publish`** — indirectly via rank/candidates outputs.
- **`perf_tracker`** — re-reads `_catalog` but consumes rank output for cohort attribution; not a direct features consumer.

## Commands

From `pyproject.toml [project.scripts]`:

- `ai-trading-pipeline` — full 11-stage orchestrator (`ai_trading_system.pipeline.orchestrator:main`); features runs second. See [../reference/commands.md](../reference/commands.md).
- `ai-trading-daily` — legacy 5-stage wrapper (`ai_trading_system.pipeline.daily_pipeline:main`); features is the second stage in this wrapper too.

There is no dedicated `ai-trading-features` script. To recompute features standalone, invoke the orchestrator with stage filters (see [../reference/commands.md](../reference/commands.md)) or call `daily_update_runner.run(features_only=True, ...)` from a Python session.

Stage parameters consumed by the service (set via orchestrator `--params` / config):

- `batch_size` (default 700), `bulk`, `symbol_limit`, `data_domain` (default `"operational"`).
- `full_rebuild` (default False; auto-True when `data_domain == "research"`).
- `feature_tail_bars` (default 252) — incremental warmup window passed into `FeatureStore.compute_and_store_features`.
- `benchmark_symbol` (default `"NIFTY_500"`) — recorded into the metadata payload's `feature_enhancements.benchmark_relative` block.

Environment variables (verified): `DATA_DOMAIN` (`platform/db/paths.py`) selects operational vs research data layout. Full list: [../reference/environment_variables.md](../reference/environment_variables.md).

See also: [./ingest.md](./ingest.md) for the upstream contract that feeds `updated_symbols` into incremental compute, and [./rank.md](./rank.md) for the primary downstream consumer.
