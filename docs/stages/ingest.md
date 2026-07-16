# Stage: ingest

- **Purpose:** Refresh the operational OHLCV catalog (and optional delivery data) for the NSE equity universe, validate it against an independent reference, and emit a stage summary that downstream stages can fingerprint.
- **Audience:** Operator, developer, debugging
- **Last verified:** 2026-05-16
- **Source of truth:** `src/ai_trading_system/pipeline/stages/ingest.py`, `src/ai_trading_system/domains/ingest/service.py`, `src/ai_trading_system/domains/ingest/daily_update_runner.py`, `src/ai_trading_system/domains/ingest/{providers/nse.py,providers/dhan.py,providers/yfinance.py,trust.py,validation.py,token_manager.py,delivery.py}`, `src/ai_trading_system/pipeline/dq/engine.py`

---

## Purpose

`ingest` is the first stage of the pipeline. It pulls fresh daily OHLCV bars for the NSE equity universe, normalizes and validates them, writes them to the operational catalog (`_catalog` table in `data/ohlcv.duckdb`), optionally cross-checks closes against an independent reference (bhavcopy or yfinance), and optionally collects security-wise delivery data. The stage's summary metadata drives the freshness gate that lets later stages decide whether they can short-circuit.

## Entrypoints

- **Stage wrapper:** `src/ai_trading_system/pipeline/stages/ingest.py` — class `IngestStage` (`name = "ingest"`), `IngestStage.run` at `ingest.py:30` delegates to `IngestOrchestrationService.run`. Smoke mode is explicitly disabled at `ingest.py:31-32`.
- **Service class:** `src/ai_trading_system/domains/ingest/service.py::IngestOrchestrationService` (`service.py:21`).

## Input data

- **NSE bhavcopy (source of record for OHLCV):** HTTP fetch via `src/ai_trading_system/domains/ingest/providers/nse.py::NSECollector.get_bhavcopy` (also re-invoked for the bhavcopy validation gate at `service.py:367-382`).
- **Dhan API (fallback for OHLC + mandatory for live execution and delivery):** `src/ai_trading_system/domains/ingest/providers/dhan.py`; the daily-update runner instantiates a `DhanCollector` at `daily_update_runner.py:1339-1344`. Dhan credentials are managed by `src/ai_trading_system/domains/ingest/token_manager.py::DhanTokenManager` (`token_manager.py:20`).
- **yfinance (last-resort fallback):** `src/ai_trading_system/domains/ingest/providers/yfinance.py` and the validation-reference path `IngestOrchestrationService.load_yfinance_close_frame` (`service.py:457`). Enabled either explicitly (`nse_allow_yfinance_fallback`) or auto-enabled when the operational catalog is `stale`/`delayed` (`service.py:120-143`).
- **Prior `_catalog` rows in `data/ohlcv.duckdb`:** read by `fetch_catalog_summary` / `fetch_catalog_close_frame` (`service.py:13`) to compute freshness and validation scope.
- **NSE delivery (MTO) reports:** `src/ai_trading_system/domains/ingest/delivery.py::DeliveryCollector` (`delivery.py:17`) — fetched per trading day via `DeliveryCollector.fetch_range`.

## Output artifacts

Per stage attempt, under `data/pipeline_runs/<run_id>/ingest/attempt_<n>/`:

- `ingest_summary.json` — full stage metadata payload (catalog summary, freshness status, bhavcopy validation result, delivery result, stale-quarantine sweep counts, downstream fingerprint). Written by `IngestOrchestrationService.run` at `service.py:29`.

DuckDB tables written / mutated (in `data/ohlcv.duckdb` unless noted):

- `_catalog` — canonical OHLCV catalog (written by `daily_update_runner.run` via `DhanCollector` / NSE-primary path; queried throughout `service.py` and DQ engine `_rule_ingest_*`).
- Provider/trust tables maintained by `domains/ingest/trust.py` — `ensure_data_trust_schema` (`trust.py:188`), provenance rows via `record_provenance_rows` (`trust.py:581`), quarantine via `quarantine_symbol_dates` (`trust.py:750`), `_symbol_state_overrides` updates via `sweep_stale_quarantine` (`trust.py:424`).
- Index/universe support tables via `ensure_index_schema` (`trust.py:508`).
- Delivery rows via `DeliveryCollector._ensure_delivery_table` + `_upsert_delivery` (`delivery.py:120`, `delivery.py:319`); delivery-derived features via `compute_delivery_features` (`delivery.py:346`).

The pipeline-run governance tables (`pipeline_artifact`, `dq_result`, etc.) live in `data/control_plane.duckdb` and are written by the orchestrator/DQ engine, not by the ingest service itself.

## Main modules

- `src/ai_trading_system/pipeline/stages/ingest.py` — thin wrapper / stage contract.
- `src/ai_trading_system/domains/ingest/service.py` — `IngestOrchestrationService`: freshness classification, fallback policy, bhavcopy validation gate, delivery collection, downstream fingerprint.
- `src/ai_trading_system/domains/ingest/daily_update_runner.py` — entry function `run(...)` (`daily_update_runner.py:1317`) that drives the actual fetch loop (NSE-primary path `_run_nse_yfinance_daily_update` at `:749` and Dhan-primary path `_run_dhan_primary_daily_update` at `:1169`).
- `src/ai_trading_system/domains/ingest/providers/nse.py` — `NSECollector` (bhavcopy fetch + caching).
- `src/ai_trading_system/domains/ingest/providers/dhan.py` — `DhanCollector` (OHLC fallback, live API).
- `src/ai_trading_system/domains/ingest/providers/yfinance.py` — last-resort fallback.
- `src/ai_trading_system/domains/ingest/trust.py` — data-trust schema, provider provenance, quarantine, stale-quarantine sweep.
- `src/ai_trading_system/domains/ingest/validation.py` — frame-level validators (`validate_ohlcv_frame` `:79`, `validate_delivery_frame` `:126`); raises `IngestValidationError`.
- `src/ai_trading_system/domains/ingest/token_manager.py` — Dhan token lifecycle (TOTP, refresh).
- `src/ai_trading_system/domains/ingest/delivery.py` — `DeliveryCollector` (NSE MTO download, upsert, delivery-feature compute).
- `src/ai_trading_system/domains/ingest/series_policy.py` — supported NSE series filter (`is_supported`).
- `src/ai_trading_system/domains/ingest/symbol_master.py` — `SymbolMaster.from_masterdb` resolves SYMBOL/ISIN → canonical symbol_id during bhavcopy validation (`service.py:409-432`).

## Process flow

1. `IngestStage.run` rejects `smoke=True` and calls the service (`ingest.py:30-33`).
2. `IngestOrchestrationService.run` calls `run_default` and then persists the resulting payload as `ingest_summary.json` (`service.py:27-37`).
3. `run_default` resolves the yfinance fallback policy via `resolve_yfinance_fallback_policy` (`service.py:120`): explicit param → `auto_enable_yfinance_fallback` → catalog-freshness probe.
4. It delegates the actual ingest to `daily_update_runner.run(...)` with `symbols_only=True` (so features are skipped here) and the resolved fallback flag. The runner picks NSE-primary or Dhan-primary based on `nse_primary`; NSE is the source of record.
5. After raw catalog upserts finish, the service synchronizes NSE split/bonus actions and recomputes adjusted OHLC for refreshed symbols that have active action history, even when the action set itself is unchanged. This ordering prevents a tail-window refresh from resetting valid adjustment factors to raw-price defaults while avoiding a full-catalog rewrite.
6. The service queries `_catalog` via `fetch_catalog_summary` and computes `freshness_status` ∈ {`fresh`, `delayed`, `stale`}.
7. `run_bhavcopy_validation` (`service.py:197`) runs the close-price reconciliation gate when `validate_bhavcopy_after_ingest=True`: loads catalog closes (`load_catalog_close_frame`), loads reference closes (`load_reference_close_frame` → bhavcopy/yfinance per `bhavcopy_validation_source`), merges by `symbol_id`, computes `coverage_ratio` and `mismatch_ratio`, and raises `DataQualityCriticalError` when `coverage < bhavcopy_min_coverage` (default 0.9) or `mismatch > bhavcopy_max_mismatch_ratio` (default 0.05) with `bhavcopy_validation_required=True` (default).
8. `run_delivery_collection` (`service.py:541`) determines the delivery date range from the last `_delivery` row (or `delivery_backfill_days`, default 30) and calls `DeliveryCollector.fetch_range`, then optionally `compute_delivery_features` (`service.py:594-595`).
9. `is_downstream_skip_eligible` (`service.py:146`) marks the stage as no-op-safe when `rows_written == 0`, no `updated_symbols`, no unresolved dates, and freshness is `fresh`.
10. `build_downstream_input_fingerprint` (`service.py:161`) emits a SHA-256 of the catalog summary + trust summary + validation counts; features and downstream stages key off this fingerprint.
11. `run_stale_quarantine_sweep` (`service.py:89`) calls `trust.sweep_stale_quarantine` to promote long-stuck quarantined symbols to `permanently_unavailable`; failures are logged-only.

## DQ / trust gates

Rules evaluated by `src/ai_trading_system/pipeline/dq/engine.py::DataQualityEngine` for `stage_name == "ingest"`:

- `ingest_catalog_not_empty` — hard-floor (never relaxed); `_catalog` must contain rows (`engine.py:153`, `:27-38`).
- `ingest_required_fields_not_null` — hard-floor; symbol_id/exchange/timestamp/OHLCV columns must be non-null (`engine.py:164`).
- `ingest_ohlc_consistency` — hard-floor; `high >= max(open,close)`, `low <= min(open,close)`, `high >= low` (`engine.py:193`).
- `ingest_duplicate_ohlcv_key` — hard-floor (SQL rule registered in DQ rule table; enforced via the generic `_evaluate_sql_rule` path, `engine.py:27-38`, `engine.py:133`).
- `ingest_recent_universe_price_jump_anomaly` — universe-wide jump detector with operator-tunable thresholds (`engine.py:245`).
- `ingest_provider_coverage_low` — fails when primary-provider share is below threshold or fallback/unknown share is above (`engine.py:333`).
- `ingest_unresolved_dates_present` — flags unresolved trade dates (`engine.py:375`).
- `ingest_segment_distribution_drift` — segment distribution drift check (`engine.py:442`).
- `ingest_latest_trade_date_quarantine_clear` — fails when the latest trade date still has active quarantine entries (`engine.py:523`).

In addition, **`run_bhavcopy_validation`** (`service.py:197-314`) is a service-level gate that raises `DataQualityCriticalError` *before* the DQ engine ever runs, when `bhavcopy_validation_required=True`. Frame-level checks in `validation.py::validate_ohlcv_frame` raise `IngestValidationError` inside provider code paths.

Hard-floor rules are never relaxed by `dq_mode=relaxed`; repairable rules are downgraded to `amber` (`engine.py:102-119`).

## Failure modes

- **`DataQualityCriticalError` from bhavcopy gate** — coverage or mismatch beyond bounds; stage aborts. Common causes: stale catalog vs bhavcopy date, ISIN/symbol mapping drift, source-mode misconfiguration (`bhavcopy_validation_source` ∈ `auto|bhavcopy|yfinance`).
- **`DataQualityCriticalError` from hard-floor DQ rules** — empty catalog, null required fields, invalid OHLC, duplicate keys. Investigate provider response and `_catalog` integrity before re-running.
- **`DataQualityRepairableError`** — non-hard-floor critical failures; in default `dq_mode=relaxed` these are downgraded to amber and the stage proceeds (`engine.py:115-118`).
- **`IngestValidationError`** raised by `validation.py` — malformed provider frame (missing columns, unparseable timestamps).
- **Dhan auth failures** — `daily_update_runner.ensure_live_dhan_access` raises `RuntimeError("Operational OHLCV ingestion requires authenticated Dhan access; synthetic fallback is disabled.")` (`daily_update_runner.py:1362-1371`). Resolve by refreshing tokens via `DhanTokenManager.ensure_valid_token` (`token_manager.py:322`).
- **Delivery collection failures** — non-blocking unless `delivery_required=True`; otherwise logged and reported as `delivery_status: "failed"` (`service.py:605-614`).
- **Smoke mode requested** — explicit `RuntimeError` (`ingest.py:31-32`); synthetic ingest is removed.

## Retry behavior

- Each pipeline attempt receives a fresh `attempt_number` and writes its `ingest_summary.json` under a new `attempt_<n>/` directory; the artifact path is built by `context.write_json` and recorded with `attempt_number=context.attempt_number` (`service.py:29-36`).
- Ingest writes are **idempotent at the symbol/date grain**: the runner upserts into `_catalog` keyed on `(symbol_id, exchange, timestamp)`; delivery upserts go through `DeliveryCollector._upsert_delivery` (`delivery.py:319`). Re-running an attempt with the same target date does not duplicate rows.
- `downstream_skip_eligible` lets the orchestrator avoid re-running features when the new attempt produced no new rows and freshness is already `fresh` (`service.py:146-158`).
- `downstream_input_fingerprint` is content-addressed (SHA-256 over the normalized payload, `service.py:161-187`), so downstream stages can cache against it.
- The stale-quarantine sweep (`service.py:89-118`) is best-effort and never fails the attempt; errors are logged.
- Manual repair workflows (full reset + re-ingest + validate for a specific date range) exist under `src/ai_trading_system/domains/ingest/{reset_reingest_validate.py,repair.py,archive_nse_bhavcopy.py}`. Current code status of those scripts as user-facing entry points: unknown — verify before use.

## Downstream consumers

- **`features`** stage — reads `ingest_summary.json` via `context.artifact_for("ingest", "ingest_summary")` to extract `updated_symbols` for incremental feature compute (`domains/features/service.py:50-58`). Also reads `_catalog` directly.
- **`rank`** stage — DQ rule `rank_artifact_not_empty` and ranker SQL queries depend on `_catalog` being populated.
- **`perf_tracker`** stage — reads OHLCV history from `_catalog`.
- Any stage that calls `load_data_trust_summary` (e.g. features stage's trust envelope) reads the trust tables maintained by ingest.

## Commands

From `pyproject.toml [project.scripts]`:

- `ai-trading-pipeline` — canonical orchestrator (`ai_trading_system.pipeline.orchestrator:main`); ingest runs first. See [../reference/commands.md](../reference/commands.md).
- `ai-trading-daily` — legacy 5-stage wrapper (`ai_trading_system.pipeline.daily_pipeline:main`) that also runs ingest first.
- `ai-trading-repair-ingest-schema` — schema-repair CLI (`ai_trading_system.interfaces.cli.repair_ingest_schema:main`) for fixing structural drift in `_catalog` / trust tables.
- `ai-trading-bootstrap-data` — bootstraps masterdata used by `SymbolMaster.from_masterdb` and the universe-index path.

Stage parameters consumed by the service (set via orchestrator `--params` / config):

- `batch_size` (default 700), `bulk`, `nse_primary` (default True), `symbol_limit`, `canary_mode`, `canary_symbol_limit`, `data_domain` (default `"operational"`), `stale_missing_symbol_grace_days` (default 3).
- `nse_allow_yfinance_fallback` (explicit) and `auto_enable_yfinance_fallback` (default True).
- Bhavcopy validation: `validate_bhavcopy_after_ingest` (default False), `bhavcopy_validation_date`, `bhavcopy_min_coverage` (0.9), `bhavcopy_max_mismatch_ratio` (0.05), `bhavcopy_close_tolerance_pct` (0.01), `bhavcopy_validation_required` (default True), `bhavcopy_validation_source` ∈ `auto|bhavcopy|yfinance`, `bhavcopy_validation_csv`, `nse_supported_series`.
- Delivery: `include_delivery` (default True), `delivery_backfill_days` (default 30), `delivery_workers` (default 4), `delivery_compute_features` (default True), `delivery_required` (default False).
- DQ tuning: `dq_mode` (default `"relaxed"`), `dq_stale_quarantine_days` (default 14), `dq_jump_*`, `dq_provider_*`, etc. (see `engine.py:245+`).

Environment variables (verified): `DHAN_API_KEY`, `DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN`, `DHAN_REFRESH_TOKEN`, `DHAN_PIN`, `DHAN_TOTP`, `DHAN_TOKEN_EXPIRY` (used by `token_manager.py`); `DATA_DOMAIN` (`platform/db/paths.py`). Full list: [../reference/environment_variables.md](../reference/environment_variables.md).

For the repair runbook (operator-side reset / re-ingest / validate workflow), see [../_legacy/archived_2026-05-16/ohlcv_reset_reingest_runbook.md](../_legacy/archived_2026-05-16/ohlcv_reset_reingest_runbook.md). Current code status of that runbook's commands: unknown — verify each `python -m` invocation against the current module paths before use.
