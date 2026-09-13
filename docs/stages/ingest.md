# Stage: ingest

- **Purpose:** Refresh the operational OHLCV catalog (and optional delivery data) for the NSE equity universe, validate it against an independent reference, and emit a stage summary that downstream stages can fingerprint.
- **Audience:** Operator, developer, debugging
- **Last verified:** 2026-09-12
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
9. `is_downstream_skip_eligible` (`service.py:146`) marks the stage as no-op-safe only when price, benchmark, and delivery write counts are zero, delivery did not fail, no symbols changed, no dates are unresolved, and freshness is `fresh`.
10. `build_downstream_input_fingerprint` (`service.py:161`) emits a SHA-256 of the catalog summary + trust summary + validation counts; the fingerprint remains diagnostic rather than a cross-run skip authority.
11. Unresolved provider gaps are `active` only for recent critical-universe
    symbols within `stale_missing_symbol_grace_days`. Gaps for symbols already
    stale beyond that grace remain `observed`, preserving the evidence without
    repeatedly degrading current trust. Quarantine writes replace the existing
    `(symbol_id, exchange, trade_date, reason)` lifecycle row, so retries do not
    accumulate duplicate observations.
12. `run_stale_quarantine_sweep` (`service.py:89`) calls `trust.sweep_stale_quarantine` to promote long-stuck quarantined symbols to `permanently_unavailable`; failures are logged-only.

## Delivery invalidation

Delivery row writes, delivery-feature writes, failed delivery collection (which
may have partially written), and benchmark writes invalidate downstream reuse,
even when an earlier ingest fingerprint happens to match. The fingerprint
includes delivery status, latest date, row counts, and changed symbols.
The collector does not return exact changed identities, so delivery writes or
failure conservatively add every catalogued NSE symbol to
`delivery_changed_symbols` and merge them into `downstream_changed_symbols`.
This keeps delivery-only updates visible to incremental feature computation
without removing already changed BSE or corporate-action symbols.

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
- `downstream_skip_eligible` and `downstream_input_fingerprint` are diagnostic receipts. They do not authorize skipping requested stages in a new run. Same-run resume remains the orchestrator checkpoint boundary.
- `downstream_input_fingerprint` is content-addressed (SHA-256 over the normalized payload, `service.py:161-187`), but it is not a complete fingerprint of downstream mutable inputs.
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


## Monthly universe onboarding

`domains.ingest.universe_refresh` runs at the beginning of
`IngestOrchestrationService.run_default`, before fallback resolution or ordinary
price ingestion, and also has a standalone maintenance CLI. It adds no logical
pipeline stage. It uses the Screener client's authenticated screen-export
method for screen 3553765; the full CSV/XLSX export must contain a listing
code or valid company ISIN and Market Capitalization in crore. ISIN-only rows
resolve by exact ISIN against the official exchange lists. Rows with no active
listing remain in `excluded` with reason `no_active_exchange_listing`; they
never enter the master or backfill. Ambiguous matches remain quarantined from onboarding. Empty exports, missing identifiers,
invalid caps and duplicates fail rather than silently reducing coverage. Rows
at or below INR 500 crore are excluded. Export names never resolve identity.
Official NSE main-board series EQ/BE/BZ and active BSE non-SME board metadata
validate discovery. Exact ISIN prevents a renamed or dual-listed company from
being inserted twice. Identity conflicts are blocked, with no overwrite or
removal. NSE additions also require a unique Dhan security ID. New companies
currently require a unique BSE listing for identity-checked sector/industry
classification; NSE-only or ambiguous identities remain reported blockers.
Discovery identifies newly mastered companies; it does not infer whether the
cause was an IPO or market-cap growth.

Preview is the default and creates no persistent state. `--apply` is restricted
to today's date because the screen and exchange masters are current snapshots.
It freezes source files and pending identities before master insertion, backs
up affected existing stores, then onboards supported data. A failed addition
remains pending even after insertion, so retries finish the backfill. An
inter-process lock serializes this command. Run it without other database writers.
NSE history uses the existing official-bhavcopy repair writer, with no yfinance
fallback. Backfill starts five years ago by default, or at the later NSE listing
date. History ends at yesterday, matching ordinary daily ingest; exchange calendars
exclude non-trading dates. Discovery still uses today’s screen. Available NSE
reports without a target scrip are not missing reports; malformed target bars
and unavailable reports still fail closed. Repair also resolves earlier tickers
by exact mastered ISIN. Targeted split/bonus parsing, adjustments, delivery collection, technical
rebuild, cross-sectional Phase 1 refresh and both Screener statement bases follow.
Post-write coverage verifies technical, Phase 1 and fundamental presence; it
does not certify complete historical delivery coverage or sufficient IPO warmup.
BSE additions reuse `new_symbol_onboarding` and its backed-up promotion/history
path. Screener downloads use numeric BSE security codes in URLs while retaining
master symbols in export filenames and stored financials. Failed BSE results
retain their step report and verification details in the universe report.
Unsupported BSE delivery and adjusted-price history remain visible as
`completed_with_gaps`; failures in supported data remain pending.

Ingest checks cadence at startup: calendar month by default, or rolling 28 days. A first-ever invocation is due immediately. A successful
empty-additions check advances cadence. Unresolved discovery quarantine and
pending backfills independently bypass that cadence on the next invocation,
except identities on the saved negative list.
Discovery identity gaps return `completed_with_gaps`, permitting daily ingest
after valid additions are processed. Refresh acquisition, infrastructure or unclassified failures raise `DataQualityCriticalError`
before ordinary ingest. Current-date direct orchestrator runs and ingest retries
use the same hook; a resumed run that already completed ingest does not rerun it.
Historical, research, canary, symbol-limited and dry-run contexts skip expansion.
Custom injected ingest operations skip by default unless explicitly enabled.
`universe_refresh_enabled`, `universe_refresh_cadence` and
`universe_refresh_lookback_years` are stage parameters; the first two fall back
to `UNIVERSE_REFRESH_ENABLED` and `UNIVERSE_REFRESH_CADENCE`.

`StageContext.report_task` emits acquisition, comparison, backup, per-company
NSE/BSE backfill and verification updates. The existing compact terminal bar
shows `[i/N] SYMBOL (exchange)` and the active operation; JSON mode retains
structured counts and verbose mode logs the detail. Refresh progress occupies
the first fifth of ingest. A not-due invocation reports that explicitly.
`universe_refresh_summary.json` is written in the ingest attempt and registered
on successful ingest; failure files remain forensic evidence. The report is
also nested in `ingest_summary.json`. Onboarded symbols (including same-day
retries) join `updated_symbols`, preventing downstream no-change shortcuts.
Technical rebuilds target additions only; no ranking formula changes or global
feature rebuild are required. The following normal pipeline computes combined
rank/DQ and downstream artifacts.

Discovery conflicts quarantine only the affected onboarding identities. Valid
additions continue. `state.json:discovery_quarantine` retains each identity,
source row, reason, first/last seen and attempt count. The queue is re-evaluated
against a fresh complete export and official sources on subsequent ingest runs,
even within the cadence interval. Entries resolved or absent from the latest
screen are closed explicitly in `quarantine_closed`; prior attempt reports retain
the evidence. A previously pending company with a new discovery conflict is
deferred without mutation. This maintenance queue neither edits existing master
identities nor replaces the market-data DQ/quarantine contract. Backfill failures
for validated companies and global acquisition errors still block ingest.

The reviewed negative list at `configs/universe_refresh_negative_list.json`
contains 59 known discovery blockers with identifiers, reasons and review dates.
SPICEJET is deliberately omitted following its resolver fix. Exact NSE code,
BSE code and export ISIN matches (ignoring market cap) are removed before
discovery; changed identifiers receive normal validation. Matching old quarantine
entries do not trigger a new refresh. Matched pending identities remain saved
but are deferred. Other pending company failures retry on their recorded schedule and remain excluded from admission.
Reports expose `negative_list` and `negative_list_count`; not-due summaries show
the configured count. No listed security is removed from existing daily coverage.

Exclusions do not expire automatically, including during monthly or forced
refreshes. After resolving a blocker, remove its entry from the JSON file and
run the normal refresh (use `--force` if the cadence is not due). Existing
quarantine entries moved into this policy close with reason `saved_negative_list`
on the next apply attempt; older attempt evidence remains intact.

Company onboarding uses at most three attempts for transport timeouts, connection
errors, HTTP 429 and HTTP 5xx, with 2- and 4-second backoff. Exhausted transient
failures retry from the next calendar day; deterministic supported-data gaps
retry after 28 days. `--force` bypasses this cooldown (not the saved negative
list). Failed companies remain in `state.json:pending` with `onboarding_failure`
error, attempts, last-attempt date and retry-after date. Their presence enforces
a read-only operational admission gate in ranking and execution, even for legacy
pending entries and reused rank artifacts. Successful verification removes the
pending identity and lifts the gate. A malformed checkpoint fails closed.

Isolated failures return `completed_with_gaps` with `quarantined_symbols`, so
ordinary daily ingest can proceed. Infrastructure/unclassified failures stop
the refresh promptly; acquisition and database failures never become successful
onboarding. Retry remains per company and can repeat completed internal steps.
Research does not apply current operational onboarding state. Existing-position
exit management is unaffected by the new-entry candidate filter.
