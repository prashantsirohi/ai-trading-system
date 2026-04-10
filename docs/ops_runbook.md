# Operations Runbook

The repo auto-loads the local `.env` for the orchestrator, dashboard, publish test, and other credential-aware runtime entrypoints. Activate `.venv` before running commands; manually sourcing `.env` is usually not required.

## Daily Commands

### Production-style run
- `python3 -m run.orchestrator`
- `python3 -m run.orchestrator --data-domain operational`
- QuantStats tear sheet publish is enabled by default; disable with `--skip-quantstats`

### Legacy wrapper
- `python3 run/daily_pipeline.py`
- `python3 run/daily_pipeline.py --skip-delivery-collect` (offline/restricted network fallback)
- QuantStats tear sheet publish is enabled by default; disable with `--skip-quantstats`

### Live canary run (recommended pre-check)
- `python3 -m run.orchestrator --canary --symbol-limit 25 --local-publish`

### Retry publish only
- `python3 -m run.orchestrator --run-id <run_id> --stages publish`
- `python3 -m run.orchestrator --run-id <run_id> --stages publish --skip-quantstats`

### Live canary run
- `python3 -m run.orchestrator --canary --symbol-limit 25 --local-publish`

### Live publish target test
- `python3 -m run.publish_test`

### Research backtest run
- `python3 -m research.backtest_pipeline`

### Research training run
- `python3 -m research.train_pipeline`

### Breakout setup study
- `python3 -m research.backtest_breakout_setups`

### Shadow monitor refresh
- `python3 -m research.shadow_monitor`

### Research UI
- `python3 -m streamlit run ui/research/app.py`

### Execution UI
- `python3 -m ui.execution.app`

### Model lifecycle checks
- inspect `model_registry`
- inspect `model_eval`
- inspect `model_deployment`

## What to Verify
1. `pipeline_run.status` reaches `completed` or `completed_with_publish_errors`
2. `pipeline_stage_run` shows ordered stage attempts
3. `pipeline_artifact` rows exist for every successful stage
4. `dq_result` rows exist for `ingest`, `features`, and `rank`
5. `publisher_delivery_log` shows publish attempts, dedupe skips, and final status
6. `model_registry`, `model_eval`, and `model_deployment` reflect model lifecycle events
7. `pipeline_alert` records critical DQ failures, preflight failures, and publish-degraded runs
8. Streamlit research UI renders all tabs without page exceptions
9. NiceGUI execution UI shows live ranking, breakout, sector, and process panels

## Runtime Clarifications
- DQ severities are `critical`, `high`, `medium`, and `low`.
- Only `critical` DQ failures stop downstream stages. `high`/`medium`/`low` are persisted for operator review.
- Ingest/features/rank are not auto-retried by the orchestrator; retries are operator-triggered by re-running stages.
- Publish channel delivery has retry/backoff and idempotency dedupe via `publisher_delivery_log`.
- `pipeline_alert` is a persisted control-plane record. Built-in alerting does not currently send standalone email/Telegram notifications directly from `AlertManager`.

## Pre-Run Checklist
- DuckDB file is writable: `data/ohlcv.duckdb`
- `.env` uses Unix line endings if the run will be shell-sourced
- Provider credentials are valid
- Google Sheets / Telegram credentials are present for networked publish runs
- `quantstats` is installed if QuantStats publish is enabled
- Prior failed run IDs are noted if a targeted retry is planned
- Preflight passes, unless explicitly bypassed with `--skip-preflight`
- Production runs use the `operational` data domain; research jobs must not point at live rolling storage.

## Failure Triage
1. Identify the failed stage from `pipeline_stage_run`.
2. Check `error_class` and `error_message`.
3. If the stage is `ingest`, `features`, or `rank`, review `dq_result` for critical failures first.
4. If the stage is `publish`, treat it as isolated delivery failure unless rank artifacts are missing.

## Recovery Guide

### `ingest` failed
- Validate provider/API access.
- Fix the issue.
- Re-run the full pipeline or targeted downstream-safe sequence if appropriate.
- Use `--canary --symbol-limit <n>` first if you want a smaller live validation run before the full universe.

### `features` failed
- Confirm ingest artifacts exist for the same `run_id`.
- Re-run `features,rank,publish` if ingest data is still valid.

### `rank` failed
- Confirm feature snapshot metadata exists.
- Review `rank_summary.degraded_outputs` before trusting the ranking outputs.
- Re-run `rank,publish`.

### `publish` failed
- Do not re-run ingest or features.
- Retry only publish with the same `run_id`.
- Check `publisher_delivery_log` to see which channels already delivered and which remain failed.
- Review `pipeline_alert` for the emitted degraded-run alert.
- If the failure mentions an unexpectedly empty artifact, compare the file contents with `pipeline_artifact.row_count` before retrying.
- If the failure mentions `quantstats_subprocess_failed`, retry publish with:
  - `--skip-quantstats` for immediate recovery, or
  - reduced scope (`--quantstats-top-n`, `--quantstats-max-runs`) for a lighter tear sheet run.

## Rollback
1. Revert the code to the last known good revision.
2. Leave historical `pipeline_*`, `dq_*`, and model governance rows in place for auditability.
3. Remove only the new governance tables if a schema rollback is required:
   - `DROP TABLE pipeline_alert;`
   - `DROP TABLE publisher_delivery_log;`
   - `DROP TABLE model_deployment;`
   - `DROP TABLE model_eval;`
   - `DROP TABLE model_registry;`
   - `DROP TABLE dq_result;`
   - `DROP TABLE dq_rule;`
   - `DROP TABLE pipeline_artifact;`
   - `DROP TABLE pipeline_stage_run;`
   - `DROP TABLE pipeline_run;`
4. Re-run from a stable code revision.

## Known Operational Limits
- Operational features now support incremental tail recompute, but full rebuilds are still the recovery path after schema or indicator changes.
- Sector strength is now computed from a liquidity-filtered broad universe by default:
  - top `800` names by recent median traded value
  - minimum `180` recent trading days
- Publish retry is isolated, but external channel throttling still needs operator awareness.
- Delivery idempotency is scoped to `run_id + channel + artifact hash`; a new artifact produces a new dedupe key.
- Synthetic smoke mode has been removed; use canary runs for operational validation.
- `run.publish_test` validates channel plumbing, but it still depends on live external services being reachable.
- QuantStats tear sheet publish is file-based and does not require Google Sheets/Telegram connectivity, but it does require local historical rank artifacts and a stable local plotting/runtime environment.
- Research entrypoints share core analytics code with production, but they should only use the research data domain and prior-year historical cutoff by default.
- Breakout v2 is now default for breakout listing: canonical taxonomy (`resistance_breakout_50d`, `high_52w_breakout`, `consolidation_breakout`, `volatility_expansion_breakout`) plus explicit states (`qualified`, `watchlist`, `filtered_by_regime`). This is separate from main composite rank and can run with optional execution soft gating.

## QuantStats Tear Sheet Publish

Enable from orchestrator:
- enabled by default during publish stage
- disable with `python3 -m run.orchestrator --skip-quantstats`
- optional tuning:
  - `--quantstats-top-n <int>`
  - `--quantstats-min-overlap <int>`
  - `--quantstats-max-runs <int>`
  - `--quantstats-write-core-html` (optional raw QuantStats HTML)

Enable from legacy wrapper:
- enabled by default
- disable with `python3 run/daily_pipeline.py --skip-quantstats`

Artifacts written to:
- `reports/quantstats/dashboard_tearsheet_<run_id>.html`
- `reports/quantstats/dashboard_tearsheet_<run_id>_returns.csv`
- `reports/quantstats/dashboard_tearsheet_<run_id>_series.csv`
- `reports/quantstats/dashboard_tearsheet_<run_id>.json`

Optional:
- `reports/quantstats/dashboard_tearsheet_<run_id>_quantstats.html` (only when `--quantstats-write-core-html` is set)
