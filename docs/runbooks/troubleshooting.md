# Troubleshooting

- **Purpose:** Symptom → diagnosis → commands → fix → verify for the most common operational failures.
- **Audience:** Operator.
- **Last verified:** 2026-05-16
- **Source of truth:** [`docs/stages/*.md`](../stages/) (Failure modes sections), `src/ai_trading_system/pipeline/orchestrator.py`, plus legacy `docs/operations/troubleshooting.md`.

For DQ-specific triage see [dq_failure_response.md](./dq_failure_response.md). For publish retries see [publish_retry.md](./publish_retry.md). For OHLCV repair see [data_repair.md](./data_repair.md).

---

## Preflight fails: Dhan credentials missing

- **Symptom:** Pipeline aborts at preflight with `dhan_api_key`, `dhan_client_id`, or `dhan_auth_material` missing.
- **Diagnosis:** Preflight requires Dhan credentials for `ingest` and `features`, even though the default ingest path is NSE bhavcopy with yfinance fallback.
- **Commands:**
  ```bash
  env | grep -E 'DHAN_(API_KEY|CLIENT_ID|ACCESS_TOKEN|REFRESH_TOKEN|TOTP)'
  ```
- **Fix:** Either set the required Dhan vars (see [`docs/reference/environment_variables.md`](../reference/environment_variables.md)), or bypass preflight for a local run:
  ```bash
  python -m ai_trading_system.pipeline.orchestrator --skip-preflight --stages ingest,features,rank,publish --local-publish
  ```
- **Verify:** Pipeline proceeds past preflight; `ingest_summary.json` is produced.

## rank blocked by trust

- **Symptom:** Rank stage aborts citing trust quarantine or `data_trust_status` blocked.
- **Diagnosis:** Ingest left active quarantine rows on the latest trade date, or trust thresholds were exceeded. See [`docs/stages/ingest.md`](../stages/ingest.md) and [`docs/stages/rank.md`](../stages/rank.md).
- **Commands:**
  ```bash
  cat data/pipeline_runs/<run_id>/ingest/attempt_*/ingest_summary.json | jq '.unresolved_dates, .quarantined_row_count, .trust_summary'
  duckdb data/ohlcv.duckdb "SELECT * FROM _catalog_quarantine WHERE trade_date >= current_date - 7;"
  ```
- **Fix:** Repair the affected date window — follow [data_repair.md](./data_repair.md), then rerun `ingest -> features -> rank`. Only use `allow_untrusted_rank` intentionally and temporarily.
- **Verify:** `freshness_status` returns to `fresh`; rank produces `ranked_signals.csv`.

## execute blocked even though rank succeeded

- **Symptom:** Execute aborts with trust or policy guard, while rank completed.
- **Diagnosis:** Rank succeeded with `degraded` trust, but execute is configured to block degraded trust; or rank payload trust is `blocked`. See [`docs/stages/execute.md`](../stages/execute.md).
- **Commands:**
  ```bash
  cat data/pipeline_runs/<run_id>/rank/attempt_*/dashboard_payload.json | jq '.data_trust_status'
  ```
- **Fix:** Resolve the upstream ingest/trust issue and rerun execute with the same run_id:
  ```bash
  python -m ai_trading_system.pipeline.orchestrator --run-id <run_id> --stages execute
  ```
- **Verify:** Rows appear in `data/execution.duckdb::execution_order`.

## publish failed after upstream stages completed

- **Symptom:** Run status `completed_with_publish_errors`; one or more blocking channels failed.
- **Diagnosis:** Networked channel (Google Sheets, Telegram) error after retries, or auth expiry.
- **Commands:**
  ```bash
  cat data/pipeline_runs/<run_id>/publish/attempt_*/publish_summary.json | jq
  duckdb data/control_plane.duckdb "SELECT channel, status, attempt FROM publisher_delivery_log WHERE run_id='<run_id>' ORDER BY channel;"
  ```
- **Fix:** See [publish_retry.md](./publish_retry.md).
- **Verify:** Each blocking channel ends `delivered` or `duplicate`.

## React console loads but shows no data

- **Symptom:** UI loads, all panels empty or API calls fail.
- **Diagnosis:** FastAPI backend not running, or `VITE_EXECUTION_API_BASE_URL` misconfigured.
- **Commands:**
  ```bash
  curl -s http://localhost:8090/api/health
  ```
- **Fix:** Start the backend:
  ```bash
  python -m ai_trading_system.ui.execution_api.app --port 8090
  ```
  Confirm the React app points at the same host/port. The FastAPI app does not serve the React build.
- **Verify:** `/api/health` responds 200; React console populates.

## API-triggered "full pipeline" does not execute trades

- **Symptom:** A run launched from the React console / API completed but no orders were emitted.
- **Diagnosis:** UI-default pipeline omits the `execute` stage.
- **Fix:** Launch with an explicit stage list including `execute`, or run from the CLI (`ai-trading-pipeline`, which includes execute by default).
- **Verify:** Stage list in task metadata includes `execute`; `executed_orders.csv` is produced.

## QuantStats report missing

- **Symptom:** Expected QuantStats tearsheet not delivered.
- **Diagnosis:** QuantStats is a `publish_optional` channel (non-blocking). It may also lack sufficient return history.
- **Commands:**
  ```bash
  cat data/pipeline_runs/<run_id>/publish/attempt_*/publish_summary.json | jq '.channels[] | select(.name | test("quantstats"))'
  ls reports/quantstats/
  ```
- **Fix:** Rerun publish with QuantStats enabled; ensure enough recent rank runs exist to build a return stream.
- **Verify:** Channel status `delivered`; report file present.

## publish_test fails

- **Symptom:** `ai-trading-publish-test` exits non-zero before delivery.
- **Diagnosis:** Missing Google or Telegram configuration, or DNS preflight check failed.
- **Commands:**
  ```bash
  env | grep -E 'GOOGLE_SPREADSHEET_ID|GOOGLE_TOKEN_PATH|TELEGRAM_BOT_TOKEN|TELEGRAM_CHAT_ID'
  python -m ai_trading_system.pipeline.publish_test
  ```
- **Fix:** Provide missing credentials. If DNS preflight itself is the blocker, allow networked runs to skip it via `--skip-publish-network-checks` on orchestrated runs (not on `publish_test`).
- **Verify:** `publish_test` exits 0.

## Tasks appear stuck in operator UI

- **Symptom:** Task in the React console shows running indefinitely.
- **Diagnosis:** Background process exited, or terminal-state refresh lag.
- **Commands:**
  ```bash
  curl -s http://localhost:8090/api/tasks/<task_id> | jq
  curl -s http://localhost:8090/api/tasks/<task_id>/logs
  curl -s http://localhost:8090/api/processes | jq
  ```
- **Fix:** Terminate the dead task/process via UI or API, then prefer a stage retry (`--run-id <id> --stages <stage>`) over a fresh run.
- **Verify:** Task transitions to `failed` or `completed`; replacement attempt produces expected artifacts.

## features: snapshot or registry empty (DQ critical)

- **Symptom:** `DataQualityCriticalError: features_snapshot_created` or `features_registry_not_empty`.
- **Diagnosis:** `_catalog` empty, or schema drift broke `record_snapshot`. See [`docs/stages/features.md`](../stages/features.md).
- **Commands:**
  ```bash
  duckdb data/ohlcv.duckdb "SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM _catalog;"
  ```
- **Fix:** If catalog empty, rerun ingest. If schema drift suspected:
  ```bash
  ai-trading-repair-ingest-schema
  ```
- **Verify:** Snapshot row appears in `_snapshots`; feature batch logs `status='completed'`.

## events: market_intel missing or degraded

- **Symptom:** `market_intel_status="missing"` or `"degraded"` in `event_packet.json`.
- **Diagnosis:** `data/market_intel.duckdb` not populated by the always-on `market_intel` runner. See [`docs/stages/events.md`](../stages/events.md).
- **Commands:**
  ```bash
  ls -l data/market_intel.duckdb
  ```
- **Fix:** Restart the market_intel runner. The events stage tolerates absence; downstream catalysts will be thin until restored.
- **Verify:** Subsequent run reports `market_intel_status="ok"`.

## execute: "Live Dhan execution is intentionally disabled"

- **Symptom:** RuntimeError from the Dhan adapter mentioning live execution disabled.
- **Diagnosis:** A caller passed `dry_run=False`. Live trading is **not** production-ready; the adapter intentionally blocks it.
- **Fix:** Run paper execute (default). Do not pass `dry_run=False`.
- **Verify:** Execute stage completes; orders recorded in `data/execution.duckdb`.

## perf_tracker failed but run is green

- **Symptom:** `perf_tracker` status `failed`, pipeline marked successful.
- **Diagnosis:** perf_tracker is non-blocking. See [`docs/stages/perf_tracker.md`](../stages/perf_tracker.md).
- **Commands:**
  ```bash
  duckdb data/research.duckdb "SELECT * FROM rank_cohort_performance ORDER BY run_date DESC LIMIT 5;"
  ```
- **Fix:** Investigate the error captured in stage metadata; rerun with `--stages perf_tracker --run-id <id>`. No action needed for normal pipeline operation.
- **Verify:** Next attempt writes rows.

## candidates: empty final_candidates.csv

- **Symptom:** `final_candidates.csv` empty, status `completed_empty`.
- **Diagnosis:** Upstream rank produced no signals (thin universe, regime filter, trust issues). See [`docs/stages/candidates.md`](../stages/candidates.md).
- **Fix:** Investigate rank output; downstream stages should tolerate empty candidates.
- **Verify:** No `KeyError` downstream.
