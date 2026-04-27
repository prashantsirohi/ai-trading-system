# Troubleshooting

## `ingest` or `features` fails preflight because Dhan vars are missing

Likely cause:
- preflight requires Dhan credentials for `ingest` and `features`
- the default ingest path is still `NSE bhavcopy -> yfinance fallback`

Exact checks:
- inspect the preflight output in the task log or CLI output
- confirm whether the failure names are `dhan_api_key`, `dhan_client_id`, or `dhan_auth_material`

Recovery:
- for a local operator validation run, rerun with `--skip-preflight`
- for Dhan-dependent flows, set the required Dhan env vars and rerun

## `rank` is blocked by trust

Likely cause:
- ingest left active quarantine rows on the latest trade date, or trust thresholds were exceeded

Exact checks:
- inspect `ingest_summary.json` for `unresolved_dates`, `quarantined_row_count`, and `trust_summary`
- inspect the latest trust snapshot from the operator API or UI
- verify whether status is `blocked` or `degraded`

Recovery:
- repair the affected date window with `collectors.reset_reingest_validate`
- rerun `ingest`, then `features`, then `rank`
- only use `allow_untrusted_rank` intentionally and temporarily

## `execute` is blocked even though rank succeeded

Likely cause:
- rank succeeded with degraded trust, but `execute` is configured to block degraded trust
- or rank payload trust is `blocked`

Exact checks:
- inspect `dashboard_payload.json` for `data_trust_status`
- inspect execute-stage params for `block_degraded_execution` and `allow_untrusted_execution`

Recovery:
- fix the underlying ingest or trust issue first
- rerun `execute` with the same `run_id` after trust is acceptable
- do not assume rank success implies execute eligibility

## `publish` failed after upstream stages completed

Likely cause:
- one or more publish channels failed after retries

Exact checks:
- inspect `publish_summary.json`
- inspect `publisher_delivery_log` for the failing channel and attempt count
- confirm whether the run status is `completed_with_publish_errors`

Recovery:
- rerun `publish` only with the same `run_id`
- use `--local-publish` to verify payload assembly without network delivery
- fix missing Google or Telegram credentials if the failing channel is networked

## React console loads but shows no data or actions fail

Likely cause:
- the React app is running without the FastAPI backend
- or `VITE_EXECUTION_API_BASE_URL` points to the wrong backend

Exact checks:
- confirm `python -m ui.execution_api.app --port 8090` is running
- inspect the React env or frontend config for the API base URL
- call `/api/execution/health` directly in the browser or with `curl`

Recovery:
- start the FastAPI backend
- point the React app at the correct backend URL
- remember that FastAPI does not serve the React build directly

## API “full pipeline” does not execute trades

Likely cause:
- UI-triggered default pipeline runs omit `execute`

Exact checks:
- inspect the task metadata or request body for the stage list
- confirm whether `execute` is present

Recovery:
- launch a run with an explicit stage list that includes `execute`
- do not rely on the UI default if you need paper execution

## QuantStats report is missing

Likely cause:
- `publish_quantstats` was disabled
- there was not enough run overlap for return-series construction
- publish failed before report completion

Exact checks:
- inspect `publish_summary.json`
- confirm `quantstats_dashboard_tearsheet` status in `publisher_delivery_log`
- inspect `reports/quantstats/`

Recovery:
- rerun `publish` with QuantStats enabled
- verify that enough recent rank runs exist to build a return stream

## React V2 dashboard pages are empty

Likely cause:
- the underlying data stores or artifacts have not been generated yet
- master data is missing

Exact checks:
- confirm `data/masterdata.db` exists
- confirm `data/ohlcv.duckdb` and `data/feature_store/` exist for operational pages
- confirm `data/research/...` exists for research pages

Recovery:
- run `python -m ai_trading_system.domains.ingest.masterdata`
- run the appropriate operational or research pipeline command to generate the missing data

## `publish_test` fails

Likely cause:
- missing Google or Telegram configuration
- network DNS checks failed

Exact checks:
- inspect the raised preflight failures
- verify `GOOGLE_SPREADSHEET_ID`, credentials or token file presence, `TELEGRAM_BOT_TOKEN`, and `TELEGRAM_CHAT_ID`

Recovery:
- fix the missing credentials
- rerun `python -m ai_trading_system.pipeline.publish_test`
- use `--skip-publish-network-checks` on orchestrated runs only when DNS checking itself is the blocker

## Tasks appear stuck in the operator UI

Likely cause:
- the background process exited unexpectedly
- the run reached a terminal state but the task view has not refreshed yet

Exact checks:
- inspect `/api/execution/tasks/{task_id}`
- inspect `/api/execution/tasks/{task_id}/logs`
- inspect `/api/execution/processes`

Recovery:
- terminate the dead task or process through the UI or API
- rerun the intended action
- if the underlying run succeeded, prefer a stage retry over a brand-new run when recovering publish or execute
