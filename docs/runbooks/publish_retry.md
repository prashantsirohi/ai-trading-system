# Publish Retry

- **Purpose:** Retry publish for an existing run_id, fall back to local publish, and recover specific channel failures.
- **Audience:** Operator.
- **Last verified:** 2026-05-16
- **Source of truth:** [`docs/stages/publish.md`](../stages/publish.md), [`docs/reference/publish_contracts.md`](../reference/publish_contracts.md), `src/ai_trading_system/domains/publish/delivery_manager.py`.

---

## Channel roles

| Role | Failure behavior |
|---|---|
| `publish_of_record` | **Blocking** — run marks `completed_with_publish_errors`. |
| `publish_auxiliary` | **Blocking**. |
| `informational` | **Blocking** (e.g., Telegram precheck). |
| `publish_optional` | **Non-blocking** (e.g., QuantStats). |
| `diagnostic` | **Non-blocking**. |

A run marked `completed_with_publish_errors` means at least one blocking channel failed after retries.

## 1. Retry publish for the same run_id

This is the standard recovery path. Previously delivered channels are deduped and marked `duplicate`:

```bash
python -m ai_trading_system.pipeline.orchestrator --run-id <run_id> --stages publish
```

## 2. Local publish fallback

Verify payload assembly without network delivery — useful when external services are down or you want to inspect generated artifacts:

```bash
python -m ai_trading_system.pipeline.orchestrator --run-id <run_id> --stages publish --local-publish
```

## 3. Diagnose channel failure

```bash
cat data/pipeline_runs/<run_id>/publish/attempt_*/publish_summary.json | jq
duckdb data/control_plane.duckdb "
  SELECT channel, status, attempt, error
  FROM publisher_delivery_log
  WHERE run_id='<run_id>'
  ORDER BY channel, attempt;
"
```

## 4. Google Sheets failure

- **Symptom:** `google_sheets_*` channel `status='failed'`, error mentioning auth or quota.
- **Diagnosis:** OAuth token expired or `GOOGLE_SPREADSHEET_ID` not set; OAuth refresh is handled by `google_sheets_manager` but persistent failure is raised.
- **Commands:**
  ```bash
  env | grep -E 'GOOGLE_SPREADSHEET_ID|GOOGLE_TOKEN_PATH|GOOGLE_SHEETS_CREDENTIALS'
  ls -l "$GOOGLE_TOKEN_PATH"
  python -m ai_trading_system.pipeline.publish_test
  ```
- **Fix:** Refresh OAuth token (rerun the OAuth flow under `google_sheets_manager`); verify the spreadsheet ID matches an accessible sheet.
- **Verify:** `publish_test` succeeds; retry publish for the run.

## 5. Telegram failure

- **Symptom:** Telegram channel `failed`, possibly with `precheck=<kind>`.
- **Diagnosis:** Token or chat ID wrong; or HTTP timeout. See `domains/publish/channels/telegram.py`.
- **Commands:**
  ```bash
  env | grep -E 'TELEGRAM_(BOT_TOKEN|CHAT_ID|CONNECT_TIMEOUT|READ_TIMEOUT|WRITE_TIMEOUT|SEND_ATTEMPTS)'
  ```
- **Fix:** Correct credentials; optionally raise `TELEGRAM_*_TIMEOUT_SECONDS` or `TELEGRAM_SEND_ATTEMPTS` for flaky networks. See [`docs/reference/environment_variables.md`](../reference/environment_variables.md).
- **Verify:** Retry publish; Telegram channel ends `delivered`.

## 6. QuantStats (optional, non-blocking)

QuantStats is a `publish_optional` channel — a failure here does not fail the run.

- **Symptom:** QuantStats tearsheet missing.
- **Diagnosis:** Insufficient return history, channel disabled, or assembly error.
- **Commands:**
  ```bash
  ls reports/quantstats/
  cat data/pipeline_runs/<run_id>/publish/attempt_*/publish_summary.json | jq '.channels[] | select(.name|test("quantstats"))'
  ```
- **Fix:** Ensure enough recent rank runs exist to construct a return stream. Retry publish.
- **Verify:** Channel `delivered`; report file present.

## 7. publish_test (target healthcheck)

Run before retrying to confirm credentials reach external services:

```bash
ai-trading-publish-test
```

Or:

```bash
python -m ai_trading_system.pipeline.publish_test
```

## 8. Verify after retry

1. `publish_summary.json` reflects all blocking channels `delivered` or `duplicate`.
2. `publisher_delivery_log` shows the new attempt rows.
3. Run status transitions from `completed_with_publish_errors` to `completed`.
