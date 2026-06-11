# Publishing Domain

- **Purpose:** Deliver pipeline artifacts to external channels (Google Sheets, Telegram, QuantStats, PDF) with per-channel blocking semantics, idempotency, and retry.
- **Audience:** Developer, operator.
- **Last verified:** 2026-05-16
- **Source of truth:** [`src/ai_trading_system/domains/publish/`](../../src/ai_trading_system/domains/publish/), [`src/ai_trading_system/pipeline/stages/publish.py`](../../src/ai_trading_system/pipeline/stages/publish.py)

---

## Responsibility

Be the **single egress** to external destinations. Pipeline artifacts → channels via a delivery manager that handles dedup, retry, and per-channel role policy.

## Package / module ownership

| Module | Role |
|---|---|
| `delivery_manager.py::PublisherDeliveryManager` | Dispatch, dedup, retry, role enforcement. |
| `channels/google_sheets.py` + `google_sheets_manager.py` | Dashboard, watchlist, portfolio, publish log sheets. OAuth flow. |
| `channels/telegram.py` | Summary digest. |
| `channels/quantstats.py` | Performance tearsheet PDF. |
| `channels/weekly_pdf/` | HTML→PDF weekly report (weasyprint). |
| `channels/daily_gainers/` | Daily gainers HTML; CLI: `ai-trading-daily-gainers-report`. |
| `channels/watchlist_digest.py` | Bucket digest messages. |
| `channels/oauth_flow.py` | Google OAuth. |

## Channel roles

| Role | Blocking? | Examples |
|---|---|---|
| `publish_of_record` | Yes | Dashboard sheet, watchlist sheet, QuantStats |
| `publish_auxiliary` | Yes | Event log sheet, publish log sheet |
| `publish_optional` | No | Portfolio sheet (tolerates live-API failures) |
| `informational` | Yes | Telegram summary, weekly PDF |
| `diagnostic` | No | Local summary JSON |

(Verify exact strings in `delivery_manager.py` when writing [`reference/publish_contracts.md`](../reference/publish_contracts.md).)

## Public contracts

- Stage artifact: `data/pipeline_runs/<run_id>/publish/attempt_<n>/publish_summary.json` (channel-by-channel status).
- `watchlist_buckets.csv` (consumed by `perf_tracker` for bucket attribution — see [`stages/perf_tracker.md`](../stages/perf_tracker.md)).
- External writes: Google Sheets, Telegram messages, PDFs.

## Storage ownership

- No exclusive DuckDB tables.
- Stage artifacts only.
- May read execution / perf data for QuantStats tearsheet.

## Dependencies

- Env vars: `GOOGLE_SPREADSHEET_ID`, `GOOGLE_TOKEN_PATH`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, plus Telegram timeout/retry tunables. See [`reference/environment_variables.md`](../reference/environment_variables.md).
- Reads all upstream stage artifacts to build channel payloads.

## Extension points

- New channel: see [`docs/development/adding_new_publisher.md`](../development/adding_new_publisher.md).
- New publish role: extend `delivery_manager.py` role enum.

## Known gaps

- Google Sheets service-account auth (`GOOGLE_SHEETS_CREDENTIALS`) is **deprecated** — OAuth flow at `GOOGLE_TOKEN_PATH` is now the canonical path.
- Channel dedupe keys are not centrally documented; surface as part of `reference/publish_contracts.md`.

## See also

- [`docs/stages/publish.md`](../stages/publish.md)
- [`docs/reference/publish_contracts.md`](../reference/publish_contracts.md)
- [`docs/runbooks/publish_retry.md`](../runbooks/publish_retry.md)
