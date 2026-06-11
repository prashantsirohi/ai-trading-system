# Publish Contracts

- **Purpose:** Catalog every publish channel: input artifact, external destination, blocking role, dedupe key, retry policy, and failure semantics.
- **Audience:** Operator, developer.
- **Last verified:** 2026-05-16
- **Source of truth:** `src/ai_trading_system/domains/publish/delivery_manager.py`, `src/ai_trading_system/pipeline/stages/publish.py`, `src/ai_trading_system/domains/publish/channels/{google_sheets,google_sheets_manager,telegram,quantstats,watchlist_digest}.py`, `src/ai_trading_system/domains/publish/channels/weekly_pdf/`, `src/ai_trading_system/domains/publish/channels/daily_gainers/`, `src/ai_trading_system/platform/utils/runtime_config.py`.

## Delivery manager and channel roles

`PublisherDeliveryManager` (`delivery_manager.py:12`) handles dedupe + retry for every channel. Roles are assigned in `PublishStage.CHANNEL_ROLES` (`publish.py:46-61`):

| Role string | Blocking? | Notes |
|---|---|---|
| `publish_of_record` | **Blocking** — failure raises `PublishStageError` | Primary outputs (`publish.py:32-33`) |
| `publish_auxiliary` | **Blocking** | Secondary outputs (`publish.py:34`) |
| `publish_optional` | Non-blocking — logged in `non_blocking_failures` (`publish.py:45`) | Best-effort live external IO (`publish.py:35-39`) |
| `informational` | **Blocking** | Notification channels (`publish.py:40`) |
| `diagnostic` | **Blocking** (but only fires in `local_publish` mode) | Local-only artifacts (`publish.py:41`) |

`NON_BLOCKING_ROLES = frozenset({"publish_optional"})` (`publish.py:45`). Only `publish_optional` bypasses the blocking gate; everything else, including `informational` and `diagnostic`, will raise `PublishStageError` on failure (`publish.py:155-178`).

## Dedupe key

`build_dedupe_key` (`delivery_manager.py:116-127`): `sha256(f"{channel}:{artifact.content_hash or artifact.uri}")`. When `artifact.metadata["event_hashes"]` is present (set by `publish.py:137`), the sorted, pipe-joined event hashes are folded into the seed so re-delivery of the same enriched-signal set is suppressed.

Dedupe is bypassed when the channel name is in `context.params["bypass_dedupe_channels"]` (`delivery_manager.py:37-38`). `weekly_pdf` is auto-added to that list because it has no external side effects (`publish.py:356-360`).

If a prior delivery succeeded under the same dedupe key, the channel is short-circuited with `status="duplicate"` and a delivery-log row recorded (`delivery_manager.py:39-61`).

## Retry policy

`PublisherDeliveryManager(__init__, delivery_manager.py:15-23)` defaults: `max_attempts=3`, `base_delay_seconds=1.0`, exponential backoff `sleep(base · 2^retry_index)` between attempts (`delivery_manager.py:112`). Each attempt logs `status="retrying"` (intermediate) or `status="failed"` (final) into the registry (`delivery_manager.py:89-113`).

Telegram has its own retry layer on top: `TELEGRAM_SEND_ATTEMPTS` (default `1`, floored at 1) controls per-call retries before the delivery manager retries (`runtime_config.py:62`, `telegram.py:46`).

## Channels

| Channel key | Module | Input artifact(s) | External destination | Role (blocking) | Dedupe key | Retry | Failure behavior |
|---|---|---|---|---|---|---|---|
| `google_sheets_dashboard` | `domains/publish/channels/google_sheets.py` + `dashboard.py` (handler `publish.py:394-434`) | `dashboard_payload` JSON + `ranked_signals`, `breakout_scan`, `sector_dashboard`, `pattern_scan`, `watchlist_candidates`, `decision_bundle` | Google Sheet (`GOOGLE_SPREADSHEET_ID`) | `publish_of_record` (**blocking**) | sha256(channel + rank artifact hash + event_hashes) | 3 attempts, exponential backoff | Raises `PublishStageError` |
| `google_sheets_watchlist` | `channels/google_sheets.py::publish_watchlist_candidates` (`publish.py:448-458`) | `watchlist_candidates` + `decision_bundle` | Google Sheet | `publish_of_record` (**blocking**) | sha256(channel + rank hash) | 3, expo | Raises |
| `google_sheets_portfolio` | `pipeline/daily_pipeline.run_portfolio_analysis` (`publish.py:490-501`) | Live YF lookups + `PORTFOLIO` sheet | Google Sheet (`PORTFOLIO` tab) | `publish_optional` (**non-blocking**) | sha256(channel + rank hash) | 3, expo | Logged in `metadata["non_blocking_failures"]`; stage continues (`publish.py:155-160`) |
| `google_sheets_publish_log` | `channels/google_sheets.py::publish_log_sheet` (`publish.py:475-488`) | `decision_bundle` (publish log) | Google Sheet | `publish_auxiliary` (**blocking**) | sha256(channel + rank hash) | 3, expo | Raises; skipped if `decision_bundle` is missing |
| `quantstats_dashboard_tearsheet` | `channels/quantstats.py::publish_dashboard_quantstats_tearsheet` (`publish.py:503-542`) | Historical rank artifacts under `pipeline_runs/`, latest rank/breakout/sector DFs | Local HTML/PDF tear sheet under reports dir | `publish_of_record` (**blocking**) | sha256(channel + rank hash) | 3, expo | Raises unless error is one of `{insufficient_rank_history_for_tearsheet, pipeline_runs_dir_missing, quantstats_not_available}` and `quantstats_required=False` (default) — then `status="skipped"` (`publish.py:524-537`) |
| `telegram_summary` | `channels/telegram.py::TelegramReporter.send_message` (`publish.py:554-593`) | `ranked_signals`, `decision_bundle.telegram_digest` or `insight_telegram_summary`, `watchlist_candidates`, weekly intel | Telegram chat (`TELEGRAM_CHAT_ID`) via Bot API | `informational` (**blocking**) | sha256(channel + rank hash + event_hashes) | Outer: 3 attempts; inner Telegram client: `TELEGRAM_SEND_ATTEMPTS` per call | Raises on final failure; precheck failure (DNS) reported in `last_health_check` |
| `weekly_pdf` | `channels/weekly_pdf/channel.py::publish_weekly_pdf` (`publish.py:544-552`) | Weekly intel + rank/breakout/sector/pattern history (`weekly_pdf/data_loader.py`) | Local HTML + optional PDF under `<attempt_dir>/weekly_pdf/` | `informational` (**blocking**) | bypasses dedupe (`publish.py:356-360`) | 3, expo | Raises on hard error; PDF-only failure recorded in `pdf_error` without raising (`channel.py:24-36`) |
| `local_summary` | `channels/watchlist_digest.py::render_watchlist_markdown` (`publish.py:363-380`) | `watchlist_candidates` | Local `watchlist_digest.md` under attempt dir | `diagnostic` (**blocking** when enabled) | sha256(channel + rank hash) | 3, expo | Only registered when `context.params["local_publish"]=True` (`publish.py:336-337`) |

### Daily gainers (separate CLI)

`domains/publish/channels/daily_gainers/cli.py::main` runs standalone via the `ai-trading-daily-gainers-report` entry point (`pyproject.toml`). It does not register a `PublisherDeliveryManager` channel role and is not part of the publish stage. It writes HTML + (optional) PDF to `--output-dir` (default `reports/daily_gainers/`) using OHLCV from `data/ohlcv.duckdb` and optional OpenRouter LLM enrichment (`cli.py:32-50`).

### Watchlist digest module

`channels/watchlist_digest.py` exports `render_watchlist_markdown` and `render_watchlist_telegram`. It is consumed by `local_summary` (`publish.py:369-374`) and by `telegram_summary` to append a watchlist tail to the message (`publish.py:578-582`); it does not register its own channel.

## OAuth and authentication

### Google Sheets (`channels/google_sheets_manager.py`)

Authentication order in `_authenticate` (`google_sheets_manager.py:61-100`):

1. If `GOOGLE_TOKEN_PATH` exists, load OAuth user credentials. Refresh via `google.auth.transport.requests.Request` if expired with a refresh token; write the refreshed JSON back to the token path.
2. Otherwise, if `GOOGLE_SHEETS_CREDENTIALS` exists and the file declares `"type": "service_account"`, authenticate as a service account. Else fall through to the OAuth installed-app flow in `channels/oauth_flow.py`.

Scopes (`google_sheets_manager.py:28-31`): `https://www.googleapis.com/auth/spreadsheets`, `https://www.googleapis.com/auth/drive`.

The spreadsheet ID comes from `GOOGLE_SPREADSHEET_ID` (`google_sheets.py:20-25`, `runtime_config.py:79`).

### Telegram (`channels/telegram.py`)

Uses `python-telegram-bot` (`telegram.py:17-23`). Config loaded via `TelegramRuntimeConfig.from_env` (`runtime_config.py:55-63`):

- `TELEGRAM_BOT_TOKEN` — bot token (required).
- `TELEGRAM_CHAT_ID` — destination chat (required).
- `TELEGRAM_CONNECT_TIMEOUT_SECONDS` — default `5.0`.
- `TELEGRAM_READ_TIMEOUT_SECONDS` — default `10.0`.
- `TELEGRAM_WRITE_TIMEOUT_SECONDS` — default `10.0`.
- `TELEGRAM_POOL_TIMEOUT_SECONDS` — default `2.0`.
- `TELEGRAM_SEND_ATTEMPTS` — default `1`, minimum `1`.
- `TELEGRAM_DNS_PRECHECK_ENABLED` — default true; performs `socket.getaddrinfo("api.telegram.org", 443)` before sending (`telegram.py:87-100`).

### QuantStats (`channels/quantstats.py`)

No external auth. Writes locally. Requires `quantstats` import to be available; otherwise the channel skips with error `quantstats_not_available` (and is non-fatal when `quantstats_required=False`). Uses `MPLCONFIGDIR` for the matplotlib cache (defaulting to `<repo>/logs/matplotlib`, `quantstats.py:21-24`).

### Weekly PDF (`channels/weekly_pdf/`)

No external auth. Pure local rendering; PDF generation requires the configured renderer to be installed (`pdf_error` returned without raising if missing).

## Delivery log

Every attempt (`delivered`, `retrying`, `failed`, `duplicate`) is recorded via `context.registry.record_delivery_log` (`delivery_manager.py:41-103`) with fields: `run_id`, `stage_name`, `channel`, `artifact_uri`, `artifact_hash`, `dedupe_key`, `attempt_number`, `status`, `external_message_id`, `external_report_id`, `error_message`, `metadata`. The publish stage summary persists targets, non-blocking failures, and (on hard failure) `failures` in `publish_summary.json` (`publish.py:164-178`).
