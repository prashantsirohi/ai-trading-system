# Stage: publish

- **Purpose:** Deliver pipeline artifacts to external channels (Google Sheets, Telegram, QuantStats, PDF) and write a local publish summary, with per-channel blocking semantics.
- **Audience:** Operator, developer, debugging
- **Last verified:** 2026-05-16
- **Source of truth:** [`src/ai_trading_system/pipeline/stages/publish.py`](../../src/ai_trading_system/pipeline/stages/publish.py), [`src/ai_trading_system/domains/publish/delivery_manager.py`](../../src/ai_trading_system/domains/publish/delivery_manager.py), [`src/ai_trading_system/domains/publish/channels/`](../../src/ai_trading_system/domains/publish/channels/)

---

## Purpose

Take rank/event/insight/narrative artifacts, attach 4-bucket watchlist, build channel datasets and a publish decision bundle, then dispatch each channel through `PublisherDeliveryManager` (which handles idempotency, dedup, retry, and delivery logging). Channel failures are gated by per-channel role.

## Entrypoints

- Stage wrapper: [`src/ai_trading_system/pipeline/stages/publish.py::PublishStage`](../../src/ai_trading_system/pipeline/stages/publish.py)
- Runs after `narrative`, before `perf_tracker` (`PIPELINE_ORDER` in [`pipeline/orchestrator.py:41`](../../src/ai_trading_system/pipeline/orchestrator.py))
- Test CLI: `ai-trading-publish-test` ([`pipeline/publish_test.py`](../../src/ai_trading_system/pipeline/publish_test.py))

## Input data

- **Required artifact:** `rank.ranked_signals` ([`publish.py:91`](../../src/ai_trading_system/pipeline/stages/publish.py))
- **Optional rank artifacts:** `breakout_scan`, `pattern_scan`, `sector_dashboard`, `dashboard_payload`, `watchlist_candidates`
- **Fundamentals fallback:** `watchlist_candidates`, `fundamental_summary`, `fundamental_scores` from `fundamentals` stage when not present in rank ([`publish.py:96-98`](../../src/ai_trading_system/pipeline/stages/publish.py))
- **Events:** `market_events_snapshot`, `events_enrichment`, `events_summary`
- **Insight/narrative:** `narrative.telegram_summary`, `insight.event_confluence`, `narrative.daily_insight_json` / `weekly_insight_json`
- **Params:** `local_publish`, `publish_quantstats` (default `True`), `publish_weekly_pdf`, `quantstats_top_n`, `quantstats_min_overlap`, `quantstats_max_runs`, `quantstats_breadth_start_date`, `quantstats_required`, `bypass_dedupe_channels`. `smoke` is rejected ([`publish.py:74-75`](../../src/ai_trading_system/pipeline/stages/publish.py)).

## Output artifacts

Under `data/pipeline_runs/<run_id>/publish/attempt_<n>/`:

| Artifact | File |
|---|---|
| `publish_summary` | `publish_summary.json` (targets, watchlist bucket counts, failures, non_blocking_failures, fundamentals top adds) |
| `watchlist_buckets` | `watchlist_buckets.csv` (Phase-5 4-bucket taxonomy) |

External side effects (per channel role, see below):
- Google Sheets writes
- Telegram message
- QuantStats tearsheet HTML (under `data/pipeline_runs/.../publish/.../`)
- Weekly PDF (when enabled)

## Main modules

- [`domains/publish/delivery_manager.py::PublisherDeliveryManager`](../../src/ai_trading_system/domains/publish/delivery_manager.py) — retry (default `max_attempts=3`, exponential `base_delay_seconds * 2^i`), dedup via SHA-256 of `channel:content_hash[|events:...]`, delivery log to registry ([`delivery_manager.py:25-114`](../../src/ai_trading_system/domains/publish/delivery_manager.py))
- [`domains/publish/publish_payloads.py`](../../src/ai_trading_system/domains/publish/publish_payloads.py) — `build_publish_datasets`, `build_publish_metadata`
- [`domains/publish/decision_bundle.py`](../../src/ai_trading_system/domains/publish/decision_bundle.py) — `build_publish_decision_bundle` (per-symbol decision rationale + telegram digest)
- [`domains/publish/watchlist_buckets.py`](../../src/ai_trading_system/domains/publish/watchlist_buckets.py) — `assign_watchlist_buckets`, `summarize_buckets`
- [`domains/publish/dashboard.py`](../../src/ai_trading_system/domains/publish/dashboard.py) — `publish_dashboard_payload`
- [`domains/publish/channels/google_sheets.py`](../../src/ai_trading_system/domains/publish/channels/google_sheets.py) — multiple sheet writers (dashboard, watchlist, event log, publish log, stock scan, sector dashboard)
- [`domains/publish/channels/google_sheets_manager.py`](../../src/ai_trading_system/domains/publish/channels/google_sheets_manager.py) — OAuth client, env: `GOOGLE_SPREADSHEET_ID`, `GOOGLE_TOKEN_PATH`, `GOOGLE_SHEETS_CREDENTIALS` (legacy)
- [`domains/publish/channels/telegram.py`](../../src/ai_trading_system/domains/publish/channels/telegram.py) — `TelegramReporter`, env: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, optional `TELEGRAM_CONNECT_TIMEOUT_SECONDS`, `TELEGRAM_READ_TIMEOUT_SECONDS`, `TELEGRAM_WRITE_TIMEOUT_SECONDS`, `TELEGRAM_SEND_ATTEMPTS`
- [`domains/publish/channels/quantstats.py`](../../src/ai_trading_system/domains/publish/channels/quantstats.py) — `publish_dashboard_quantstats_tearsheet`
- [`domains/publish/channels/weekly_pdf/`](../../src/ai_trading_system/domains/publish/channels/weekly_pdf/) — weekly report generator
- [`domains/publish/channels/daily_gainers/`](../../src/ai_trading_system/domains/publish/channels/daily_gainers/) — standalone CLI `ai-trading-daily-gainers-report`
- [`domains/publish/channels/watchlist_digest.py`](../../src/ai_trading_system/domains/publish/channels/watchlist_digest.py) — markdown / Telegram watchlist rendering

## Channel roles

From [`publish.py:30-61`](../../src/ai_trading_system/pipeline/stages/publish.py). Verified strings:

| Role string | Blocking on failure? | Channels (default) |
|---|---|---|
| `publish_of_record` | yes | `google_sheets_dashboard`, `google_sheets_watchlist`, `quantstats_dashboard_tearsheet` |
| `publish_auxiliary` | yes (default for unknown channels) | `google_sheets_event_log`, `google_sheets_publish_log` |
| `publish_optional` | **no** (recorded in `non_blocking_failures`) | `google_sheets_portfolio` |
| `informational` | yes | `telegram_summary`, `weekly_pdf` |
| `diagnostic` | yes | `local_summary` (used only when `local_publish=true`) |

`NON_BLOCKING_ROLES = {"publish_optional"}` ([`publish.py:45`](../../src/ai_trading_system/pipeline/stages/publish.py)). Any channel whose role is `publish_optional` (currently only `google_sheets_portfolio`) is treated as best-effort: failures are appended to `publish_summary.non_blocking_failures` but do not raise `PublishStageError`. All other roles, including `informational`, retain blocking semantics.

## Process flow

1. Reject `smoke=true`.
2. Require `rank.ranked_signals`. Build datasets via `build_publish_datasets`.
3. Attach event datasets (snapshot + enrichment + summary), insight datasets (telegram summary + confluence + latest insight), and decision bundle.
4. Compute watchlist buckets; persist `watchlist_buckets.csv`; attach to datasets.
5. Select channel handlers in `_build_handlers`. `local_publish=true` overrides to a single `local_summary` channel. `quantstats_dashboard_tearsheet` is enabled by default; `weekly_pdf` requires `publish_weekly_pdf=true` and bypasses delivery dedup.
6. For each channel: `delivery_manager.deliver(...)` runs idempotency check, then up to `max_attempts=3` retries with exponential backoff, records every attempt in the delivery log, and returns one of `delivered` / `duplicate` / `failed`.
7. Build `publish_summary` metadata + fundamentals adds.
8. If any blocking-role channel failed → raise `PublishStageError` with concatenated messages.

## DQ / trust gates

- `publish_trust_status` carried into the decision bundle and surfaced in dashboards.
- `market_intel_status` ∈ `{missing, stale, degraded}` adds an `event_freshness_warning` to datasets.
- QuantStats failures with codes `insufficient_rank_history_for_tearsheet`, `pipeline_runs_dir_missing`, `quantstats_not_available` are treated as `skipped` (non-fatal) unless `quantstats_required=true` ([`publish.py:524-537`](../../src/ai_trading_system/pipeline/stages/publish.py)).

## Failure modes

- **Missing `rank.ranked_signals`:** stage aborts.
- **Smoke mode requested:** explicit `RuntimeError`.
- **Channel error (blocking role):** `PublishStageError` after retries exhausted.
- **Channel error (`publish_optional`):** recorded only.
- **Telegram precheck failure:** raised with `precheck=<kind>` detail; blocking (`informational` role).
- **Sheets auth expiry:** OAuth refresh handled by `google_sheets_manager`; persistent failure is raised by the handler.
- **EmptyDataError on CSV artifact with non-zero expected rows:** raises `PublishStageError` ([`publish.py:317-322`](../../src/ai_trading_system/pipeline/stages/publish.py)).

## Retry behavior

- Per-channel: 3 attempts inside `PublisherDeliveryManager.deliver`, exponential backoff `1 * 2^i` seconds (configurable on the manager).
- Per-stage: orchestrator-controlled re-runs create new `attempt_<n>` directories.
- Dedup: a successful delivery for a given `dedupe_key` (sha256 of channel + artifact content hash + sorted event hashes) is replayed as `duplicate` and not resent. Override by adding the channel to `context.params["bypass_dedupe_channels"]` (the stage auto-adds `weekly_pdf` to that list).

## Downstream consumers

- [`perf_tracker`](perf_tracker.md): runs after publish; does not consume publish artifacts directly but uses the latest `rank.ranked_signals` that publish has already exposed.
- FastAPI `/api/publish/*` endpoints (if present) and operator UI read `publish_summary.json` and the delivery log table.

## Commands

```bash
# Full pipeline (publishes to all enabled channels)
ai-trading-pipeline

# Local-only run (skips external channels, writes watchlist_digest.md + local_summary)
ai-trading-pipeline --local-publish

# Publish-only smoke against an existing run
ai-trading-publish-test

# Bypass dedup for the dashboard channel on a re-run
ai-trading-pipeline --bypass-dedupe-channels google_sheets_dashboard

# Inspect publish summary
jq '.targets[] | {channel, status, delivery_role}' data/pipeline_runs/<run_id>/publish/attempt_1/publish_summary.json
```
