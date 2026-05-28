# Data Sources

- **Purpose:** Catalogue every external data source the operational pipeline reads, with its role, endpoint, auth, and failure mode.
- **Audience:** Operator, developer.
- **Last verified:** 2026-05-16
- **Source of truth:** `src/ai_trading_system/domains/ingest/`, `src/ai_trading_system/domains/fundamentals/import_screener.py`, `src/ai_trading_system/domains/catalysts/collector.py`, `src/ai_trading_system/integrations/market_intel_client.py`.

> **Source-of-record order.** NSE bhavcopy is the source-of-record for OHLCV. Dhan is the fallback provider for prices and is also mandatory for live execution and (via the NSE MTO/security-wise scrapers in `domains/ingest/delivery.py`) for delivery data. yfinance is last-resort fill. The older "Dhan-first ingest" claim in legacy docs is **wrong** — confirm by reading `domains/ingest/service.py` before changing this ordering.

---

## Source: NSE bhavcopy (source-of-record)

- **Module:** `src/ai_trading_system/domains/ingest/providers/nse.py` (`NSECollector`)
- **Role:** source-of-record (OHLCV for NSE equities)
- **Endpoint or input:** Two URL candidates tried in order (`nse.py:23-30`):
  - `https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_<DDMMYYYY>.csv`
  - `https://www.nseindia.com/content/nsccl/CM<YYYYMMDD>bhav.csv.zip`
  - Plus historical OHLC via `https://www.nseindia.com/api/historical/cm/equity/<symbol>` (`nse.py:142`).
- **Auth:** None. Uses a browser-like `User-Agent` (`nse.py:50-55`); no API key.
- **Rate limits:** None enforced in code; relies on NSE's anonymous rate limits. Local caching at `data/raw/NSE_EQ/nse_<DDMmmYYYY>.csv` (`nse.py:32-35, 68-73`) avoids redundant downloads.
- **Failure behavior:** Each URL returning 404 is skipped (`nse.py:78-79`); any other exception is caught and logged, returning an empty `DataFrame` (`nse.py:89-91`). Ingest stage downstream then escalates to Dhan / yfinance fallback per `IngestOrchestrationService`.
- **Used by stage(s):** `ingest`.

## Source: NSE delivery (source-of-record for delivery data)

- **Module:** `src/ai_trading_system/domains/ingest/delivery.py` (`DeliveryCollector`)
- **Role:** source-of-record (delivery quantity / delivery % per symbol-day)
- **Endpoint or input:**
  - Primary `mto`: `https://nsearchives.nseindia.com/archives/equities/mto/MTO_<date_str>.DAT` (`delivery.py:30-32`)
  - Fallback `nse_securitywise`: NSE security-wise historical CSV endpoint (delegated to `NseHistoricalDeliveryScraper`, `delivery.py:68-72`)
- **Auth:** None. Session is primed with browser headers and a warm-up GET to `https://www.nseindia.com/` plus `Referer: https://www.nseindia.com/all-reports-derivatives` (`delivery.py:77-93`).
- **Rate limits:** None in code. Per-file retry: up to 3 attempts with `time.sleep(min(2**attempt, 5))` backoff; refreshes the NSE session on 401/403 (`delivery.py:95-118`).
- **Failure behavior:** 404 breaks the retry loop (date not available). Other request failures bubble up to the caller after retries. If `source="mto"` yields nothing for a date, the `fallback_source` (`nse_securitywise` by default) is tried.
- **Used by stage(s):** `ingest` (writes `ohlcv.duckdb::_delivery` and partitioned parquet under `feature_store/delivery/NSE/`).

## Source: Dhan (fallback OHLC + live execution)

- **Module:** `src/ai_trading_system/domains/ingest/providers/dhan.py` (`DhanCollector`); execution adapter `src/ai_trading_system/domains/execution/adapters/dhan.py`
- **Role:** fallback (intraday + EOD prices) and mandatory broker for live execution. **Live execution is gated off** — see `docs/reference/execution_policy.md`.
- **Endpoint or input:** REST base `https://api.dhan.co/v2` (`dhan.py:133`); `dhanhq` Python SDK for quote / historical calls.
- **Auth:** `DHAN_API_KEY`, `DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN` (`dhan.py:124-126`). Token lifecycle in `domains/ingest/token_manager.py::DhanTokenManager`:
  - Reads `DHAN_ACCESS_TOKEN`, `DHAN_TOKEN_EXPIRY`, `DHAN_CLIENT_ID`, `DHAN_PIN`, `DHAN_API_KEY` from environment via `DhanRuntimeConfig.from_env()` (`token_manager.py:32-36`).
  - Renewal flow uses TOTP (`pyotp`) against `https://auth.dhan.co` (`token_manager.py:38-39`); gated by env flag `DHAN_ENABLE_RENEW_TOKEN` (`token_manager.py:40`).
  - Token validity probed via `GET https://api.dhan.co/v2/profile` returning 401/403 or Dhan error codes `DH-901` / `DH-905` (`token_manager.py:101-117`).
  - Renewed token is written back to `.env` (`token_manager.py:49-72`).
- **Rate limits (per the collector docstring at `dhan.py:88-91`):**
  - 5 requests/second
  - Up to 1000 instruments per bulk request, 1 req/sec for bulk
  - 1000 API calls/day
  - Enforced by `asyncio.Semaphore(max_concurrent)` (default 5, `dhan.py:106-138`) and per-batch sleep.
- **Failure behavior:** Client init failure flips `use_api = False` (`dhan.py:166-168`); subsequent calls degrade to no-op. Token expiry triggers automatic renewal if `DHAN_ENABLE_RENEW_TOKEN=1`, otherwise logs an error. **The collector no longer synthesizes OHLCV when Dhan is unavailable** (per docstring `dhan.py:93-95`) — it fails loudly so the orchestrator can fall through to yfinance or skip the symbol.
- **Used by stage(s):** `ingest` (fallback OHLC), `execute` (live adapter — currently dry-run only).

## Source: yfinance (last-resort OHLC)

- **Module:** `src/ai_trading_system/domains/ingest/providers/yfinance.py` (`YFinanceCollector`)
- **Role:** last-resort fallback (OHLCV when both NSE and Dhan fail or are unavailable in a given environment)
- **Endpoint or input:** `yfinance.download(...)` against Yahoo Finance. NSE symbols are translated to `<SYMBOL>.NS` (`yfinance.py:47`).
- **Auth:** None.
- **Rate limits:** None enforced by Yahoo; collector self-throttles with `delay_between_batches` (default 1.0s, `yfinance.py:29`) between batches of `batch_size` symbols (default 100). Batch download uses `threads=True, auto_adjust=True` (`yfinance.py:50-56`).
- **Failure behavior:** Exceptions in any batch are caught and logged at warning level; the batch returns empty and the loop continues (`yfinance.py:89-91`, `151-153`). Symbol list is read from local `data/masterdata.db` SQLite (`yfinance.py:35-42`) — note this hard-coded path bypasses `platform/db/paths.py`.
- **Used by stage(s):** `ingest` (last-resort).

## Source: Screener.in (fundamentals enrichment)

- **Module:** `src/ai_trading_system/domains/fundamentals/import_screener.py`
- **Role:** enrichment (fundamental scores and trend deltas, optional stage)
- **Endpoint or input:** **No live API call.** The importer reads a manually-exported CSV from `--file` (`import_screener.py:72, 101-108`). The operator is expected to download the Screener export and feed it in via the CLI `python -m ai_trading_system.domains.fundamentals.import_screener --file ... --snapshot-date YYYY-MM-DD`.
- **Auth:** None (no automated Screener login in the operational ingest path).
- **Rate limits:** N/A — file import.
- **Failure behavior:** Malformed CSV raises from `pd.read_csv`. The `fundamentals` pipeline stage is skipped entirely if credentials/inputs are missing (per truth map §3).
- **Used by stage(s):** `fundamentals` (optional).

## Source: NSE corporate actions / market_intel (catalysts)

- **Module:** trading-system side: `src/ai_trading_system/integrations/market_intel_client.py`; the catalyst universe selector lives in `src/ai_trading_system/domains/catalysts/collector.py` (which itself does **not** scrape NSE — it only filters ranked/watchlist/breakout/trend frames to choose which symbols to enrich).
- **Role:** catalyst (corporate actions, news events, materiality enrichment for the `events` stage)
- **Endpoint or input:** Read-only DuckDB at `data/market_intel.duckdb` (override via env `AI_TRADING_MARKET_INTEL_DB`, `market_intel_client.py:32-48`). Writes are owned by the separately-running `market_intel` collector process; this side only reads.
- **Auth:** None (file access). Requires the `market_intel` Python package to be installed and the DB to exist (`market_intel_client.py:73-87`).
- **Rate limits:** N/A — local DB read. Cached `EventQueryService` is process-wide (`market_intel_client.py:35-37, 62-72`).
- **Failure behavior:**
  - Missing package → `ImportError` only when the service is first requested (lazy import, `market_intel_client.py:73-81`).
  - Missing DB file → `FileNotFoundError` with message "start the collector before expecting event snapshots" (`market_intel_client.py:83-87`).
  - Catalyst evidence CSVs in `domains/catalysts/collector.py::load_evidence_csvs` are best-effort: missing files, empty files, and `EmptyDataError` are silently skipped (`collector.py:50-57`).
- **Used by stage(s):** `events`, `insight`, `narrative` (downstream consumers of enriched event packets).

---

## Where each source lands

| Source | Primary table / file |
|---|---|
| NSE bhavcopy | `data/ohlcv.duckdb::ohlcv` |
| NSE delivery | `data/ohlcv.duckdb::_delivery`, `data/feature_store/delivery/NSE/data_*.parquet` |
| Dhan | `data/ohlcv.duckdb::ohlcv` (fallback rows) |
| yfinance | `data/ohlcv.duckdb::ohlcv` (last-resort rows) |
| Screener.in | `data/fundamentals/screener_financials.db` for Excel-sync ingestion; mirrored into `data/fundamentals.duckdb` for analytical readmodels such as `company_growth_features`, `company_insight_tags`, `sector_earnings_leadership`, and `universe_valuation_daily` |
| market_intel | `data/market_intel.duckdb` (read-only from trading system) |

For storage internals see `docs/architecture/storage_and_lineage.md`. For env-var reference see `docs/reference/environment_variables.md`.
