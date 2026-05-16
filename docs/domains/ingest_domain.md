# Ingest Domain

- **Purpose:** Acquire OHLCV and corporate-action data into the operational store with provenance, validation, and trust gating.
- **Audience:** Developer, operator.
- **Last verified:** 2026-05-16
- **Source of truth:**
  - [`src/ai_trading_system/domains/ingest/`](../../src/ai_trading_system/domains/ingest/)
  - [`src/ai_trading_system/pipeline/stages/ingest.py`](../../src/ai_trading_system/pipeline/stages/ingest.py)

---

## Responsibility

Own the boundary between external data providers and the internal OHLCV store. Enforce **NSE bhavcopy as the source-of-record** for NSE equities. Track provenance, validate every row, quarantine bad data, and emit a trust envelope downstream stages can inspect.

## Package / module ownership

| Module | Role |
|---|---|
| `service.py::IngestOrchestrationService` | Top-level orchestration. Bhavcopy gate, stale-quarantine sweep, fingerprint. |
| `daily_update_runner.py::run` | Incremental update driver. NSE-primary / Dhan-primary branches. |
| `providers/nse.py` | NSE bhavcopy HTTP scraper. **Source of record.** |
| `providers/dhan.py` | Dhan API OHLC poller. Fallback only for price data. |
| `providers/yfinance.py` | yfinance last-resort fallback. |
| `trust.py` | Freshness checking, trust summary. |
| `validation.py` | Per-row bhavcopy validation. |
| `delivery.py` | NSE delivery-data scraper (sidecar). |
| `token_manager.py` | Dhan OAuth refresh, TOTP-driven. |

## Public contracts

- **Stage artifact:** `data/pipeline_runs/<run_id>/ingest/attempt_<n>/ohlc.csv` + `ingest_summary.json` metadata.
- **DuckDB write:** `_catalog` and related tables in `data/ohlcv.duckdb`.
- **Trust envelope:** consumed by every downstream stage via the StageContext. See [`docs/architecture/data_trust_and_dq.md`](../architecture/data_trust_and_dq.md).
- **Stage details:** [`docs/stages/ingest.md`](../stages/ingest.md).

## Storage ownership

- `data/ohlcv.duckdb` — sole writer.
- `data/pipeline_runs/<run_id>/ingest/attempt_<n>/*` — stage artifacts.
- Quarantine state — recorded in control_plane DuckDB (verify exact table when writing [`reference/database_schema.md`](../reference/database_schema.md)).

## Dependencies

- External: NSE bhavcopy HTTP endpoints, Dhan REST API, yfinance.
- Internal: `platform/db/paths.py` for DuckDB locations; `platform/config/settings.py` for credentials.
- Env vars: `DHAN_API_KEY`, `DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN`, `DHAN_REFRESH_TOKEN`, `DHAN_PIN`, `DHAN_TOTP`, `DHAN_TOKEN_EXPIRY` — see [`reference/environment_variables.md`](../reference/environment_variables.md).

## Extension points

- New provider: subclass the provider interface in `providers/` and register in `daily_update_runner`.
- New validation rule: add to `validation.py` and ensure a matching DQ rule exists in `pipeline/dq/`.
- New corporate-action source: extend `delivery.py` or add a sibling scraper.

## Known gaps

- yfinance path is best-effort; do not rely on it for production.
- Dhan rate limits (5 req/sec, 1000/day per truth-map agent — verify in provider code) are not centrally enforced.
- Delivery sidecar coverage varies by symbol; check trust envelope rather than assuming completeness.

## See also

- [`docs/stages/ingest.md`](../stages/ingest.md)
- [`docs/reference/data_sources.md`](../reference/data_sources.md)
