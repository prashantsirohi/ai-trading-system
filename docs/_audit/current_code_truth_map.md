# Current Code Truth Map

- **Purpose:** Snapshot of what the code actually does today, used as source of truth for rewriting docs. Read code, not docs.
- **Audience:** Doc authors (human and agent) writing Phases 1–9.
- **Last verified:** 2026-05-16
- **Source of truth:** Direct read of `src/ai_trading_system/`, `pyproject.toml`, `scripts/`, `tools/`, root-level legacy modules.
- **Status:** Phase 0 deliverable. Treat each entry as a load-bearing claim — if it turns out to be wrong, fix it here before writing user-facing docs that rely on it.

> **Verification rule.** Every fact below cites a file or module path. Before any new doc states "X exists" or "X does Y", re-grep the cited path. Code moves; this snapshot does not.

---


> **Note for `scripts/check_docs.py`:** This file intentionally lists stale terms (the whole point of an audit). Forbidden-term checks below this banner are not enforced — the report itself is the documentation of the stale claims being retired.

## 1. Package layout

### `src/ai_trading_system/`

| Subpackage | Responsibility (one line) | Status |
|---|---|---|
| `analytics/` | Cross-cutting: DQ stubs, alpha factors, pattern/indicator registries (`dq/`, `alpha/`, `patterns/`, `registry/`) | Active. Distinct from root-level `analytics/`. |
| `domains/` | Domain-driven business logic (10 subpackages — see below) | Canonical layer. |
| `pipeline/` | Stage orchestration, DQ engine, contracts, SQL migrations | Canonical orchestration layer. |
| `ui/execution_api/` | FastAPI app + 14 router modules powering the React console | Active. |
| `interfaces/cli/` | Operator CLIs (healthcheck, bootstrap, repair, export) | Active. |
| `interfaces/api/` | Minimal; most API code is in `ui/execution_api/` | Mostly empty — verify. |
| `platform/` | Config (Pydantic settings), `db/paths.py`, logging, utils | Cross-cutting foundation. |
| `research/` | Backtesting, optimization, perf tracker — isolated from operational pipeline | Active, separate code path. |
| `integrations/` | Outbound integrations (e.g., `market_intel_client.py`) | Active. |

### `src/ai_trading_system/domains/` (10 subdomains)

| Domain | Key responsibility |
|---|---|
| `ingest/` | NSE bhavcopy, yfinance, Dhan providers; delivery scraper; trust & validation |
| `features/` | Indicators, feature store, sector RS, universe index, pattern features |
| `ranking/` | Composite scoring, breakout, regime detection, sector dashboard, stage classifier |
| `candidates/` | Deterministic filtering from rank outputs |
| `fundamentals/` | Screener.in importer, scoring, enrich-rank |
| `catalysts/` | Corporate action analyzer, catalyst collector |
| `events/` | Trigger collector, event packet builder, LLM router, noise filter, enrichment |
| `execution/` | Paper/Dhan adapters, autotrader, portfolio mgr, execution store, policies |
| `publish/` | Multi-channel delivery (Telegram, Google Sheets, QuantStats, PDF, local, daily gainers, watchlist digest) |
| `risk/` | Risk profile loader, guardrails |
| `strategy/` | Strategy rule packs, bounds, compiler |

### Legacy / non-package modules

| Location | Status | Notes |
|---|---|---|
| `analytics/` (root) | **Legacy, still present** | stage_gate_backtest, stage classifiers. Mostly superseded by `domains/ranking/`. Referenced by some tests. |
| `audit_rank.py` (root) | **Active script** | Standalone — reads control_plane.duckdb. |
| `scripts/` | Active | Data repair, weekly stage runner, shell helpers. |
| `tools/` | Active (ad-hoc) | Universe index builder. |
| `models/` | Unknown — needs read | Likely ML model artifacts dir. |
| `config/` | Active | YAML + JSON config (llm_brain.yaml, strategies, risk_profiles, rank_factor_weights.json, events_filters.json, research_recipes.toml). |

---

## 2. Pipeline entrypoints

### CLI commands from `pyproject.toml [project.scripts]`

| Command | Entrypoint | Purpose |
|---|---|---|
| `ai-trading-pipeline` | `pipeline.orchestrator:main` | **Full orchestrator** (all stages) |
| `ai-trading-daily` | `pipeline.daily_pipeline:main` | Legacy daily wrapper (ingest→features→rank→execute→publish only) |
| `ai-trading-publish-test` | `pipeline.publish_test:main` | Publish-only test |
| `ai-trading-execution-api` | `ui.execution_api.app:main` | FastAPI backend (default port 8090) |
| `ai-trading-healthcheck` | `interfaces.cli.healthcheck:main` | Health probe |
| `ai-trading-bootstrap-data` | `interfaces.cli.bootstrap_runtime_data:main` | Bootstrap masterdata |
| `ai-trading-repair-ingest-schema` | `interfaces.cli.repair_ingest_schema:main` | Schema repair |
| `ai-trading-daily-gainers-report` | `domains.publish.channels.daily_gainers.cli:main` | Daily gainers HTML |
| `ai-trading-research-recipe` | `research.run_recipe:main` | Research recipe runner |

### Stage sequence — `pipeline/orchestrator.py:41` `PIPELINE_ORDER`

```
ingest → features → rank → fundamentals* → candidates → events → execute → insight → narrative → publish → perf_tracker
```

`fundamentals` is optional (skipped unless explicitly enabled).

**Important:** The spec assumed 5 stages (`ingest → features → rank → execute → publish`). Real pipeline has 11. The two legacy CLI `ai-trading-daily` matches the 5-stage spec; full `ai-trading-pipeline` runs all 11.

---

## 3. Stages

Each stage entry below cites: stage wrapper, service class, key sub-modules, reads, writes, DQ, CLI flags.

### `ingest`
- Wrapper: `pipeline/stages/ingest.py`
- Service: `domains/ingest/service.py::IngestOrchestrationService`
- Sub-modules: `providers/nse.py`, `providers/dhan.py`, `providers/yfinance.py`, `trust.py`, `validation.py`, `daily_update_runner.py`, `token_manager.py`, `delivery.py`
- **Reads:** NSE bhavcopy HTTP, Dhan API, yfinance, prior `ohlcv` DuckDB table
- **Writes:** `data/ohlcv.duckdb::ohlcv` table; artifact CSV `data/pipeline_runs/<run_id>/ingest/attempt_<n>/ohlc.csv`; metadata (freshness_status, trust summary, new symbol count)
- **DQ:** Source availability, bhavcopy row validation, close-price continuity
- **CLI flags:** `--force`, `--validation-date`, risk/strategy selectors

### `features`
- Wrapper: `pipeline/stages/features.py`
- Service: `domains/features/service.py::FeaturesOrchestrationService`
- Sub-modules: `indicators.py`, `feature_store.py`, `compute_features_batch.py`, `sector_rs.py`, `universe_index.py`, `pattern_features.py`
- **Reads:** `data/ohlcv.duckdb::ohlcv`, prior feature snapshots
- **Writes:** Parquet under `data/feature_store/<symbol_id>/`; artifact CSV; snapshot metadata
- **DQ:** Feature NaN ratios, RSI/MACD outlier bounds
- **CLI flags:** `--incremental`, feature selectors

### `rank`
- Wrapper: `pipeline/stages/rank.py`
- Service: `domains/ranking/service.py::RankOrchestrationService`
- Sub-modules: `ranker.py`, `factors.py`, `breakout.py`, `market_stage.py`, `stage_classifier.py`, `sector_dashboard.py`, `screener.py`, `input_loader.py`, `patterns/`
- **Reads:** Feature store Parquet, prior rank outputs
- **Writes:** `ranked_signals.csv`, `breakout_signals.csv`, `pattern_signals.csv`, `stock_scan_output.csv`, `sector_dashboard.csv` (all under attempt dir); dashboard JSON payload in stage metadata
- **ML overlay:** Optional LightGBM via `model_registry` table (`--ml-mode none|shadow|production`)
- **DQ:** Row count validation, score distribution checks

### `fundamentals` (optional)
- Wrapper: `pipeline/stages/fundamentals.py`
- Service: `domains/fundamentals/service.py::FundamentalsOrchestrationService`
- Sub-modules: `import_screener.py`, `scoring.py`, `enrich_rank.py`
- **Reads:** Rank artifacts, Screener.in API
- **Writes:** `fundamental_scores.csv`, `fundamental_summary.csv`
- **Skips:** If Screener credentials missing

### `candidates`
- Wrapper: `pipeline/stages/candidates.py`
- Builder: `domains/candidates/builder.py::ExecutionCandidateBuilder`
- **Reads:** Rank artifacts, risk profiles
- **Writes:** `candidates.json` with entry/exit logic
- **DQ:** Candidate count bounds, duplicate-symbol checks

### `events`
- Wrapper: `pipeline/stages/events.py`
- Service: `domains/events/service.py::EventsOrchestrationService`
- Sub-modules: `trigger_collector.py`, `event_packet_builder.py`, `event_llm_router.py`, `noise_filter.py`, `enrichment_service.py`
- **Reads:** Rank artifacts, NSE corporate actions API, market_intel
- **Writes:** `event_packet.json`, `event_enriched_rank.csv`

### `execute`
- Wrapper: `pipeline/stages/execute.py`
- Service: `domains/execution/service.py::ExecutionService`
- Sub-modules: `autotrader.py`, `adapters/paper.py`, `adapters/dhan.py`, `store.py`, `portfolio.py`, `policies.py`, `models.py`
- **Reads:** Candidates, prior execution store, portfolio state, risk profiles
- **Writes:** `trade_actions.csv`, `executed_orders.csv`, `fills.csv`; DuckDB tables `execution_order`, `execution_fill` **in `data/control_plane.duckdb`** (no separate `execution.duckdb` found)
- **Paper vs live:** Adapter selection; default paper. Live Dhan adapter requires full credentials.
- **DQ:** Quantity bounds, margin checks, position limit validation

### `insight`
- Wrapper: `pipeline/stages/insight.py`
- Logic: `domains/events/analyst_brief_builder.py`
- **Writes:** `market_insight.json`

### `narrative`
- Wrapper: `pipeline/stages/narrative.py`
- Logic: `domains/events/event_llm_router.py` + LLM client; config from `config/llm_brain.yaml` (override `LLM_BRAIN_CONFIG`)
- **Reads:** Insight packet
- **Writes:** `market_report.json` (LLM-generated narrative)

### `publish`
- Wrapper: `pipeline/stages/publish.py`
- Service: `domains/publish/delivery_manager.py::PublisherDeliveryManager`
- Channels (under `domains/publish/channels/`): `google_sheets.py`, `google_sheets_manager.py`, `telegram.py`, `quantstats.py`, `weekly_pdf/`, `daily_gainers/`, `watchlist_digest.py`
- **Writes:** External (Google Sheets, Telegram, PDF) + local `publish_summary.json`
- **Channel roles:** publish_of_record (blocking) / publish_auxiliary (blocking) / publish_optional (non-blocking) / informational (blocking) / diagnostic (non-blocking)

### `perf_tracker`
- Wrapper: `pipeline/stages/perf_tracker.py`
- Logic: `research/perf_tracker/forward_returns.py`, `schema.py`, `digest.py`
- **Reads:** Rank artifacts, OHLCV history
- **Writes:** `rank_cohort_performance` table in research-domain DuckDB (`data/research.duckdb` or `data/research_ohlcv.duckdb` — needs verification)

---

## 4. Storage

### DuckDB files (confirmed via grep for `.duckdb` + `duckdb.connect`)

| Path | Purpose | Owner |
|---|---|---|
| `data/ohlcv.duckdb` | Operational OHLCV | Ingest stage; `pipeline/migrations/` |
| `data/control_plane.duckdb` | Pipeline governance (runs, stages, artifacts, DQ rules, model registry, pattern cache, watchlist/event tables) | Orchestrator + multiple readers |
| `data/execution.duckdb` | `execution_order`, `execution_fill`, `execution_trade_note`, `execution_position_stop`, drawdown snapshots | Execute stage via `domains/execution/store.py:29` |
| `data/research.duckdb` | Perf tracker — `rank_cohort_performance` table (DDL in `research/perf_tracker/schema.py`) | perf_tracker stage + weekly digest |
| `data/research_ohlcv.duckdb` | Research-domain OHLCV isolation | `research/sync_operational_data.py`; consumed when `DATA_DOMAIN=research` |
| `data/market_intel.duckdb` | Corporate-action / events feed populated by the always-on `market_intel` runner | Read-only by `integrations/market_intel_client.py:32` and `interfaces/cli/healthcheck.py` |

**CORRECTION (Phase 6, 2026-05-16):** The original Phase 0 entry asserted that execution tables live in `control_plane.duckdb` and that no `data/execution.duckdb` exists. **This was wrong.** `ExecutionStore.__init__` at [`domains/execution/store.py:29`](../../src/ai_trading_system/domains/execution/store.py) defaults `db_path` to `project_root / "data" / "execution.duckdb"`, and the execute stage instantiates it without overriding. Both `data/execution.duckdb` and `data/market_intel.duckdb` were missed by the Phase 0 sweep. The table is now corrected; downstream docs (`storage_and_lineage.md`, `target_architecture.md`, `execution_domain.md`, `stages/execute.md`) reflect the real layout.

### Feature store layout

`data/feature_store/<symbol_id>/features_<start_date>_<end_date>.parquet` — columnar (RSI, MACD, Supertrend, ATR, EMA_20/50/200, VWAP, volume_ratio, swing_low_20, sector_rs, etc.)

### Pipeline runs

`data/pipeline_runs/<run_id>/<stage>/attempt_<n>/<artifact>`

### Schema source

17 SQL files under `src/ai_trading_system/pipeline/migrations/`. Key tables:
- `pipeline_run` (run lifecycle)
- `pipeline_stage_run` (per-stage attempts)
- `pipeline_artifact` (URI + content_hash registry)
- `dq_rule`, `dq_result`
- `model_registry`
- `rank_cohort_performance`
- `execution_order`, `execution_fill`
- Migration 015+ adds optimization/strategy tables

---

## 5. Data sources

| Source | Module | Role |
|---|---|---|
| NSE bhavcopy | `domains/ingest/providers/nse.py` | **Source-of-record** for NSE equities |
| Dhan API | `domains/ingest/providers/dhan.py` + `adapters/dhan.py` | OHLC fallback + **live execution** + delivery data |
| yfinance | `domains/ingest/providers/yfinance.py` | Last-resort OHLC fallback |
| Screener.in | `domains/fundamentals/import_screener.py` | Optional fundamentals enrichment |
| NSE corporate actions + market_intel | `domains/catalysts/collector.py` + `domains/events/trigger_collector.py` + `integrations/market_intel_client.py` | Catalyst/event sourcing |

> Old docs claim "Dhan-first ingest" — this is **wrong** per current code. NSE bhavcopy is primary; Dhan is fallback for price data and mandatory for live trading.

---

## 6. UI surfaces

### FastAPI backend (`ai-trading-execution-api`)
- App: `src/ai_trading_system/ui/execution_api/app.py`
- Default: localhost:8090
- CORS: `allow_origins=["*"]`
- Auth: API key header (`configured_api_key()`)

### FastAPI routes (14 routers under `ui/execution_api/routes/`)

`health`, `pipeline`, `runs`, `snapshots`, `artifacts`, `stocks`, `ranking_detail`, `fundamentals`, `insight`, `sectors`, `tasks`, `processes`, `backtest`, `perf_tracker`

(Path prefixes follow conventional `/api/<name>` per truth-map agent; exact route bodies need re-reading when writing `docs/reference/api_reference.md`.)

### React console
- `web/execution-console-v2/` (Vite + React + TypeScript)
- Consumes FastAPI `/api/*` routes
- Mock-data mode toggle for offline dev

### Streamlit
- **None found** in active code paths. Any old Streamlit references in docs are stale.

---

## 7. Publishing

Channels enumerated above in §3 (publish stage). Each channel's role (blocking/non-blocking, of-record/auxiliary/optional/informational/diagnostic) is encoded in the delivery manager — to be transcribed into `docs/reference/publish_contracts.md` from `domains/publish/delivery_manager.py` directly.

Auth:
- Google Sheets: OAuth flow (`oauth_flow.py`), token at `GOOGLE_TOKEN_PATH`
- Telegram: `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID`
- QuantStats: no external auth

---

## 8. Execution

- Paper default; Dhan live adapter conditional on full credential set
- Risk gates applied in `autotrader.py` + `policies.py` **before** adapter dispatch
- Storage: `execution_order` + `execution_fill` tables in `data/control_plane.duckdb`
- Order types currently hardcoded to MARKET + INTRADAY (per `models.py`) — verify before documenting "live trading is production-ready"

> Per spec STRICT RULES: do not describe live trading as production-ready unless code guardrails prove it. **Current code status: live Dhan adapter exists but production-readiness gates have not been verified.** New docs should mark this explicitly.

---

## 9. Configuration

### Config files (`config/`)
- `llm_brain.yaml` — LLM prompts + model selection
- `strategies/` — strategy rule packs
- `risk_profiles/` — risk gates
- `rank_factor_weights.json` — composite scoring weights
- `events_filters.json` — event materiality filters
- `research_recipes.toml` — research workflow recipes

### Pydantic settings: `platform/config/settings.py`

### Environment variables actually read (cite each)

| Env var | Read by | Purpose |
|---|---|---|
| `DHAN_API_KEY` | `domains/ingest/providers/dhan.py`, `platform/config/settings.py` | Dhan auth |
| `DHAN_CLIENT_ID` | same | Dhan client |
| `DHAN_ACCESS_TOKEN` | same | Dhan OAuth |
| `DHAN_REFRESH_TOKEN` | `domains/ingest/token_manager.py` | Token refresh |
| `DHAN_PIN` | `token_manager.py` | 2FA PIN |
| `DHAN_TOTP` | `token_manager.py`, `settings.py` | TOTP secret |
| `DHAN_TOKEN_EXPIRY` | `token_manager.py` | Cached expiry |
| `TELEGRAM_BOT_TOKEN` | `domains/publish/channels/telegram.py`, `settings.py` | Bot token |
| `TELEGRAM_CHAT_ID` | same | Destination |
| `TELEGRAM_CONNECT_TIMEOUT_SECONDS` | `telegram.py` | HTTP timeout (default 5.0) |
| `TELEGRAM_READ_TIMEOUT_SECONDS` | `telegram.py` | (default 10.0) |
| `TELEGRAM_WRITE_TIMEOUT_SECONDS` | `telegram.py` | (default 10.0) |
| `TELEGRAM_SEND_ATTEMPTS` | `telegram.py` | Retry count |
| `GOOGLE_SPREADSHEET_ID` | `google_sheets.py`, `settings.py` | Target sheet |
| `GOOGLE_SHEETS_CREDENTIALS` | `google_sheets_manager.py` | Service-account path (deprecated; OAuth flow now) |
| `GOOGLE_TOKEN_PATH` | `google_sheets_manager.py` | Cached OAuth token |
| `ALERT_TELEGRAM_MIN_SEVERITY` | `pipeline/alerts.py` | Min severity to Telegram |
| `RISK_PROFILE` | execute stage | Risk profile name |
| `LLM_BRAIN_CONFIG` | `domains/events/event_llm_router.py` | Override `config/llm_brain.yaml` |
| `OPENROUTER_KEY` / `OPENROUTER_API_KEY` | `event_llm_router.py` | LLM API |
| `DATA_DOMAIN` | `platform/db/paths.py` | "operational" / "research" |
| `ENV` | `pipeline/daily_pipeline.py` | Deployment env label |
| `MPLCONFIGDIR` | `platform/logging/` | Matplotlib cache dir |

`EXECUTION_MODE` is **inferred from Dhan credential presence**, not an explicit env var. Verify by reading execute stage before writing this in docs.

---

## 10. Research vs operational

- `research/` has its own backtesting, optimization, perf_tracker subpackages
- Data isolation via `DATA_DOMAIN=research` + `platform/db/paths.py::resolve_data_domain()` → separate `research_ohlcv.duckdb`
- Research recipes run **standalone** via `ai-trading-research-recipe`; **not wired into operational pipeline** except: ML models trained in research are loaded via `model_registry` table in rank stage's optional ML overlay
- `research/sync_operational_data.py` copies operational OHLCV to research domain

---

## Things flagged as unknown / needs verification before publishing docs

1. `data/research.duckdb` vs `data/research_ohlcv.duckdb` — which is canonical for perf_tracker writes?
2. `src/ai_trading_system/interfaces/api/` — described as "mostly empty"; verify before docs imply otherwise.
3. `models/` dir at repo root — purpose not yet read.
4. `EXECUTION_MODE` env var — exists or inferred? Re-grep execute stage.
5. Whether root-level `analytics/` is still imported by tests or only legacy.
6. Streamlit references — confirm zero usages by grepping for `streamlit`.
7. Live trading guardrails — what prevents accidental live execution if Dhan creds are set in env?
8. Old API doc lists routes but only 14 routers known by name; need full route bodies (method + path) when writing `api_reference.md`.

These items must be resolved *before* the rewritten docs ship — otherwise the new docs will inherit the same staleness problem the cleanup is meant to fix.
