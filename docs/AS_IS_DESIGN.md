# AS-IS Design Document — AI Trading System
**Revision 2** — corrected to reflect `src/ai_trading_system/` as the canonical post-refactor package.

Source of truth: repository at commit `4dfc618` on branch `claude/xenodochial-payne-53735e`.
Evidence drawn from direct inspection of `src/ai_trading_system/` (276 files, 42,447 LOC), `run/`, `docs/architecture/`, `docs/refactor/`, `AGENTS.md`, and live import traces.

> **Correction notice:** Revision 1 of this document incorrectly described `src/ai_trading_system/` as a "migration in progress / not yet the runtime". That was wrong. `src/ai_trading_system/` is the **completed, canonical post-refactor package** containing all domain and orchestration logic. `run/` and the top-level packages are the compatibility / legacy layer sitting in front of it.

---

## A. Project Purpose

**Business problem:** A local-first, staged NSE (Indian equities) trading workflow that turns raw end-of-day market data into ranked trade candidates, paper executions, and multi-channel operator reports. Explicitly **not** a live trading engine — live broker adapters exist under `execution/adapters/` but the orchestrated stage is paper-only.

**Main workflows supported today:**

1. **Operational daily pipeline** — `ingest → features → rank → execute → publish`, orchestrated by `python -m run.orchestrator` (which is a shim; the real orchestrator is `src/ai_trading_system/pipeline/orchestrator.py`).
2. **Research flow** — separate `data_domain="research"` path for dataset prep, LightGBM training, backtesting, and shadow-monitor evaluation; shares domain code but writes to a shadow data layout.
3. **Operator surfaces** — Streamlit dashboards (research/ML), NiceGUI Python console, FastAPI + React execution console.
4. **Repair tooling** — collector-side repair/reset/quarantine utilities for recovering from schema drift or bad ingest.

---

## B. High-Level Architecture

### Two-tier layout

The repository has a **completed two-tier layout**:

| Tier | Location | Role |
|---|---|---|
| **Canonical (src)** | `src/ai_trading_system/` | 276 files · 42,447 LOC · all domain logic, orchestration, platform utilities |
| **Compatibility / legacy** | `run/`, `analytics/`, `collectors/`, `features/`, `execution/`, `publishers/`, `channel/` | Shims, wrappers, or legacy operational code still in maintenance mode |

**Key fact:** `run/orchestrator.py` imports `from ai_trading_system.pipeline.orchestrator import *` and delegates entirely to the src orchestrator. `run/stages/*.py` each do `from ai_trading_system.pipeline.stages.<stage> import *`. They are thin compatibility shims, not the implementation.

### Logical layers (all inside `src/ai_trading_system/`)

| Layer | Purpose | Canonical location |
|---|---|---|
| **Pipeline orchestration** | Stage sequencing, run records, DQ gates, alerts, retries | `pipeline/orchestrator.py`, `pipeline/stages/`, `pipeline/contracts.py`, `pipeline/registry.py`, `pipeline/dq/` |
| **Domain services — Ingest** | NSE bhavcopy, yfinance fallback, delivery, masterdata, trust lineage, quarantine | `domains/ingest/` (10 modules incl. `service.py`, `providers/`, `trust.py`, `repair.py`) |
| **Domain services — Features** | Indicator computation, sector RS, feature store persistence | `domains/features/` (10 modules incl. `feature_store.py`, `indicators.py`, `sector_rs.py`) |
| **Domain services — Ranking** | Factor scores, composite, eligibility, regime, breakout scan, patterns, screener | `domains/ranking/` (18 modules incl. `ranker.py`, `breakout.py`, `eligibility.py`, `patterns/`) |
| **Domain services — Execution** | Paper autotrader, portfolio, entry/exit policies, trade store | `domains/execution/` (11 modules incl. `autotrader.py`, `entry_policy.py`, `portfolio.py`) |
| **Domain services — Publish** | Telegram summary, channel delivery, dashboard payload, portfolio analytics | `domains/publish/` (9 modules incl. `telegram_summary_builder.py`, `delivery_manager.py`) |
| **Platform utilities** | Logging, DB path resolution, config, env | `platform/` (`logging/`, `db/paths.py`, `config/`) |
| **Interfaces** | API schemas, FastAPI routers | `interfaces/` |
| **Research** | LightGBM training, backtesting, evaluation | `research/` |

### Interaction pattern

```
python -m run.orchestrator          ← compatibility shim
        │ (re-exports)
        ▼
src/ai_trading_system/pipeline/orchestrator.py   ← REAL orchestrator
        │ per stage
        ▼
src/ai_trading_system/pipeline/stages/<stage>.py  ← thin stage wrappers
        │ delegates to
        ▼
src/ai_trading_system/domains/<stage>/service.py  ← business logic
        │ uses
        ▼
src/ai_trading_system/domains/<stage>/<primitives>   ← computation modules
```

All stages write versioned artifacts under `data/pipeline_runs/<run_id>/<stage>/attempt_<n>/` and record run state in `data/control_plane.duckdb`. UIs read artifacts + control plane; they do **not** invoke the pipeline.

---

## C. Repository Structure Mapping

| Folder | Responsibility (observed) | Status |
|---|---|---|
| `main.py` | **Deprecated shim.** Exits with code 2 pointing to `run.orchestrator`. | Deleted in Phase 1a |
| `run/` | Orchestrator **shim** (`run/orchestrator.py` → re-exports `src/ai_trading_system/pipeline/orchestrator`), per-stage shims (`run/stages/*` → re-exports pipeline stages), publisher, preflight, alerts, `daily_pipeline` wrapper | **Compatibility shims** — real logic is in `src/` |
| `services/` | Directory structure exists but **contains no Python implementation files** — all subdirectory `__pycache__` only | **Empty** — placeholder for a future step or residue |
| `collectors/` | NSE bhavcopy, yfinance, Dhan, delivery, masterdata, repair tools | **Legacy operational** — still used at runtime; canonical replacement is `src/ai_trading_system/domains/ingest/` |
| `features/` | Indicator math, feature store persistence, sector RS | **Legacy operational** — canonical replacement is `src/ai_trading_system/domains/features/` |
| `analytics/` | Ranker, DQ engine, registry, trust, risk, ML/backtester, patterns, screener, alpha, regime | **Legacy operational + research** — canonical replacement is `src/ai_trading_system/domains/ranking/` + `research/` |
| `execution/` | Paper autotrader, portfolio, policies, models, store, broker adapters | **Legacy operational** — canonical replacement is `src/ai_trading_system/domains/execution/` |
| `publishers/` | Telegram, Google Sheets, dashboard HTML, QuantStats | **Legacy operational** — canonical replacement is `src/ai_trading_system/domains/publish/` |
| `channel/` | Side outputs: breakout_scan, stock_scan, sector_dashboard, portfolio_analyzer, telegram_reporter, google_sheets_manager | **Mixed** — some operational sidecars, some legacy; hardcoded operational paths |
| `research/` | `train_pipeline.py`, `backtest_*`, `prepare_training_dataset.py`, `run_lightgbm_workflow.py`, `eval_*` | **Active research path**, imports from `src/ai_trading_system/research/` |
| `core/` | `contracts.py`, `paths.py`, `logging.py`, `env.py`, `runtime_config.py`, `symbol_master.py`, `trust_confidence.py` | **Active runtime foundation** used by both legacy and src layers |
| `utils/` | `data_config.py`, `data_domains.py`, `env.py`, `compact_features.py`, `pyarrow_utils.py` | **Active but secondary** — AGENTS.md mandates preferring `core.*` over `utils.*` |
| `config/` | `settings.py` (pydantic, legacy), `rank_factor_weights.json`, `research_recipes.toml` | Partial — pydantic model is non-canonical; JSON/TOML live files are used |
| `data/` | DuckDB stores, SQLite masterdata, generated run/feature/report trees | **Active runtime state** |
| `sql/` | 9 migrations for control plane | **Active** |
| `ui/` | 4 distinct surfaces: `research/` (Streamlit), `ml/` (Streamlit), `execution/` (NiceGUI), `execution_api/` (FastAPI) | **Active** |
| `web/execution-console/` | React frontend for FastAPI backend | **Active**, separate Vite dev server |
| `src/ai_trading_system/` | **Canonical post-refactor package** — 276 files, 42,447 LOC — all domain logic, orchestration, platform utilities, research | **CANONICAL. This is the runtime.** `run/` is a shim pointing here. |
| `scripts/` | `bootstrap_runtime_data.py`, `repair_ingest_schema.py` | **Active** ops helpers |
| `tests/` | 76+ files across smoke/, integration/, ingest/, features/, rank/, execute/, publish/, fixtures/ | **Active** |
| `docs/` | `architecture/` (4 files), `operations/`, `refactor/` (5 files), `reference/`, `archive/` | **Active** |

**Mixed / unclear ownership hotspots:**
- `analytics/` mixes operational ranker with research ML / backtester code — no hard boundary between legacy and canonical.
- `channel/` is flagged as both "current operational" and "legacy fallback" in `module-map.md:125`; several scripts hardcode operational paths.
- Top-level packages (`features/`, `collectors/`, `analytics/`) contain live operational implementations that have canonical equivalents in `src/ai_trading_system/domains/*` — both trees are simultaneously active during the CLI-cutover phase.

---

## D. Data Architecture

**Persistent stores (all local):**

| Store | Path | Contents | Written by | Read by |
|---|---|---|---|---|
| Operational market store | `data/ohlcv.duckdb` | OHLCV, delivery rows, trust catalog (`_catalog`, `_catalog_history`, `_catalog_provenance`), quarantine (`_catalog_quarantine`), feature snapshots (`_snapshots`) | Ingest stage (`domains/ingest/service.py` via `domains/ingest/providers/`) | Features, rank, UI |
| Control plane | `data/control_plane.duckdb` | Pipeline runs, stage attempts, artifacts, DQ results, publish logs, repair runs, model governance, operator tasks | Orchestrator (`pipeline/orchestrator.py`), stages, alerts | Orchestrator, UIs, APIs |
| Execution store | `data/execution.duckdb` | Paper orders, fills, positions, trade notes | Execute stage (`domains/execution/service.py`) | Operator console, reports |
| Reference store | `data/masterdata.db` (SQLite, ~1.5 MB) | Symbol metadata, sector mapping, holiday calendar | `domains/ingest/masterdata.py`, bootstrap script | Any stage needing symbol/sector lookup |
| Feature store | `data/feature_store/` (parquet) | Per-symbol technical features (including Stage 2 after Sprint 1), sector RS | Features stage (`domains/features/feature_store.py`) | Rank stage, research |
| Pipeline artifacts | `data/pipeline_runs/<run_id>/<stage>/attempt_<n>/` | Stage outputs (JSON summaries, payloads) | Each stage | UIs, next-stage inputs |
| Research shadow | `data/research/{research_ohlcv.duckdb, feature_store/, pipeline_runs/}` | Same shape, separate domain | Research flows when `data_domain="research"` | Research UIs / training |

**Config / reference files (non-DB):**
- `config/rank_factor_weights.json` — rank factor weights (live-used by `domains/ranking/composite.py`).
- `config/research_recipes.toml` — research recipes.
- `config/settings.py` — legacy pydantic model, non-canonical.
- `.env` loaded via `core/env.py` `load_project_env()`.

**Data flow (happy path — canonical layer):**

```
NSE bhavcopy (+ yfinance fallback)
        │
        ▼
domains/ingest/providers/nse.py + daily_update_runner
        │
        ▼
ohlcv.duckdb (_catalog, _catalog_provenance, _catalog_quarantine)
        │
        ▼
domains/features/feature_store.py
  ├── compute_sma / compute_rsi / compute_stage2 / ...
  └── add_stage2_features() [indicators.py — Stage 2 uptrend score]
        │
        ▼
data/feature_store/<feature_type>/<exchange>/<symbol>.parquet
        │
        ▼
domains/ranking/ranker.py (StockRanker.rank_all())
  ├── factors (RS, volume, trend, proximity, delivery, sector)
  ├── compute_factor_scores → composite_score
  ├── stage2_score_bonus (+0-5 pts if stage2 features present)
  ├── apply_rank_eligibility (optional stage2_gate in stage2_breakout mode)
  ├── penalty, confidence, stability
  └── rank modes: default | momentum | breakout | defensive | watchlist | stage2_breakout
        │
        ├── domains/ranking/breakout.py (compute_breakout_v2_scores)
        │       Stage 2 score-driven Tier A/B/C/D (with legacy fallback)
        │
        └── domains/ranking/patterns/ (CwH, round_bottom, double_bottom, flag, HTF, ascending_triangle, vcp, flat_base)
                Bug-fixed: O(n²) flag detector, round-bottom index fix
                Stage 2 integration: bonus scoring + pre-filter gate
        │
        ▼
data/pipeline_runs/<run_id>/rank/dashboard_payload.json
        │
        ▼
domains/execution/ (candidate_builder → entry_policy → exit_policy)
        │
        ▼
execution.duckdb (paper positions)
        │
        ▼
domains/publish/ (telegram_summary_builder, delivery_manager)
        │
        ▼
publishers/{telegram, google_sheets, dashboard, quantstats}
```

**Sources of truth (by concept):**
- Symbol universe + sector: `data/masterdata.db`.
- Market data + trust/quarantine lineage: `data/ohlcv.duckdb`.
- Run/attempt state: `data/control_plane.duckdb`.
- Trade state: `data/execution.duckdb`.
- Rank factor weights: `config/rank_factor_weights.json`.
- All domain logic: `src/ai_trading_system/domains/`.
- Pipeline orchestration: `src/ai_trading_system/pipeline/`.

---

## E. Pipeline / Execution Flow

**Canonical entry:** `python -m run.orchestrator` (shim) → `src/ai_trading_system/pipeline/orchestrator.py` (real). Ordered stages: `ingest → features → rank → execute → publish`. Wrapper `run/daily_pipeline.py` delegates to the orchestrator shim.

**Orchestrator responsibilities** (`src/ai_trading_system/pipeline/orchestrator.py`):
1. Create or resume a run record in `data/control_plane.duckdb`.
2. Run preflight.
3. Instantiate `pipeline/registry.py` `RegistryStore`, `pipeline/dq/` `DataQualityEngine`, alerts.
4. For each stage: create `StageContext`, invoke `Stage.run()`, capture `StageResult`, persist artifacts, evaluate DQ gates, emit alerts.
5. Handle `DataQualityCriticalError` and `PublishStageError` (`pipeline/contracts.py`).

**Stage wrappers** (`src/ai_trading_system/pipeline/stages/` + shims in `run/stages/`): thin. Each calls the matching `domains/<stage>/service.py`, which is where real business logic lives.

**Stage-level flow:**

| Stage | Pipeline stage | Domain service | Key libraries | Key artifact |
|---|---|---|---|---|
| ingest | `pipeline/stages/ingest.py` | `domains/ingest/service.py` | `domains/ingest/providers/nse.py`, `yfinance.py`, `delivery.py`, `trust.py` | `ingest_summary.json` |
| features | `pipeline/stages/features.py` | `domains/features/service.py` | `domains/features/feature_store.py`, `indicators.py`, `sector_rs.py` | `feature_snapshot.json` |
| rank | `pipeline/stages/rank.py` | `domains/ranking/service.py` | `domains/ranking/ranker.py`, `breakout.py`, `patterns/`, `screener.py`, `regime_detector.py` | `dashboard_payload.json` |
| execute | `pipeline/stages/execute.py` | `domains/execution/service.py` | `domains/execution/autotrader.py`, `candidate_builder.py`, `entry_policy.py`, `exit_policy.py` | paper trades in `execution.duckdb` |
| publish | `pipeline/stages/publish.py` | `domains/publish/service.py` (or `delivery_manager.py`) | `domains/publish/telegram_summary_builder.py`, `publish_payloads.py`, `channels/` | publish logs + external deliveries |

**Stage gates (observed):**
- Rank blocks on untrusted trust status unless `allow_untrusted_rank=True`; blocks on critical DQ failures.
- Publish failures do not fail the run — recorded as `completed_with_publish_errors`.

**Scheduled vs manual:** No cron, systemd, or Airflow in-repo. All runs are manual CLI or invoked from the operator UIs.

---

## F. Domain Logic Mapping

### Ingestion (`domains/ingest/`)
- **Primary:** `domains/ingest/service.py` → `IngestOrchestrationService` coordinates providers.
- **Providers:** `domains/ingest/providers/nse.py` (NSE bhavcopy, primary), `providers/yfinance.py` (fallback).
- **Delivery:** `domains/ingest/delivery.py`.
- **Trust lineage:** `domains/ingest/trust.py` — writes `_catalog_provenance`, manages `_catalog_quarantine`.
- **Masterdata:** `domains/ingest/masterdata.py` → SQLite at `data/masterdata.db`.
- **Repair:** `domains/ingest/repair.py`, `collectors/repair_ohlcv_window.py`, `scripts/repair_ingest_schema.py`.
- **Repository:** `domains/ingest/repository.py` — DuckDB OHLCV read/write.

### Masterdata
- Store: `data/masterdata.db` (SQLite), managed by `domains/ingest/masterdata.py`.
- Bootstrap: `scripts/bootstrap_runtime_data.py`.
- Consumers: symbol universe, sector assignments, holiday calendar.

### Feature engineering (`domains/features/`)
- **Indicator library:** `domains/features/indicators.py` — SMA, EMA, RSI, MACD, ATR, Supertrend, `add_multi_timeframe_returns()`, `add_stage2_features()` (Stage 2 uptrend scoring, Sprint 1).
- **Feature store:** `domains/features/feature_store.py` — `FeatureStore` class with `compute_and_store_features()` loop; per-feature-type methods (`compute_sma`, `compute_rsi`, `compute_stage2`, …). Persists parquet under `data/feature_store/`.
- **Sector RS:** `domains/features/sector_rs.py`.
- **Pattern features:** `domains/features/pattern_features.py`.

**Stage 2 uptrend scoring** (added Sprint 1, April 2026):
- `add_stage2_features(df)` — 9-condition Weinstein scoring (max 100 pts); produces `stage2_score`, `is_stage2_uptrend`, `stage2_label` (strong_stage2 / stage2 / stage1_to_stage2 / non_stage2), `stage2_fail_reason`, `sma_150`, `sma200_slope_20d_pct`.
- `FeatureStore.compute_stage2()` — DuckDB SQL pulls SMA 150/200 + 52w high; calls `add_stage2_features()`.
- `STAGE2_FEATURE_COLUMNS` constant — defines the 9 feature columns persisted to parquet.

### Ranking (`domains/ranking/`)
- **Core engine:** `domains/ranking/ranker.py` — `StockRanker.rank_all()` composing factors, Stage 2 enrichment, regime overlay, trust gate.
- **Factors:** `domains/ranking/factors.py` — `apply_relative_strength`, `apply_volume_intensity`, `apply_trend_persistence`, `apply_proximity_highs`, `apply_delivery`, `apply_sector_strength`.
- **Composite scoring:** `domains/ranking/composite.py` — weighted factor score assembly using `config/rank_factor_weights.json`.
- **Eligibility:** `domains/ranking/eligibility.py` — `apply_rank_eligibility()` with `stage2_gate_enabled` param (Sprint 1).
- **Contracts:** `domains/ranking/contracts.py` — `RANK_MODES` (6 modes incl. `stage2_breakout`), `RANKED_SIGNAL_COLUMNS` (46 cols incl. 7 Stage 2 cols), `PRIMARY_FACTORS`, `DEFAULT_FACTOR_WEIGHTS`.
- **Breakout:** `domains/ranking/breakout.py` — `compute_breakout_v2_scores()` with Stage 2 score-driven Tier A/B/C/D tiering (legacy 3-condition fallback preserved). Sprint 2 adds `stage2_score`, `is_stage2_uptrend`, `stage2_gate_passed` enrichment columns.
- **Patterns:** `domains/ranking/patterns/` — `detectors.py` (CwH, round bottom, double bottom, flag, HTF, ascending_triangle, vcp, flat_base; Sprint 2 bug-fixes + 3 new bullish patterns), `evaluation.py`, `contracts.py`, `data.py`, `signal.py`.
- **Regime:** `domains/ranking/regime_detector.py`.
- **Screener:** `domains/ranking/screener.py`.
- **Sector dashboard:** `domains/ranking/sector_dashboard.py`.

### Execution (`domains/execution/`)
- **Service:** `domains/execution/service.py`.
- **Candidate builder:** `domains/execution/candidate_builder.py`.
- **Entry / exit policy:** `domains/execution/entry_policy.py`, `exit_policy.py`.
- **Autotrader:** `domains/execution/autotrader.py` — paper execution engine.
- **Portfolio:** `domains/execution/portfolio.py`, `domains/execution/store.py`.
- **Adapters:** `domains/execution/adapters/paper.py` (active), `adapters/dhan.py` (present, not orchestrated).

### Reporting / Publish (`domains/publish/`)
- **Telegram:** `domains/publish/telegram_summary_builder.py`.
- **Payload assembly:** `domains/publish/publish_payloads.py`.
- **Delivery:** `domains/publish/delivery_manager.py` — retry-safe; soft-fail model.
- **Dashboard:** `domains/publish/dashboard.py`.
- **Portfolio analytics:** `domains/publish/portfolio_analyzer.py`.
- **Channels:** `domains/publish/channels/` — per-channel delivery adapters.

### Operator UIs
- `ui/research/app.py` — canonical analyst Streamlit.
- `ui/ml/app.py` — ML workbench Streamlit.
- `ui/execution/app.py` — NiceGUI operator console.
- `ui/execution_api/app.py` — FastAPI JSON + SSE backend; models in `src/ai_trading_system/interfaces/`.
- `web/execution-console/` — React frontend (separate Vite dev server).

---

## G. Current Strengths

1. **`src/ai_trading_system/` is complete and canonical.** 276 files · 42,447 LOC. All five pipeline domains are fully implemented: ingest, features, ranking, execution, publish. The migration from top-level to `src/` is done at the logic level.
2. **Clear staged contract.** `ingest → features → rank → execute → publish` is consistent across `pipeline/orchestrator.py`, `pipeline/stages/`, and `domains/`. Stage wrappers in `run/` are thin shims.
3. **Control-plane-first governance.** All runs, attempts, artifacts, DQ results, publish logs, and operator tasks are recorded in `data/control_plane.duckdb` via 9 explicit SQL migrations.
4. **Idempotent / retry-safe publish.** `domains/publish/delivery_manager.py` + the soft-fail model (`completed_with_publish_errors`) keeps publish errors from poisoning the run.
5. **Trust lineage and quarantine are first-class.** `_catalog`, `_catalog_history`, `_catalog_provenance`, `_catalog_quarantine` in the OHLCV DB give a traceable data-quality story.
6. **Operational/research domain split.** Parallel path resolution (`ensure_domain_layout(domain=…)`) keeps research experiments from corrupting operational state.
7. **Stage 2 uptrend scoring is a clean extension.** The Weinstein-methodology Stage 2 system (Sprint 1) adds 200 lines across 5 files with zero breaking changes: additive columns, backward-compatible fallback in breakout.py, optional gate controlled by `rank_mode`.
8. **Test footprint is real.** 76+ test files including smoke tests for the orchestrator and execution API.
9. **Explicit shim strategy.** `run/` compatibility wrappers allow the CLI entry point and any external tooling to remain unchanged while the canonical implementation lives entirely in `src/`.

---

## H. Current Design Problems

1. **CLI cutover is the only remaining migration step.** The domain logic is fully in `src/ai_trading_system/`. What remains is: (a) collapsing `run/` shims into a single `__main__.py` that imports from `pipeline/`, and (b) removing the top-level legacy packages once all callers in `channel/`, `research/`, and UI read-models are updated. Until that happens, both trees are simultaneously active — a maintenance burden.

2. **Top-level legacy packages (`analytics/`, `collectors/`, `features/`, `execution/`, `publishers/`) still contain live operational code.** They are not shims — they contain real implementations. Whether a given caller uses the canonical `src/` version or the legacy top-level version depends on import paths, not on contract enforcement. Drift risk is real.

3. **`channel/` has split personality.** `module-map.md:125-143` labels it both "current operational" and "legacy fallback". `stock_scan.py` and `sector_dashboard.py` hardcode operational paths, breaking the research-domain abstraction. `channel/` should eventually collapse into `domains/ranking/` sidecars.

4. **`services/` directory is empty.** The `services/` directory exists with subdirectories (`ingest/`, `features/`, `rank/`, `execute/`, `publish/`) but contains no Python implementation files — only `__pycache__` artifacts. Either it's a residue of the refactor or a future placeholder, but currently it signals misleading structure to new contributors.

5. **Pattern detector imports from `analytics.patterns.*` (legacy shim).** `src/ai_trading_system/domains/ranking/patterns/detectors.py` imports via `analytics.patterns.contracts` which aliases back to `src/ai_trading_system/domains/ranking/patterns/contracts`. This round-trip import works but is confusing. Direct imports from the canonical path would be cleaner.

6. **Configuration is fragmented.** CLI args, `.env`, `config/rank_factor_weights.json`, `config/research_recipes.toml`, and per-service defaults coexist. `config/settings.py` is a pydantic model explicitly flagged as "not canonical" but kept. No single authoritative config layer.

7. **`utils/` vs `core/` overlap.** `utils/env.py` + `core/env.py` and similar pairs exist. AGENTS.md mandates "prefer `core.*` over `utils.*`" — confirming the drift. `core/` is in the legacy layer; `src/ai_trading_system/platform/` is the canonical equivalent for the src tree.

8. **Four UI surfaces, loosely coordinated.** `ui/research/` (Streamlit), `ui/ml/` (Streamlit), `ui/execution/` (NiceGUI), `ui/execution_api/` + `web/execution-console/` (FastAPI + React). The React app requires a separate Vite dev server; it is not served by FastAPI.

9. **Stage gate policy is inconsistent.** Rank hard-blocks on untrusted data (unless flagged); Publish soft-fails. No single matrix documents which failures stop the pipeline vs degrade it gracefully.

10. **No in-repo scheduler.** No cron, Makefile, Airflow, or systemd unit. Scheduling is operator memory — a single point of operational failure for a daily automated pipeline.

11. **Hardcoded operational paths in `channel/` and some publishers.** Several helpers bypass `core.paths` / `platform.db.paths` — enforced by the path-hygiene ratchet lint test (`tests/lint/test_path_hygiene.py`), which tracks 13 known violators in the Phase 1a allowlist.

---

## I. Risks

**Operational**
- **No in-repo scheduler.** Reliance on external cron/manual runs; a missed run silently produces stale operator reports.
- **Single-host assumption.** All DuckDB/SQLite stores are local files. No concurrency guards visible beyond DuckDB's own write lock — two parallel runs could corrupt `control_plane.duckdb` or `ohlcv.duckdb`.
- **Dhan live adapter exists** (`domains/execution/adapters/dhan.py`). If accidentally wired in by a config change, paper-only guarantees break. AGENTS.md line 67 explicitly forbids live integration but nothing in code enforces it.

**Architectural**
- **Dual-tree drift window.** Until the CLI cutover is complete, both the `src/` canonical tree and the top-level legacy packages coexist. A change to `analytics/ranker.py` (legacy) will not automatically propagate to `domains/ranking/ranker.py` (canonical). Developers must know which file is in the hot path.
- **Stage wrappers could absorb logic.** The thin-wrapper convention in `pipeline/stages/` is social, not structural. Easy to accidentally put logic in stage wrappers instead of domain services.
- **Path-domain abstraction is leaky.** `channel/` and several helpers hardcode operational paths. Research-mode runs may silently write or read operational locations. The path-hygiene lint test is the primary guard.
- **`rel_strength_score` not available in per-symbol feature loop.** Stage 2 condition 7/8 (RS percentile ≥ 70/85) defaults to 0 in `compute_stage2()` because cross-sectional RS is computed in the rank stage. This under-scores Stage 2 in the feature store; RS enrichment happens at rank time via `ranker.py`. Operators should not compare `stage2_score` in the feature parquet vs the final ranked output — they will differ.

**Data quality**
- **Fragile ingest fallback chain** (NSE → yfinance). A bad bhavcopy date can cascade into a yfinance fill with different precision/corporate-action handling than NSE.
- **Schema drift recovery requires scripts.** Existence of `repair_ohlcv_window.py`, `repair_ingest_schema.py` implies drift has happened in practice.
- **Masterdata in SQLite while market data is DuckDB.** Two DB engines means two lock semantics, two backup paths.

**Maintainability**
- **Large legacy monoliths** remain: `collectors/daily_update_runner.py`, `collectors/dhan_collector.py` (~82 KB each), `analytics/ranker.py`. These are change-hotspots in the legacy tree.
- **Config fragmentation** means any behavior change potentially needs edits in multiple formats.
- **Duplicate pattern/breakout paths** (legacy `analytics/patterns/` + canonical `domains/ranking/patterns/`) risk behavioral divergence between dashboard, rank sidecar, and backtest.

---

## J. Unknowns / Ambiguities

1. **What actually schedules production runs?** No cron/systemd/Airflow hint in-repo.
2. ~~**Is `src/ai_trading_system/` intended to replace top-level entirely, or coexist?**~~ **Resolved:** `src/ai_trading_system/` IS the replacement. `run/` and top-level packages are compatibility wrappers. CLI cutover is the remaining work.
3. **Which `channel/*` modules are still invoked in production?** Whether Telegram reports flow through `channel/telegram_reporter.py` vs `domains/publish/channels/` is unclear without tracing live config.
4. **How is `data/masterdata.db` refreshed on a cadence?** `domains/ingest/masterdata.py` exists, but whether it runs scheduled or only via bootstrap script isn't confirmed.
5. **Authoritative rank factor weights.** `config/rank_factor_weights.json` is read by `domains/ranking/composite.py`, but whether there are any in-code weight overrides wasn't verified end-to-end.
6. **ML shadow activation path.** `ml_mode="shadow_ml"` is mentioned but how it's set operationally (env var, CLI flag, config file) is not confirmed.
7. **React execution console status.** `web/execution-console/` exists. Unclear whether operators actually use it or whether `ui/execution/` (NiceGUI) is the real console.
8. **How much of `domains/execution/adapters/dhan.py` is live-functional vs stubbed.**
9. **Whether `config/settings.py` has any live importers.** Labeled non-canonical and noted as unused in docs, but not confirmed by import trace.
10. **Backup/retention story for DuckDB/SQLite stores.** Nothing in-repo; assumed operator-managed.
11. **NSE bhavcopy rate-limit/auth constraints.** Not inspected.
12. **Status of `services/` empty directory.** Residue of refactor? Future migration target? Unclear.

---

## K. What Has Changed Since Revision 1 (April 2026 Sprint 1 + 2)

The following changes were made to `src/ai_trading_system/` during the implementation session that preceded this revision:

| File | Change |
|---|---|
| `domains/features/indicators.py` | Added `add_stage2_features()` — 9-condition Weinstein Stage 2 scoring |
| `domains/features/feature_store.py` | Added `STAGE2_FEATURE_COLUMNS`, `compute_stage2()` method, `'stage2'` entry in `feature_methods` dict |
| `domains/ranking/contracts.py` | Added `'stage2_breakout'` to `RANK_MODES`; added 7 Stage 2 columns to `RANKED_SIGNAL_COLUMNS` |
| `domains/ranking/eligibility.py` | Added `stage2_gate_enabled` and `stage2_min_score` params to `apply_rank_eligibility()` |
| `domains/ranking/ranker.py` | Added Stage 2 bonus (+0–5 pts), `stage2_breakout` mode pre-filter; wired `stage2_gate_enabled` to eligibility |
| `domains/ranking/breakout.py` | Replaced 3-condition Tier A/B/C with Stage 2 score-driven Tier A/B/C/D (legacy fallback preserved) |
| `domains/ranking/patterns/detectors.py` | Bug Fix 1: O(n³)→O(n²) flag detector; Bug Fix 2: round-bottom positional argmax; Bug Fix 3: `smoothing_method` added; Bug Fix 4: stale watchlist recency guards; Bug Fix 5: round-bottom low-volume watchlist guard; + 3 new bullish patterns: ascending_triangle, vcp, flat_base; Stage 2 bonus in `_score_signal_rows()` |
| `tests/test_stage2_features.py` | New test file — 30 test cases covering all 4 labels, scoring arithmetic, fail reasons, missing columns |
| `tests/lint/test_path_hygiene.py` | New ratchet lint test — 13-entry allowlist, fails on new raw `data/...` literals |

---

**One-line summary of the AS-IS:** A single-operator, local-first, staged NSE pipeline with `src/ai_trading_system/` (276 files · 42K LOC) as the canonical post-refactor implementation of all five pipeline domains; `run/` and top-level packages (`analytics/`, `collectors/`, `features/`, etc.) are compatibility shims or legacy operational code awaiting CLI-cutover; the four-DB-store control plane provides strong run governance and trust lineage; Stage 2 Weinstein scoring, five pattern-engine bug fixes, and 3 new bullish patterns (ascending_triangle, vcp, flat_base) were applied in Sprint 1–3.
