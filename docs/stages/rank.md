# Stage: rank

- **Purpose:** Build the canonical ranked-signal artifact set (composite ranking, breakout scan, pattern scan, sector dashboard, dashboard payload) consumed by every downstream stage.
- **Audience:** Operator, developer, debugging
- **Last verified:** 2026-09-11
- **Source of truth:**
  - `src/ai_trading_system/pipeline/stages/rank.py`
  - `src/ai_trading_system/domains/ranking/service.py` (`RankOrchestrationService`)
  - `src/ai_trading_system/domains/ranking/{ranker,factors,breakout,market_stage,stage_classifier,sector_dashboard,screener,input_loader,volume_shocker,strategy_router,payloads}.py`
  - `src/ai_trading_system/domains/ranking/patterns/`
  - `src/ai_trading_system/platform/config/rank_factor_weights.json`

---

## Purpose

The rank stage turns the per-symbol, per-exchange feature store into a prioritized list of trade-worthy symbols and supporting evidence artifacts. The daily pipeline ranks the combined NSE+BSE universe and retains `exchange` on canonical rows. It is the central operational stage: every other stage downstream of rank reads its CSV/JSON outputs.

## Entrypoints

- Stage wrapper: `src/ai_trading_system/pipeline/stages/rank.py` (`RankStage`)
- Service: `src/ai_trading_system/domains/ranking/service.py` (`RankOrchestrationService.run`, `service.py:316`)
- CLI: `ai-trading-pipeline --stages rank` (subset run) or as part of the full `PIPELINE_ORDER` (`pipeline/orchestrator.py:41`).
- Smoke mode is **disabled** — `rank.py:38` raises `RuntimeError` if `params.smoke` is set.

## Input data

- `data/ohlcv.duckdb::_catalog` — accessed via `StockRanker(ohlcv_db_path=…)`.
  The base market snapshot, return windows, volume windows, delivery, stage,
  benchmark, and persisted feature reads are bounded to the requested rank date.
- Feature store Parquet under `data/feature_store/<symbol_id>/` — passed via `ensure_domain_layout(...).feature_store_dir` (`service.py:436`).
- Prior rank artifacts (last 8) from the registry to compute factor turnover / load resumable task state (`service.py:530`–`555`, `service.py:1290`).
- Data-trust summary from `control_plane.duckdb` via `analytics.data_trust.load_data_trust_summary` (`service.py:440`).
- Factor weights from `src/ai_trading_system/platform/config/rank_factor_weights.json`.

## Output artifacts

Under `data/pipeline_runs/<run_id>/rank/attempt_<n>/`:

| Artifact type | File | Source |
|---|---|---|
| `ranked_signals` | `ranked_signals.csv` | `StockRanker.rank_all` → `service.py:511` |
| `volume_shockers` | `volume_shockers.csv` | `domains/ranking/volume_shocker.py::detect_volume_shockers` (`service.py:570`) |
| `breakout_scan` | `breakout_scan.csv` | `domains/ranking/breakout.py::scan_breakouts` (`service.py:595`) |
| `pattern_scan` | `pattern_scan.csv` | `domains/ranking/patterns/` + `analytics/patterns/build_pattern_signals` (`service.py:426`–`427`) |
| `stage1_scan` | `stage1_scan.csv` | Canonical research-only maturity/eligibility scan. Score bands remain diagnostic; operational substates and promotion require structural eligibility. |
| `early_accumulation_scan` | `early_accumulation_scan.csv` | Compatibility output feeding the Stage-1 model during migration. |
| `stock_scan` | `stock_scan.csv` | `domains/ranking/stock_scan.py` (integrated view from `build_integrated_stock_scan_view`, `service.py:179`) |
| `sector_dashboard` | `sector_dashboard.csv` | `domains/ranking/sector_dashboard.py` |
| `watchlist_prefilter` | `watchlist_prefilter.csv` | `domains/ranking/watchlist.py` |
| `watchlist_rejections` | `watchlist_rejections.csv` + `watchlist_rejections.json` sidecar | V2 watchlist gate diagnostics from `domains/ranking/watchlist.py` |
| `watchlist_catalyst` | `watchlist_catalyst.json` | `domains/ranking/watchlist_catalyst.py` |
| `watchlist_final` | `watchlist_candidates.csv` + `watchlist_candidates.json` + `watchlist_digest.md` sidecars | `service.py:375`–`394` |
| `dashboard_payload` | `dashboard_payload.json` | `domains/ranking/payloads.py::build_dashboard_payload` |
| `ml_overlay` (optional) | `ml_overlay.csv` | Only present when `ml_mode=shadow_ml` and the overlay builder succeeds (`service.py:1204`–`1208`) |
| `rank_summary` | `rank_summary.json` | Stage metadata aggregate (`service.py:404`) |

Per-task statuses are persisted to `task_status.json` for resumability (`service.py:1283`).

Before final rank artifacts are emitted, the rank stage transactionally upserts the current run into `control_plane.duckdb::{rank_history,stage_history,stage1_history,pattern_history}`. `decision_persistence_summary.json` records write counts and validation status. The CSV files are materialized run outputs, not historical storage.

## Main modules

- `domains/ranking/service.py::RankOrchestrationService` — orchestrates resumable tasks, dashboard payload assembly, ML overlay, registry writes.
- `domains/ranking/ranker.py` (`StockRanker`) — composite scoring driver.
- `domains/ranking/factors.py` — factor implementations: `apply_relative_strength`, `apply_momentum_acceleration`, `apply_volume_intensity`, `apply_trend_persistence`, `apply_proximity_highs`, `apply_delivery`, `apply_sector_strength`, `compute_penalty_score`, `add_signal_freshness`.
- `domains/ranking/composite.py` — `compute_factor_correlations`, `compute_factor_turnover`.
- `domains/ranking/breakout.py::scan_breakouts` — Tier A/B breakout detection, gated by market-stage allowlist and enriched from the full per-exchange rank universe.
- `domains/ranking/volume_shocker.py::detect_volume_shockers` — volume z-score outliers.
- `domains/ranking/market_stage.py::get_market_stage` + `strategy_router.py::route` — selects rank mode, breakout activation, and stage gate from fresh governed stock-stage breadth. The legacy OHLCV snapshot is a freshness-checked compatibility fallback.
- `domains/ranking/stage_classifier.py` / `stage_eligibility.py` / `stage_store.py` — weekly Stage 2 classification used as a gate when `weekly_stage_gate` is on.
- `domains/ranking/screener.py` — supplementary screener pipeline (consumed via stock scan / watchlist).
- `domains/ranking/input_loader.py` — feature/return loaders shared by the ranker and factor functions.
- `domains/ranking/patterns/` — pattern detection package: `universe.py::build_pattern_seed_universe`, `detectors.py`, `evaluation.py`, `cache.py` (lifecycle state), `data.py` (frame loaders), plus `signal.py` and `contracts.py`. See [`../architecture/pattern-scan.md`](../_legacy/archived_2026-05-16/architecture_pattern-scan.md) for the deeper pattern subsystem write-up.
- `domains/ranking/sector_dashboard.py` + `sector_health.py` — sector quadrant/leader inputs.
- `domains/ranking/payloads.py` — `build_dashboard_payload`, `augment_dashboard_payload_with_ml`, `summarize_task_statuses`.

## Process flow

1. Resolve effective params, load data-trust summary; abort if `trust_summary.status == "blocked"` and `allow_untrusted_rank` is not set (`service.py:441`).
2. Resolve correction-aware NSE stage breadth point-in-time from
   `control_plane.duckdb::weekly_stock_stage_history`. Prefer that governed
   source when it has at least 200 classified symbols and is no more than ten
   calendar days old. Use `ohlcv.duckdb::weekly_stage_snapshot` only as a
   compatibility fallback under the same coverage/freshness contract; otherwise
   return the explicit MIXED fallback with source and freshness diagnostics.
   Reuse that governed-first contract for the per-symbol weekly-stage context:
   normalize canonical stages into `S1`–`S4`, retain row-level source/as-of/hash
   lineage, and never let a future or stale row drive freshness bonuses or the
   weekly gate. BSE rows receive no synthetic NSE stage join.
   Merge the resulting `StrategyConfig` (rank mode, breakout activation, weekly
   stage gate, execution regime) into `effective_params`.
3. Build one `RankInputSnapshot` with an inclusive run-date cutoff and route the
   market, return, volume, ADX, SMA, highs, delivery, sector, Stage 2, weekly-stage,
   and persisted Phase 1 reads through it. Repeated reads such as SMA are cached
   within that decision. The current default decision-history version is
   `point_in_time_v2`, and the rank-core task fingerprint includes this input
   contract so retries cannot reuse pre-fix output.
   The current `point_in_time_multi_exchange_v4` input contract partitions ADX,
   SMA, highs, returns, volume, Stage 2, and governed weekly-stage inputs by
   `(symbol_id, exchange)` and fingerprints the resolved weekly-stage context.
   NSE market regime and sector context remain the common India-market context;
   unavailable BSE delivery or fundamental fields degrade through existing
   missing-feature confidence rules rather than being synthesized.
   The market-regime snapshot must be no more than
   `rank_regime_stale_days` calendar days old (default 30). This applies after
   any research compatibility fallback. A stale snapshot raises
   `MarketRegimeFreshnessError` and fails rank before it can determine the
   regime profile, market direction, or exposure guidance.
   Persisted Phase 1 breadth is attached with its source date, calendar age,
   and freshness status. By default (`phase1_breadth_stale_days=0`), only a row
   dated exactly to the rank run may populate current dashboard summary
   metrics; older rows remain in the diagnostic payload but are not promoted
   as current values.
4. Run resumable tasks in order — each is fingerprinted, persisted in `task_status.json`, and skipped on retry if the fingerprint matches (`service.py:495`–end of `run_default`):
   - `rank_core` → combined NSE+BSE `ranked_signals.csv`; filter
     `eligible_rank == true` before the adjusted-score cutoff and `top_n`
   - `rank_universe` → full scored `ranked_universe.csv`, including ineligible
     rows and rejection reasons for audit
   - volume shockers → `volume_shockers.csv`
   - `breakout_scan` (one scan per ranked exchange using `ranked_universe` as
     scoring context; a true S4 market records
     `skipped: disabled_by_market_stage:S4` and emits an empty artifact)
   - `pattern_scan` (one seed/scan lifecycle per ranked exchange, with ranked-symbol fallback)
   - `stock_scan` (integrated view via `build_integrated_stock_scan_view`)
   - `sector_dashboard`
   - `watchlist_prefilter` / `watchlist_catalyst` / `watchlist_final` (+ markdown digest)
   - dashboard payload assembly
5. Apply optional ML overlay (`service.py:1141`, see below).
6. Write all DataFrames as CSVs, sidecar JSON/MD, then `rank_summary.json`. Register every file as a `StageArtifact` (`service.py:340`–`414`).

## ML overlay (optional)

- Controlled by the `ml_mode` pipeline param (read at `service.py:1149`). Supported values:
  - `baseline_only` (default) — overlay disabled, `ml_status="disabled"`.
  - `shadow_ml` — invokes `default_ml_overlay_builder`, which delegates to `analytics.alpha.scoring.OperationalMLOverlayService.build_shadow_overlay` (`service.py:1222`).
  - Any other value → `ml_status="unsupported_mode"` and a degradation warning (`service.py:1177`).
- Models are loaded by `OperationalMLOverlayService` from the `model_registry` table in `control_plane.duckdb` (per truth map §3; the table schema lives in `pipeline/migrations/`).
- Successful runs write `ml_overlay.csv` and per-horizon rows into the prediction-log table via `RankOrchestrationService.write_prediction_logs` (`service.py:1248`).
- A CLI flag named `--ml-mode` is **not** wired in `orchestrator.py` as of this verification — the mode is supplied through pipeline params (e.g., via `ai-trading-research-recipe` or programmatic invocation). Mark as unverified if your runbook claims a CLI flag exists.

## DQ / trust gates

- **Trust-window gate.** `trust_summary.status == "blocked"` aborts with `RuntimeError("Ranking blocked because active data quarantine remains for the current trust window.")` (`service.py:441`). `degraded` emits a warning containing the latest fallback ratio.
- **Pattern-seed fallback.** If `build_pattern_seed_universe` raises or yields zero symbols, the stage falls back to the ranked universe and records `fallback_used=True` in `pattern_seed_metadata` (`service.py:691`).
- **Breakout availability.** If the breakout task ends in `failed | timed_out | degraded`, a warning is appended; the stage continues with an empty breakout frame (`service.py:646`).
- **Market-stage freshness.** Governed and legacy structural breadth retain
  decision cutoff, actual source date, calendar age, source identity, freshness,
  and fallback reason in `market_stage_info`. Stale legacy breadth cannot keep
  breakout routing disabled indefinitely.
- **Market-regime freshness.** The newest regime source row must be within
  `rank_regime_stale_days` calendar days of the decision date (default 30).
  Stale operational or research-fallback data fails rank; the warning-only
  decision-history receipt remains audit evidence and is not the enforcement
  boundary. `regime_age_days` records how long the classified regime has
  persisted; it is not source age and does not make a same-date snapshot stale.
- **Factor correlation / turnover.** Computed but not enforced as gates — they appear in `rank_summary.json` for observability.
- DQ rules in `pipeline/migrations/` (`dq_rule`, `dq_result`) drive the row-count / score-distribution checks declared in the truth map. Specific rule names should be confirmed against the migrations before being cited here.

## Failure modes

- Data trust quarantine blocks the run (see above).
- Feature store missing symbols → `StockRanker.rank_all` returns an empty or thin frame, `ranked_rows=0`, downstream stages emit empty artifacts.
- Unsupported `rank_exchanges` values fail before ranking. The operational
  daily default is `[NSE, BSE]`; direct callers without that parameter retain
  the legacy NSE-only default.
- Pattern subsystem failure → fallback path engages; `pattern_seed_metadata.fallback_reason` records the cause.
- Governed stage history missing or stale and legacy breadth also unusable →
  explicit MIXED fallback plus a degraded-output warning; no stale S4 decision
  is reused.
- Market-regime data older than `rank_regime_stale_days` → rank fails closed
  with `MarketRegimeFreshnessError`; no dashboard or exposure banner is
  produced from that snapshot.
- ML overlay exception → `ml_status="degraded"`, overlay omitted, run continues (`service.py:1192`).
- Optional task failure (`breakout_scan`, `pattern_scan`, `sector_dashboard`, watchlist sub-tasks) is recorded in `task_status.json` with status `failed | timed_out | degraded`, surfaced via warnings, and does not fail the stage.

## Retry behavior

- Each task is fingerprinted by `(task_name, payload)` (`service.py:1325`). On retry, prior attempts' `task_status.json` is read (`previous_task_snapshot`, `service.py:1290`); tasks whose fingerprint matches a `completed` previous attempt are skipped and their CSV/JSON re-used.
- `RankStage.__init__` accepts an injectable `operation` and `ml_overlay_builder` for tests.
- The stage itself is invoked by the orchestrator with the standard attempt loop; per-stage retry/backoff lives in `pipeline/orchestrator.py`.

## Rebuild impact

AUD-001 changes historical rank inputs but not feature formulas. A full feature
rebuild is not required. Historical rank and research artifacts created with the
old latest-row behavior must be recomputed under `point_in_time_v2`; retain the
old immutable attempts as superseded evidence rather than overwriting them.

## Offline R0 pattern-lane calibration

`research/pattern_lane_calibration/` implements the ADR-0007 R0 harness outside
the rank stage. It reconstructs daily OHLCV, structural fields,
cross-sectional relative strength, weekly-stage freshness, and liquidity
eligibility for every historical as-of date. Market and weekly-stage rows after
that boundary cannot enter classification; only outcome calculation may read
later market rows.

The frozen `pattern-lane-r0-policy-v1` assigns one of
`stage2_continuation`, `stage1_base`, `young_listing_base`,
`ipo_early_base`, or `no_lane`. Its complete family matrix covers all emitted
families for five lane/history-band combinations. Allowed families are selected
before detector calls; `head_shoulders` is suppression-only where its 120-bar
history requirement is met. The 35–49-bar lane uses
`ipo-early-liquidity-policy-v1`, not rank's 50-bar `feature_ready` gate.

The harness produces only explicitly rooted `r0_*` research evidence. It does
not invoke `RankOrchestrationService`, register a `StageArtifact`, write pattern
cache, replace `pattern_scan.csv`, or call a rank consumer. No feature rebuild
or operational rank rebuild is required. See
[`commands`](../reference/commands.md#adr-0007-r0-pattern-calibration) and
[`artifacts`](../reference/artifacts.md#adr-0007-r0-pattern-calibration-artifacts).
The CLI reports live date/symbol throughput and ETA, uses parallel symbol
workers, and commits a resumable checkpoint after every completed date.

## Downstream consumers

- [`fundamentals`](./fundamentals.md) — reads `ranked_signals` and writes alongside under `rank/attempt_<n>/`.
- [`candidates`](./candidates.md) — requires `ranked_signals` (hard), optionally reads `breakout_scan`, `pattern_scan`, `sector_dashboard`, plus `fundamentals.watchlist_candidates`.
- [`events`](./events.md) — reads `breakout_scan` (Tier A/B) and `volume_shockers`.
- `execute`, `insight`, `narrative`, `publish` — consume `ranked_signals` and/or `dashboard_payload` indirectly via the candidates/events outputs. The Google Sheets daily report may use ranked rows from the full-universe `stock_scan` to fill its operator-only top-25 view; this does not widen the regime-aware execution shortlist.
- Execution independently defaults to `execution_exchanges=[NSE]`. Including
  BSE in analytical ranking does not widen broker placement unless the operator
  explicitly changes that execution parameter.

## Commands

```bash
# Run rank as part of the full pipeline (recommended).
ai-trading-pipeline --run-date 2026-05-16

# Re-run only the rank stage (resumes via task_status.json when fingerprints match).
ai-trading-pipeline --run-date 2026-05-16 --stages rank

# Inspect emitted artifacts.
ls data/pipeline_runs/<run_id>/rank/attempt_1/
cat data/pipeline_runs/<run_id>/rank/attempt_1/rank_summary.json
```

> Live trading / production-readiness disclaimer: rank only emits artifacts; it never places orders. Anything downstream of `execute` should be treated as paper by default until execution guardrails are independently audited.

Operational admission excludes identities pending universe onboarding using
`domains.ingest.onboarding_gate`. Ranking filters before scoring and when
consuming reused results; execution independently filters new-entry candidates
from saved rank artifacts. Completed onboarding lifts the exclusion. Research
does not consume this mutable operational state; existing-position exits remain
separate. Corrupt admission state fails closed. No full feature rebuild is needed.
