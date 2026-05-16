# Ranking Domain

- **Purpose:** Convert features into composite scores, breakout signals, pattern scans, regime tags, and sector strength — producing the canonical `ranked_signals.csv` plus sidecar artifacts.
- **Audience:** Developer.
- **Last verified:** 2026-05-16
- **Source of truth:** [`src/ai_trading_system/domains/ranking/`](../../src/ai_trading_system/domains/ranking/), [`src/ai_trading_system/pipeline/stages/rank.py`](../../src/ai_trading_system/pipeline/stages/rank.py)

---

## Responsibility

Be the **scoring layer**. Every downstream stage (candidates, events, execute, publish, perf_tracker) reads `ranked_signals.csv` or the sidecar artifacts produced here. Includes deterministic candidate filtering via the sibling `domains/candidates/` package.

## Package / module ownership

| Module | Role |
|---|---|
| `service.py::RankOrchestrationService` | Stage orchestration. |
| `ranker.py` | Composite scoring (weighted factor sum). |
| `factors.py` | Individual factor definitions. |
| `breakout.py` | Breakout pattern scoring. |
| `patterns/` | Pattern scan family. |
| `market_stage.py` | Regime detection (bull/bear/ranging). |
| `stage_classifier.py` | Stock stage classification. |
| `sector_dashboard.py` | Sector-level aggregations. |
| `screener.py` | Screener.in client (stock universe). |
| `input_loader.py` | Loads rank inputs from feature store + prior outputs. |
| `volume_shocker.py`, `strategy_router.py`, `payloads.py` | Sidecar logic. |

The deterministic candidate filter lives next door in [`domains/candidates/builder.py::ExecutionCandidateBuilder`](../../src/ai_trading_system/domains/candidates/builder.py) and is covered in [`candidates.md`](../stages/candidates.md).

## Public contracts

Outputs under `data/pipeline_runs/<run_id>/rank/attempt_<n>/`:

| Artifact | Consumer |
|---|---|
| `ranked_signals.csv` | candidates, events, execute, publish, perf_tracker |
| `breakout_signals.csv` | candidates, publish |
| `pattern_signals.csv` | publish |
| `stock_scan_output.csv` | publish |
| `sector_dashboard.csv` | publish |
| `dashboard_payload.json` | publish (embedded in stage metadata) |

Factor weights: [`src/ai_trading_system/platform/config/rank_factor_weights.json`](../../src/ai_trading_system/platform/config/rank_factor_weights.json).

## Storage ownership

No exclusive DuckDB tables. Writes only stage artifacts.

## Dependencies

- Reads feature store + sector RS artifacts.
- Optional ML overlay via `model_registry` table in `control_plane.duckdb` (selectable via `--ml-mode none|shadow|production` — see [`stages/rank.md`](../stages/rank.md)).

## Extension points

- New factor: see [`docs/development/adding_new_factor.md`](../development/adding_new_factor.md).
- New pattern: add to `patterns/` and register in the scan dispatcher.
- ML overlay: register a model in `model_registry` (see [`stages/rank.md`](../stages/rank.md)).

## Known gaps

- Factor IC measurement currently lives in `research/perf_tracker/digest.py`, not in this domain — see [`stages/perf_tracker.md`](../stages/perf_tracker.md).

## See also

- [`docs/stages/rank.md`](../stages/rank.md)
- [`docs/stages/candidates.md`](../stages/candidates.md)
- [`docs/reference/ranking_factors.md`](../reference/ranking_factors.md)
- [`docs/reference/breakout_and_patterns.md`](../reference/breakout_and_patterns.md)
