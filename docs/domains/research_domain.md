# Research Domain

- **Purpose:** Backtesting, optimization, and forward-return performance tracking. Isolated from the operational pipeline except for the optional ML overlay in rank.
- **Audience:** Developer, researcher.
- **Last verified:** 2026-05-16
- **Source of truth:** [`src/ai_trading_system/research/`](../../src/ai_trading_system/research/)

---

## Responsibility

Be the **non-operational** experimentation layer. Research code reads its own data domain (`DATA_DOMAIN=research` → `data/research_ohlcv.duckdb`) and writes its own artifacts. Research models can be promoted into the operational pipeline only via the `model_registry` table in `data/control_plane.duckdb`.

## Package / module ownership

| Module | Role |
|---|---|
| `backtesting/research_loader.py` | Load OHLCV from `data/research_ohlcv.duckdb`. |
| `backtesting/winner_capture.py` | Capture winning trades for review. |
| `backtest_pipeline.py` | Full-chain historical simulation: rank → execute → perf. |
| `train_pipeline.py` | Prepare training dataset (feature + label alignment). |
| `run_recipe.py` | Execute research recipes from `config/research_recipes.toml`. CLI: `ai-trading-research-recipe`. |
| `recipes.py` | Recipe DSL (backtest, optimize, eval). |
| `shadow_monitor.py` | Monitor live pipeline against research baselines. |
| `sync_operational_data.py` | Copy operational OHLCV into research domain. |
| `optimization/` | Optuna-based hyperparameter tuning — see [`optimization_domain`](optimization_domain.md). |
| `perf_tracker/` | Forward-return computation — see [`stages/perf_tracker.md`](../stages/perf_tracker.md). |

## Public contracts

- Backtest result artifacts (paths TBD; **Current code status: unknown — verify when writing `reference/artifacts.md`**).
- `model_registry` table in `control_plane.duckdb` is the bridge between research and operational (ML overlay in rank stage).
- `rank_cohort_performance` table in `data/research.duckdb` — owned by perf_tracker.

## Storage ownership

- `data/research_ohlcv.duckdb` — research OHLCV isolation.
- `data/research.duckdb` — perf tracker (`rank_cohort_performance`) + future research tables.
- `data/research/perf_digests/` — weekly digest output.

## Dependencies

- Reads operational OHLCV via `sync_operational_data.py`.
- Reads pipeline run artifacts under `data/pipeline_runs/`.
- `DATA_DOMAIN` env var selects which domain `platform/db/paths.py` resolves to.

## Extension points

- New recipe type: extend `recipes.py` and recipe registry.
- New optimization objective: see [`optimization_domain`](optimization_domain.md).
- New ML model: register via `model_registry` for rank stage consumption.

## Known gaps

- Backtest artifact paths and schemas not yet enumerated in docs.
- Shadow-trading dataflow into the operational pipeline is partial — see `shadow_monitor.py`.

## See also

- [`docs/stages/perf_tracker.md`](../stages/perf_tracker.md)
- [`docs/domains/optimization_domain.md`](optimization_domain.md)
