# Optimization Domain

- **Purpose:** Research-only strategy rule-pack tuning via Optuna. Walk-forward validation, overfitting controls, acceptance gates, champion guards. Not part of the operational pipeline.
- **Audience:** Researcher, developer, operator.
- **Last verified:** 2026-05-16
- **Source of truth:** [`src/ai_trading_system/research/optimization/`](../../src/ai_trading_system/research/optimization/)

---

## Responsibility

Search the space of strategy rule packs for improved baseline performance, without contaminating production with overfit results. Output is a candidate rule pack + diagnostics; promotion is a manual decision (see [`docs/runbooks/optimization.md`](../runbooks/optimization.md)).

## Package / module ownership

| Module | Role |
|---|---|
| `cli.py` | `ai-trading-optimize` console alias; recipe resolution; report auto-write toggle. |
| `__main__.py` | `python -m ai_trading_system.research.optimization` entry. |
| `recipe.py` | `OptimizationRecipe` (frozen dataclass), `load_recipe()`, `resolve_baseline_path()` (bare-name → file), `SearchSpaceOverride` (Wave 4 recipe-level search-space overrides). |
| `domains/strategy/bounds.py` | `KNOWN_PARAMS` (single source of truth for search-space parameter surface) + `build_search_space(trial, *, strategy_id, overrides=None)`. |
| `templates/` | YAML templates shipped with the package; rendered by `cli init`. |
| `runner.py` | `run_optimization()` — fresh study (Optuna study orchestration, walk-forward, acceptance, champion guards, auto-report). `resume_optimization()` — reopens an existing journal and continues. Persistent storage via Optuna `JournalStorage` at `data/optuna/<run_id>.log`. |
| `bounds.py` | `build_search_space()` + `KNOWN_PARAMS` — parameter surface for ranking weights + risk knobs. Defaults may be narrowed per-run via the recipe's `search_space:` block (Wave 4). |
| `evaluator.py` | `Metrics`, `compute_metrics`, `fitness`. |
| `acceptance.py` | Per-trial acceptance gate (worst-fold-vs-benchmark, MDD ratio, fold-rate, etc.). |
| `guards.py` | End-of-study champion guards (weight pinning, zero-trade folds). |
| `walkforward.py` | `build_folds()` — train/val/step window builder. |
| `backtest_adapter.py` | Engine wiring (compile pack → run backtest → return Metrics). |
| `baselines.py` | Baseline backtest + benchmark buy-and-hold per fold. |
| `store.py` | DuckDB persistence (`strategy_rule_pack`, `strategy_optimization_run`, `strategy_iteration_result`, `strategy_backtest_trade`); lifecycle status writes; champion lookup helpers. |
| `reports.py` | `build_markdown_report()`, `write_report()`. |
| `promote.py` | `ai-trading-optimize-promote` console alias; lifecycle ladder enforcement; `promote-latest` recipe shortcut. |

## Public contracts

### CLI

| Command | Purpose | Added |
|---|---|---|
| `ai-trading-optimize init <name> [--force]` | Scaffold `config/strategies/<name>_v1.yaml` + `config/strategies/recipes/<name>.yaml` from templates. | Wave 3 |
| `ai-trading-optimize validate <recipe> [--with-backtest]` | Dry-run a recipe (schema + path + pack load; `--with-backtest` adds a one-fold baseline backtest). | Wave 3 |
| `ai-trading-optimize run --recipe <name-or-path>` | Run a study end-to-end. Bare name resolves to `config/strategies/recipes/<name>.yaml`. | Wave 3 form |
| `ai-trading-optimize --recipe <name-or-path>` | Legacy flat form — equivalent to `run --recipe ...`. Kept for backwards compatibility. | Wave 1 |
| `ai-trading-optimize resume <optimization_run_id>` | Re-open the Optuna journal at `data/optuna/<run_id>.log` and continue until `max_trials` is hit. | Wave 5a |
| `ai-trading-optimize-promote --rule-pack-id <hash> --to <status>` | Promote a specific rule pack along the lifecycle ladder. | (pre-existing, kept) |
| `ai-trading-optimize-promote promote-latest --recipe-name <name> [--to <status>]` | Promote the champion of the latest completed run for the named recipe. Defaults to `--to shadow`. | Wave 1 |

See [`docs/runbooks/optimization.md`](../runbooks/optimization.md) for the end-to-end operator flow.

### Auto-written reports

After every successful run, the markdown report is written to:

- `reports/optimization/<recipe>/<optimization_run_id>.md` — immutable per run
- `reports/optimization/<recipe>/latest.md` — overwritten each run

Pass `--no-report` to skip (CI/scripted callers).

### HTTP API (added in Wave 2 of the optimizer convenience plan)

The execution console exposes read endpoints under `/api/execution/optimization/*`. All require the `x-api-key` header. Backed by [`services/readmodels/optimization_runs.py`](../../src/ai_trading_system/ui/execution_api/services/readmodels/optimization_runs.py).

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/execution/optimization/runs?recipe=&status=&limit=` | List runs (latest first). |
| `GET` | `/api/execution/optimization/runs/{run_id}` | Run header + baseline/champion per-fold metrics + report path. |
| `GET` | `/api/execution/optimization/runs/{run_id}/trials?limit=&sort=` | Per-trial aggregate rows (sort columns whitelisted in the readmodel). |
| `GET` | `/api/execution/optimization/leaderboard?metric=sharpe&top=20` | Latest champion per recipe, ranked by metric. |
| `GET` | `/api/execution/optimization/runs/{run_id}/report` | Auto-written markdown report content (404 if missing). |
| `POST` | `/api/execution/optimization/runs/{run_id}/promote` | One-click promote (Wave 5b). Body: `{"to": "shadow"}`; defaults to shadow; 422 on backwards moves. |

See [`docs/reference/api_reference.md`](../reference/api_reference.md) for response schemas (Pydantic models in [`schemas/optimization.py`](../../src/ai_trading_system/ui/execution_api/schemas/optimization.py)).

### DuckDB tables

In `data/control_plane.duckdb` (resolved via `RegistryStore`):

| Table | Purpose |
|---|---|
| `strategy_rule_pack` | All rule packs (id, parent, strategy_id, version, rule_yaml, rule_json, lifecycle_status, description, created_at). |
| `strategy_optimization_run` | One row per run: recipe_name, strategy_id, baseline pack id, dates, seed, max_trials, status, champion pack id, recipe_json, error, started_at, completed_at. |
| `strategy_iteration_result` | Per-trial per-fold rows: run_id, iteration, fold_index, fitness, metrics, accepted, rejection_reason. Iteration `-1` is the baseline marker. |
| `strategy_backtest_trade` | Optional per-trial per-fold trade log. |

## Storage ownership

- All four tables above in `data/control_plane.duckdb` — sole writer.
- Per-run markdown reports under `reports/optimization/`.
- Stage artifacts: none — optimizer is not a pipeline stage.

## Dependencies

- External: Optuna 4.x, tqdm.
- Internal: `domains/strategy/` (rule pack schema + compiler), `research/backtesting/` (via `backtest_adapter`).
- Reads: `config/strategies/*.yaml` (baselines), `config/strategies/recipes/*.yaml` (recipes), `data/ohlcv.duckdb` (price data via the backtester).

## Extension points

- **New baseline strategy** — add `config/strategies/<name>_v1.yaml` matching the `StrategyRulePack` schema; reference it from a new recipe in `config/strategies/recipes/`.
- **New objective** — extend `evaluator.py::fitness` and (if exposing new weights) `FitnessWeights` in the recipe.
- **New acceptance rule** — add to `acceptance.py::AcceptanceThresholds` and the `is_accepted` predicate.
- **Narrow an existing dimension per-run** — add a `search_space:` block to the recipe YAML (no code change). See [`docs/runbooks/optimization.md`](../runbooks/optimization.md).
- **Add a brand-new dimension** — extend `KNOWN_PARAMS` in `bounds.py` with a new `ParamSpec` and wire a matching `_suggest_*` call into `build_search_space`. New categorical values for an existing dimension also require this — recipes can only narrow categoricals to a subset of the defaults.

## Known gaps

- _(Wave 4 added recipe-level `search_space:` overrides. Default bounds still live in `bounds.KNOWN_PARAMS`; recipes may narrow any parameter and the validator rejects unknown names or out-of-default categorical choices.)_
- _(Wave 5b added the React Optimization page at `/optimization` in `web/execution-console-v2/`. Runs table, run detail with baseline/champion folds, top-trials by fitness, leaderboard tab, embedded markdown report, and one-click promote via the new `POST .../promote` endpoint. Auto-refetches every 5s while a selected run is `status='running'`.)_
- _(Wave 5a added resumability via `JournalStorage` + `ai-trading-optimize resume <run_id>`. Per-run journals at `data/optuna/<run_id>.log`; URI persisted in `strategy_optimization_run.study_storage_uri`.)_
- _(Wave 3 added `init` + `validate` subcommands and name-based `baseline_pack_path` resolution.)_

## When not to use

- Small backtest windows where overfitting risk dominates Sharpe gain.
- Without walk-forward validation enabled.
- Before defining a baseline you want to beat — the acceptance gate is comparative.

## See also

- [`docs/runbooks/optimization.md`](../runbooks/optimization.md) — end-to-end operator flow
- [`docs/domains/research_domain.md`](research_domain.md)
- [`docs/_legacy/archived_2026-05-16/architecture_strategy-optimizer.md`](../_legacy/archived_2026-05-16/architecture_strategy-optimizer.md) — historical design context
