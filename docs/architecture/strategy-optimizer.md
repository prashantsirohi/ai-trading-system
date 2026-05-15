# Strategy Optimizer

Research-only module that searches `StrategyRulePack` variants with Optuna
(TPE) over walk-forward folds, produces a champion rule pack, and writes
artifacts to the control-plane DuckDB. Never touches the operational
`rank`/`execute` stages.

Status: Phases 0–4 implemented and merged on `main`. Phases 5–6
(shadow-only and live rule-pack ingestion) are planned, not built.

## What it does today

1. Loads a baseline `StrategyRulePack` from
   [`config/strategies/momentum_breakout_v1.yaml`](../../config/strategies/momentum_breakout_v1.yaml)
   (encodes current production defaults).
2. Backtests the baseline across every walk-forward validation fold to
   establish per-fold metrics.
3. Runs an Optuna study. Each trial samples a new rule pack via
   [`domains/strategy/bounds.py`](../../src/ai_trading_system/domains/strategy/bounds.py),
   backtests it on the same folds, and applies a per-trial acceptance
   gate. The study returns the mean per-fold fitness; TPE optimises that.
4. Persists every trial (accepted or rejected) plus per-fold and aggregate
   metrics to four DuckDB tables (migration `015_strategy_optimizer.sql`).
5. After the study, applies a holistic champion-guard (parameter pinning,
   worst-fold-vs-NIFTY). On pass, the champion pack auto-advances
   `backtested → walkforward_passed`. No further auto-promotion.
6. Operators advance the lifecycle (`walkforward_passed → shadow →
   paper_approved → production_candidate → active`) manually via
   `python -m ai_trading_system.research.optimization.promote`.

## Module layout

```
src/ai_trading_system/
  domains/
    strategy/                         # declarative rule-pack contract
      rule_pack.py                    # Pydantic StrategyRulePack + RankingConfig
      compiler.py                     # pack -> existing engine entry points
      bounds.py                       # Optuna search-space builder
      io.py                           # YAML load/save + SHA256 hash
  research/
    optimization/
      backtest_adapter.py             # run_backtest(pack, ...) thin wrapper
      walkforward.py                  # build_folds(start, end, ...)
      evaluator.py                    # Metrics + fitness composite
      baselines.py                    # NIFTY buy-hold per fold
      acceptance.py                   # per-trial gate (worst-fold guards)
      guards.py                       # champion-final guards
      runner.py                       # Optuna orchestration
      store.py                        # DuckDB writes
      reports.py                      # markdown reports
      recipe.py                       # OptimizationRecipe dataclass + loader
      cli.py + __main__.py            # python -m ...optimization
      promote.py                      # python -m ...optimization.promote
  pipeline/
    migrations/
      015_strategy_optimizer.sql      # four tables + indexes
config/
  strategies/
    momentum_breakout_v1.yaml         # baseline / parity reference
    recipes/momentum_v1.yaml          # sample OptimizationRecipe
```

## End-to-end flow

```
OptimizationRecipe (YAML)
        │
        ▼
runner.run_optimization
        │
        ├─► load_rule_pack(baseline_pack_path)               -> StrategyRulePack
        ├─► walkforward.build_folds(from, to, 12/3/3)        -> [WalkForwardFold]
        ├─► baselines.benchmark_buyhold_return per fold      -> NIFTY return ref
        ├─► evaluate baseline on every val fold              -> [FoldResult]
        ├─► store.create_run(...)                            -> DuckDB row
        │
        └─► optuna.create_study(TPESampler(seed=...))
                │
                └─► objective(trial):
                        pack = bounds.build_search_space(trial)
                        for fold in folds:
                            run_backtest(pack, fold.val_start, fold.val_end)
                            metrics = evaluator.compute_metrics(result)
                            fitness = evaluator.fitness(metrics, weights)
                        verdict = acceptance.is_accepted(...)
                        store.insert_iteration_result(...)
                        if verdict.accepted: update champion
                        return mean fitness
        │
        ▼
champion_guards(champion_pack, champion_folds)
        │
        ├─ promote=True  -> store.set_lifecycle_status(..., walkforward_passed)
        └─ promote=False -> stays at 'backtested', reason recorded
        │
        ▼
store.complete_run(status="completed", champion_rule_pack_id=...)
```

## Backtest adapter

[`backtest_adapter.run_backtest`](../../src/ai_trading_system/research/optimization/backtest_adapter.py)
is the only entry point optimizer code calls to evaluate a pack:

```python
result: BacktestResult = run_backtest(
    pack,
    project_root=...,
    from_date=fold.val_start,
    to_date=fold.val_end,
    starting_equity=recipe.starting_equity,
    commission_bps=recipe.commission_bps,
    slippage_bps=recipe.slippage_bps,
)
```

Internally:

1. `compiler.to_ranking_weights(pack)` → dict passed to
   `research_loader.load_research_ranked_by_date(..., weights_override=...)`.
2. `compiler.to_risk_policy_config(pack)` → `RiskPolicyConfig` passed to
   `EngineBacktestRunner`.
3. `EngineBacktestRunner.run(ranked_by_date)` returns the same
   `BacktestResult(trades, equity_curve)` shape used elsewhere.

No new backtester code. The Phase 1 baseline-parity test
([`tests/research/optimization/test_baseline_parity.py`](../../tests/research/optimization/test_baseline_parity.py))
enforces that the adapter path is byte-identical to running the loader +
runner directly with engine defaults under the v1 yaml.

## Rule pack schema

[`StrategyRulePack`](../../src/ai_trading_system/domains/strategy/rule_pack.py)
is Pydantic, `extra="forbid"`. Phase 1 narrow scope:

| Section            | Drives                                         |
| ------------------ | ---------------------------------------------- |
| `ranking.weights`  | `compute_factor_scores(weights=...)`           |
| `risk.entry`       | `RiskPolicyConfig.entry`                       |
| `risk.stop`        | `RiskPolicyConfig.stop`                        |
| `risk.exit`        | `RiskPolicyConfig.exit`                        |
| `risk.sizing`      | `RiskPolicyConfig.sizing`                      |
| `risk.constraints` | `RiskPolicyConfig.constraints`                 |

`ranking.weights` is validated to sum to 1.0 over the seven
`FACTOR_KEYS`. Unknown factors are rejected. Empty `risk: {}` → engine
defaults (so the parity gate works against the v1 yaml).

Pattern detection, breakout-tier filters, event types, and screening
thresholds beyond what `apply_rank_eligibility` already supports are
explicitly out of scope. They land in a later phase or never, depending
on whether the basic-knob search converges to a useful champion first.

## Fitness composite

```
fitness =
   0.25 · CAGR
 + 0.20 · clamp(Sharpe, ±5) / 5
 + 0.10 · clamp(Sortino, ±5) / 5
 + 0.05 · win_rate
 - 0.30 · |max_drawdown_pct| / 100
 - 0.10 · max(0, turnover_per_year - 20) / 80
```

Weights live in `FitnessWeights` (in the recipe). **Do not retune these
based on outcomes** — that's meta-overfitting. Treat them as a governance
artifact: pick once, document why, change only with explicit review.

Top-winners oracle and capture-rate diagnostics are deliberately excluded
from fitness; they only enter reports.

## Acceptance gate (per-trial)

[`acceptance.is_accepted`](../../src/ai_trading_system/research/optimization/acceptance.py)
runs inside the Optuna objective. Hard rejects in order:

1. Zero-trade fold present.
2. Worst fold's total return < NIFTY return for that fold.
3. Worst fold MDD > baseline worst-fold MDD × 1.10.
4. Mean trades/year across folds < 40 (recipe-configurable).
5. Fewer than 60% of folds beat the baseline on fitness.
6. Mean fitness improvement over current champion < threshold.
7. Mean MDD vs champion > 1.10 ratio.

Verdict captured in `strategy_iteration_result.rejection_reason`. Rejected
trials are persisted (for diagnostic value) but never become champion.

## Champion guards (post-study, holistic)

[`guards.champion_guards`](../../src/ai_trading_system/research/optimization/guards.py)
runs after the study completes. Checks:

1. Zero-trade fold (redundant with acceptance — defensive).
2. Worst-fold return < NIFTY (idem).
3. **Parameter pinning**: > 50% of ranking weights at search-space
   bounds. Catches under-constrained sampling.

On pass → auto-advance `backtested → walkforward_passed`. On fail →
champion remains `backtested`; reason logged to
`strategy_optimization_run.error`.

## Walk-forward

[`walkforward.build_folds`](../../src/ai_trading_system/research/optimization/walkforward.py)
generates rolling `(train_start, train_end, val_start, val_end)` windows.
Defaults: 12-month train, 3-month validation, 3-month step. **Today the
optimizer only uses the validation window** — there's no separate model
fit on the training window because the rule pack doesn't fit anything
data-driven. The train window exists in the schema for the
LightGBM-style ML strategies that will plug in later.

## Storage

Migration `015_strategy_optimizer.sql` creates four tables in the same
control-plane DuckDB that `PipelineRegistry` (the existing pipeline
control plane, class name `RegistryStore`) manages. Migrations are
auto-applied on first connection — no separate runner.

| Table                          | Rows per study                                                                 |
| ------------------------------ | ------------------------------------------------------------------------------ |
| `strategy_rule_pack`           | one per unique pack (deduped by SHA256 of canonical JSON)                      |
| `strategy_optimization_run`    | one per study (UUID `optimization_run_id`)                                     |
| `strategy_iteration_result`    | one per (run, iteration, fold). `iteration=-1` is the baseline; `fold_index=-1` is the aggregate row |
| `strategy_backtest_trade`      | one per closed trade                                                           |

Unique indexes replace primary-key constraints on tables we UPDATE, to
work around DuckDB's UPDATE-on-indexed-row limitation. `complete_run`
and `set_lifecycle_status` use DELETE+INSERT.

`rule_pack_id` is SHA256 of the canonical JSON dump of the
`StrategyRulePack`. Two trials producing identical packs share one
`strategy_rule_pack` row.

## Lifecycle

```
draft
  └─ first persistence ──► backtested
                              │
                              ├─ champion_guards pass ──► walkforward_passed   (auto)
                              │
                              ├─ operator promote ──────► shadow
                              ├─ operator promote ──────► paper_approved
                              ├─ operator promote ──────► production_candidate
                              └─ operator promote ──────► active
```

Operators move past `walkforward_passed` only via the explicit CLI:

```
python -m ai_trading_system.research.optimization.promote \
    --rule-pack-id <hash> --to shadow --project-root .
```

Forward-only. The CLI rejects backwards transitions.

## Reports

[`reports.build_markdown_report(project_root, run_id)`](../../src/ai_trading_system/research/optimization/reports.py)
emits a self-contained markdown report:

- Run summary (status, window, trial count, baseline/champion pack hashes).
- Per-fold metrics tables for baseline and (if any) champion: fitness,
  CAGR, Sharpe, MDD, win-rate, trade count, total return vs NIFTY.
- Top-10 trials by fitness with acceptance verdict and rejection reason.

`reports.parameter_importances(study)` wraps
`optuna.importance.get_param_importances` for an in-memory study (Optuna
trial state is not persisted; the function is for live or pickled
studies).

## Running an optimisation

```bash
# Sample recipe ships at config/strategies/recipes/momentum_v1.yaml.
python -m ai_trading_system.research.optimization \
    --recipe config/strategies/recipes/momentum_v1.yaml \
    --project-root .
```

Outputs `optimization_run_id=… trials=… champion=… best_value=…`.
Inspect persisted state in the control-plane DuckDB:

```sql
SELECT * FROM strategy_optimization_run ORDER BY started_at DESC;
SELECT * FROM strategy_iteration_result
 WHERE optimization_run_id = '...' AND fold_index = -1
 ORDER BY fitness DESC LIMIT 10;
```

Then render the markdown report with `build_markdown_report` and inspect
the champion YAML via:

```python
from ai_trading_system.research.optimization.store import OptimizationStore
from ai_trading_system.domains.strategy.io import save_rule_pack
# Reconstruct StrategyRulePack from strategy_rule_pack.rule_yaml and save
# to config/strategies/champion_momentum_breakout.yaml when ready.
```

## Tests

| Suite                                              | Purpose                                                 |
| -------------------------------------------------- | ------------------------------------------------------- |
| `tests/domains/risk/`                              | Risk engine including intrabar stop firing              |
| `tests/research/backtesting/test_golden_backtest.py` | Locks engine numerical output                         |
| `tests/research/backtesting/test_research_loader.py::test_no_lookahead_truncation_invariance` | Locks no-lookahead property |
| `tests/domains/strategy/test_rule_pack.py`         | Schema validation + compiler + io                       |
| `tests/research/optimization/test_baseline_parity.py` | Hard parity gate (v1 yaml ≡ default backtest)        |
| `tests/research/optimization/test_walkforward.py`  | Fold scheduling                                         |
| `tests/research/optimization/test_evaluator.py`    | Metrics + fitness                                       |
| `tests/research/optimization/test_acceptance.py`   | Per-trial acceptance gate                               |
| `tests/research/optimization/test_guards.py`       | Final champion guards                                   |
| `tests/research/optimization/test_runner_integration.py` | Small Optuna study end-to-end                     |
| `tests/research/optimization/test_reports_and_promote.py` | Markdown report + lifecycle CLI                  |

104 tests total at the merge point (`7e437f8`).

## Future roadmap

### Phase 5 — shadow-only integration (planned)

- Add a research-domain rank entrypoint (separate script or
  `--strategy-rule-pack PATH` flag scoped to non-operational
  `data_domain`) that loads a YAML via `domains/strategy/io.py` and
  routes its weights + risk into the rank orchestrator.
- Live `rank` stage **refuses** the flag — assertion at the CLI
  boundary.
- Comparator (`compare_shadow_to_live.py`): for each session, diff
  shadow vs live `ranked_signals.csv`. Track rank correlation, top-N
  overlap, in/out churn.
- Recipe-configurable minimum shadow days before Phase 6 promotion.

### Phase 6 — live rule-pack ingestion (planned)

- Drop the `data_domain != "operational"` assertion at the rank CLI.
- Live `rank` accepts `--strategy-rule-pack PATH`.
- Promotion CLI advances `shadow → paper_approved → production_candidate
  → active` and copies the champion YAML to `config/strategies/active/`.
- No auto-promotion. Every transition is explicit and logged.

### Beyond Phase 6

- **Screening thresholds** in the rule pack. `apply_rank_eligibility` is
  fully parameterised but the research loader doesn't apply it today;
  adding it as a Phase 7 broadens the search space (min_price,
  min_liquidity_score, stage2_min_score) without touching the engine.
- **Point-in-time universe**. The current universe loader pulls active
  NSE symbols, so 2021–2022 backtests carry survivorship bias. Document
  the bias today; replace with a point-in-time membership table when
  reliable historical NSE membership is available.
- **Event and pattern rules** in the schema. Requires lifting
  `EventBacktester`'s hardcoded event detectors into the rule pack
  (PR-sized refactor). Justify only after weight + risk search has been
  shown to plateau.
- **Equity-curve and capture-rate visuals** in reports. Markdown table
  output is enough for ops review today; HTML+plot is one Jinja template
  + Plotly call away.
- **Multi-strategy registry**. Today the optimizer searches one
  `strategy_id` per recipe. A future ladder lets multiple
  strategies coexist as parallel champions, each with its own lifecycle.
- **Distributed Optuna**. For studies above ~100 trials, switch to
  `optuna.create_study(storage=...)` against a shared backend so trials
  can fan out across workers.

## Risks and known limitations

1. **Survivorship bias** in the symbol universe (above). Real today; not
   the optimizer's bug to fix but the optimizer's results inherit it.
2. **Cost-model conservatism**. 35 bps slippage default for Indian
   mid-caps is a working estimate, not validated against real fills.
   Worth backtesting against actual paper-trading fills once available.
3. **Fitness weight tuning** is meta-overfitting if done from outcomes.
   Treat the weights as governance config.
4. **DuckDB UPDATE limitation** forces DELETE+INSERT in two paths. Watch
   for DuckDB releases that fix this and simplify `store.py`.
5. **Top-winners oracle** is not yet wired into reports. The
   `winner_capture` module exists separately; integration is a Phase 4
   follow-up (not blocking).
