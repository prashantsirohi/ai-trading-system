# Runbook: Strategy Optimization

- **Purpose:** End-to-end operator workflow for running a strategy optimization study: write recipe → run → review report → promote champion.
- **Audience:** Operator, researcher.
- **Last verified:** 2026-05-16
- **Source of truth:**
  - [`src/ai_trading_system/research/optimization/cli.py`](../../src/ai_trading_system/research/optimization/cli.py)
  - [`src/ai_trading_system/research/optimization/runner.py`](../../src/ai_trading_system/research/optimization/runner.py)
  - [`src/ai_trading_system/research/optimization/promote.py`](../../src/ai_trading_system/research/optimization/promote.py)
  - [`docs/domains/optimization_domain.md`](../domains/optimization_domain.md)

---

## Daily / weekly flow

### 1. Pick an existing recipe (or scaffold a new one)

Existing recipes live under `config/strategies/recipes/`. Today there is one: `momentum_breakout_optuna_v1.yaml`.

A recipe references a baseline rule pack (under `config/strategies/`) and configures the Optuna search: date window, benchmark, walk-forward folds, fitness weights, acceptance thresholds, stopping rules. See [`docs/domains/optimization_domain.md`](../domains/optimization_domain.md) for the field-by-field reference.

### 1a. Scaffold a new recipe + rule pack (init)

```bash
ai-trading-optimize init my_strategy
```

Creates:
- `config/strategies/my_strategy_v1.yaml` — minimal rule pack (factor weights summing to 1.0, empty risk block).
- `config/strategies/recipes/my_strategy.yaml` — minimal recipe pointing at the rule pack with a 4-year window ending today.

Refuses to overwrite either file unless `--force` is passed. The recipe's `baseline_pack_path` is set to the bare name `my_strategy_v1` (resolved to `config/strategies/my_strategy_v1.yaml` at run time — see "Bare-name lookups" below). Tune the YAML files before running.

### 1b. Dry-run the recipe (validate)

```bash
# Cheap checks (default): schema + path + pack load.
ai-trading-optimize validate my_strategy

# Engine wiring check (slower; needs research OHLCV seeded):
ai-trading-optimize validate my_strategy --with-backtest
```

Without `--with-backtest`, validate is fast (no Optuna, no engine import) and only confirms:

1. Recipe YAML parses against `OptimizationRecipe`.
2. `baseline_pack_path` resolves to an existing file.
3. The baseline rule pack parses against `StrategyRulePack`.

With `--with-backtest`, it additionally builds the first walk-forward fold and runs a single baseline backtest to surface compiler/engine wiring failures before you commit Optuna trial budget. Exits non-zero on any failure.

### 2. Run the optimization

```bash
# Bare recipe name resolves to config/strategies/recipes/<name>.yaml
ai-trading-optimize run --recipe momentum_breakout_optuna_v1

# Legacy flat form (no subcommand) — equivalent to the above
ai-trading-optimize --recipe momentum_breakout_optuna_v1
```

Or with a literal path (backwards compatible):

```bash
ai-trading-optimize run --recipe config/strategies/recipes/momentum_breakout_optuna_v1.yaml
```

### Bare-name lookups

Two places accept bare names instead of literal paths:

| Where | Bare name | Resolves to |
|---|---|---|
| `--recipe <name>` (CLI) | `momentum_breakout_optuna_v1` | `<project_root>/config/strategies/recipes/momentum_breakout_optuna_v1.yaml` |
| `baseline_pack_path:` (recipe YAML) | `momentum_breakout_v1` | `<project_root>/config/strategies/momentum_breakout_v1.yaml` |

In both, a value containing `/` or ending in `.yaml`/`.yml` is treated as a literal path (relative paths resolve against `--project-root`, absolute paths pass through unchanged).

Useful flags:

| Flag | Effect |
|---|---|
| `--project-root <path>` | Override repo root (defaults to cwd) |
| `--log-level DEBUG` | Verbose Optuna + adapter logging |
| `--show-pandas-warnings` | Re-enable suppressed pandas FutureWarnings (debugging) |
| `--no-report` | Skip the auto-written markdown report (CI/scripted callers) |

During the run you'll see a tqdm progress bar — trial count, current fitness, accepted/rejected status, and a 👑 marker if a champion exists.

### 3. Read the auto-written report

When the run completes, the CLI prints:

```
optimization_run_id=<hex> trials=<n> champion=<pack_hash> best_value=<float>
report=<path>
```

The report is auto-written to:

- `reports/optimization/<recipe>/<optimization_run_id>.md` — immutable per run
- `reports/optimization/<recipe>/latest.md` — overwritten every run (operator bookmark)

The report (see [`reports.py::build_markdown_report`](../../src/ai_trading_system/research/optimization/reports.py)) contains:

- Run header: recipe, status, date range, trial count, baseline/champion pack IDs
- Baseline per-fold metrics: fitness, CAGR, Sharpe, MDD, win rate, trade count, return vs benchmark
- Champion per-fold metrics (if one was accepted)
- Top 10 trials by fitness with rejection reasons for those that failed acceptance

### 4. Promote the champion

If the report looks good, promote the latest champion to `shadow` with one command:

```bash
ai-trading-optimize-promote promote-latest --recipe-name momentum_breakout_optuna_v1
# defaults: --to shadow
```

You can also promote to a later lifecycle stage in one shot:

```bash
ai-trading-optimize-promote promote-latest \
  --recipe-name momentum_breakout_optuna_v1 \
  --to paper_approved
```

If you already know the 40-character `rule_pack_id` (e.g. from the report), the original explicit form still works:

```bash
ai-trading-optimize-promote --rule-pack-id <hash> --to shadow
```

### 5. Verify the lifecycle change

```bash
duckdb data/control_plane.duckdb \
  -c "SELECT rule_pack_id, lifecycle_status FROM strategy_rule_pack ORDER BY created_at DESC LIMIT 5;"
```

The champion's row should now read the new lifecycle status.

---

## Lifecycle ladder

Promotion is one-way (`promote.py::LIFECYCLE_ORDER`):

```
draft → backtested → walkforward_passed → shadow
      → paper_approved → production_candidate → active
```

After a successful run with passing champion guards, the champion lands at `walkforward_passed` automatically. Everything beyond is a manual operator decision.

## Common pitfalls

### "no completed run with a champion found"

The latest run for that recipe either:

- never reached `status='completed'` (failed mid-run), or
- completed but all trials were rejected by the acceptance gate (so no champion).

Re-inspect the report and the `strategy_optimization_run` row:

```bash
duckdb data/control_plane.duckdb \
  -c "SELECT optimization_run_id, status, champion_rule_pack_id, error
      FROM strategy_optimization_run
      WHERE recipe_name = 'momentum_breakout_optuna_v1'
      ORDER BY started_at DESC LIMIT 5;"
```

### Report didn't get written

The runner catches all exceptions from the report writer so a report failure never fails a run — but it does log a warning. Check the CLI log for `auto-report write failed for run_id=...`. The report can be regenerated on demand:

```python
from ai_trading_system.research.optimization.reports import write_report
from pathlib import Path
write_report(Path("."), "<optimization_run_id>", Path("reports/optimization/<recipe>/<run_id>.md"))
```

### Preflight / data-source errors before any trials

The optimizer relies on operational OHLCV data (`data/ohlcv.duckdb`) and the rest of the platform's wiring. If a run dies before the first trial, run the standard pipeline preflight first:

```bash
ai-trading-pipeline --run-preflight --stages ingest,features
```

See [`docs/runbooks/troubleshooting.md`](troubleshooting.md) for general data-side issues.

## See also

- [`docs/domains/optimization_domain.md`](../domains/optimization_domain.md)
- [`docs/domains/research_domain.md`](../domains/research_domain.md)
- [`docs/_legacy/archived_2026-05-16/architecture_strategy-optimizer.md`](../_legacy/archived_2026-05-16/architecture_strategy-optimizer.md) — deeper historical design context
