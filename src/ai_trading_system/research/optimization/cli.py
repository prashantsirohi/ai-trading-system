"""Optimizer CLI: ``ai-trading-optimize`` (or ``python -m ai_trading_system.research.optimization``).

Subcommand layout (sniffed at top of ``main`` so the legacy flat form keeps
working unchanged):

    ai-trading-optimize init <name> [--force]
        Scaffold config/strategies/<name>_v1.yaml + config/strategies/recipes/<name>.yaml
        from templates. Refuses to overwrite unless --force.

    ai-trading-optimize validate <recipe> [--with-backtest]
        Dry-run a recipe: load the YAML (Pydantic), resolve baseline_pack_path,
        load the baseline pack. With --with-backtest also runs a single
        baseline backtest on the first walk-forward fold to confirm
        compiler+engine wiring (slower, needs research OHLCV seeded).

    ai-trading-optimize run --recipe <name-or-path> [flags]
    ai-trading-optimize --recipe <name-or-path> [flags]            (legacy form)
        Execute one Optuna study end-to-end. Both forms are equivalent.

Bare recipe names (no ``/`` or ``.yaml`` suffix) are resolved to
``<project_root>/config/strategies/recipes/<name>.yaml``.
"""

from __future__ import annotations

import argparse
import logging
import string
import sys
import warnings
from datetime import date
from importlib import resources
from pathlib import Path
from typing import Sequence

from ai_trading_system.domains.strategy import load_rule_pack
from ai_trading_system.research.optimization.recipe import (
    load_recipe,
    resolve_baseline_path,
)

# NOTE: ``runner.run_optimization`` is intentionally imported lazily inside
# ``_cmd_run`` so that ``init`` and ``validate`` do not pay the ``import optuna``
# cost (and so they remain usable in environments where optuna is not yet
# installed).


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Recipe path resolution
# ---------------------------------------------------------------------------


def _resolve_recipe_path(value: str, project_root: Path) -> Path:
    """Resolve a recipe argument that may be a bare name or a literal path."""
    if "/" in value or value.endswith((".yaml", ".yml")):
        p = Path(value)
        return p if p.is_absolute() else (project_root / p)
    return project_root / "config" / "strategies" / "recipes" / f"{value}.yaml"


# ---------------------------------------------------------------------------
# Pandas warning suppression (shared across subcommands)
# ---------------------------------------------------------------------------


def _silence_pandas_future_warnings() -> None:
    """Pandas 2.x ships preview-CoW warnings on assignments that are correct
    today but will change in 3.0. Cosmetic for the optimizer's backtest hot
    path; suppress only inside the optimizer process so other code paths still
    see them and can be fixed over time.
    """
    warnings.filterwarnings("ignore", category=FutureWarning, message=r".*ChainedAssignmentError.*")
    warnings.filterwarnings("ignore", category=FutureWarning, message=r".*Downcasting object dtype arrays.*")


# ---------------------------------------------------------------------------
# init subcommand
# ---------------------------------------------------------------------------


def _read_template(name: str) -> string.Template:
    """Read a packaged template file via importlib.resources."""
    text = resources.files("ai_trading_system.research.optimization.templates").joinpath(name).read_text()
    return string.Template(text)


def _cmd_init(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="ai-trading-optimize init",
        description=(
            "Scaffold a rule pack and recipe pair under config/strategies/. "
            "Refuses to overwrite either file unless --force is passed."
        ),
    )
    parser.add_argument("name", help="Recipe + rule pack name (also used as strategy_id by default).")
    parser.add_argument("--strategy-id", default=None, help="Override the strategy_id field (default: NAME).")
    parser.add_argument("--project-root", default=".", help="Project root (default: cwd).")
    parser.add_argument("--force", action="store_true", help="Overwrite existing files.")
    args = parser.parse_args(argv)

    project_root = Path(args.project_root).resolve()
    strategy_id = args.strategy_id or args.name

    rule_pack_path = project_root / "config" / "strategies" / f"{args.name}_v1.yaml"
    recipe_path = project_root / "config" / "strategies" / "recipes" / f"{args.name}.yaml"

    existing = [p for p in (rule_pack_path, recipe_path) if p.exists()]
    if existing and not args.force:
        print("error: file(s) already exist; pass --force to overwrite:")
        for p in existing:
            print(f"  - {p}")
        return 2

    rule_pack_path.parent.mkdir(parents=True, exist_ok=True)
    recipe_path.parent.mkdir(parents=True, exist_ok=True)

    today = date.today().isoformat()
    # Default to a 4-year window ending today.
    from_date = today.replace(today[:4], str(int(today[:4]) - 4), 1)

    rule_pack_path.write_text(
        _read_template("rule_pack.yaml.tmpl").substitute(
            name=args.name, strategy_id=strategy_id
        )
    )
    recipe_path.write_text(
        _read_template("recipe.yaml.tmpl").substitute(
            name=args.name,
            strategy_id=strategy_id,
            from_date=from_date,
            to_date=today,
        )
    )

    print(f"created: {rule_pack_path}")
    print(f"created: {recipe_path}")
    print()
    print("Next steps:")
    print(f"  1. Edit {recipe_path} (window, fitness weights, stopping rules).")
    print(f"  2. ai-trading-optimize validate {args.name}")
    print(f"  3. ai-trading-optimize run --recipe {args.name}")
    return 0


# ---------------------------------------------------------------------------
# validate subcommand
# ---------------------------------------------------------------------------


def _cmd_validate(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="ai-trading-optimize validate",
        description=(
            "Dry-run a recipe: validate schema, resolve baseline path, load pack. "
            "With --with-backtest also runs a single baseline backtest on the "
            "first walk-forward fold to confirm compiler+engine wiring."
        ),
    )
    parser.add_argument("recipe", help="Recipe name or path.")
    parser.add_argument("--project-root", default=".", help="Project root (default: cwd).")
    parser.add_argument(
        "--with-backtest",
        action="store_true",
        help="Also run a one-fold baseline backtest to confirm engine wiring (slower).",
    )
    args = parser.parse_args(argv)

    project_root = Path(args.project_root).resolve()
    recipe_path = _resolve_recipe_path(args.recipe, project_root)
    if not recipe_path.exists():
        print(f"error: recipe not found: {recipe_path}")
        return 2

    # Step 1 — recipe schema
    try:
        recipe = load_recipe(recipe_path)
    except Exception as exc:  # noqa: BLE001
        print(f"error: recipe schema invalid ({recipe_path}):\n  {type(exc).__name__}: {exc}")
        return 1

    # Step 2 — baseline pack path
    baseline_full = resolve_baseline_path(recipe.baseline_pack_path, project_root=project_root)
    if not baseline_full.exists():
        print(
            f"error: baseline pack not found at {baseline_full}\n"
            f"  recipe baseline_pack_path: {recipe.baseline_pack_path!r}\n"
            f"  (bare names resolve to <project_root>/config/strategies/<name>.yaml)"
        )
        return 1

    # Step 3 — baseline pack schema
    try:
        baseline_pack = load_rule_pack(baseline_full)
    except Exception as exc:  # noqa: BLE001
        print(f"error: baseline pack schema invalid ({baseline_full}):\n  {type(exc).__name__}: {exc}")
        return 1

    print("OK: recipe schema, baseline path, baseline pack schema all valid.")
    print(f"  recipe:        {recipe_path}")
    print(f"  baseline pack: {baseline_full}")
    print(f"  strategy_id:   {recipe.strategy_id}")
    print(f"  window:        {recipe.from_date} → {recipe.to_date}")
    print(f"  walkforward:   train={recipe.walkforward.train_months}m val={recipe.walkforward.validation_months}m step={recipe.walkforward.step_months}m")
    print(f"  stopping:      max_trials={recipe.stopping.max_trials} patience={recipe.stopping.patience}")

    if not args.with_backtest:
        return 0

    # Step 4 — optional one-fold baseline backtest (real engine wiring check)
    print()
    print("running --with-backtest: one-fold baseline backtest...")
    # Imports here so the cheap path doesn't pay the optuna / engine import cost.
    from ai_trading_system.research.optimization.backtest_adapter import run_backtest
    from ai_trading_system.research.optimization.baselines import benchmark_buyhold_return
    from ai_trading_system.research.optimization.walkforward import build_folds

    folds = build_folds(
        recipe.from_date,
        recipe.to_date,
        train_months=recipe.walkforward.train_months,
        validation_months=recipe.walkforward.validation_months,
        step_months=recipe.walkforward.step_months,
    )
    if not folds:
        print(
            f"error: no walk-forward folds fit in [{recipe.from_date}, {recipe.to_date}] "
            f"with train={recipe.walkforward.train_months}m val={recipe.walkforward.validation_months}m"
        )
        return 1

    fold = folds[0]
    try:
        bench = benchmark_buyhold_return(
            project_root, benchmark=recipe.benchmark,
            from_date=fold.val_start, to_date=fold.val_end, exchange=recipe.exchange,
        )
        bench_pct = bench.return_pct if bench is not None else None
        backtest_result = run_backtest(
            baseline_pack, fold=fold, recipe=recipe, project_root=project_root,
            benchmark_return_pct=bench_pct,
        )
    except Exception as exc:  # noqa: BLE001 — surface engine wiring issues clearly
        print(f"error: backtest wiring failed on fold 0 ({fold.val_start}..{fold.val_end}):\n  {type(exc).__name__}: {exc}")
        return 1

    print(
        "OK: baseline backtest completed on fold 0 "
        f"({fold.val_start}..{fold.val_end}); "
        f"trades={getattr(backtest_result, 'trade_count', '?')}"
    )
    return 0


# ---------------------------------------------------------------------------
# run subcommand (also the legacy default)
# ---------------------------------------------------------------------------


def _build_run_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a strategy optimisation study.",
        epilog=(
            "RECIPE may be a bare recipe name (e.g. 'momentum_breakout_optuna_v1'), "
            "which resolves to config/strategies/recipes/<name>.yaml, or a literal path."
        ),
    )
    parser.add_argument("--recipe", required=True, help="Recipe name or path to OptimizationRecipe YAML")
    parser.add_argument(
        "--project-root",
        default=".",
        help="Project root containing data/ and config/ (default: cwd)",
    )
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--quiet-pandas-warnings",
        action="store_true",
        default=True,
        help="Suppress pandas FutureWarnings during the run (default: on)",
    )
    parser.add_argument(
        "--show-pandas-warnings",
        dest="quiet_pandas_warnings",
        action="store_false",
        help="Show pandas FutureWarnings (useful for debugging)",
    )
    parser.add_argument(
        "--no-report",
        action="store_true",
        help=(
            "Skip auto-writing the markdown report at run end. "
            "Default behaviour writes to reports/optimization/<recipe>/{<run_id>,latest}.md."
        ),
    )
    return parser


def _cmd_run(argv: Sequence[str]) -> int:
    parser = _build_run_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.quiet_pandas_warnings:
        _silence_pandas_future_warnings()

    project_root = Path(args.project_root)
    recipe_path = _resolve_recipe_path(args.recipe, project_root)
    if not recipe_path.exists():
        parser.error(f"recipe not found: {recipe_path}")
    recipe = load_recipe(recipe_path)
    # Lazy import so init/validate don't pay the optuna import cost.
    from ai_trading_system.research.optimization.runner import run_optimization
    result = run_optimization(
        recipe,
        project_root=project_root,
        write_report=not args.no_report,
    )
    print(
        f"optimization_run_id={result['optimization_run_id']} "
        f"trials={result['trials']} "
        f"champion={result['champion_rule_pack_id']} "
        f"best_value={result['best_value']}"
    )
    if result.get("report_path"):
        # Last line on stdout — easy for scripts to capture.
        print(f"report={result['report_path']}")
    return 0


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


_SUBCOMMANDS = {
    "init": _cmd_init,
    "validate": _cmd_validate,
    "run": _cmd_run,
}


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if argv and argv[0] in _SUBCOMMANDS:
        return _SUBCOMMANDS[argv[0]](argv[1:])
    # Legacy flat form: ai-trading-optimize --recipe ...
    return _cmd_run(argv)


if __name__ == "__main__":
    raise SystemExit(main())
