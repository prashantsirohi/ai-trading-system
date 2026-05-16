"""Optimizer CLI: ``ai-trading-optimize`` (or ``python -m ai_trading_system.research.optimization``)."""

from __future__ import annotations

import argparse
import logging
import warnings
from pathlib import Path

from ai_trading_system.research.optimization.recipe import load_recipe
from ai_trading_system.research.optimization.runner import run_optimization


def _resolve_recipe_path(value: str, project_root: Path) -> Path:
    """Resolve a recipe argument that may be a bare name or a literal path.

    - If ``value`` contains a path separator or ends in ``.yaml``/``.yml``, treat
      as a literal path (current behaviour).
    - Otherwise look up ``<project_root>/config/strategies/recipes/<value>.yaml``.

    The literal-path form is preserved for backwards compatibility and scripts.
    The bare-name form is the operator-friendly default.
    """
    if "/" in value or value.endswith((".yaml", ".yml")):
        return Path(value)
    return project_root / "config" / "strategies" / "recipes" / f"{value}.yaml"


def _silence_pandas_future_warnings() -> None:
    """Pandas 2.x ships preview-CoW warnings on assignments that are correct
    today but will change in 3.0. They are cosmetic for the optimizer's
    backtest hot path; suppress only inside the optimizer process so other
    code paths still see them and can be fixed over time.
    """
    warnings.filterwarnings(
        "ignore",
        category=FutureWarning,
        message=r".*ChainedAssignmentError.*",
    )
    warnings.filterwarnings(
        "ignore",
        category=FutureWarning,
        message=r".*Downcasting object dtype arrays.*",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a strategy optimisation study.",
        epilog=(
            "RECIPE may be a bare recipe name (e.g. 'momentum_breakout_optuna_v1'), "
            "which resolves to config/strategies/recipes/<name>.yaml, or a literal path."
        ),
    )
    parser.add_argument(
        "--recipe",
        required=True,
        help="Recipe name or path to OptimizationRecipe YAML",
    )
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


if __name__ == "__main__":
    raise SystemExit(main())
