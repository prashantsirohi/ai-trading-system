"""Optimizer CLI: ``python -m ai_trading_system.research.optimization``."""

from __future__ import annotations

import argparse
import logging
import warnings
from pathlib import Path

from ai_trading_system.research.optimization.recipe import load_recipe
from ai_trading_system.research.optimization.runner import run_optimization


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
    parser = argparse.ArgumentParser(description="Run a strategy optimisation study.")
    parser.add_argument("--recipe", required=True, help="Path to OptimizationRecipe YAML")
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
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.quiet_pandas_warnings:
        _silence_pandas_future_warnings()

    recipe = load_recipe(args.recipe)
    result = run_optimization(recipe, project_root=Path(args.project_root))
    print(
        f"optimization_run_id={result['optimization_run_id']} "
        f"trials={result['trials']} "
        f"champion={result['champion_rule_pack_id']} "
        f"best_value={result['best_value']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
