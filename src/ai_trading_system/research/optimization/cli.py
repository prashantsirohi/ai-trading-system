"""Optimizer CLI: ``python -m ai_trading_system.research.optimization``."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from ai_trading_system.research.optimization.recipe import load_recipe
from ai_trading_system.research.optimization.runner import run_optimization


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a strategy optimisation study.")
    parser.add_argument("--recipe", required=True, help="Path to OptimizationRecipe YAML")
    parser.add_argument(
        "--project-root",
        default=".",
        help="Project root containing data/ and config/ (default: cwd)",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

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
