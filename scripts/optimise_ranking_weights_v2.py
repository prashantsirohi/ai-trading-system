"""v2 walk-forward ranking-weight optimiser (CLI).

Uses production factor scores (rel_strength_score, prox_high_score, ...) from
the live ranker's scoring path. Output candidate config can be merged into
``rank_factor_weights.json`` directly — no proxy-to-production translation.

Usage:
    uv run python scripts/optimise_ranking_weights_v2.py
    uv run python scripts/optimise_ranking_weights_v2.py --n-trials 200
    uv run python scripts/optimise_ranking_weights_v2.py --objective combined
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np

from ai_trading_system.research.ranking_optimisation import (
    DEFAULT_CANDIDATE_CONFIG_PATH,
    DEFAULT_COMPARISON_REPORT_PATH,
    DEFAULT_WALKFORWARD_JSON_PATH,
    WEIGHT_KEYS,
    build_comparison_report,
    load_production_weights,
    run_walkforward_v2,
    write_candidate_config,
    write_walkforward_json,
)
from ai_trading_system.research.ranking_optimisation.promote import (
    _mean_weights_across_folds,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", type=int, nargs="+", default=[2020, 2021, 2022, 2023, 2024])
    parser.add_argument("--horizon-days", type=int, default=20)
    parser.add_argument("--top-n", type=int, default=100)
    parser.add_argument("--n-trials", type=int, default=200)
    parser.add_argument("--min-train-years", type=int, default=3)
    parser.add_argument("--rebalance-freq", default="quarterly", choices=["quarterly"])
    parser.add_argument(
        "--objective",
        default="combined",
        choices=["ic_only", "lift_only", "hit_only", "combined"],
    )
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--candidate",
        type=Path,
        default=DEFAULT_CANDIDATE_CONFIG_PATH,
        help="Output JSON path for candidate weights (never production).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_WALKFORWARD_JSON_PATH,
        help="Walk-forward result JSON output.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=DEFAULT_COMPARISON_REPORT_PATH,
        help="Markdown side-by-side comparison report.",
    )
    args = parser.parse_args(argv)

    print(f"Years           : {args.years}")
    print(f"Horizon         : {args.horizon_days} trading days")
    print(f"Top-N           : {args.top_n}")
    print(f"Trials per fold : {args.n_trials}")
    print(f"Objective       : {args.objective}")
    print(f"Rebalance freq  : {args.rebalance_freq}")
    print()

    result = run_walkforward_v2(
        years=args.years,
        horizon_days=args.horizon_days,
        top_n=args.top_n,
        n_trials=args.n_trials,
        min_train_years=args.min_train_years,
        project_root=args.project_root,
        rebalance_freq=args.rebalance_freq,
        objective_mode=args.objective,
        seed=args.seed,
        log=True,
    )

    if not result.folds:
        print("\nNo OOS folds produced. Aborting before writing artifacts.")
        return 1

    candidate = _mean_weights_across_folds(result)
    candidate_path = write_candidate_config(candidate, target=args.candidate)
    out_path = write_walkforward_json(result, target=args.out, candidate_weights=candidate)
    report_path = build_comparison_report(result, target=args.report, candidate_weights=candidate)

    production = load_production_weights()
    print("\n" + "=" * 88)
    print("CANDIDATE WEIGHTS (mean across folds)  vs  PRODUCTION")
    print("=" * 88)
    for key in WEIGHT_KEYS:
        p = float(production.get(key, 0.0))
        c = float(candidate.get(key, 0.0))
        delta = c - p
        bar = "█" * int(round(c * 40))
        print(f"  {key:<24} prod={p:.3f}  cand={c:.3f}  Δ={delta:+.3f}  {bar}")

    summary_ic   = float(np.nanmean([f.oos_mean_ic for f in result.folds]))
    summary_lift = float(np.nanmean([f.oos_mean_lift for f in result.folds]))
    summary_hit  = float(np.nanmean([f.oos_mean_hit for f in result.folds]))
    print()
    print(f"OOS mean IC   : {summary_ic:+.3f}")
    print(f"OOS mean lift : {summary_lift:+.1%}")
    print(f"OOS mean hit  : {summary_hit:.0%}")
    print()
    print(f"Candidate config : {candidate_path}")
    print(f"Walk-forward JSON: {out_path}")
    print(f"Comparison report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
