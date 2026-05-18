"""Walk-forward ranking-weight optimiser CLI.

Searches for ranking-factor weights that maximise the rank correlation
between produced composite scores and realised forward returns. Walks
forward year-by-year so each weight set is tested on years it was not
fitted on.

Usage:
    uv run python scripts/optimise_ranking_weights.py
    uv run python scripts/optimise_ranking_weights.py --years 2020 2021 2022 2023 2024
    uv run python scripts/optimise_ranking_weights.py --n-trials 200 --top-n 100
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

from ai_trading_system.research.ranking_optimisation import (
    FACTOR_NAMES,
    run_walkforward,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=[2020, 2021, 2022, 2023, 2024],
        help="Calendar years to use (chronological).",
    )
    parser.add_argument("--horizon-days", type=int, default=252)
    parser.add_argument("--top-n", type=int, default=100)
    parser.add_argument("--n-trials", type=int, default=100)
    parser.add_argument("--min-train-years", type=int, default=3)
    parser.add_argument(
        "--min-turnover-cr",
        type=float,
        default=1.0,
        help="Liquidity floor in Crores of daily rupee turnover.",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Optional path to write final per-fold table as JSON.",
    )
    args = parser.parse_args(argv)

    print(f"Years           : {args.years}")
    print(f"Horizon         : {args.horizon_days} trading days")
    print(f"Top-N           : {args.top_n}")
    print(f"Trials per fold : {args.n_trials}")
    print(f"Min train years : {args.min_train_years}")
    print(f"Factors searched: {', '.join(FACTOR_NAMES)}")
    print()

    result = run_walkforward(
        years=args.years,
        horizon_days=args.horizon_days,
        top_n=args.top_n,
        n_trials=args.n_trials,
        min_train_years=args.min_train_years,
        min_turnover_crores=args.min_turnover_cr,
        seed=args.seed,
        log=True,
    )

    df = result.to_dataframe()
    if df.empty:
        print("\nNo OOS folds produced.")
        return 1

    weight_cols = [c for c in df.columns if c.startswith("w_")]
    weights_mean = df[weight_cols].mean()
    weights_std = df[weight_cols].std()

    print("\n" + "=" * 88)
    print("OOS PERFORMANCE BY FOLD")
    print("=" * 88)
    print(
        df[["test_year", "train_ic", "oos_ic", "oos_hit", "oos_lift", "oos_n"]].to_string(
            index=False,
            formatters={
                "train_ic": "{:+.3f}".format,
                "oos_ic":   "{:+.3f}".format,
                "oos_hit":  "{:.0%}".format,
                "oos_lift": "{:+.1%}".format,
            },
        )
    )

    print("\n" + "=" * 88)
    print("WEIGHTS — mean ± std across folds (sorted by mean weight)")
    print("=" * 88)
    rows = sorted(zip(weight_cols, weights_mean, weights_std), key=lambda r: -r[1])
    for col, m, s in rows:
        factor = col[2:]
        bar = "█" * int(round(m * 40))
        print(f"  {factor:<22} {m:.3f} ± {s:.3f}  {bar}")

    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "years": args.years,
            "horizon_days": args.horizon_days,
            "top_n": args.top_n,
            "n_trials": args.n_trials,
            "folds": df.to_dict(orient="records"),
            "weights_mean": {col[2:]: float(weights_mean[col]) for col in weight_cols},
            "weights_std":  {col[2:]: float(weights_std[col])  for col in weight_cols},
            "summary": {
                "mean_oos_ic":   float(np.nanmean(df["oos_ic"])),
                "mean_oos_hit":  float(np.nanmean(df["oos_hit"])),
                "mean_oos_lift": float(np.nanmean(df["oos_lift"])),
            },
        }
        args.out.write_text(json.dumps(payload, indent=2, default=float))
        print(f"\nWrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
