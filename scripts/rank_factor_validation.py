"""One-year ranking-factor validation.

Question: of the simple price/volume factors used in our ranker, which ones
actually correlate with the year's top price-action winners?

For a given as-of date and horizon:
  1. Build a universe of NSE symbols with enough history (>= 252 days before
     as-of, full data through as-of + horizon).
  2. Compute factor values for every symbol as of as-of date.
  3. Compute forward return from as-of to as-of + horizon as ground truth.
  4. Define winners = top-N by forward return.
  5. For each factor, report Spearman IC, top-N hit rate, and decile lift.

Usage:
    uv run python scripts/rank_factor_validation.py --as-of 2023-01-02
    uv run python scripts/rank_factor_validation.py --as-of 2022-01-03 --top-n 50
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from ai_trading_system.research.ranking_optimisation.data import (
    DEFAULT_DB_PATH,
    FACTOR_NAMES,
    FactorPanel,
    load_factor_panel,
)
from ai_trading_system.research.ranking_optimisation.fitness import score_weights

# Proxy of DEFAULT_FACTOR_WEIGHTS for factors derivable from price+volume only.
# Original ranker weights (from contracts.py): relative_strength 0.38,
# trend_persistence 0.22, proximity_highs 0.18, sector_strength 0.22 (omitted),
# remainder 0. Renormalise non-omitted to sum to 1.0 for like-for-like comparison.
CURRENT_WEIGHTS_PROXY = {
    "rs_12_1":           0.38 / 0.78,
    "trend_persistence": 0.22 / 0.78,
    "prox_52w_high":     0.18 / 0.78,
}


@dataclass
class FactorRow:
    name: str
    description: str
    ic: float
    hit_rate: float
    top_decile_lift: float
    n: int


def _evaluate_single_factor(
    panel: FactorPanel, factor: str, top_n: int
) -> FactorRow:
    df = panel.df
    sub = df[[factor, "forward_return"]].dropna()
    if sub.empty:
        return FactorRow(factor, "", float("nan"), float("nan"), float("nan"), 0)
    ic = sub[factor].corr(sub["forward_return"], method="spearman")
    top_by_factor = sub.nlargest(top_n, factor).index
    top_by_return = sub["forward_return"].nlargest(top_n).index
    hit_rate = len(set(top_by_factor) & set(top_by_return)) / top_n
    deciles = pd.qcut(sub[factor].rank(method="first"), 10, labels=False)
    top_decile_mean = sub.loc[deciles == 9, "forward_return"].mean()
    overall_mean = sub["forward_return"].mean()
    return FactorRow(
        factor, "", float(ic), float(hit_rate), float(top_decile_mean - overall_mean), len(sub)
    )


_DESCRIPTIONS = {
    "rs_12_1":           "12-1 month momentum (skip-1)",
    "rs_6m":             "6-month price return",
    "rs_3m":             "3-month price return",
    "prox_52w_high":     "close / 52w-high (proximity)",
    "above_200dma_pct":  "(close - sma200) / sma200",
    "trend_persistence": "% of last 50d closed above sma50",
    "volume_ratio":      "median 20d vol / median 100d vol",
    "low_vol":           "-1 * std of daily returns last 50d",
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default="2023-01-02", help="Cross-section date (anchor)")
    parser.add_argument("--horizon-days", type=int, default=252, help="Forward window (trading days)")
    parser.add_argument("--top-n", type=int, default=100, help="Define 'winners' as top-N by forward return")
    parser.add_argument("--min-turnover-cr", type=float, default=1.0, help="Liquidity floor (Crores)")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    args = parser.parse_args(argv)

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 1

    print(f"Loading panel as-of {args.as_of} horizon={args.horizon_days}d top_n={args.top_n}")
    panel = load_factor_panel(
        args.as_of,
        horizon_days=args.horizon_days,
        db_path=db_path,
        min_turnover_crores=args.min_turnover_cr,
    )
    if panel.n == 0:
        print("No factor rows.")
        return 1
    print(f"  symbols after liquidity filter (>= ₹{args.min_turnover_cr} Cr/day): {panel.n}")

    df = panel.df
    if len(df) < args.top_n * 2:
        print(f"  WARN: very few symbols ({len(df)}) for top_n={args.top_n}")

    winners = df.nlargest(10, "forward_return")[["symbol_id", "forward_return"]]
    print("\nTop 10 realised winners over horizon:")
    print(winners.to_string(index=False, formatters={"forward_return": "{:.1%}".format}))
    print(
        f"\nWinner cohort (top-{args.top_n}) avg forward return: "
        f"{df.nlargest(args.top_n, 'forward_return')['forward_return'].mean():.1%}"
    )
    print(f"Universe              avg forward return: {df['forward_return'].mean():.1%}")

    rows: list[FactorRow] = []
    for name in FACTOR_NAMES:
        r = _evaluate_single_factor(panel, name, args.top_n)
        r.description = _DESCRIPTIONS.get(name, "")
        rows.append(r)

    # Composite under current proxy weights.
    cur = score_weights(panel, CURRENT_WEIGHTS_PROXY, top_n=args.top_n)
    rows.append(FactorRow(
        "composite (current)",
        "DEFAULT_FACTOR_WEIGHTS proxy (rs_12_1=0.49, trend=0.28, prox=0.23)",
        cur.ic, cur.hit_rate, cur.top_decile_lift, cur.n,
    ))
    # Equal-weight composite over all factors.
    eq = score_weights(panel, {n: 1.0 for n in FACTOR_NAMES}, top_n=args.top_n)
    rows.append(FactorRow(
        "composite (equal-wt)",
        "equal weights across all factors",
        eq.ic, eq.hit_rate, eq.top_decile_lift, eq.n,
    ))

    print(f"\n{'Factor':<24} {'IC':>7} {'Hit%':>6} {'TopDecLift':>11} {'N':>6}  Description")
    print("-" * 110)
    for r in rows:
        print(
            f"{r.name:<24} {r.ic:>7.3f} {r.hit_rate * 100:>5.1f}% {r.top_decile_lift * 100:>10.1f}% {r.n:>6}  {r.description}"
        )
    print()
    print("Reading guide:")
    print("  IC          : Spearman rank correlation between factor and forward return.")
    print("  Hit%        : of top-N by factor, fraction also in top-N realised winners.")
    print("  TopDecLift  : top decile by factor — mean forward return minus universe mean.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
