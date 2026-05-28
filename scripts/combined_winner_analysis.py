"""Combine 2025 and 2026 winner factor analysis for robust statistical conclusions."""

import pandas as pd
import numpy as np
from scipy.stats import spearmanr

# Load both years (2025 at root, 2026 in subdirectory)
df_2025 = pd.read_csv("reports/winner_analysis/per_winner_factors.csv")
df_2026 = pd.read_csv("reports/winner_analysis/2026/per_winner_factors.csv")

df = pd.concat([df_2025, df_2026], ignore_index=True)

print(f"Combined analysis: {len(df)} winners (2025: {len(df_2025)}, 2026: {len(df_2026)})")
print(f"Median rally return: {df['rally_return_pct'].median():.1f}%")
print(f"Median rally duration: {df['rally_trading_days'].median():.0f} trading days")
print(f"Best winner: {df.loc[df['rally_return_pct'].idxmax(), 'symbol_id']} ({df['rally_return_pct'].max():.1f}%)")
print(f"Worst winner: {df.loc[df['rally_return_pct'].idxmin(), 'symbol_id']} ({df['rally_return_pct'].min():.1f}%)")
print()

factors = ["rs_12_1", "rs_6m", "rs_3m", "prox_52w_high", "above_200dma_pct", "trend_persistence", "volume_ratio", "low_vol"]

print("## Factor Statistics (Winners vs Universe)")
print()
print("| Factor | Winner Median | Universe Median | Winner P75 | Universe P75 | IC | Hit Rate | % Above Median |")
print("|---|---|---|---|---|---|---|---|")

for f in factors:
    w_med = df[f].median()
    u_med = df[f"{f}_univ_median"].median()
    w_p75 = df[f].quantile(0.75)
    u_p75 = df[f"{f}_univ_median"].quantile(0.75)
    
    # Spearman IC
    valid = df[[f, "rally_return_pct"]].dropna()
    if len(valid) >= 3:
        ic_result = spearmanr(valid[f], valid["rally_return_pct"])
        ic = ic_result.correlation if not np.isnan(ic_result.correlation) else np.nan
    else:
        ic = np.nan
    
    # Hit rate (top quartile of universe)
    threshold = df[f"{f}_univ_median"].quantile(0.75)
    hit_rate = (df[f] >= threshold).mean() * 100
    
    # % above universe median
    above_med = (df[f] > df[f"{f}_univ_median"]).mean() * 100
    
    ic_str = f"{ic:+.3f}" if not np.isnan(ic) else "nan"
    print(f"| {f} | {w_med:.3f} | {u_med:.3f} | {w_p75:.3f} | {u_p75:.3f} | {ic_str} | {hit_rate:.1f}% | {above_med:.1f}% |")

print()

# Composite analysis
factor_weights_current = {
    "rs_12_1": 0.00,
    "rs_6m": 0.00,
    "rs_3m": 0.00,
    "prox_52w_high": 0.00,
    "above_200dma_pct": 0.00,
    "trend_persistence": 0.22,
    "volume_ratio": 0.00,
    "low_vol": 0.00,
}

# Current weights composite
valid_rows = df.dropna(subset=["trend_persistence"])
if len(valid_rows) > 0:
    composite_current = valid_rows["trend_persistence"] * factor_weights_current["trend_persistence"]
    ic_current = spearmanr(composite_current, valid_rows.loc[composite_current.index, "rally_return_pct"]).correlation
else:
    ic_current = np.nan

# Equal weight composite
valid_factors = [f for f in factors if df[f].notna().sum() > len(df) * 0.3]
if len(valid_factors) > 1:
    df_valid = df.dropna(subset=valid_factors)
    composite_equal = df_valid[valid_factors].apply(lambda x: (x - x.mean()) / x.std(), axis=0).mean(axis=1)
    ic_equal = spearmanr(composite_equal, df_valid.loc[composite_equal.index, "rally_return_pct"]).correlation
else:
    ic_equal = np.nan

print("## Composite Analysis")
print()
print("| Composite | Spearman IC |")
print("|---|---|")
print(f"| Current weights (DEFAULT_FACTOR_WEIGHTS) | {ic_current:+.3f} |")
print(f"| Equal-weight all factors | {ic_equal:+.3f} |")
print()

# Recommendations
print("## Recommendations")
print()
print("Based on the combined analysis:")
print()

for f in factors:
    valid = df[[f, "rally_return_pct"]].dropna()
    if len(valid) >= 3:
        ic = spearmanr(valid[f], valid["rally_return_pct"]).correlation
    else:
        ic = np.nan
    
    if np.isnan(ic):
        rec = "No change"
    elif ic < -0.3:
        rec = "**Strongly decrease or invert**"
    elif ic < -0.1:
        rec = "**Decrease or invert**"
    elif ic < 0.1:
        rec = "No change"
    elif ic < 0.3:
        rec = "**Increase weight**"
    else:
        rec = "**Strongly increase weight**"
    
    print(f"- **{f}**: {rec} — IC={ic:+.3f}" if not np.isnan(ic) else f"- **{f}**: No change — IC=nan")

print()
print("---")
print()
print("*Generated: 2026-05-20*")
print("*Data: data/research/research_ohlcv.duckdb*")
