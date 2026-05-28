#!/usr/bin/env python3
"""Winner factor analysis — what factor values preceded the biggest rallies?

Analyzes the top N stocks by return in a given year, computes all ranking factors at
the signal date (the rally bottom), and compares winners against the broader NSE universe.

Usage:
    uv run python scripts/winner_factor_analysis.py --year 2025
    uv run python scripts/winner_factor_analysis.py --year 2026
"""

from __future__ import annotations

import argparse
import duckdb
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import date, timedelta
from scipy.stats import spearmanr

from ai_trading_system.domains.ranking.contracts import DEFAULT_FACTOR_WEIGHTS
from ai_trading_system.platform.db.paths import get_domain_paths

RESEARCH_DB = get_domain_paths(data_domain="research").ohlcv_db_path
OUTPUT_DIR = Path("reports/winner_analysis")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TOP_N_WINNERS = 20
MIN_HISTORY_DAYS = 30
MIN_TURNOVER_CR_UNIVERSE = 0.5

FACTOR_NAMES = [
    "rs_12_1", "rs_6m", "rs_3m", "prox_52w_high",
    "above_200dma_pct", "trend_persistence", "volume_ratio", "low_vol",
]


def load_winners(con: duckdb.DuckDBPyConnection, year: int) -> pd.DataFrame:
    """Load top N winners of the given year with low/high dates and prices.
    
    For partial years (like 2026), uses a trailing 12-month window.
    """
    # For current/partial years, use trailing 12 months instead of calendar year
    from datetime import date as dt_date
    today = dt_date.today()
    if year == today.year:
        start_date = today - timedelta(days=365)
        end_date = today
        min_days = 150  # Lower threshold for partial year
    else:
        start_date = dt_date(year, 1, 1)
        end_date = dt_date(year, 12, 31)
        min_days = 200

    df = con.execute("""
        WITH yearly AS (
            SELECT
                symbol_id,
                ROUND((LAST(close) / FIRST(close) - 1) * 100, 2) AS year_return_pct
            FROM _catalog
            WHERE exchange = 'NSE'
              AND CAST(timestamp AS DATE) >= ?::DATE
              AND CAST(timestamp AS DATE) <= ?::DATE
              AND close > 0
            GROUP BY 1
            HAVING COUNT(*) >= ?
            ORDER BY year_return_pct DESC
            LIMIT ?
        ),
        extremes AS (
            SELECT
                y.symbol_id,
                y.year_return_pct,
                (SELECT CAST(timestamp AS DATE) FROM _catalog c2
                 WHERE c2.symbol_id = y.symbol_id AND c2.exchange = 'NSE'
                   AND CAST(c2.timestamp AS DATE) >= ?::DATE
                   AND CAST(c2.timestamp AS DATE) <= ?::DATE
                   AND c2.close > 0
                 ORDER BY c2.close ASC LIMIT 1) AS low_date,
                (SELECT MIN(close) FROM _catalog c2
                 WHERE c2.symbol_id = y.symbol_id AND c2.exchange = 'NSE'
                   AND CAST(c2.timestamp AS DATE) >= ?::DATE
                   AND CAST(c2.timestamp AS DATE) <= ?::DATE
                   AND c2.close > 0) AS low_price,
                (SELECT CAST(timestamp AS DATE) FROM _catalog c2
                 WHERE c2.symbol_id = y.symbol_id AND c2.exchange = 'NSE'
                   AND CAST(c2.timestamp AS DATE) >= ?::DATE
                   AND CAST(c2.timestamp AS DATE) <= ?::DATE
                   AND c2.close > 0
                 ORDER BY c2.close DESC LIMIT 1) AS high_date,
                (SELECT MAX(close) FROM _catalog c2
                 WHERE c2.symbol_id = y.symbol_id AND c2.exchange = 'NSE'
                   AND CAST(c2.timestamp AS DATE) >= ?::DATE
                   AND CAST(c2.timestamp AS DATE) <= ?::DATE
                   AND c2.close > 0) AS high_price
            FROM yearly y
        ),
        history AS (
            SELECT
                symbol_id,
                MIN(CAST(timestamp AS DATE)) AS first_date,
                COUNT(*) AS total_days,
                ROUND(AVG(close * volume) / 10000000, 2) AS avg_turnover_cr
            FROM _catalog
            WHERE exchange = 'NSE' AND close > 0
            GROUP BY 1
        )
        SELECT
            e.*,
            h.first_date,
            h.total_days,
            h.avg_turnover_cr
        FROM extremes e
        JOIN history h ON e.symbol_id = h.symbol_id
        ORDER BY e.year_return_pct DESC
    """, [
        str(start_date), str(end_date), min_days, TOP_N_WINNERS,
        str(start_date), str(end_date),
        str(start_date), str(end_date),
        str(start_date), str(end_date),
        str(start_date), str(end_date),
    ]).fetchdf()

    # Signal date = low_date (the bottom of the rally)
    df["signal_date"] = df["low_date"]
    df["days_of_history"] = (df["signal_date"] - df["first_date"]).dt.days

    return df


def compute_factors_for_stock(
    con: duckdb.DuckDBPyConnection,
    symbol_id: str,
    signal_date: date,
) -> dict | None:
    """Compute all 8 ranking factors for one stock at signal_date."""
    result = con.execute("""
        SELECT CAST(timestamp AS DATE) AS d, close, volume
        FROM _catalog
        WHERE symbol_id = ? AND exchange = 'NSE'
          AND CAST(timestamp AS DATE) <= ?::DATE
          AND close > 0
        ORDER BY d
    """, [symbol_id, str(signal_date)]).fetchdf()

    if result.empty or len(result) < MIN_HISTORY_DAYS:
        return None

    closes = result["close"].values.astype(float)
    vols = result["volume"].values.astype(float)
    n = len(closes)

    sma50 = pd.Series(closes).rolling(50).mean().values
    sma200 = closes[-200:].mean() if n >= 200 else np.nan
    rets = np.diff(closes[-51:]) / closes[-51:-1] if n >= 51 else np.array([])
    realized_vol = float(rets.std()) if rets.size else np.nan

    return {
        "rs_12_1": closes[-21] / closes[-252] - 1.0 if n >= 252 else np.nan,
        "rs_6m": closes[-1] / closes[-126] - 1.0 if n >= 126 else np.nan,
        "rs_3m": closes[-1] / closes[-63] - 1.0 if n >= 63 else np.nan,
        "prox_52w_high": closes[-1] / closes[-252:].max() if n >= 252 else np.nan,
        "above_200dma_pct": (closes[-1] - sma200) / sma200 if not np.isnan(sma200) and sma200 > 0 else np.nan,
        "trend_persistence": float((closes[-50:] > sma50[-50:]).mean()) if n >= 100 else np.nan,
        "volume_ratio": (
            float(np.median(vols[-20:]) / np.median(vols[-100:]))
            if n >= 100 and np.median(vols[-100:]) > 0
            else np.nan
        ),
        "low_vol": -realized_vol if not np.isnan(realized_vol) else np.nan,
    }


def load_universe_panel(
    con: duckdb.DuckDBPyConnection,
    signal_date: date,
) -> pd.DataFrame | None:
    """Load factor panel for all NSE stocks at signal_date."""
    eligible = con.execute("""
        SELECT symbol_id
        FROM _catalog
        WHERE exchange = 'NSE' AND close > 0
        GROUP BY 1
        HAVING COUNT(*) FILTER (WHERE CAST(timestamp AS DATE) < ?::DATE) >= ?
    """, [str(signal_date), MIN_HISTORY_DAYS]).fetchall()

    if not eligible:
        return None

    symbols = [r[0] for r in eligible]
    placeholders = ",".join(["?"] * len(symbols))
    lookback_start = (signal_date - timedelta(days=400)).strftime("%Y-%m-%d")

    bars = con.execute(f"""
        SELECT symbol_id, CAST(timestamp AS DATE) AS d, close, volume
        FROM _catalog
        WHERE exchange = 'NSE'
          AND symbol_id IN ({placeholders})
          AND CAST(timestamp AS DATE) BETWEEN ?::DATE AND ?::DATE
          AND close > 0
        ORDER BY symbol_id, d
    """, symbols + [lookback_start, str(signal_date)]).fetchdf()

    if bars.empty:
        return None

    results = []
    for symbol_id, group in bars.groupby("symbol_id"):
        group = group.sort_values("d").reset_index(drop=True)
        pre = group[group["d"] <= pd.Timestamp(signal_date)]
        if len(pre) < MIN_HISTORY_DAYS:
            continue

        closes = pre["close"].values.astype(float)
        vols = pre["volume"].values.astype(float)
        n = len(closes)

        sma50 = pd.Series(closes).rolling(50).mean().values
        sma200 = closes[-200:].mean() if n >= 200 else np.nan
        rets = np.diff(closes[-51:]) / closes[-51:-1] if n >= 51 else np.array([])
        realized_vol = float(rets.std()) if rets.size else np.nan

        results.append({
            "symbol_id": symbol_id,
            "rs_12_1": closes[-21] / closes[-252] - 1.0 if n >= 252 else np.nan,
            "rs_6m": closes[-1] / closes[-126] - 1.0 if n >= 126 else np.nan,
            "rs_3m": closes[-1] / closes[-63] - 1.0 if n >= 63 else np.nan,
            "prox_52w_high": closes[-1] / closes[-252:].max() if n >= 252 else np.nan,
            "above_200dma_pct": (closes[-1] - sma200) / sma200 if not np.isnan(sma200) and sma200 > 0 else np.nan,
            "trend_persistence": float((closes[-50:] > sma50[-50:]).mean()) if n >= 100 else np.nan,
            "volume_ratio": (
                float(np.median(vols[-20:]) / np.median(vols[-100:]))
                if n >= 100 and np.median(vols[-100:]) > 0
                else np.nan
            ),
            "low_vol": -realized_vol if not np.isnan(realized_vol) else np.nan,
        })

    if not results:
        return None

    df = pd.DataFrame(results)

    # Apply turnover filter
    turnover = con.execute(f"""
        SELECT symbol_id, ROUND(AVG(close * volume) / 10000000, 2) AS avg_turnover_cr
        FROM _catalog
        WHERE exchange = 'NSE' AND symbol_id IN ({placeholders})
        GROUP BY 1
    """, symbols).fetchdf()

    if not turnover.empty:
        liquid = turnover[turnover["avg_turnover_cr"] >= MIN_TURNOVER_CR_UNIVERSE]["symbol_id"].tolist()
        df = df[df["symbol_id"].isin(liquid)]

    return df


def compute_rally_return(
    con: duckdb.DuckDBPyConnection,
    symbol_id: str,
    low_date: date,
    high_date: date,
) -> tuple[float, int] | None:
    """Compute actual return from low_date to high_date."""
    result = con.execute("""
        SELECT
            (SELECT close FROM _catalog WHERE symbol_id = ? AND exchange = 'NSE'
             AND CAST(timestamp AS DATE) <= ?::DATE AND close > 0
             ORDER BY CAST(timestamp AS DATE) DESC LIMIT 1) AS low_px,
            (SELECT close FROM _catalog WHERE symbol_id = ? AND exchange = 'NSE'
             AND CAST(timestamp AS DATE) <= ?::DATE AND close > 0
             ORDER BY CAST(timestamp AS DATE) DESC LIMIT 1) AS high_px
    """, [symbol_id, str(low_date), symbol_id, str(high_date)]).fetchone()

    if result is None or result[0] is None or result[1] is None:
        return None

    low_px, high_px = result
    rally_return = (high_px / low_px) - 1.0

    trading_days = con.execute("""
        SELECT COUNT(DISTINCT CAST(timestamp AS DATE))
        FROM _catalog
        WHERE symbol_id = ? AND exchange = 'NSE'
          AND CAST(timestamp AS DATE) BETWEEN ?::DATE AND ?::DATE
    """, [symbol_id, str(low_date), str(high_date)]).fetchone()[0]

    return rally_return, trading_days


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2025, help="Year to analyze")
    parser.add_argument("--top-n", type=int, default=20, help="Number of top winners")
    args = parser.parse_args()

    global TOP_N_WINNERS
    TOP_N_WINNERS = args.top_n

    year = args.year
    print("=" * 60)
    print(f"Winner Factor Analysis — {year} Top Winners")
    print("=" * 60)
    print(f"Min history: {MIN_HISTORY_DAYS} days | Universe turnover: ₹{MIN_TURNOVER_CR_UNIVERSE} Cr/day")

    con = duckdb.connect(str(RESEARCH_DB), read_only=True)

    # Step 1: Load winners
    print(f"\n1. Loading top {TOP_N_WINNERS} winners of {year}...")
    winners_df = load_winners(con, year)
    print(f"   Found {len(winners_df)} winners")
    for _, row in winners_df.head(5).iterrows():
        print(f"   {row['symbol_id']}: {row['year_return_pct']:.1f}% (signal: {row['signal_date'].date()}, history: {row['days_of_history']}d)")

    # Step 2: Analyze each winner
    print(f"\n2. Analyzing winners...")
    results = []
    universe_cache = {}

    for _, row in winners_df.iterrows():
        symbol_id = row["symbol_id"]
        signal_date = row["signal_date"].date()
        low_date = row["low_date"]
        high_date = row["high_date"]

        print(f"   {symbol_id}: signal={signal_date}, history={row['days_of_history']}d...")

        # Compute winner's factors
        winner_factors = compute_factors_for_stock(con, symbol_id, signal_date)
        if winner_factors is None:
            print(f"     SKIPPED (insufficient history)")
            continue

        # Load universe panel (cached by signal_date)
        signal_key = str(signal_date)
        if signal_key not in universe_cache:
            print(f"     Loading universe panel for {signal_key}...")
            universe_df = load_universe_panel(con, signal_date)
            if universe_df is None:
                print(f"     SKIPPED (no universe data)")
                continue
            universe_cache[signal_key] = universe_df
        else:
            universe_df = universe_cache[signal_key]

        # Compute universe statistics
        universe_stats = {}
        for f in FACTOR_NAMES:
            col = universe_df[f].dropna()
            if len(col) > 0:
                universe_stats[f] = {
                    "median": col.median(),
                    "p25": col.quantile(0.25),
                    "p75": col.quantile(0.75),
                    "n": len(col),
                }
            else:
                universe_stats[f] = {"median": np.nan, "p25": np.nan, "p75": np.nan, "n": 0}

        # Compute winner's percentile rank
        percentile_ranks = {}
        for f in FACTOR_NAMES:
            col = universe_df[f].dropna()
            val = winner_factors[f]
            if len(col) > 0 and not np.isnan(val):
                percentile_ranks[f] = (col <= val).mean() * 100
            else:
                percentile_ranks[f] = np.nan

        # Compute actual rally return
        rally_result = compute_rally_return(con, symbol_id, low_date, high_date)
        if rally_result is None:
            print(f"     SKIPPED (could not compute rally return)")
            continue
        actual_rally_return, rally_trading_days = rally_result

        # Build result row
        result = {
            "symbol_id": symbol_id,
            "signal_date": signal_date,
            "low_date": low_date,
            "high_date": high_date,
            "year_return_pct": row["year_return_pct"],
            "rally_return_pct": actual_rally_return * 100,
            "rally_trading_days": rally_trading_days,
            "days_of_history": row["days_of_history"],
            "avg_turnover_cr": row["avg_turnover_cr"],
        }
        result.update(winner_factors)
        result.update({f"{f}_univ_median": universe_stats[f]["median"] for f in FACTOR_NAMES})
        result.update({f"{f}_univ_p25": universe_stats[f]["p25"] for f in FACTOR_NAMES})
        result.update({f"{f}_univ_p75": universe_stats[f]["p75"] for f in FACTOR_NAMES})
        result.update({f"{f}_pctile": percentile_ranks[f] for f in FACTOR_NAMES})

        results.append(result)
        print(f"     OK: rally={actual_rally_return*100:.1f}%, rs_6m={winner_factors['rs_6m']:.3f}")

    con.close()
    print(f"\n   Successfully analyzed {len(results)} of {len(winners_df)} winners")

    if len(results) < 3:
        print("ERROR: Too few winners analyzed. Check data availability.")
        return

    # Step 3: Aggregate analysis
    print("\n3. Computing aggregate statistics...")
    df = pd.DataFrame(results)

    # Per-factor statistics
    factor_stats = {}
    for f in FACTOR_NAMES:
        winner_vals = df[f].dropna()
        univ_median_vals = df[f"{f}_univ_median"].dropna()
        pctile_vals = df[f"{f}_pctile"].dropna()

        # Spearman IC (only for non-NaN pairs)
        valid = df[[f, "rally_return_pct"]].dropna()
        if len(valid) >= 3:
            ic_result = spearmanr(valid[f], valid["rally_return_pct"])
            ic = ic_result.correlation if not np.isnan(ic_result.correlation) else np.nan
        else:
            ic = np.nan

        # Hit rate: % in top quartile
        hit_rate = (pctile_vals >= 75).mean() * 100 if len(pctile_vals) > 0 else np.nan

        # % above universe median
        valid_mask = winner_vals.notna() & univ_median_vals.notna()
        if valid_mask.sum() > 0:
            pct_above = ((winner_vals[valid_mask] > univ_median_vals[valid_mask]).mean()) * 100
        else:
            pct_above = np.nan

        factor_stats[f] = {
            "winner_median": winner_vals.median(),
            "winner_p25": winner_vals.quantile(0.25),
            "winner_p75": winner_vals.quantile(0.75),
            "universe_median": univ_median_vals.median(),
            "universe_p25": univ_median_vals.quantile(0.25),
            "universe_p75": univ_median_vals.quantile(0.75),
            "ic": ic,
            "hit_rate": hit_rate,
            "pct_above_median": pct_above,
            "n": len(winner_vals),
        }

    # Composite analysis
    current_weighted = pd.Series(0.0, index=df.index)
    for f in FACTOR_NAMES:
        w = DEFAULT_FACTOR_WEIGHTS.get(f, 0.0)
        if w > 0 and f in df.columns:
            current_weighted += w * df[f].fillna(0)

    # Normalize for equal-weight
    normalized = pd.DataFrame()
    for f in FACTOR_NAMES:
        col = df[f].dropna()
        if len(col) > 0 and col.std() > 0:
            normalized[f] = (df[f] - col.mean()) / col.std()
        else:
            normalized[f] = 0
    normalized = normalized.fillna(0)

    equal_weight = normalized.mean(axis=1)

    current_ic = spearmanr(current_weighted, df["rally_return_pct"])
    equal_ic = spearmanr(equal_weight, df["rally_return_pct"])

    # Optimal weights via regression
    try:
        from sklearn.linear_model import LinearRegression
        model = LinearRegression()
        model.fit(normalized, df["rally_return_pct"])
        optimal_weights = {f: w for f, w in zip(FACTOR_NAMES, model.coef_)}
        total = sum(abs(w) for w in optimal_weights.values())
        if total > 0:
            optimal_weights = {f: w / total for f, w in optimal_weights.items()}
        optimal_r2 = model.score(normalized, df["rally_return_pct"])
    except ImportError:
        optimal_weights = {}
        for f in FACTOR_NAMES:
            r = spearmanr(df[f], df["rally_return_pct"])
            optimal_weights[f] = r.correlation if not np.isnan(r.correlation) else 0
        total = sum(abs(w) for w in optimal_weights.values())
        if total > 0:
            optimal_weights = {f: w / total for f, w in optimal_weights.items()}
        optimal_r2 = np.nan

    # Step 4: Write outputs
    print("\n4. Writing outputs...")
    year_dir = OUTPUT_DIR / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)

    # Per-winner CSV
    core_cols = ["symbol_id", "signal_date", "low_date", "high_date",
                 "year_return_pct", "rally_return_pct", "rally_trading_days",
                 "days_of_history", "avg_turnover_cr"]
    csv_cols = core_cols + FACTOR_NAMES + [f"{f}_univ_median" for f in FACTOR_NAMES] + [f"{f}_pctile" for f in FACTOR_NAMES]
    df[csv_cols].to_csv(year_dir / "per_winner_factors.csv", index=False)
    print(f"   Saved: {year_dir / 'per_winner_factors.csv'}")

    # Summary markdown
    lines = []
    lines.append(f"# Winner Factor Analysis — {year} Top Winners\n")
    lines.append(f"**Analyzed:** {len(results)} winners | **Signal date:** low_date (rally bottom)")
    lines.append(f"**Universe:** NSE stocks with ≥₹{MIN_TURNOVER_CR_UNIVERSE} Cr/day turnover\n")

    lines.append("## Rally Summary\n")
    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    lines.append(f"| Median rally return | {df['rally_return_pct'].median():.1f}% |")
    lines.append(f"| Median rally duration | {df['rally_trading_days'].median():.0f} trading days |")
    lines.append(f"| Best winner | {df.loc[df['rally_return_pct'].idxmax(), 'symbol_id']} ({df['rally_return_pct'].max():.1f}%) |")
    lines.append(f"| Worst winner | {df.loc[df['rally_return_pct'].idxmin(), 'symbol_id']} ({df['rally_return_pct'].min():.1f}%) |")
    lines.append(f"| Median history at signal | {df['days_of_history'].median():.0f} days |\n")

    lines.append("## Factor Statistics (Winners vs Universe)\n")
    lines.append("| Factor | Winner Median | Universe Median | Winner P75 | Universe P75 | IC | Hit Rate | % Above Median |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for f in FACTOR_NAMES:
        s = factor_stats[f]
        lines.append(
            f"| {f} | {s['winner_median']:.3f} | {s['universe_median']:.3f} | "
            f"{s['winner_p75']:.3f} | {s['universe_p75']:.3f} | "
            f"{s['ic']:.3f} | {s['hit_rate']:.1f}% | {s['pct_above_median']:.1f}% |"
        )
    lines.append("")
    lines.append("*IC = Spearman correlation between factor value and actual rally return*")
    lines.append("*Hit Rate = % of winners in top quartile (≥75th percentile) of universe*\n")

    lines.append("## Composite Analysis\n")
    lines.append("| Composite | Spearman IC |")
    lines.append("|---|---|")
    lines.append(f"| Current weights (DEFAULT_FACTOR_WEIGHTS) | {current_ic.correlation:.3f} |")
    lines.append(f"| Equal-weight all factors | {equal_ic.correlation:.3f} |")
    lines.append(f"| Optimal weights (regression R²={optimal_r2:.3f}) | {np.sqrt(optimal_r2) if not np.isnan(optimal_r2) else float('nan'):.3f} |\n")

    lines.append("## Optimal Weights (Linear Regression)\n")
    lines.append("| Factor | Current Weight | Optimal Weight | Change |")
    lines.append("|---|---|---|---|")
    for f in FACTOR_NAMES:
        current_w = DEFAULT_FACTOR_WEIGHTS.get(f, 0.0)
        optimal_w = optimal_weights.get(f, 0.0)
        change = optimal_w - current_w
        direction = "↑" if change > 0.05 else "↓" if change < -0.05 else "↔"
        lines.append(f"| {f} | {current_w:.3f} | {optimal_w:.3f} | {direction} ({change:+.3f}) |")
    lines.append("")

    lines.append("## Recommendations\n")
    lines.append("Based on the analysis:\n")
    for f in FACTOR_NAMES:
        s = factor_stats[f]
        ic = s["ic"]
        hit_rate = s["hit_rate"]
        pct_above = s["pct_above_median"]
        current_w = DEFAULT_FACTOR_WEIGHTS.get(f, 0.0)
        optimal_w = optimal_weights.get(f, 0.0)

        if not np.isnan(ic) and ic > 0.2 and pct_above > 60:
            rec = f"**Increase weight** — IC={ic:.2f}, {pct_above:.0f}% above median"
        elif not np.isnan(ic) and ic < -0.1:
            rec = f"**Decrease or invert** — negative IC ({ic:.2f})"
        elif not np.isnan(hit_rate) and hit_rate > 40:
            rec = f"**Keep or slightly increase** — {hit_rate:.0f}% hit rate"
        elif current_w > 0 and optimal_w < current_w * 0.5:
            rec = f"**Reduce weight** — optimal ({optimal_w:.2f}) << current ({current_w:.2f})"
        else:
            rec = f"**No change** — IC={ic:.2f}, hit rate={hit_rate:.0f}%"

        lines.append(f"- **{f}**: {rec}")

    lines.append("\n---\n")
    lines.append(f"*Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append(f"*Data: {RESEARCH_DB}*")

    (year_dir / "factor_summary.md").write_text("\n".join(lines))
    print(f"   Saved: {year_dir / 'factor_summary.md'}")

    print("\n" + "=" * 60)
    print("Analysis complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
