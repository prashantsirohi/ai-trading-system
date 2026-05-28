#!/usr/bin/env python3
"""Visualize historical breadth analysis for regime threshold calibration.

Generates charts from research_ohlcv.duckdb and saves to reports/regime_analysis/.
"""

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from pathlib import Path
from datetime import datetime

from ai_trading_system.platform.db.paths import get_domain_paths

DB_PATH = get_domain_paths(data_domain="research").ohlcv_db_path
OUTPUT_DIR = Path("reports/regime_analysis")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Data Loading ──────────────────────────────────────────────────────────────

def load_breadth_data():
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = conn.execute("""
        WITH symbol_roll AS (
            SELECT
                symbol_id,
                CAST(timestamp AS DATE) AS d,
                close,
                AVG(close) OVER (
                    PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE)
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ) AS sma50,
                AVG(close) OVER (
                    PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE)
                    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                ) AS sma200,
                MAX(close) OVER (
                    PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE)
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                ) AS high252,
                COUNT(close) OVER (
                    PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE)
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ) AS n50,
                COUNT(close) OVER (
                    PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE)
                    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                ) AS n200,
                COUNT(close) OVER (
                    PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE)
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                ) AS n252
            FROM _catalog
            WHERE exchange = 'NSE' AND close IS NOT NULL AND close > 0
        ),
        breadth AS (
            SELECT
                d,
                COUNT(*) FILTER (WHERE n200 = 200) AS universe_count,
                SUM(CASE WHEN n50 = 50 AND close > sma50 THEN 1 ELSE 0 END)::DOUBLE
                    / NULLIF(COUNT(*) FILTER (WHERE n50 = 50), 0) AS pct_above_50dma,
                SUM(CASE WHEN n200 = 200 AND close > sma200 THEN 1 ELSE 0 END)::DOUBLE
                    / NULLIF(COUNT(*) FILTER (WHERE n200 = 200), 0) AS pct_above_200dma,
                SUM(CASE WHEN n252 = 252 AND close >= high252 * 0.98 THEN 1 ELSE 0 END)::DOUBLE
                    / NULLIF(COUNT(*) FILTER (WHERE n252 = 252), 0) AS pct_at_52w_high,
                SUM(CASE WHEN n252 = 252 AND close >= high252 * 0.90 THEN 1 ELSE 0 END)::DOUBLE
                    / NULLIF(COUNT(*) FILTER (WHERE n252 = 252), 0) AS pct_near_52w_high
            FROM symbol_roll
            GROUP BY d
        ),
        monthly AS (
            SELECT
                DATE_TRUNC('month', d) AS month,
                ROUND(AVG(pct_above_200dma) * 100, 1) AS pct_200dma,
                ROUND(AVG(pct_above_50dma) * 100, 1) AS pct_50dma,
                ROUND(AVG(COALESCE(pct_at_52w_high, 0)) * 100, 1) AS pct_at_52w_high,
                ROUND(AVG(COALESCE(pct_near_52w_high, 0)) * 100, 1) AS pct_near_52w_high,
                AVG(universe_count) AS universe_count
            FROM breadth
            WHERE universe_count > 10
            GROUP BY 1
        ),
        idx AS (
            SELECT
                DATE_TRUNC('month', date) AS month,
                FIRST(close) AS open_price,
                LAST(close) AS close_price,
                MIN(close) AS min_price
            FROM _index_catalog
            WHERE index_code = 'UNIV_TOP1000'
            GROUP BY 1
        )
        SELECT
            m.month,
            m.pct_200dma,
            m.pct_50dma,
            m.pct_at_52w_high,
            m.pct_near_52w_high,
            ROUND(m.universe_count) AS universe_count,
            ROUND((i.close_price / i.open_price - 1) * 100, 2) AS idx_return,
            ROUND((i.min_price / i.open_price - 1) * 100, 2) AS idx_mdd
        FROM monthly m
        JOIN idx i ON m.month = i.month
        ORDER BY m.month
    """).fetchdf()
    conn.close()

    # Classify regime
    conditions = [
        df["idx_return"] > 10,
        df["idx_return"] < -5,
    ]
    choices = ["BULL", "BEAR"]
    df["regime"] = np.select(conditions, choices, default="SIDEWAYS")
    return df


# ── Chart Styles ──────────────────────────────────────────────────────────────

REGIME_COLORS = {"BULL": "#2ecc71", "SIDEWAYS": "#95a5a6", "BEAR": "#e74c3c"}
REGIME_BG = {"BULL": "#d5f5e3", "SIDEWAYS": "#f0f0f0", "BEAR": "#fadbd8"}
plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "axes.grid": True,
    "grid.alpha": 0.3,
    "font.size": 10,
})


# ── Chart 1: Regime Timeline ─────────────────────────────────────────────────

def chart_regime_timeline(df):
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 12), sharex=True,
                                         gridspec_kw={"height_ratios": [2, 1, 1]})
    fig.suptitle("Market Regime & Breadth Timeline (2000–2026)", fontsize=14, fontweight="bold")

    # Color background by regime
    for _, row in df.iterrows():
        color = REGIME_BG.get(row["regime"], "#f0f0f0")
        ax1.axvspan(row["month"], row["month"] + np.timedelta64(32, "D"),
                    alpha=0.15, color=color, zorder=0)
        ax2.axvspan(row["month"], row["month"] + np.timedelta64(32, "D"),
                    alpha=0.15, color=color, zorder=0)
        ax3.axvspan(row["month"], row["month"] + np.timedelta64(32, "D"),
                    alpha=0.15, color=color, zorder=0)

    # Panel 1: Index return + breadth
    ax1_twin = ax1.twinx()
    bars = ax1.bar(df["month"], df["idx_return"], width=20, color=[REGIME_COLORS[r] for r in df["regime"]],
                   alpha=0.7, label="Index Return %", zorder=2)
    ax1.plot(df["month"], df["pct_200dma"], color="#2980b9", linewidth=1.5, label="% Above 200DMA", zorder=3)
    ax1.plot(df["month"], df["pct_50dma"], color="#8e44ad", linewidth=1, alpha=0.7, label="% Above 50DMA", zorder=3)
    ax1.axhline(y=30, color="#e74c3c", linestyle="--", alpha=0.5, linewidth=1, label="risk_off threshold (30%)")
    ax1.axhline(y=55, color="#f39c12", linestyle="--", alpha=0.5, linewidth=1, label="bull threshold (55%)")
    ax1.axhline(y=75, color="#2ecc71", linestyle="--", alpha=0.5, linewidth=1, label="strong_bull threshold (75%)")
    ax1.set_ylabel("Index Return (%)", fontsize=11)
    ax1_twin.set_ylabel("% of Stocks", fontsize=11)
    ax1_twin.set_ylim(0, 100)
    ax1.legend(loc="upper left", fontsize=8)
    ax1.set_title("Monthly Index Return & Market Breadth", fontsize=12)

    # Panel 2: 52-week high metrics
    ax2.plot(df["month"], df["pct_at_52w_high"], color="#e67e22", linewidth=1.5, label="% at 52w High")
    ax2.fill_between(df["month"], df["pct_near_52w_high"], alpha=0.3, color="#e67e22", label="% near 52w High (10%)")
    ax2.axhline(y=5, color="#e74c3c", linestyle="--", alpha=0.5, linewidth=1, label="bear threshold (5%)")
    ax2.axhline(y=12, color="#2ecc71", linestyle="--", alpha=0.5, linewidth=1, label="bull threshold (12%)")
    ax2.set_ylabel("% of Stocks", fontsize=11)
    ax2.legend(loc="upper left", fontsize=8)
    ax2.set_title("52-Week High Breadth", fontsize=12)

    # Panel 3: Universe count
    ax3.bar(df["month"], df["universe_count"], color="#34495e", alpha=0.6, width=20)
    ax3.set_ylabel("Stocks with 200DMA", fontsize=11)
    ax3.set_title("Universe Coverage (stocks with ≥200 trading days)", fontsize=12)

    ax3.xaxis.set_major_locator(mdates.YearLocator(2))
    ax3.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    fig.autofmt_xdate()

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "01_regime_timeline.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {OUTPUT_DIR / '01_regime_timeline.png'}")


# ── Chart 2: Distribution by Regime ──────────────────────────────────────────

def chart_distribution_by_regime(df):
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Breadth Metric Distributions by Market Regime", fontsize=14, fontweight="bold")

    regimes_order = ["BEAR", "SIDEWAYS", "BULL"]
    regime_labels = {"BEAR": "Bear (38 mo)", "SIDEWAYS": "Sideways (207 mo)", "BULL": "Bull (37 mo)"}

    metrics = [
        ("pct_200dma", "% Above 200DMA", axes[0, 0]),
        ("pct_50dma", "% Above 50DMA", axes[0, 1]),
        ("pct_at_52w_high", "% at 52-Week High", axes[1, 0]),
        ("pct_near_52w_high", "% near 52-Week High", axes[1, 1]),
    ]

    for col, title, ax in metrics:
        data = [df.loc[df["regime"] == r, col].values for r in regimes_order]
        bp = ax.boxplot(data, labels=[regime_labels[r] for r in regimes_order],
                        patch_artist=True, widths=0.5)
        for patch, r in zip(bp["boxes"], regimes_order):
            patch.set_facecolor(REGIME_COLORS[r])
            patch.set_alpha(0.7)
        ax.set_title(title, fontsize=12)
        ax.set_ylabel("% of Stocks")
        ax.grid(axis="y", alpha=0.3)

        # Add median labels
        for i, r in enumerate(regimes_order):
            median = np.median(df.loc[df["regime"] == r, col])
            ax.text(i + 1, median + 2, f"median={median:.1f}%", ha="center", fontsize=8, fontweight="bold")

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "02_distribution_by_regime.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {OUTPUT_DIR / '02_distribution_by_regime.png'}")


# ── Chart 3: Scatter — Breadth vs Index Return ───────────────────────────────

def chart_scatter_breadth_return(df):
    fig, ax = plt.subplots(figsize=(12, 7))
    fig.suptitle("Breadth vs Index Return — Regime Classification", fontsize=14, fontweight="bold")

    for regime in ["BULL", "SIDEWAYS", "BEAR"]:
        mask = df["regime"] == regime
        ax.scatter(df.loc[mask, "pct_200dma"], df.loc[mask, "idx_return"],
                   c=REGIME_COLORS[regime], label=f"{regime} ({mask.sum()} months)",
                   alpha=0.6, s=40, edgecolors="white", linewidth=0.5)

    # Threshold lines
    ax.axhline(y=10, color="#2ecc71", linestyle="--", alpha=0.7, linewidth=1.5, label="BULL threshold (+10%)")
    ax.axhline(y=-5, color="#e74c3c", linestyle="--", alpha=0.7, linewidth=1.5, label="BEAR threshold (-5%)")
    ax.axvline(x=30, color="#e74c3c", linestyle=":", alpha=0.5, linewidth=1.5, label="risk_off breadth (<30%)")
    ax.axvline(x=55, color="#f39c12", linestyle=":", alpha=0.5, linewidth=1.5, label="bull breadth (>55%)")
    ax.axvline(x=75, color="#2ecc71", linestyle=":", alpha=0.5, linewidth=1.5, label="strong_bull breadth (>75%)")

    ax.set_xlabel("% of Stocks Above 200DMA", fontsize=12)
    ax.set_ylabel("UNIV_TOP1000 Monthly Return (%)", fontsize=12)
    ax.legend(fontsize=9, loc="upper left")
    ax.set_xlim(0, 105)
    ax.set_ylim(-35, 40)

    # Annotate extreme months
    extremes = df[(df["idx_return"] > 20) | (df["idx_return"] < -20)]
    for _, row in extremes.iterrows():
        ax.annotate(f"{row['month'].strftime('%b %Y')}",
                    (row["pct_200dma"], row["idx_return"]),
                    fontsize=7, ha="center", va="bottom", alpha=0.8)

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "03_scatter_breadth_return.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {OUTPUT_DIR / '03_scatter_breadth_return.png'}")


# ── Chart 4: Yearly Regime Heatmap ───────────────────────────────────────────

def chart_yearly_heatmap(df):
    yearly = df.groupby(df["month"].dt.year).agg(
        annual_return=("idx_return", "sum"),
        avg_200dma=("pct_200dma", "mean"),
        avg_50dma=("pct_50dma", "mean"),
        avg_at_52w_high=("pct_at_52w_high", "mean"),
        worst_mdd=("idx_mdd", "min"),
    ).reset_index()

    conditions = [
        yearly["annual_return"] > 15,
        yearly["annual_return"] < -10,
    ]
    choices = ["BULL", "BEAR"]
    yearly["regime"] = np.select(conditions, choices, default="SIDEWAYS")

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), gridspec_kw={"height_ratios": [2, 1]})
    fig.suptitle("Annual Regime Summary (2002–2026)", fontsize=14, fontweight="bold")

    years = yearly["month"].astype(int).values
    x = np.arange(len(years))
    width = 0.35

    # Panel 1: Annual return
    colors = [REGIME_COLORS[r] for r in yearly["regime"]]
    bars = ax1.bar(x, yearly["annual_return"], width, color=colors, alpha=0.8)
    for bar, ret in zip(bars, yearly["annual_return"]):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + (1 if ret > 0 else -3),
                 f"{ret:.0f}%", ha="center", fontsize=7, fontweight="bold")
    ax1.set_xticks(x)
    ax1.set_xticklabels(years, rotation=45)
    ax1.set_ylabel("Annual Return (%)", fontsize=11)
    ax1.axhline(y=0, color="black", linewidth=0.5)
    ax1.set_title("Annual Index Return (colored by regime)", fontsize=12)

    # Panel 2: Breadth metrics
    ax2.plot(years, yearly["avg_200dma"], "o-", color="#2980b9", linewidth=2, label="Avg 200DMA")
    ax2.plot(years, yearly["avg_at_52w_high"] * 5, "s--", color="#e67e22", linewidth=1.5,
             label="Avg at 52wH × 5 (scaled)")
    ax2.axhline(y=30, color="#e74c3c", linestyle="--", alpha=0.5, label="risk_off (30%)")
    ax2.axhline(y=55, color="#f39c12", linestyle="--", alpha=0.5, label="bull (55%)")
    ax2.axhline(y=75, color="#2ecc71", linestyle="--", alpha=0.5, label="strong_bull (75%)")
    ax2.set_xticks(x)
    ax2.set_xticklabels(years, rotation=45)
    ax2.set_ylabel("% of Stocks", fontsize=11)
    ax2.set_title("Average Annual Breadth Metrics", fontsize=12)
    ax2.legend(fontsize=9)

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "04_yearly_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {OUTPUT_DIR / '04_yearly_heatmap.png'}")


# ── Chart 5: Bear Market Deep Dive ───────────────────────────────────────────

def chart_bear_market_analysis(df):
    bears = df[df["regime"] == "BEAR"].sort_values("idx_return")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle("Bear Market Analysis — 38 Bear Months", fontsize=14, fontweight="bold")

    # Left: Index return vs breadth
    sc = ax1.scatter(bears["pct_200dma"], bears["idx_return"],
                     c=bears["pct_at_52w_high"], cmap="Oranges", s=60,
                     edgecolors="white", linewidth=0.5)
    plt.colorbar(sc, ax=ax1, label="% at 52w High")
    ax1.axvline(x=30, color="#e74c3c", linestyle="--", alpha=0.5, label="risk_off threshold")
    ax1.set_xlabel("% Above 200DMA")
    ax1.set_ylabel("Index Return (%)")
    ax1.set_title("Bear Months: Breadth vs Return\n(color = % at 52-week high)")
    ax1.legend()

    # Annotate extreme bears
    extremes = bears[bears["idx_return"] < -15]
    for _, row in extremes.iterrows():
        ax1.annotate(f"{row['month'].strftime('%b %Y')}",
                     (row["pct_200dma"], row["idx_return"]),
                     fontsize=7, ha="center", va="bottom", fontweight="bold")

    # Right: Breadth distribution in bears
    ax2.boxplot([bears["pct_200dma"], bears["pct_50dma"], bears["pct_at_52w_high"], bears["pct_near_52w_high"]],
                labels=["200DMA", "50DMA", "at 52wH", "near 52wH"],
                patch_artist=True, widths=0.5)
    ax2.set_title("Breadth Metric Distribution in Bear Months")
    ax2.set_ylabel("% of Stocks")

    # Add stats
    stats_text = (
        f"Median 200DMA: {bears['pct_200dma'].median():.1f}%\n"
        f"Median 50DMA:  {bears['pct_50dma'].median():.1f}%\n"
        f"Median at 52wH: {bears['pct_at_52w_high'].median():.1f}%\n"
        f"Median near 52wH: {bears['pct_near_52w_high'].median():.1f}%"
    )
    ax2.text(0.95, 0.95, stats_text, transform=ax2.transAxes,
             fontsize=8, verticalalignment="top", horizontalalignment="right",
             bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5))

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "05_bear_market_analysis.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {OUTPUT_DIR / '05_bear_market_analysis.png'}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading breadth data from research_ohlcv.duckdb...")
    df = load_breadth_data()
    print(f"  Loaded {len(df)} months of data ({df['month'].min().date()} to {df['month'].max().date()})")
    print(f"  Regime distribution: {df['regime'].value_counts().to_dict()}")
    print()

    print("Generating charts...")
    chart_regime_timeline(df)
    chart_distribution_by_regime(df)
    chart_scatter_breadth_return(df)
    chart_yearly_heatmap(df)
    chart_bear_market_analysis(df)

    print(f"\nAll charts saved to: {OUTPUT_DIR}/")
    print("Files:")
    for f in sorted(OUTPUT_DIR.glob("*.png")):
        print(f"  - {f.name}")


if __name__ == "__main__":
    main()
