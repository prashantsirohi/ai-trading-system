"""Chart rendering for the weekly PDF report.

Each helper writes a PNG to disk and returns the path. All chart errors
are swallowed and logged — charts are illustrative; their absence must
never abort report generation.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Use non-interactive backend so we never depend on a display server.
import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


_FIG_DPI = 110
_BREADTH_FIG_SIZE = (8.5, 3.2)
_BAR_FIG_SIZE = (8.5, 3.6)
_CANDLE_FIG_SIZE = (8.0, 4.6)
_VALUATION_FIG_SIZE = (8.5, 5.8)


def _safe_save(fig, path: Path) -> Optional[Path]:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=_FIG_DPI, bbox_inches="tight")
        plt.close(fig)
        return path
    except Exception as exc:  # noqa: BLE001
        logger.warning("chart save failed at %s: %s", path, exc)
        plt.close(fig)
        return None


def breadth_chart(breadth: pd.DataFrame, output_path: Path) -> Optional[Path]:
    """26-week line chart of % stocks above SMA20/50/200."""
    if breadth is None or breadth.empty:
        return None
    needed = {"trade_date", "pct_above_sma20", "pct_above_sma50", "pct_above_sma200"}
    if not needed.issubset(breadth.columns):
        return None
    try:
        df = breadth.copy()
        df.loc[:, "trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values("trade_date")
        fig, ax = plt.subplots(figsize=_BREADTH_FIG_SIZE)
        ax.plot(df["trade_date"], df["pct_above_sma20"], label="% > SMA20", color="#1f77b4", linewidth=1.4)
        ax.plot(df["trade_date"], df["pct_above_sma50"], label="% > SMA50", color="#2ca02c", linewidth=1.4)
        ax.plot(df["trade_date"], df["pct_above_sma200"], label="% > SMA200", color="#d62728", linewidth=1.4)
        ax.axhline(50, color="#888", linestyle="--", linewidth=0.6)
        ax.set_ylim(0, 100)
        ax.set_ylabel("% of universe")
        ax.set_title("Market Breadth — % stocks above moving averages")
        ax.grid(True, alpha=0.25)
        ax.legend(loc="lower left", fontsize=8, frameon=False)
        fig.autofmt_xdate()
        return _safe_save(fig, output_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("breadth_chart failed: %s", exc)
        return None


def sector_rs_bars(sectors: pd.DataFrame, output_path: Path, top_n: int = 12) -> Optional[Path]:
    """Horizontal bar chart of sector RS, colored by quadrant."""
    if sectors is None or sectors.empty or "Sector" not in sectors.columns:
        return None
    rs_col = next((c for c in ("RS", "RS_20", "RS_rank") if c in sectors.columns), None)
    if rs_col is None:
        return None
    try:
        df = sectors.dropna(subset=[rs_col]).copy()
        df = df.sort_values(rs_col, ascending=True).tail(top_n)
        quadrant = df["Quadrant"] if "Quadrant" in df.columns else pd.Series([""] * len(df))
        color_map = {
            "Leading":   "#2ca02c",
            "Improving": "#9467bd",
            "Weakening": "#ff7f0e",
            "Lagging":   "#d62728",
        }
        colors = [color_map.get(str(q), "#7f7f7f") for q in quadrant]
        fig, ax = plt.subplots(figsize=_BAR_FIG_SIZE)
        ax.barh(df["Sector"].astype(str), df[rs_col].astype(float), color=colors)
        ax.set_xlabel(rs_col)
        ax.set_title(f"Sector relative strength ({rs_col})")
        ax.grid(True, axis="x", alpha=0.25)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
        return _safe_save(fig, output_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("sector_rs_bars failed: %s", exc)
        return None


def rank_mover_bars(
    improvers: pd.DataFrame,
    decliners: pd.DataFrame,
    output_path: Path,
    top_n: int = 10,
) -> Optional[Path]:
    """Diverging bar chart: top improvers (green) and decliners (red) by rank_change."""
    imp = improvers if improvers is not None else pd.DataFrame()
    dec = decliners if decliners is not None else pd.DataFrame()
    if imp.empty and dec.empty:
        return None
    try:
        rows = []
        if not imp.empty and {"symbol_id", "rank_change"}.issubset(imp.columns):
            for _, r in imp.head(top_n).iterrows():
                rows.append((str(r["symbol_id"]), float(r["rank_change"]), "improver"))
        if not dec.empty and {"symbol_id", "rank_change"}.issubset(dec.columns):
            for _, r in dec.head(top_n).iterrows():
                rows.append((str(r["symbol_id"]), float(r["rank_change"]), "decliner"))
        if not rows:
            return None
        rows.sort(key=lambda x: x[1])
        labels = [r[0] for r in rows]
        values = [r[1] for r in rows]
        colors = ["#2ca02c" if v >= 0 else "#d62728" for v in values]
        fig, ax = plt.subplots(figsize=_BAR_FIG_SIZE)
        ax.barh(labels, values, color=colors)
        ax.axvline(0, color="#444", linewidth=0.6)
        ax.set_xlabel("Rank change (positive = improving)")
        ax.set_title("Top rank movers — week over week")
        ax.grid(True, axis="x", alpha=0.25)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
        return _safe_save(fig, output_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("rank_mover_bars failed: %s", exc)
        return None


def universe_valuation_cycle(universe: pd.DataFrame, cycle: pd.DataFrame, output_path: Path) -> Optional[Path]:
    """Three-panel PE cycle chart: index, PE moving average, and percentile bands."""
    valuation = universe if universe is not None else pd.DataFrame()
    features = cycle if cycle is not None else pd.DataFrame()
    if valuation.empty and features.empty:
        return None
    try:
        if not valuation.empty and "date" in valuation.columns:
            valuation = valuation.copy()
            valuation.loc[:, "date"] = pd.to_datetime(valuation["date"], errors="coerce")
            valuation = valuation.dropna(subset=["date"]).sort_values("date")
        if not features.empty and "date" in features.columns:
            features = features.copy()
            features.loc[:, "date"] = pd.to_datetime(features["date"], errors="coerce")
            features = features.dropna(subset=["date"]).sort_values("date")
        fig, axes = plt.subplots(3, 1, figsize=_VALUATION_FIG_SIZE, sharex=True)
        if not valuation.empty:
            index_col = next((c for c in ("index_level_mcap_weight", "index_level_equal_weight", "index_level") if c in valuation.columns), None)
            if index_col:
                axes[0].plot(valuation["date"], pd.to_numeric(valuation[index_col], errors="coerce"), color="#1f77b4", linewidth=1.2, label="Universe index")
                if len(valuation) >= 20:
                    axes[0].plot(
                        valuation["date"],
                        pd.to_numeric(valuation[index_col], errors="coerce").rolling(200, min_periods=20).mean(),
                        color="#444",
                        linewidth=1.0,
                        label="200DMA",
                    )
            axes[0].set_title("Universe index vs 200DMA")
            axes[0].legend(loc="upper left", fontsize=8, frameon=False)
        pe_source = features if not features.empty else valuation
        if not pe_source.empty:
            axes[1].plot(pe_source["date"], pd.to_numeric(pe_source.get("pe_ttm"), errors="coerce"), color="#2ca02c", linewidth=1.2, label="Universe PE")
            if "pe_200dma" in pe_source.columns:
                axes[1].plot(pe_source["date"], pd.to_numeric(pe_source["pe_200dma"], errors="coerce"), color="#444", linewidth=1.0, label="PE 200DMA")
            median_col = next((c for c in ("pe_5y_median", "pe_3y_median") if c in pe_source.columns), None)
            if median_col:
                axes[1].plot(pe_source["date"], pd.to_numeric(pe_source[median_col], errors="coerce"), color="#9467bd", linewidth=0.9, linestyle="--", label=median_col)
            axes[1].set_title("Universe PE cycle")
            axes[1].legend(loc="upper left", fontsize=8, frameon=False)
        percentile_source = features if not features.empty and "pe_percentile_5y" in features.columns else valuation
        if not percentile_source.empty and "pe_percentile_5y" in percentile_source.columns:
            axes[2].plot(percentile_source["date"], pd.to_numeric(percentile_source["pe_percentile_5y"], errors="coerce"), color="#d62728", linewidth=1.2, label="PE percentile 5Y")
            for value, color in ((90, "#d62728"), (80, "#ff7f0e"), (20, "#2ca02c"), (10, "#1f77b4")):
                axes[2].axhline(value, color=color, linestyle="--", linewidth=0.7, alpha=0.75)
            axes[2].set_ylim(0, 100)
            axes[2].set_title("PE percentile zones")
            axes[2].legend(loc="upper left", fontsize=8, frameon=False)
        for ax in axes:
            ax.grid(True, alpha=0.25)
            for spine in ("top", "right"):
                ax.spines[spine].set_visible(False)
        fig.autofmt_xdate()
        return _safe_save(fig, output_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("universe_valuation_cycle failed: %s", exc)
        return None


def _load_symbol_ohlcv(
    ohlcv_db_path: Path, symbol_id: str, end_date: date, days: int = 180
) -> pd.DataFrame:
    """Read recent OHLCV for one symbol."""
    if not ohlcv_db_path.exists():
        return pd.DataFrame()
    try:
        import duckdb  # type: ignore
    except ImportError:
        return pd.DataFrame()
    start = end_date - timedelta(days=days + 30)  # padding for weekends
    sql = """
        SELECT CAST(timestamp AS DATE) AS dt, open, high, low, close, volume
        FROM _catalog
        WHERE symbol_id = ?
          AND CAST(timestamp AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
        ORDER BY dt
    """
    try:
        con = duckdb.connect(str(ohlcv_db_path), read_only=True)
        try:
            df = con.execute(sql, [symbol_id, start.isoformat(), end_date.isoformat()]).fetchdf()
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001
        logger.warning("ohlcv read failed for %s: %s", symbol_id, exc)
        return pd.DataFrame()
    if df.empty:
        return df
    df.loc[:, "dt"] = pd.to_datetime(df["dt"])
    df = df.set_index("dt")
    df.columns = [c.capitalize() for c in df.columns]  # mplfinance expects Open/High/Low/Close/Volume
    return df


def candlestick(
    ohlcv_db_path: Path,
    symbol_id: str,
    end_date: date,
    output_path: Path,
    breakout_level: Optional[float] = None,
    days: int = 180,
) -> Optional[Path]:
    """6-month OHLCV candle + SMA20/50/200 + 52W high; optional breakout level."""
    df = _load_symbol_ohlcv(ohlcv_db_path, symbol_id, end_date, days=days)
    if df.empty or len(df) < 30:
        return None
    try:
        import mplfinance as mpf  # type: ignore
    except ImportError:
        logger.warning("mplfinance not installed; skipping candlestick for %s", symbol_id)
        return None
    try:
        # Compute overlays directly to avoid mplfinance's mav cache fragility.
        df = df.copy()
        df = df.assign(
            SMA20=df["Close"].rolling(20, min_periods=5).mean(),
            SMA50=df["Close"].rolling(50, min_periods=10).mean(),
            SMA200=df["Close"].rolling(200, min_periods=30).mean(),
        )
        # Take the last `days` calendar days of trading sessions for the plot window.
        plot_df = df.tail(min(len(df), days))
        # Use the full df to compute the trailing 52w high so it doesn't truncate.
        hi_52w = df["Close"].rolling(252, min_periods=20).max().reindex(plot_df.index)

        addplots = [
            mpf.make_addplot(plot_df["SMA20"], color="#1f77b4", width=1.0),
            mpf.make_addplot(plot_df["SMA50"], color="#2ca02c", width=1.0),
            mpf.make_addplot(plot_df["SMA200"], color="#d62728", width=1.0),
            mpf.make_addplot(hi_52w, color="#888", width=0.8, linestyle="--"),
        ]
        if breakout_level is not None and breakout_level > 0:
            addplots.append(
                mpf.make_addplot(
                    pd.Series(breakout_level, index=plot_df.index),
                    color="#ff7f0e", width=0.9, linestyle=":",
                )
            )

        style = mpf.make_mpf_style(base_mpf_style="charles", rc={"axes.grid": True})
        fig, _axes = mpf.plot(
            plot_df,
            type="candle",
            style=style,
            volume=True,
            addplot=addplots,
            figsize=_CANDLE_FIG_SIZE,
            tight_layout=True,
            returnfig=True,
            ylabel="",
            ylabel_lower="Vol",
            datetime_format="%b-%d",
            xrotation=0,
            warn_too_much_data=10000,
        )
        fig.suptitle(f"{symbol_id} · 6-month OHLCV", fontsize=10, y=0.995)
        return _safe_save(fig, output_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("candlestick failed for %s: %s", symbol_id, exc)
        return None


def pick_candle_targets(
    ranked: pd.DataFrame,
    improvers: pd.DataFrame,
    breakouts: pd.DataFrame,
    patterns_best: pd.DataFrame | None = None,
    fund_value_tech_overlap: pd.DataFrame | None = None,
    candidate_tracker_current: pd.DataFrame | None = None,
    n_each: int = 3,
    cap: int = 12,
) -> List[Dict[str, Any]]:
    """Compose a deduped list of (symbol_id, breakout_level, source_tag).

    Pulls top-N from ranked composite_score, rank improvers, Tier-A breakouts,
    pattern setups, fund/value/tech overlap, and deteriorating tracker rows.
    """
    out: List[Dict[str, Any]] = []
    seen: set = set()

    def _normalize_symbol(symbol: Any) -> str:
        if symbol is None or pd.isna(symbol):
            return ""
        normalized = str(symbol).strip().upper()
        if normalized.lower() in {"", "none", "nan", "<na>"}:
            return ""
        return normalized

    def _add(symbol: Any, level: Optional[float], tag: str) -> None:
        symbol_id = _normalize_symbol(symbol)
        if not symbol_id or symbol_id in seen:
            return
        seen.add(symbol_id)
        out.append({"symbol_id": symbol_id, "breakout_level": level, "source": tag})

    if isinstance(ranked, pd.DataFrame) and not ranked.empty and "composite_score" in ranked.columns:
        top = ranked.sort_values("composite_score", ascending=False).head(n_each)
        for _, r in top.iterrows():
            _add(r.get("symbol_id"), None, "top_ranked")

    if isinstance(improvers, pd.DataFrame) and not improvers.empty:
        for _, r in improvers.head(n_each).iterrows():
            _add(r.get("symbol_id"), None, "rank_improver")

    if isinstance(breakouts, pd.DataFrame) and not breakouts.empty and "candidate_tier" in breakouts.columns:
        tier_a = breakouts[breakouts["candidate_tier"] == "A"]
        for _, r in tier_a.head(n_each).iterrows():
            level = r.get("prior_range_high")
            try:
                level_f = float(level) if level is not None and pd.notna(level) else None
            except (TypeError, ValueError):
                level_f = None
            _add(r.get("symbol_id"), level_f, "tier_a_breakout")

    if isinstance(patterns_best, pd.DataFrame) and not patterns_best.empty:
        for _, r in patterns_best.head(n_each).iterrows():
            level = r.get("breakout_level")
            try:
                level_f = float(level) if level is not None and pd.notna(level) else None
            except (TypeError, ValueError):
                level_f = None
            _add(r.get("symbol_id"), level_f, "pattern_setup")

    if isinstance(fund_value_tech_overlap, pd.DataFrame) and not fund_value_tech_overlap.empty:
        for _, r in fund_value_tech_overlap.head(2).iterrows():
            _add(r.get("symbol") if pd.notna(r.get("symbol")) else r.get("symbol_id"), None, "fund_value_tech")

    if isinstance(candidate_tracker_current, pd.DataFrame) and not candidate_tracker_current.empty:
        tracker = candidate_tracker_current.copy()
        status_col = "current_status" if "current_status" in tracker.columns else "status"
        status = tracker.get(status_col, pd.Series("", index=tracker.index)).astype(str).str.upper()
        tracker = tracker.loc[status.isin({"DETERIORATING", "RESULT_FAILURE", "TECHNICAL_FAILURE", "REMOVE_FROM_TRACKING"})]
        for _, r in tracker.head(2).iterrows():
            _add(r.get("symbol") if pd.notna(r.get("symbol")) else r.get("symbol_id"), None, "tracker_deteriorating")

    return out[:cap]
