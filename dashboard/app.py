"""
AI Trading System — Streamlit Command Center Dashboard

Usage: streamlit run dashboard/app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import plotly.graph_objects as go
import plotly.express as px
import plotly.subplots as make_subplots
from plotly.subplots import make_subplots
import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict

import streamlit.components.v1 as components

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analytics.regime_detector import RegimeDetector
from analytics.ranker import StockRanker
from analytics.risk_manager import RiskManager
from analytics.visualizations import Visualizer

logging.getLogger("streamlit").setLevel(logging.WARNING)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OHLCV_DB = os.path.join(PROJECT_ROOT, "data", "ohlcv.duckdb")
FEATURE_STORE = os.path.join(PROJECT_ROOT, "data", "feature_store")
MASTER_DB = os.path.join(PROJECT_ROOT, "data", "masterdata.db")
REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports")


def get_db_stats() -> Dict:
    """Fetch quick stats from DuckDB."""
    try:
        conn = duckdb.connect(OHLCV_DB, read_only=True)
        try:
            total_rows = conn.execute("SELECT COUNT(*) FROM _catalog").fetchone()[0]
            total_syms = conn.execute(
                "SELECT COUNT(DISTINCT symbol_id) FROM _catalog WHERE exchange = 'NSE'"
            ).fetchone()[0]
            latest = conn.execute(
                "SELECT MAX(timestamp) FROM _catalog WHERE exchange = 'NSE'"
            ).fetchone()[0]
            latest_str = str(latest)[:10] if latest else "N/A"
        finally:
            conn.close()
        return {"rows": total_rows, "symbols": total_syms, "latest_date": latest_str}
    except Exception as e:
        return {"rows": 0, "symbols": 0, "latest_date": "Error", "error": str(e)}


def get_sectors() -> List[str]:
    """Get list of sectors from master DB."""
    try:
        import sqlite3

        conn = sqlite3.connect(MASTER_DB)
        try:
            rows = conn.execute(
                'SELECT DISTINCT "Industry Group" FROM stock_details WHERE "Industry Group" IS NOT NULL'
            ).fetchall()
            return sorted([r[0] for r in rows if r[0]])
        finally:
            conn.close()
    except Exception:
        return []


def load_ohlcv(symbol: str, exchange: str = "NSE", days: int = 365) -> pd.DataFrame:
    """Load OHLCV data for a symbol from DuckDB."""
    try:
        conn = duckdb.connect(OHLCV_DB, read_only=True)
        try:
            cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            df = conn.execute(f"""
                SELECT timestamp::DATE AS date, open, high, low, close, volume
                FROM _catalog
                WHERE symbol_id = '{symbol}'
                  AND exchange = '{exchange}'
                  AND timestamp >= '{cutoff}'
                ORDER BY timestamp ASC
            """).fetchdf()
        finally:
            conn.close()
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)
        return df
    except Exception:
        return pd.DataFrame()


def load_features(symbol: str, exchange: str = "NSE") -> Dict[str, pd.DataFrame]:
    """Load all available feature DataFrames for a symbol."""
    features = {}
    feature_names = [
        "rsi",
        "adx",
        "sma",
        "ema",
        "macd",
        "atr",
        "bb",
        "roc",
        "supertrend",
    ]
    for feat in feature_names:
        path = os.path.join(FEATURE_STORE, feat, exchange, f"{symbol}.parquet")
        if os.path.exists(path):
            df = pd.read_parquet(path)
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df.set_index("timestamp", inplace=True)
            features[feat] = df
    return features


def compute_dynamic_rank(
    df: pd.DataFrame,
    w_rs: float,
    w_vol: float,
    w_trend: float,
    w_high: float,
) -> pd.DataFrame:
    """Recompute composite score with custom weights."""
    if df.empty or "composite_score" not in df.columns:
        return df
    total = w_rs + w_vol + w_trend + w_high
    if total == 0:
        return df
    df = df.copy()
    df["custom_score"] = (
        df["rel_strength_score"] * (w_rs / total)
        + df["vol_intensity_score"] * (w_vol / total)
        + df["trend_score_score"] * (w_trend / total)
        + df["prox_high_score"] * (w_high / total)
    )
    df = df.sort_values("custom_score", ascending=False)
    return df.reset_index(drop=True)


def plot_candlestick_with_features(
    ohlcv: pd.DataFrame,
    features: Dict[str, pd.DataFrame],
    symbol: str,
) -> go.Figure:
    """Build a Plotly figure with OHLCV + overlaid indicators."""
    if ohlcv.empty:
        fig = go.Figure()
        fig.update_layout(title=f"No data for {symbol}")
        return fig

    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.5, 0.17, 0.17, 0.17],
        subplot_titles=("Price + MAs", "Volume", "RSI", "MACD"),
    )

    fig.add_trace(
        go.Candlestick(
            x=ohlcv.index,
            open=ohlcv["open"],
            high=ohlcv["high"],
            low=ohlcv["low"],
            close=ohlcv["close"],
            name="OHLCV",
            increasing_line_color="#26a69a",
            decreasing_line_color="#ef5350",
        ),
        row=1,
        col=1,
    )

    if "close" in ohlcv.columns and len(ohlcv) > 50:
        sma20 = ohlcv["close"].rolling(20).mean()
        sma50 = ohlcv["close"].rolling(50).mean()
        ema20 = ohlcv["close"].ewm(span=20).mean()
        fig.add_trace(
            go.Scatter(
                x=ohlcv.index,
                y=sma20,
                name="SMA 20",
                line=dict(color="#FF9800", width=1.2),
                opacity=0.8,
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=ohlcv.index,
                y=sma50,
                name="SMA 50",
                line=dict(color="#2196F3", width=1.2),
                opacity=0.8,
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=ohlcv.index,
                y=ema20,
                name="EMA 20",
                line=dict(color="#9C27B0", width=1, dash="dot"),
                opacity=0.7,
            ),
            row=1,
            col=1,
        )

    if "supertrend" in features:
        st_df = features["supertrend"].copy()
        st_df = st_df[st_df.index.isin(ohlcv.index)]
        if "supertrend" in st_df.columns and "close" in ohlcv.columns:
            above = ohlcv["close"][ohlcv.index.isin(st_df.index)]
            below = ohlcv["close"][ohlcv.index.isin(st_df.index)]
            up = st_df["supertrend"].reindex(ohlcv.index).ffill()
            fig.add_trace(
                go.Scatter(
                    x=ohlcv.index,
                    y=up,
                    name="Supertrend",
                    line=dict(color="#00BCD4", width=1),
                    opacity=0.6,
                ),
                row=1,
                col=1,
            )

    fig.add_trace(
        go.Bar(
            x=ohlcv.index,
            y=ohlcv["volume"],
            name="Volume",
            marker_color=np.where(
                ohlcv["close"] >= ohlcv["open"], "#26a69a", "#ef5350"
            ),
            opacity=0.6,
        ),
        row=2,
        col=1,
    )

    if "rsi" in features:
        rsi_df = features["rsi"].copy()
        rsi_df = rsi_df[rsi_df.index.isin(ohlcv.index)]
        if "rsi" in rsi_df.columns:
            fig.add_trace(
                go.Scatter(
                    x=rsi_df.index,
                    y=rsi_df["rsi"],
                    name="RSI(14)",
                    line=dict(color="#E91E63", width=1.5),
                    fill="tozeroy",
                    fillcolor="rgba(233,30,99,0.05)",
                ),
                row=3,
                col=1,
            )
            fig.add_hline(
                y=70, line_dash="dash", line_color="red", opacity=0.5, row=3, col=1
            )
            fig.add_hline(
                y=30, line_dash="dash", line_color="green", opacity=0.5, row=3, col=1
            )

    if "macd" in features:
        macd_df = features["macd"].copy()
        macd_df = macd_df[macd_df.index.isin(ohlcv.index)]
        macd_cols = [c for c in ["macd", "signal", "histogram"] if c in macd_df.columns]
        colors = {"macd": "#2196F3", "signal": "#FF9800", "histogram": "#9E9E9E"}
        for col in macd_cols:
            fig.add_trace(
                go.Scatter(
                    x=macd_df.index,
                    y=macd_df[col],
                    name=col.upper(),
                    line=dict(color=colors.get(col, "#9E9E9E"), width=1.5),
                ),
                row=4,
                col=1,
            )

    fig.update_layout(
        title=f"{symbol} — Price Chart & Indicators",
        template="plotly_dark",
        height=750,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_rangeslider_visible=False,
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(255,255,255,0.05)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.05)")
    return fig


def plot_radar_chart(row: pd.Series) -> go.Figure:
    """Build a radar/spider chart for a single stock's factor scores."""
    categories = [
        "Relative<br>Strength",
        "Volume<br>Intensity",
        "Trend<br>Persistence",
        "Proximity<br>to Highs",
    ]
    values = [
        row.get("rel_strength_score", 0) or 0,
        row.get("vol_intensity_score", 0) or 0,
        row.get("trend_score_score", 0) or 0,
        row.get("prox_high_score", 0) or 0,
    ]
    values += values[:1]

    fig = go.Figure()
    fig.add_trace(
        go.Scatterpolar(
            r=values,
            theta=categories,
            fill="toself",
            fillcolor="rgba(0,176,246,0.2)",
            line_color="#00bcd4",
            name=row.get("symbol_id", "Stock"),
            hovertemplate="%{theta}: %{r:.1f}<extra></extra>",
        )
    )
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=False,
        height=280,
        margin=dict(l=20, r=20, t=30, b=20),
        template="plotly_dark",
    )
    return fig


def style_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply conditional formatting to ranking DataFrame."""
    return df.style.format(
        {
            "composite_score": "{:.1f}",
            "rel_strength_score": "{:.1f}",
            "vol_intensity_score": "{:.1f}",
            "trend_score_score": "{:.1f}",
            "prox_high_score": "{:.1f}",
            "close": "{:.2f}",
        },
        na_rep="—",
    )


def main():
    st.set_page_config(
        page_title="AI Trading Command Center",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.html("""
    <style>
    .stMainBlockContainer {padding-top: 1rem;}
    [data-testid="stMetricValue"] {font-size: 1.6rem !important;}
    [data-testid="stMetricLabel"] {font-size: 0.85rem !important;}
    .stock-row:hover > div {background: rgba(0,176,246,0.08);}
    section[data-testid="stSidebar"] > div {padding-top: 1rem;}
    div[data-testid="stTabs"] button {font-size: 0.9rem;}
    </style>
    """)

    st.title("📊 AI Trading Command Center")
    st.caption(f"Project root: `{PROJECT_ROOT}`")

    if "rank_df" not in st.session_state:
        st.session_state.rank_df = None
    if "selected_symbol" not in st.session_state:
        st.session_state.selected_symbol = None
    if "weights" not in st.session_state:
        st.session_state.weights = {"rs": 35, "vol": 25, "trend": 15, "high": 30}
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = None

    with st.sidebar:
        st.header("⚙️ Settings")

        with st.expander("📁 Data Status", expanded=True):
            stats = get_db_stats()
            st.metric("Symbols", f"{stats.get('symbols', 0):,}")
            st.metric("OHLCV Rows", f"{stats.get('rows', 0):,}")
            st.metric("Latest Date", stats.get("latest_date", "—"))
            if st.button("🔄 Refresh Rankings", use_container_width=True):
                st.session_state.rank_df = None
                st.rerun()

        st.divider()

        with st.expander("⚖️ Ranking Weights", expanded=True):
            st.caption("Adjust factor weights to recalculate composite scores")
            w_rs = st.slider(
                "Relative Strength", 0, 100, st.session_state.weights["rs"], key="w_rs"
            )
            w_vol = st.slider(
                "Volume Intensity", 0, 100, st.session_state.weights["vol"], key="w_vol"
            )
            w_trend = st.slider(
                "Trend Persistence",
                0,
                100,
                st.session_state.weights["trend"],
                key="w_trend",
            )
            w_high = st.slider(
                "Proximity to Highs",
                0,
                100,
                st.session_state.weights["high"],
                key="w_high",
            )

            if (w_rs, w_vol, w_trend, w_high) != tuple(
                st.session_state.weights.values()
            ):
                st.session_state.weights = {
                    "rs": w_rs,
                    "vol": w_vol,
                    "trend": w_trend,
                    "high": w_high,
                }
                st.session_state.rank_df = None
                st.rerun()

            st.session_state.weights = {
                "rs": w_rs,
                "vol": w_vol,
                "trend": w_trend,
                "high": w_high,
            }

            total = w_rs + w_vol + w_trend + w_high
            st.progress(min(total / 100, 1.0), text=f"Total: {total}% (normalized)")

        st.divider()

        with st.expander("🔍 Filters", expanded=False):
            sectors = ["All"] + get_sectors()
            selected_sector = st.selectbox("Sector", sectors, key="sector_filter")
            score_min = st.slider("Min Score", 0.0, 100.0, 40.0, key="score_min")
            top_n_default = st.slider("Show Top N", 10, 200, 50, key="top_n_default")

        st.divider()

        with st.expander("📈 Market Regime", expanded=False):
            try:
                rd = RegimeDetector(
                    ohlcv_db_path=OHLCV_DB, feature_store_dir=FEATURE_STORE
                )
                conn = duckdb.connect(OHLCV_DB, read_only=True)
                try:
                    latest = conn.execute(
                        "SELECT MAX(timestamp)::DATE FROM _catalog WHERE exchange = 'NSE'"
                    ).fetchone()[0]
                    latest_str = str(latest)[:10] if latest else None
                finally:
                    conn.close()

                if latest_str:
                    regime_info = rd.get_market_regime(date=latest_str)
                    regime = regime_info.get("market_regime", "UNKNOWN")
                    regime_color = {
                        "TREND": "🟢",
                        "STRONG_TREND": "🟢🟢",
                        "MEAN_REV": "🟡",
                        "RANGE_BOUND": "🔴",
                        "MIXED": "🟡",
                    }.get(regime, "⚪")
                    st.markdown(f"**Regime:** {regime_color} {regime}")
                    st.caption(f"ADX Median: {regime_info.get('adx_median', 'N/A')}")
                    st.caption(f"Trending %: {regime_info.get('trending_pct', 'N/A')}")
                    strategy = {
                        "TREND": "Trend-Follow",
                        "STRONG_TREND": "Trend-Follow",
                        "MEAN_REV": "Mean Reversion",
                        "RANGE_BOUND": "Mean Reversion",
                        "MIXED": "Mixed / Low Exposure",
                    }.get(regime, "Unknown")
                    st.info(f"Strategy: **{strategy}**")
            except Exception as e:
                st.warning(f"Could not detect regime: {e}")

        st.divider()
        st.caption(f"Last refresh: {st.session_state.last_refresh or 'Never'}")

    tab_overview, tab_ranking, tab_chart, tab_portfolio = st.tabs(
        ["📋 Overview", "🏆 Ranking", "📈 Chart", "💼 Portfolio"]
    )

    with tab_overview:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📦 Total Symbols", f"{stats.get('symbols', 0):,}")
        with col2:
            st.metric("📊 OHLCV Rows", f"{stats.get('rows', 0):,}")
        with col3:
            st.metric("🗓️ Latest Data", stats.get("latest_date", "—"))
        with col4:
            feat_count = len(
                [
                    f
                    for f in os.listdir(FEATURE_STORE)
                    if os.path.isdir(os.path.join(FEATURE_STORE, f))
                    and not f.startswith("_")
                ]
            )
            st.metric("🔢 Feature Types", str(feat_count))

        st.divider()

        col_left, col_right = st.columns([1, 1])

        with col_left:
            st.subheader("📊 Sector Distribution (Top Ranked)")
            try:
                import sqlite3

                if st.session_state.rank_df is None:
                    with st.spinner("Running ranking query..."):
                        t0 = time.time()
                        ranker = StockRanker(
                            ohlcv_db_path=OHLCV_DB, feature_store_dir=FEATURE_STORE
                        )
                        rank_df = ranker.rank_all(
                            date=None, exchanges=["NSE"], min_score=0.0, top_n=None
                        )
                        st.session_state.rank_df = rank_df
                        st.session_state.last_refresh = datetime.now().strftime(
                            "%H:%M:%S"
                        )
                    st.success(
                        f"Ranked {len(rank_df):,} stocks in {time.time() - t0:.1f}s"
                    )

                if (
                    st.session_state.rank_df is not None
                    and not st.session_state.rank_df.empty
                ):
                    ranked_symbols = (
                        st.session_state.rank_df["symbol_id"].head(100).tolist()
                    )

                    conn = sqlite3.connect(MASTER_DB)
                    try:
                        placeholders = ",".join("?" * len(ranked_symbols))
                        rows = conn.execute(
                            f"""
                            SELECT "Industry Group", COUNT(*) as count
                            FROM stock_details
                            WHERE "Symbol" IN ({placeholders})
                              AND "Industry Group" IS NOT NULL
                            GROUP BY "Industry Group"
                            ORDER BY count DESC
                            LIMIT 20
                        """,
                            ranked_symbols,
                        ).fetchall()
                        sector_df = pd.DataFrame(
                            rows, columns=["Industry Group", "count"]
                        )
                    finally:
                        conn.close()

                    if sector_df.empty:
                        st.info("No sector data found for ranked symbols.")
                    else:
                        fig = px.bar(
                            sector_df,
                            x="count",
                            y="Industry Group",
                            orientation="h",
                            title="Sectors in Top 100 Momentum Stocks",
                            color="count",
                            color_continuous_scale="Viridis",
                        )
                        fig.update_layout(
                            template="plotly_dark",
                            height=450,
                            showlegend=False,
                            yaxis={"tickfont": {"size": 11}},
                        )
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Run ranking first to see sector distribution.")
            except Exception as e:
                st.warning(f"Could not load sector data: {e}")

        with col_right:
            st.subheader("📈 Score Distribution")
            if (
                st.session_state.rank_df is not None
                and not st.session_state.rank_df.empty
            ):
                df = st.session_state.rank_df
                score_col = (
                    "custom_score"
                    if "custom_score" in df.columns
                    else "composite_score"
                )
                fig = px.histogram(
                    df,
                    x=score_col,
                    nbins=30,
                    title="Composite Score Distribution",
                    color_discrete_sequence=["#00bcd4"],
                )
                fig.update_layout(
                    template="plotly_dark",
                    height=450,
                    showlegend=False,
                    xaxis_title="Composite Score",
                    yaxis_title="Count",
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                with st.spinner("Running ranking query..."):
                    t0 = time.time()
                    ranker = StockRanker(
                        ohlcv_db_path=OHLCV_DB, feature_store_dir=FEATURE_STORE
                    )
                    weights_dict = {
                        "relative_strength": st.session_state.weights["rs"] / 100,
                        "volume_intensity": st.session_state.weights["vol"] / 100,
                        "trend_persistence": st.session_state.weights["trend"] / 100,
                        "proximity_highs": st.session_state.weights["high"] / 100,
                    }
                    rank_df = ranker.rank_all(
                        date=None,
                        exchanges=["NSE"],
                        min_score=0.0,
                        top_n=None,
                        weights=weights_dict,
                    )
                    st.session_state.rank_df = rank_df
                    st.session_state.last_refresh = datetime.now().strftime("%H:%M:%S")
                st.success(f"Ranked {len(rank_df):,} stocks in {time.time() - t0:.1f}s")

    with tab_ranking:
        st.subheader("🏆 Multi-Factor Stock Ranking")

        weights_dict = {
            "relative_strength": st.session_state.weights["rs"] / 100,
            "volume_intensity": st.session_state.weights["vol"] / 100,
            "trend_persistence": st.session_state.weights["trend"] / 100,
            "proximity_highs": st.session_state.weights["high"] / 100,
        }
        force_refresh = st.button("🔄 Refresh Rankings", use_container_width=True)

        if st.session_state.rank_df is None or force_refresh:
            with st.spinner("Running ranking query across all symbols..."):
                t0 = time.time()
                try:
                    ranker = StockRanker(
                        ohlcv_db_path=OHLCV_DB, feature_store_dir=FEATURE_STORE
                    )
                    rank_df = ranker.rank_all(
                        date=None,
                        exchanges=["NSE"],
                        min_score=0.0,
                        top_n=None,
                        weights=weights_dict,
                    )
                    st.session_state.rank_df = rank_df
                    st.session_state.last_refresh = datetime.now().strftime("%H:%M:%S")
                    st.success(
                        f"Ranked {len(rank_df):,} stocks in {time.time() - t0:.1f}s"
                    )
                except Exception as e:
                    st.error(f"Ranking failed: {e}")
                    rank_df = pd.DataFrame()
        else:
            rank_df = st.session_state.rank_df

        if rank_df is not None and not rank_df.empty:
            w_rs = st.session_state.weights["rs"]
            w_vol = st.session_state.weights["vol"]
            w_trend = st.session_state.weights["trend"]
            w_high = st.session_state.weights["high"]

            score_col = "composite_score"

            if selected_sector != "All":
                try:
                    import sqlite3

                    conn = sqlite3.connect(MASTER_DB)
                    try:
                        rows = conn.execute(
                            'SELECT "Symbol" FROM stock_details WHERE "Industry Group" = ?',
                            (selected_sector,),
                        ).fetchall()
                        syms_in_sector = [r[0] for r in rows]
                        rank_df = rank_df[rank_df["symbol_id"].isin(syms_in_sector)]
                    finally:
                        conn.close()
                except Exception:
                    pass

            rank_df = rank_df[rank_df[score_col] >= score_min]
            top_df = rank_df.head(top_n_default).copy()
            top_df.index = range(1, len(top_df) + 1)
            top_df.index.name = "Rank"

            cols_show = [
                "symbol_id",
                "close",
                score_col,
                "rel_strength_score",
                "vol_intensity_score",
                "trend_score_score",
                "prox_high_score",
            ]
            cols_show = [c for c in cols_show if c in top_df.columns]
            rename_cols = {
                "symbol_id": "Symbol",
                "close": "Price",
                score_col: "Score",
                "rel_strength_score": "RS",
                "vol_intensity_score": "Vol",
                "trend_score_score": "Trend",
                "prox_high_score": "Highs",
            }
            display_df = top_df[cols_show].rename(columns=rename_cols)
            display_df.columns = [rename_cols.get(c, c) for c in display_df.columns]

            col_order = ["Rank"] + list(display_df.columns)
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=False,
                height=600,
            )

            col1, col2 = st.columns([2, 1])
            with col1:
                selected = st.selectbox(
                    "🔍 Select a stock for detailed view:",
                    options=[""] + top_df["symbol_id"].tolist(),
                    index=0,
                    label_visibility="collapsed",
                )
            with col2:
                if selected:
                    st.metric("Selected", selected)

            if selected:
                st.session_state.selected_symbol = selected

        else:
            st.info(
                "No ranking data available. Click 'Refresh Rankings' in the sidebar."
            )

    with tab_chart:
        symbol = st.session_state.selected_symbol

        if not symbol:
            st.info("Select a stock from the Ranking tab or search below.")
            symbol = (
                st.text_input("Or enter symbol manually (e.g. RELIANCE):", "")
                .strip()
                .upper()
            )

        if symbol:
            col1, col2 = st.columns([1, 3])
            with col1:
                st.subheader(f"📋 {symbol}")
                ohlcv = load_ohlcv(symbol, days=365)
                if not ohlcv.empty:
                    latest = ohlcv.iloc[-1]
                    prev = ohlcv.iloc[-2] if len(ohlcv) > 1 else latest
                    chg = latest["close"] - prev["close"]
                    chg_pct = (chg / prev["close"]) * 100 if prev["close"] else 0
                    st.metric(
                        "Price",
                        f"₹{latest['close']:.2f}",
                        f"{chg:+.2f} ({chg_pct:+.1f}%)",
                    )
                    st.metric("Volume", f"{latest['volume']:,.0f}")
                    st.metric("52W High", f"₹{ohlcv['high'].max():.2f}")
                    st.metric("52W Low", f"₹{ohlcv['low'].min():.2f}")

                    features = load_features(symbol)
                    if not features:
                        st.warning(
                            "No feature data found. Run feature computation first."
                        )
                else:
                    st.warning("No OHLCV data for this symbol.")

            with col2:
                if not ohlcv.empty:
                    features = load_features(symbol)
                    fig = plot_candlestick_with_features(ohlcv, features, symbol)
                    st.plotly_chart(fig, use_container_width=True)

                    row = None
                    if (
                        st.session_state.rank_df is not None
                        and not st.session_state.rank_df.empty
                    ):
                        row = st.session_state.rank_df[
                            st.session_state.rank_df["symbol_id"] == symbol
                        ]
                        if not row.empty:
                            row = row.iloc[0]
                            st.subheader(f"📡 Factor Profile — {symbol}")
                            col_r, col_v = st.columns([1, 1])
                            with col_r:
                                st.plotly_chart(
                                    plot_radar_chart(row), use_container_width=True
                                )
                            with col_v:
                                st.markdown("**Factor Scores**")
                                score_data = {
                                    "Factor": [
                                        "Relative Strength",
                                        "Volume Intensity",
                                        "Trend Persistence",
                                        "Proximity to Highs",
                                        "Composite",
                                    ],
                                    "Score": [
                                        f"{row.get('rel_strength_score', 0):.1f}",
                                        f"{row.get('vol_intensity_score', 0):.1f}",
                                        f"{row.get('trend_score_score', 0):.1f}",
                                        f"{row.get('prox_high_score', 0):.1f}",
                                        f"{row.get('composite_score', 0):.1f}",
                                    ],
                                }
                                score_st_df = pd.DataFrame(score_data)
                                st.dataframe(
                                    score_st_df,
                                    use_container_width=True,
                                    hide_index=True,
                                )
                else:
                    st.info(f"No data for {symbol}. Try another symbol.")

    with tab_portfolio:
        st.subheader("💼 Portfolio Builder")

        if st.session_state.rank_df is None or st.session_state.rank_df.empty:
            st.info("Run ranking first in the Ranking tab.")
        else:
            rank_df = st.session_state.rank_df
            score_col = (
                "custom_score"
                if "custom_score" in rank_df.columns
                else "composite_score"
            )
            top_signals = rank_df.head(20).copy()

            capital = st.number_input(
                "💰 Capital (₹)", value=1_000_000.0, step=100_000.0, format="%.0f"
            )

            try:
                rm = RiskManager(
                    ohlcv_db_path=OHLCV_DB, feature_store_dir=FEATURE_STORE
                )
                top_signals["probability"] = top_signals[score_col] / 100
                top_signals["prediction"] = 1
                top_signals["direction"] = "LONG"

                portfolio = rm.build_portfolio(
                    signals=top_signals,
                    capital=capital,
                    regime="TREND",
                    regime_multiplier=1.0,
                )

                if not portfolio.empty:
                    display_port = portfolio.copy()
                    display_port["weight_pct"] = (display_port["weight"] * 100).round(2)
                    display_port["position_value"] = display_port[
                        "position_value"
                    ].round(0)
                    display_port["shares"] = display_port["shares"].astype(int)
                    display_port["stop_loss"] = display_port["stop_loss"].round(2)
                    display_port["target"] = display_port["target"].round(2)
                    display_port["risk_rupees"] = display_port["risk_rupees"].round(0)

                    show_cols = [
                        "symbol_id",
                        "weight_pct",
                        "position_value",
                        "shares",
                        "close",
                        "stop_loss",
                        "target",
                        "risk_rupees",
                    ]
                    show_cols = [c for c in show_cols if c in display_port.columns]
                    rename_p = {
                        "symbol_id": "Symbol",
                        "weight_pct": "Weight %",
                        "position_value": "Pos Value (₹)",
                        "shares": "Shares",
                        "close": "Price",
                        "stop_loss": "SL",
                        "target": "Target",
                        "risk_rupees": "Risk (₹)",
                    }
                    st.dataframe(
                        display_port[show_cols].rename(columns=rename_p),
                        use_container_width=True,
                        hide_index=True,
                    )

                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Positions", len(portfolio))
                    with col2:
                        total_w = portfolio["weight"].sum() * 100
                        st.metric("Total Exposure", f"{total_w:.1f}%")
                    with col3:
                        total_risk = portfolio["risk_rupees"].sum()
                        st.metric("Total Risk", f"₹{total_risk:,.0f}")
                    with col4:
                        risk_pct = (total_risk / capital * 100) if capital else 0
                        st.metric("Risk % of Capital", f"{risk_pct:.1f}%")
                else:
                    st.warning("Could not size positions. Check ATR data availability.")
            except Exception as e:
                st.error(f"Portfolio builder error: {e}")


if __name__ == "__main__":
    main()
