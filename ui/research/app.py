"""
AI Trading System — Streamlit Command Center Dashboard

Usage: streamlit run ui/research/app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import json
import sqlite3
import plotly.graph_objects as go
import plotly.express as px
import plotly.subplots as make_subplots
from plotly.subplots import make_subplots
import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict

import streamlit.components.v1 as components

from core.bootstrap import ensure_project_root_on_path
ensure_project_root_on_path(__file__)
from analytics.regime_detector import RegimeDetector
from analytics.ranker import StockRanker
from analytics.risk_manager import RiskManager
from analytics.registry import RegistryStore
from analytics.visualizations import Visualizer
from core.env import load_project_env
from core.paths import get_domain_paths

logging.getLogger("streamlit").setLevel(logging.WARNING)

PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
load_project_env(PROJECT_ROOT)

DOMAIN_PATHS = get_domain_paths(PROJECT_ROOT, "operational")
OHLCV_DB = str(DOMAIN_PATHS.ohlcv_db_path)
FEATURE_STORE = str(DOMAIN_PATHS.feature_store_dir)
MASTER_DB = str(DOMAIN_PATHS.master_db_path)
REPORTS_DIR = str(DOMAIN_PATHS.reports_dir)
PIPELINE_RUNS_DIR = str(DOMAIN_PATHS.pipeline_runs_dir)

RESEARCH_DOMAIN_PATHS = get_domain_paths(PROJECT_ROOT, "research")
RESEARCH_OHLCV_DB = str(RESEARCH_DOMAIN_PATHS.ohlcv_db_path)
RESEARCH_FEATURE_STORE = str(RESEARCH_DOMAIN_PATHS.feature_store_dir)
RESEARCH_MODELS_DIR = str(RESEARCH_DOMAIN_PATHS.model_dir)
RESEARCH_REPORTS_DIR = str(RESEARCH_DOMAIN_PATHS.reports_dir)


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


@st.cache_data(show_spinner=False, ttl=60 * 60)
def load_research_breadth_history(start_date: str = "2010-01-01") -> pd.DataFrame:
    """Load long-run breadth series for percent of stocks above key SMAs."""
    conn = duckdb.connect(RESEARCH_OHLCV_DB, read_only=True)
    try:
        query = f"""
            WITH base AS (
                SELECT
                    CAST(timestamp AS DATE) AS trade_date,
                    symbol_id,
                    close,
                    AVG(close) OVER (
                        PARTITION BY symbol_id
                        ORDER BY CAST(timestamp AS DATE)
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS sma_20,
                    COUNT(close) OVER (
                        PARTITION BY symbol_id
                        ORDER BY CAST(timestamp AS DATE)
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS obs_20,
                    AVG(close) OVER (
                        PARTITION BY symbol_id
                        ORDER BY CAST(timestamp AS DATE)
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) AS sma_50,
                    COUNT(close) OVER (
                        PARTITION BY symbol_id
                        ORDER BY CAST(timestamp AS DATE)
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) AS obs_50,
                    AVG(close) OVER (
                        PARTITION BY symbol_id
                        ORDER BY CAST(timestamp AS DATE)
                        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                    ) AS sma_200,
                    COUNT(close) OVER (
                        PARTITION BY symbol_id
                        ORDER BY CAST(timestamp AS DATE)
                        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                    ) AS obs_200
                FROM _catalog
                WHERE exchange = 'NSE'
                  AND CAST(timestamp AS DATE) >= DATE '{start_date}'
            )
            SELECT
                trade_date,
                SUM(CASE WHEN obs_20 >= 20 THEN 1 ELSE 0 END) AS eligible_20,
                SUM(CASE WHEN obs_20 >= 20 AND close > sma_20 THEN 1 ELSE 0 END) AS above_20_count,
                ROUND(
                    SUM(CASE WHEN obs_20 >= 20 AND close > sma_20 THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(SUM(CASE WHEN obs_20 >= 20 THEN 1 ELSE 0 END), 0),
                    2
                ) AS pct_above_20,
                SUM(CASE WHEN obs_50 >= 50 THEN 1 ELSE 0 END) AS eligible_50,
                SUM(CASE WHEN obs_50 >= 50 AND close > sma_50 THEN 1 ELSE 0 END) AS above_50_count,
                ROUND(
                    SUM(CASE WHEN obs_50 >= 50 AND close > sma_50 THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(SUM(CASE WHEN obs_50 >= 50 THEN 1 ELSE 0 END), 0),
                    2
                ) AS pct_above_50,
                SUM(CASE WHEN obs_200 >= 200 THEN 1 ELSE 0 END) AS eligible_200,
                SUM(CASE WHEN obs_200 >= 200 AND close > sma_200 THEN 1 ELSE 0 END) AS above_200_count,
                ROUND(
                    SUM(CASE WHEN obs_200 >= 200 AND close > sma_200 THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(SUM(CASE WHEN obs_200 >= 200 THEN 1 ELSE 0 END), 0),
                    2
                ) AS pct_above_200
            FROM base
            GROUP BY trade_date
            ORDER BY trade_date
        """
        df = conn.execute(query).fetchdf()
    finally:
        conn.close()
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def get_dashboard_health(payload: Dict | None = None) -> Dict[str, object]:
    """Collect lightweight operational health checks for the dashboard."""
    checks: list[dict[str, object]] = []
    summary: dict[str, object] = {}
    payload = payload or {}

    pending_symbols = 0
    unexpected_symbols = 0
    try:
        conn = duckdb.connect(OHLCV_DB, read_only=True)
        try:
            latest_ohlcv = conn.execute(
                "SELECT MAX(CAST(timestamp AS DATE)) FROM _catalog WHERE exchange = 'NSE'"
            ).fetchone()[0]
            latest_delivery = conn.execute(
                "SELECT MAX(CAST(timestamp AS DATE)) FROM _delivery WHERE exchange = 'NSE'"
            ).fetchone()[0]
            swapped_catalog = conn.execute(
                "SELECT COUNT(*) FROM _catalog WHERE symbol_id IN ('NSE','BSE') AND exchange NOT IN ('NSE','BSE')"
            ).fetchone()[0]
            swapped_delivery = conn.execute(
                "SELECT COUNT(*) FROM _delivery WHERE symbol_id IN ('NSE','BSE') AND exchange NOT IN ('NSE','BSE')"
            ).fetchone()[0]
            catalog_symbols = {
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT symbol_id FROM _catalog WHERE exchange = 'NSE'"
                ).fetchall()
            }
        finally:
            conn.close()
    except Exception as exc:
        return {
            "status": "error",
            "summary": {"error": str(exc)},
            "checks": [{"name": "db_connection", "status": "error", "detail": str(exc)}],
        }

    try:
        master_conn = sqlite3.connect(MASTER_DB)
        try:
            master_symbols = {
                row[0]
                for row in master_conn.execute(
                    "SELECT DISTINCT Symbol FROM stock_details WHERE exchange = 'NSE'"
                ).fetchall()
            }
        finally:
            master_conn.close()
        pending_symbols = len(master_symbols - catalog_symbols)
        unexpected_symbols = len(catalog_symbols - master_symbols)
    except Exception as exc:
        checks.append(
            {
                "name": "universe_alignment",
                "status": "error",
                "detail": f"Failed to compare master universe: {exc}",
            }
        )

    latest_payload_path = payload.get("_artifact_path")
    latest_payload_time = None
    payload_age_minutes = None
    if latest_payload_path and Path(latest_payload_path).exists():
        latest_payload_time = datetime.fromtimestamp(Path(latest_payload_path).stat().st_mtime)
        payload_age_minutes = round((datetime.now() - latest_payload_time).total_seconds() / 60, 1)

    delivery_lag_days = None
    if latest_ohlcv and latest_delivery:
        delivery_lag_days = (pd.Timestamp(latest_ohlcv) - pd.Timestamp(latest_delivery)).days

    checks.append(
        {
            "name": "pipeline_payload",
            "status": "ok" if latest_payload_path else "warn",
            "detail": latest_payload_path or "No dashboard payload found",
        }
    )
    checks.append(
        {
            "name": "delivery_freshness",
            "status": "ok" if delivery_lag_days is not None and delivery_lag_days <= 3 else "warn",
            "detail": f"Delivery lag: {delivery_lag_days} day(s)" if delivery_lag_days is not None else "No delivery data",
        }
    )
    checks.append(
        {
            "name": "catalog_schema",
            "status": "ok" if swapped_catalog == 0 else "error",
            "detail": f"Swapped _catalog rows: {swapped_catalog}",
        }
    )
    checks.append(
        {
            "name": "delivery_schema",
            "status": "ok" if swapped_delivery == 0 else "error",
            "detail": f"Swapped _delivery rows: {swapped_delivery}",
        }
    )
    checks.append(
        {
            "name": "universe_alignment",
            "status": "ok" if pending_symbols == 0 and unexpected_symbols == 0 else "warn",
            "detail": (
                f"Pending symbols: {pending_symbols}, unexpected symbols: {unexpected_symbols}"
            ),
        }
    )

    overall_status = "ok"
    if any(check["status"] == "error" for check in checks):
        overall_status = "error"
    elif any(check["status"] == "warn" for check in checks):
        overall_status = "warn"

    summary.update(
        {
            "latest_ohlcv_date": str(latest_ohlcv) if latest_ohlcv else None,
            "latest_delivery_date": str(latest_delivery) if latest_delivery else None,
            "delivery_lag_days": delivery_lag_days,
            "payload_age_minutes": payload_age_minutes,
            "swapped_catalog_rows": int(swapped_catalog),
            "swapped_delivery_rows": int(swapped_delivery),
            "pending_symbol_count": int(pending_symbols),
            "unexpected_symbol_count": int(unexpected_symbols),
        }
    )
    return {"status": overall_status, "summary": summary, "checks": checks}


def load_latest_dashboard_payload() -> Dict:
    """Load the most recent rank-stage dashboard payload if present."""
    runs_dir = Path(PIPELINE_RUNS_DIR)
    if not runs_dir.exists():
        return {}

    candidates = sorted(
        runs_dir.glob("*/rank/attempt_*/dashboard_payload.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return {}

    latest_path = candidates[0]
    with latest_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    payload["_artifact_path"] = str(latest_path)
    return payload


def load_latest_rank_fallback() -> Dict:
    """Build a minimal payload from the latest rank artifacts when no dashboard payload exists yet."""
    runs_dir = Path(PIPELINE_RUNS_DIR)
    if not runs_dir.exists():
        return {}

    ranked_candidates = sorted(
        runs_dir.glob("*/rank/attempt_*/ranked_signals.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not ranked_candidates:
        return {}

    ranked_path = ranked_candidates[0]
    rank_dir = ranked_path.parent
    ranked_df = pd.read_csv(ranked_path)
    breakout_path = rank_dir / "breakout_scan.csv"
    stock_scan_path = rank_dir / "stock_scan.csv"
    sector_path = rank_dir / "sector_dashboard.csv"

    breakout_df = pd.read_csv(breakout_path) if breakout_path.exists() else pd.DataFrame()
    stock_scan_df = pd.read_csv(stock_scan_path) if stock_scan_path.exists() else pd.DataFrame()
    sector_df = pd.read_csv(sector_path) if sector_path.exists() else pd.DataFrame()

    top_sector = None
    if not sector_df.empty:
        sector_col = "Sector" if "Sector" in sector_df.columns else sector_df.columns[0]
        top_sector = sector_df.iloc[0].get(sector_col)

    return {
        "summary": {
            "run_id": ranked_path.parts[-4],
            "ranked_count": int(len(ranked_df)),
            "breakout_count": int(len(breakout_df)),
            "stock_scan_count": int(len(stock_scan_df)),
            "sector_count": int(len(sector_df)),
            "top_symbol": ranked_df.iloc[0].get("symbol_id") if not ranked_df.empty else None,
            "top_sector": top_sector,
        },
        "ranked_signals": ranked_df.head(10).to_dict(orient="records"),
        "breakout_scan": breakout_df.head(10).to_dict(orient="records"),
        "stock_scan": stock_scan_df.head(10).to_dict(orient="records"),
        "sector_dashboard": sector_df.head(10).to_dict(orient="records"),
        "warnings": ["Dashboard payload missing; showing latest rank artifacts fallback."],
        "_artifact_path": str(ranked_path),
    }


def list_research_model_metadata_paths() -> List[Path]:
    """List research model metadata files sorted by most recent first."""
    model_dir = Path(RESEARCH_MODELS_DIR)
    if not model_dir.exists():
        return []
    return sorted(
        model_dir.glob("*.metadata.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def load_model_metadata(metadata_path: Path) -> Dict:
    """Load one research model metadata file."""
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["_metadata_path"] = str(metadata_path)
    model_path = metadata_path.with_name(metadata_path.name.replace(".metadata.json", ".txt"))
    if model_path.exists():
        metadata["_model_path"] = str(model_path)
    return metadata


def ensure_feature_importance_plot(metadata_path: Path, top_n: int = 20) -> Optional[str]:
    """Return a feature-importance chart path, generating it if needed."""
    report_dir = Path(RESEARCH_REPORTS_DIR)
    report_dir.mkdir(parents=True, exist_ok=True)
    output_path = report_dir / (
        metadata_path.name.replace(".metadata.json", "") + "_feature_importance.png"
    )
    if output_path.exists():
        return str(output_path)

    visualizer = Visualizer(
        ohlcv_db_path=RESEARCH_OHLCV_DB,
        feature_store_dir=RESEARCH_FEATURE_STORE,
        output_dir=RESEARCH_REPORTS_DIR,
    )
    generated = visualizer.plot_feature_importance(
        metadata_path=str(metadata_path),
        top_n=top_n,
        output_path=str(output_path),
    )
    return generated


def load_shadow_overlay() -> pd.DataFrame:
    """Load the latest recorded shadow-monitor overlay."""
    registry = RegistryStore(PROJECT_ROOT)
    rows = registry.get_shadow_overlay()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "prediction_date" in df.columns:
        df["prediction_date"] = pd.to_datetime(df["prediction_date"])
    return df


def load_shadow_period_summary(grain: str, horizon: int, periods: int = 12) -> pd.DataFrame:
    """Load weekly/monthly shadow-monitor comparison summaries."""
    registry = RegistryStore(PROJECT_ROOT)
    rows = registry.get_shadow_period_summary(grain=grain, horizon=horizon, periods=periods)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["period_start"] = pd.to_datetime(df["period_start"])
    return df


def pivot_shadow_summary(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Pivot summary rows so technical / ML / blend sit side by side."""
    if summary_df.empty:
        return summary_df
    pivoted = summary_df.pivot(index="period_start", columns="variant", values=["picks", "hit_rate", "avg_return"])
    pivoted.columns = [f"{metric}_{variant}" for metric, variant in pivoted.columns]
    pivoted = pivoted.reset_index().sort_values("period_start", ascending=False)
    return pivoted


def normalize_rank_df(df: pd.DataFrame) -> pd.DataFrame:
    """Repair rank outputs if symbol/exchange columns were swapped upstream."""
    if df is None or df.empty or "symbol_id" not in df.columns or "exchange" not in df.columns:
        return df
    normalized = df.copy()
    valid_exchanges = {"NSE", "BSE"}
    swap_mask = normalized["symbol_id"].isin(valid_exchanges) & ~normalized["exchange"].isin(valid_exchanges)
    if swap_mask.any():
        original = normalized.loc[swap_mask, "symbol_id"].copy()
        normalized.loc[swap_mask, "symbol_id"] = normalized.loc[swap_mask, "exchange"].astype(str)
        normalized.loc[swap_mask, "exchange"] = original.astype(str)
    return normalized


def reorder_columns(df: pd.DataFrame, preferred: list[str]) -> pd.DataFrame:
    """Move important columns to the front while preserving the remaining order."""
    if df is None or df.empty:
        return df
    ordered = [column for column in preferred if column in df.columns]
    remaining = [column for column in df.columns if column not in ordered]
    return df[ordered + remaining]


def is_suspicious_rank_df(df: pd.DataFrame) -> bool:
    """Detect stale legacy ranking frames that collapse factors to ~50."""
    if df is None or df.empty:
        return False

    factor_cols = [
        col
        for col in [
            "rel_strength_score",
            "vol_intensity_score",
            "trend_score_score",
            "prox_high_score",
            "delivery_pct_score",
            "sector_strength_score",
        ]
        if col in df.columns
    ]
    if not factor_cols:
        return False

    sample = df.head(50)
    unique_counts = [sample[col].nunique(dropna=False) for col in factor_cols]
    if all(count <= 1 for count in unique_counts):
        return True

    suspicious_value = 50.052192066805844
    near_flat = 0
    for col in factor_cols:
        series = pd.to_numeric(sample[col], errors="coerce").dropna()
        if not series.empty and np.isclose(series, suspicious_value, atol=0.01).all():
            near_flat += 1
    return near_flat >= max(2, len(factor_cols) // 2)


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
    partitioned_features = {
        "rsi": ("rsi", "close, rs, rsi_14 AS rsi"),
        "adx": ("adx", "adx_14 AS adx_value, plus_di_14 AS adx_plus, minus_di_14 AS adx_minus"),
        "atr": ("atr", "atr_14 AS atr_value"),
        "roc": ("roc", "close, roc_20 AS roc_value"),
        "supertrend": (
            "supertrend",
            "close, supertrend_10_3 AS st_upper, supertrend_10_3 AS st_lower, supertrend_dir_10_3 AS st_signal",
        ),
    }
    per_symbol_features = ["ema", "macd"]

    conn = duckdb.connect(OHLCV_DB, read_only=True)
    try:
        for feat, (subdir, cols) in partitioned_features.items():
            pattern = os.path.join(
                FEATURE_STORE, subdir, exchange, "*.parquet"
            ).replace("\\", "/")
            if os.path.exists(os.path.join(FEATURE_STORE, subdir, exchange)):
                result = conn.execute(
                    f"""
                    SELECT timestamp, {cols} FROM read_parquet('{pattern}')
                    WHERE symbol_id = '{symbol}' AND exchange = '{exchange}'
                    ORDER BY timestamp
                    """
                ).fetchdf()
                if not result.empty:
                    result["timestamp"] = pd.to_datetime(result["timestamp"])
                    result["timestamp"] = result["timestamp"].dt.normalize()
                    result.set_index("timestamp", inplace=True)
                    features[feat] = result
    finally:
        conn.close()

    bb_dir = os.path.join(FEATURE_STORE, "bb", exchange)
    if os.path.exists(bb_dir):
        pattern = os.path.join(bb_dir, "*.parquet").replace("\\", "/")
        conn = duckdb.connect(OHLCV_DB, read_only=True)
        try:
            result = conn.execute(
                f"""
                SELECT timestamp, close, bb_upper, bb_middle, bb_lower
                FROM read_parquet('{pattern}')
                WHERE symbol_id = '{symbol}' AND exchange = '{exchange}'
                ORDER BY timestamp
                """
            ).fetchdf()
        except Exception:
            result = pd.DataFrame()
        finally:
            conn.close()
        if not result.empty:
            result["timestamp"] = pd.to_datetime(result["timestamp"])
            result["timestamp"] = result["timestamp"].dt.normalize()
            result.set_index("timestamp", inplace=True)
            features["bb"] = result

    for feat in per_symbol_features:
        path = os.path.join(FEATURE_STORE, feat, exchange, f"{symbol}.parquet")
        if os.path.exists(path):
            df = pd.read_parquet(path)
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df["timestamp"] = df["timestamp"].dt.normalize()
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
        rows=5,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.40, 0.14, 0.14, 0.14, 0.18],
        subplot_titles=("Price + MAs", "Volume", "RSI", "MACD", "Supertrend"),
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
        if "st_signal" in st_df.columns:
            st_signal = st_df["st_signal"].reindex(ohlcv.index).ffill().fillna(0)
            up = st_df["st_upper"].reindex(ohlcv.index).ffill()
            down = st_df["st_lower"].reindex(ohlcv.index).ffill()
            fig.add_trace(
                go.Scatter(
                    x=ohlcv.index,
                    y=up,
                    name="ST Upper",
                    line=dict(color="#00BCD4", width=1),
                    opacity=0.5,
                ),
                row=1,
                col=1,
            )
            fig.add_trace(
                go.Scatter(
                    x=ohlcv.index,
                    y=down,
                    name="ST Lower",
                    line=dict(color="#FF5722", width=1),
                    opacity=0.5,
                ),
                row=1,
                col=1,
            )
            bullish_mask = st_signal > 0
            bearish_mask = st_signal < 0
            colors_st = np.where(
                bullish_mask, "#26a69a", np.where(bearish_mask, "#ef5350", "#9E9E9E")
            )
            fig.add_trace(
                go.Bar(
                    x=st_df.index,
                    y=np.where(bullish_mask, 1, np.where(bearish_mask, -1, 0)),
                    marker_color=colors_st,
                    name="ST Signal",
                    opacity=0.8,
                ),
                row=5,
                col=1,
            )
            fig.add_annotation(
                x=st_df.index[-1] if not st_df.empty else ohlcv.index[-1],
                y=st_signal.iloc[-1] if not st_df.empty else 0,
                text=f"ST: {'BULL' if st_signal.iloc[-1] > 0 else 'BEAR'}",
                showarrow=True,
                arrowhead=2,
                font=dict(
                    color="#26a69a" if st_signal.iloc[-1] > 0 else "#ef5350",
                    size=12,
                ),
                row=5,
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
        col_map = {
            "macd_line": ("MACD", "#2196F3"),
            "macd_signal_9": ("Signal", "#FF9800"),
            "macd_histogram": ("Histogram", "#9E9E9E"),
        }
        for col, (label, color) in col_map.items():
            if col in macd_df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=macd_df.index,
                        y=macd_df[col],
                        name=label,
                        line=dict(color=color, width=1.5),
                    ),
                    row=4,
                    col=1,
                )

    fig.update_layout(
        title=f"{symbol} — Price Chart & Indicators",
        template="plotly_dark",
        height=950,
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


def build_dashboard_weights() -> Dict[str, float]:
    """Merge sidebar slider weights with ranker defaults for hidden factors."""
    defaults = StockRanker.WEIGHTS.copy()
    defaults.update(
        {
            "relative_strength": st.session_state.weights["rs"] / 100,
            "volume_intensity": st.session_state.weights["vol"] / 100,
            "trend_persistence": st.session_state.weights["trend"] / 100,
            "proximity_highs": st.session_state.weights["high"] / 100,
        }
    )
    return defaults


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
    if "rank_df_source" not in st.session_state:
        st.session_state.rank_df_source = None

    dashboard_payload = load_latest_dashboard_payload() or load_latest_rank_fallback()
    dashboard_health = get_dashboard_health(dashboard_payload)
    if st.session_state.rank_df is not None:
        st.session_state.rank_df = normalize_rank_df(st.session_state.rank_df)
        score_col = "custom_score" if "custom_score" in st.session_state.rank_df.columns else "composite_score"
        is_flat_scores = (
            score_col in st.session_state.rank_df.columns
            and not st.session_state.rank_df.empty
            and st.session_state.rank_df[score_col].nunique(dropna=False) <= 1
        )
        if is_flat_scores or is_suspicious_rank_df(st.session_state.rank_df):
            st.session_state.rank_df = None
            st.session_state.rank_df_source = None

    latest_artifact_path = dashboard_payload.get("_artifact_path") if dashboard_payload else None
    if dashboard_payload and st.session_state.rank_df is None:
        seeded_rank_df = normalize_rank_df(pd.DataFrame(dashboard_payload.get("ranked_signals", [])))
        if not seeded_rank_df.empty and not is_suspicious_rank_df(seeded_rank_df):
            st.session_state.rank_df = seeded_rank_df
            st.session_state.rank_df_source = latest_artifact_path
    elif (
        dashboard_payload
        and latest_artifact_path
        and st.session_state.rank_df_source not in {None, "live_query", latest_artifact_path}
    ):
        seeded_rank_df = normalize_rank_df(pd.DataFrame(dashboard_payload.get("ranked_signals", [])))
        if not seeded_rank_df.empty and not is_suspicious_rank_df(seeded_rank_df):
            st.session_state.rank_df = seeded_rank_df
            st.session_state.rank_df_source = latest_artifact_path

    with st.sidebar:
        st.header("⚙️ Settings")

        with st.expander("📁 Data Status", expanded=True):
            stats = get_db_stats()
            st.metric("Symbols", f"{stats.get('symbols', 0):,}")
            st.metric("OHLCV Rows", f"{stats.get('rows', 0):,}")
            st.metric("Latest Date", stats.get("latest_date", "—"))
            health_summary = dashboard_health.get("summary", {})
            st.metric("Delivery Date", health_summary.get("latest_delivery_date", "—"))
            st.metric("Delivery Lag", health_summary.get("delivery_lag_days", "—"))
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
                        "STRONG_BULL_TREND": "🟢🟢",
                        "BULLISH_MIXED": "🟢",
                        "STRONG_BEAR_TREND": "🔻",
                        "BEARISH_MIXED": "🟠",
                        "MEAN_REV": "🟡",
                        "RANGE_BOUND": "🔴",
                        "MIXED": "🟡",
                    }.get(regime, "⚪")
                    st.markdown(f"**Regime:** {regime_color} {regime}")
                    st.caption(f"ADX Median: {regime_info.get('adx_median', 'N/A')}")
                    st.caption(f"Trending %: {regime_info.get('trending_pct', 'N/A')}")
                    st.caption(
                        "Breadth: "
                        f"{regime_info.get('pct_above_50', 'N/A')}% >50DMA, "
                        f"{regime_info.get('pct_above_200', 'N/A')}% >200DMA"
                    )
                    strategy = {
                        "TREND": "Trend-Follow",
                        "STRONG_TREND": "Trend-Follow",
                        "STRONG_BULL_TREND": "Trend-Follow",
                        "BULLISH_MIXED": "Selective Longs",
                        "STRONG_BEAR_TREND": "Defensive / Capital Protection",
                        "BEARISH_MIXED": "Low Exposure / Defensive",
                        "MEAN_REV": "Mean Reversion",
                        "RANGE_BOUND": "Mean Reversion",
                        "MIXED": "Mixed / Low Exposure",
                    }.get(regime, "Unknown")
                    st.info(f"Strategy: **{strategy}**")
            except Exception as e:
                st.warning(f"Could not detect regime: {e}")

        with st.expander("📊 Breadth History", expanded=False):
            try:
                breadth_df = load_research_breadth_history()
                if breadth_df.empty:
                    st.info("No breadth history available.")
                else:
                    latest_breadth = breadth_df.iloc[-1]
                    b1, b2, b3 = st.columns(3)
                    with b1:
                        st.metric("% Above 20 SMA", f"{latest_breadth['pct_above_20']:.2f}%")
                    with b2:
                        st.metric("% Above 50 SMA", f"{latest_breadth['pct_above_50']:.2f}%")
                    with b3:
                        st.metric("% Above 200 SMA", f"{latest_breadth['pct_above_200']:.2f}%")

                    fig_breadth = go.Figure()
                    fig_breadth.add_trace(
                        go.Scatter(
                            x=breadth_df["trade_date"],
                            y=breadth_df["pct_above_20"],
                            mode="lines",
                            name="% Above 20 SMA",
                            line=dict(color="#2563eb", width=1.6),
                        )
                    )
                    fig_breadth.add_trace(
                        go.Scatter(
                            x=breadth_df["trade_date"],
                            y=breadth_df["pct_above_50"],
                            mode="lines",
                            name="% Above 50 SMA",
                            line=dict(color="#14b8a6", width=1.6),
                        )
                    )
                    fig_breadth.add_trace(
                        go.Scatter(
                            x=breadth_df["trade_date"],
                            y=breadth_df["pct_above_200"],
                            mode="lines",
                            name="% Above 200 SMA",
                            line=dict(color="#dc2626", width=1.8),
                        )
                    )
                    fig_breadth.add_hline(y=60, line_dash="dash", line_color="#16a34a", opacity=0.5)
                    fig_breadth.add_hline(y=40, line_dash="dash", line_color="#ea580c", opacity=0.5)
                    fig_breadth.update_layout(
                        height=380,
                        margin=dict(l=20, r=20, t=20, b=20),
                        yaxis_title="% of Stocks Above SMA",
                        xaxis_title="Date",
                        legend_title="Breadth",
                    )
                    st.plotly_chart(fig_breadth, use_container_width=True)
                    st.caption(
                        "Research universe breadth since 2010 using NSE symbols with enough history for each SMA window."
                    )
            except Exception as e:
                st.warning(f"Could not load breadth history: {e}")

        st.divider()
        st.caption(f"Last refresh: {st.session_state.last_refresh or 'Never'}")

    tab_pipeline, tab_overview, tab_ranking, tab_chart, tab_ml, tab_portfolio = st.tabs(
        ["🧭 Pipeline", "📋 Overview", "🏆 Ranking", "📈 Chart", "🧠 ML", "💼 Portfolio"]
    )

    with tab_pipeline:
        st.subheader("🧭 Unified Pipeline Dashboard")
        health_cols = st.columns(4)
        with health_cols[0]:
            st.metric("Health", str(dashboard_health.get("status", "unknown")).upper())
        with health_cols[1]:
            st.metric("OHLCV Date", dashboard_health.get("summary", {}).get("latest_ohlcv_date", "—"))
        with health_cols[2]:
            st.metric("Delivery Date", dashboard_health.get("summary", {}).get("latest_delivery_date", "—"))
        with health_cols[3]:
            st.metric("Payload Age (min)", dashboard_health.get("summary", {}).get("payload_age_minutes", "—"))

        with st.expander("🩺 Self Check", expanded=False):
            checks_df = pd.DataFrame(dashboard_health.get("checks", []))
            if not checks_df.empty:
                st.dataframe(checks_df, use_container_width=True, hide_index=True)
            else:
                st.info("No health checks available.")

        if not dashboard_payload:
            st.info("No dashboard payload found yet. Run the rank stage first.")
        else:
            summary = dashboard_payload.get("summary", {})
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.metric("Run Date", summary.get("run_date", "—"))
            with c2:
                st.metric("Top Symbol", summary.get("top_symbol", "—"))
            with c3:
                st.metric("Breakouts", summary.get("breakout_count", 0))
            with c4:
                st.metric("Leading Sector", summary.get("top_sector", "—"))

            st.caption(f"Payload: `{dashboard_payload.get('_artifact_path', 'n/a')}`")

            col_left, col_right = st.columns([1, 1])
            with col_left:
                st.markdown("**Top Ranked Signals**")
                ranked_df = normalize_rank_df(pd.DataFrame(dashboard_payload.get("ranked_signals", [])))
                if ranked_df.empty:
                    st.info("No ranked signals in payload.")
                else:
                    ranked_df = reorder_columns(
                        ranked_df,
                        [
                            "symbol_id",
                            "exchange",
                            "composite_score",
                            "close",
                            "rel_strength_score",
                            "sector_strength_score",
                            "vol_intensity_score",
                            "trend_score_score",
                            "prox_high_score",
                            "delivery_pct_score",
                        ],
                    )
                    st.dataframe(ranked_df, use_container_width=True, hide_index=True)

                st.markdown("**Breakout Monitor**")
                breakout_df = pd.DataFrame(dashboard_payload.get("breakout_scan", []))
                if breakout_df.empty:
                    st.info("No breakout candidates in payload.")
                else:
                    st.dataframe(breakout_df, use_container_width=True, hide_index=True)

            with col_right:
                st.markdown("**Sector Dashboard**")
                sector_df = pd.DataFrame(dashboard_payload.get("sector_dashboard", []))
                if sector_df.empty:
                    st.info("No sector dashboard rows in payload.")
                else:
                    sector_df = reorder_columns(
                        sector_df,
                        ["Sector", "RS", "Momentum", "Quadrant", "Top Stocks"],
                    )
                    st.dataframe(sector_df, use_container_width=True, hide_index=True)

                st.markdown("**Stock Scan**")
                stock_scan_df = pd.DataFrame(dashboard_payload.get("stock_scan", []))
                if stock_scan_df.empty:
                    st.info("No stock scan rows in payload.")
                else:
                    stock_scan_df = reorder_columns(
                        stock_scan_df,
                        ["Symbol", "symbol_id", "category", "score", "sector", "why"],
                    )
                    st.dataframe(stock_scan_df, use_container_width=True, hide_index=True)

                warnings = dashboard_payload.get("warnings", [])
                if warnings:
                    st.markdown("**Warnings**")
                    for warning in warnings:
                        st.warning(warning)

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

        st.subheader("📊 Long-Term Breadth")
        try:
            breadth_df = load_research_breadth_history()
            if breadth_df.empty:
                st.info("No breadth history available.")
            else:
                latest_breadth = breadth_df.iloc[-1]
                m1, m2, m3, m4 = st.columns(4)
                with m1:
                    st.metric("Eligible 200 SMA Universe", f"{int(latest_breadth['eligible_200']):,}")
                with m2:
                    st.metric("% > 20 SMA", f"{latest_breadth['pct_above_20']:.2f}%")
                with m3:
                    st.metric("% > 50 SMA", f"{latest_breadth['pct_above_50']:.2f}%")
                with m4:
                    st.metric("% > 200 SMA", f"{latest_breadth['pct_above_200']:.2f}%")

                fig_breadth_overview = go.Figure()
                fig_breadth_overview.add_trace(
                    go.Scatter(
                        x=breadth_df["trade_date"],
                        y=breadth_df["pct_above_20"],
                        mode="lines",
                        name="% Above 20 SMA",
                        line=dict(color="#2563eb", width=1.6),
                    )
                )
                fig_breadth_overview.add_trace(
                    go.Scatter(
                        x=breadth_df["trade_date"],
                        y=breadth_df["pct_above_50"],
                        mode="lines",
                        name="% Above 50 SMA",
                        line=dict(color="#14b8a6", width=1.6),
                    )
                )
                fig_breadth_overview.add_trace(
                    go.Scatter(
                        x=breadth_df["trade_date"],
                        y=breadth_df["pct_above_200"],
                        mode="lines",
                        name="% Above 200 SMA",
                        line=dict(color="#dc2626", width=1.8),
                    )
                )
                fig_breadth_overview.add_hline(y=60, line_dash="dash", line_color="#16a34a", opacity=0.5)
                fig_breadth_overview.add_hline(y=40, line_dash="dash", line_color="#ea580c", opacity=0.5)
                fig_breadth_overview.update_layout(
                    height=420,
                    margin=dict(l=20, r=20, t=20, b=20),
                    yaxis_title="% of Stocks Above SMA",
                    xaxis_title="Date",
                    legend_title="Breadth",
                )
                st.plotly_chart(fig_breadth_overview, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not render long-term breadth chart: {e}")

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
                        rank_df = normalize_rank_df(rank_df)
                        st.session_state.rank_df = rank_df
                        st.session_state.rank_df_source = "live_query"
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
                    weights_dict = build_dashboard_weights()
                    rank_df = ranker.rank_all(
                        date=None,
                        exchanges=["NSE"],
                        min_score=0.0,
                        top_n=None,
                        weights=weights_dict,
                    )
                    rank_df = normalize_rank_df(rank_df)
                    st.session_state.rank_df = rank_df
                    st.session_state.rank_df_source = "live_query"
                    st.session_state.last_refresh = datetime.now().strftime("%H:%M:%S")
                st.success(f"Ranked {len(rank_df):,} stocks in {time.time() - t0:.1f}s")

    with tab_ranking:
        st.subheader("🏆 Multi-Factor Stock Ranking")

        weights_dict = build_dashboard_weights()
        force_refresh = st.button("🔄 Refresh Rankings", use_container_width=True)

        needs_live_query = (
            st.session_state.rank_df is None
            or force_refresh
            or st.session_state.rank_df_source != "live_query"
        )

        if needs_live_query:
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
                    rank_df = normalize_rank_df(rank_df)
                    st.session_state.rank_df = rank_df
                    st.session_state.rank_df_source = "live_query"
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
                "delivery_pct_score",
                "sector_strength_score",
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
                "delivery_pct_score": "Delivery",
                "sector_strength_score": "Sector",
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

    with tab_ml:
        st.subheader("🧠 LightGBM Research Models")

        metadata_paths = list_research_model_metadata_paths()
        if not metadata_paths:
            st.info("No research model metadata found yet. Train a model first.")
        else:
            model_options = {
                f"{path.stem.replace('.metadata', '')}  |  "
                f"{datetime.fromtimestamp(path.stat().st_mtime).strftime('%Y-%m-%d %H:%M')}": path
                for path in metadata_paths
            }
            selected_model_label = st.selectbox(
                "Model Snapshot",
                options=list(model_options.keys()),
                index=0,
            )
            metadata_path = model_options[selected_model_label]
            metadata = load_model_metadata(metadata_path)
            evaluation = metadata.get("evaluation", {})
            dataset_metadata = metadata.get("dataset_metadata", {})
            top_features = metadata.get("top_features", [])

            metric_cols = st.columns(5)
            with metric_cols[0]:
                st.metric("Validation AUC", f"{evaluation.get('validation_auc', 0):.4f}")
            with metric_cols[1]:
                st.metric(
                    "Precision @ 10%",
                    f"{evaluation.get('precision_at_10pct', 0):.3f}",
                )
            with metric_cols[2]:
                st.metric(
                    "Avg Return Top 10%",
                    f"{evaluation.get('avg_return_top_10pct', 0):.4f}",
                )
            with metric_cols[3]:
                st.metric(
                    "Baseline Positive Rate",
                    f"{evaluation.get('baseline_positive_rate', 0):.3f}",
                )
            with metric_cols[4]:
                st.metric("Best Iteration", str(metadata.get("best_iteration", "—")))

            info_cols = st.columns(4)
            with info_cols[0]:
                st.metric("Training Rows", f"{metadata.get('training_rows', 0):,}")
            with info_cols[1]:
                st.metric("Symbols", f"{metadata.get('training_symbols', 0):,}")
            with info_cols[2]:
                st.metric("Train End", dataset_metadata.get("train_end", "—"))
            with info_cols[3]:
                st.metric("Validation Start", dataset_metadata.get("validation_start", "—"))

            with st.expander("Model Metadata", expanded=False):
                metadata_summary = pd.DataFrame(
                    [
                        ("Engine", metadata.get("engine", "—")),
                        ("Horizon", metadata.get("horizon", "—")),
                        ("Feature Count", metadata.get("feature_count", "—")),
                        ("Dataset Ref", metadata.get("dataset_ref", "—")),
                        ("Dataset URI", metadata.get("dataset_uri", "—")),
                        ("Metadata Path", metadata.get("_metadata_path", "—")),
                        ("Model Path", metadata.get("_model_path", "—")),
                    ],
                    columns=["Field", "Value"],
                )
                metadata_summary["Value"] = metadata_summary["Value"].astype(str)
                st.dataframe(metadata_summary, use_container_width=True, hide_index=True)

            left_col, right_col = st.columns([1, 2])
            with left_col:
                st.markdown("**Top Features**")
                if top_features:
                    top_features_df = pd.DataFrame(
                        top_features, columns=["Feature", "Importance"]
                    )
                    st.dataframe(
                        top_features_df,
                        use_container_width=True,
                        hide_index=True,
                        height=520,
                    )
                else:
                    st.info("No feature importance data available.")

            with right_col:
                st.markdown("**Feature Importance Plot**")
                chart_path = ensure_feature_importance_plot(metadata_path, top_n=20)
                if chart_path and Path(chart_path).exists():
                    st.image(
                        chart_path,
                        caption=f"Feature importance for {metadata_path.stem.replace('.metadata', '')}",
                        use_container_width=True,
                    )
                    st.caption(f"Chart: `{chart_path}`")
                else:
                    st.info("Feature importance plot could not be generated.")

        st.divider()
        st.subheader("📊 Shadow Monitor")
        shadow_overlay_df = load_shadow_overlay()
        if shadow_overlay_df.empty:
            st.info(
                "No shadow-monitor predictions recorded yet. Run `python -m research.shadow_monitor` "
                "from the project venv to generate the latest overlay and comparison summaries."
            )
        else:
            latest_prediction_date = shadow_overlay_df["prediction_date"].max().date().isoformat()
            summary_cols = st.columns(4)
            with summary_cols[0]:
                st.metric("Latest Prediction Date", latest_prediction_date)
            with summary_cols[1]:
                st.metric("Tracked Symbols", f"{len(shadow_overlay_df):,}")
            with summary_cols[2]:
                st.metric(
                    "5D ML Top-Decile Picks",
                    str(int(pd.to_numeric(shadow_overlay_df["ml_5d_top_decile"], errors="coerce").fillna(0).sum())),
                )
            with summary_cols[3]:
                st.metric(
                    "20D Blend Top-Decile Picks",
                    str(int(pd.to_numeric(shadow_overlay_df["blend_20d_top_decile"], errors="coerce").fillna(0).sum())),
                )

            display_overlay_df = shadow_overlay_df.copy()
            display_overlay_df["prediction_date"] = display_overlay_df["prediction_date"].dt.date.astype(str)
            display_overlay_df = reorder_columns(
                display_overlay_df,
                [
                    "prediction_date",
                    "symbol_id",
                    "exchange",
                    "close",
                    "technical_score",
                    "technical_rank",
                    "ml_5d_prob",
                    "ml_5d_rank",
                    "blend_5d_score",
                    "blend_5d_rank",
                    "ml_20d_prob",
                    "ml_20d_rank",
                    "blend_20d_score",
                    "blend_20d_rank",
                ],
            )
            st.markdown("**Latest Overlay Snapshot**")
            st.dataframe(display_overlay_df.head(30), use_container_width=True, hide_index=True, height=500)

            weekly_cols = st.columns(2)
            for horizon, column in zip((5, 20), weekly_cols):
                with column:
                    st.markdown(f"**{horizon}D Weekly Comparison**")
                    weekly_df = pivot_shadow_summary(load_shadow_period_summary("week", horizon, periods=8))
                    if weekly_df.empty:
                        st.caption("Not enough matured weekly outcomes yet.")
                    else:
                        weekly_display = weekly_df.copy()
                        weekly_display["period_start"] = weekly_display["period_start"].dt.date.astype(str)
                        st.dataframe(weekly_display, use_container_width=True, hide_index=True, height=280)

            monthly_cols = st.columns(2)
            for horizon, column in zip((5, 20), monthly_cols):
                with column:
                    st.markdown(f"**{horizon}D Monthly Comparison**")
                    monthly_df = pivot_shadow_summary(load_shadow_period_summary("month", horizon, periods=6))
                    if monthly_df.empty:
                        st.caption("Not enough matured monthly outcomes yet.")
                    else:
                        monthly_display = monthly_df.copy()
                        monthly_display["period_start"] = monthly_display["period_start"].dt.date.astype(str)
                        st.dataframe(monthly_display, use_container_width=True, hide_index=True, height=240)

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
