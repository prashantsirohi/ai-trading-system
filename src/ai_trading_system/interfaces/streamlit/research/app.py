"""
AI Trading System — Streamlit Command Center Dashboard

Usage: streamlit run src/ai_trading_system/interfaces/streamlit/research/app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import json
import sqlite3
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from urllib.parse import quote, unquote_plus

import streamlit.components.v1 as components

# Ensure imports work even when Streamlit is launched from outside repo root.
PROJECT_ROOT_PATH = Path(__file__).resolve().parents[5]
if str(PROJECT_ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_PATH))

from ai_trading_system.platform.utils.bootstrap import ensure_project_root_on_path
ensure_project_root_on_path(__file__)
from ai_trading_system.analytics.regime_detector import RegimeDetector
from ai_trading_system.analytics.ranker import StockRanker
from ai_trading_system.analytics.risk_manager import RiskManager
from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.analytics.visualizations import Visualizer
from ai_trading_system.analytics.patterns import PatternBacktestConfig, ensure_pattern_event_chart, run_pattern_backtest
from ai_trading_system.analytics.patterns.signal import kernel_smooth
from ai_trading_system.platform.utils.env import load_project_env
from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.platform.db import paths as db_paths
from ai_trading_system.interfaces.streamlit.research.data_access import (
    load_data_trust_snapshot,
    load_drilldown_history_for_symbols,
    load_latest_rank_frames,
    load_ops_health_snapshot,
    load_pattern_backtest_bundle,
    load_portfolio_workspace_report,
    load_rank_history_for_symbols,
    load_sector_history_for_sectors,
    load_symbol_trust_snapshot,
    load_trade_report,
    list_pattern_backtest_bundles,
    save_trade_journal_note,
)
from ai_trading_system.interfaces.streamlit.research.dashboard_helpers import (
    build_breakout_evidence_frame,
    build_rank_sparkline_payload,
    build_value_sparkline_payload,
    enrich_ranked_table_with_context,
)
from ai_trading_system.interfaces.streamlit.research.widgets import (
    render_factor_attribution_widget,
    render_ops_health_ribbon,
    render_sector_dashboard_links_table,
    render_sector_rotation_heatmap,
    render_symbol_rank_history,
)
from ai_trading_system.domains.execution.adapters import PaperExecutionAdapter
from ai_trading_system.domains.execution.models import OrderIntent
from ai_trading_system.domains.execution.service import ExecutionService
from ai_trading_system.domains.execution.store import ExecutionStore
logging.getLogger("streamlit").setLevel(logging.WARNING)

PROJECT_ROOT = str(PROJECT_ROOT_PATH)
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

_SCHEMA_REPAIR_HINT = "Run `python scripts/repair_ingest_schema.py --apply`."


def _schema_check_detail(table_name: str, swapped_rows: int) -> str:
    detail = f"Swapped {table_name} rows: {swapped_rows}"
    if swapped_rows > 0:
        return f"{detail}. {_SCHEMA_REPAIR_HINT}"
    return detail
RESEARCH_REPORTS_DIR = str(RESEARCH_DOMAIN_PATHS.reports_dir)


def open_position_trade_ref(symbol_id: str, exchange: str = "NSE") -> str:
    """Stable journal reference for an active position."""
    return f"open:{str(exchange or 'NSE').upper()}:{str(symbol_id or '').upper()}"


def closed_trade_ref(fill_id: str) -> str:
    """Stable journal reference for a realized trade row."""
    return f"closed:{str(fill_id or '').strip()}"


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
def get_breadth_year_bounds(data_source: str = "operational") -> tuple[int, int]:
    """Return min/max year available in the selected breadth data source."""
    source = str(data_source or "operational").strip().lower()
    db_path = OHLCV_DB if source == "operational" else RESEARCH_OHLCV_DB
    try:
        conn = duckdb.connect(db_path, read_only=True)
        try:
            row = conn.execute(
                """
                SELECT
                    MIN(CAST(timestamp AS DATE)) AS min_date,
                    MAX(CAST(timestamp AS DATE)) AS max_date
                FROM _catalog
                WHERE exchange = 'NSE'
                """
            ).fetchone()
        finally:
            conn.close()
        if not row or not row[0] or not row[1]:
            current_year = datetime.now().year
            return (2010, current_year)
        return (pd.Timestamp(row[0]).year, pd.Timestamp(row[1]).year)
    except Exception:
        current_year = datetime.now().year
        return (2010, current_year)


@st.cache_data(show_spinner=False, ttl=60 * 60)
def load_breadth_history(start_date: str = "2010-01-01", data_source: str = "operational") -> pd.DataFrame:
    """Load breadth series for percent of stocks above key SMAs."""
    source = str(data_source or "operational").strip().lower()
    db_path = OHLCV_DB if source == "operational" else RESEARCH_OHLCV_DB
    conn = duckdb.connect(db_path, read_only=True)
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
    df.loc[:, "trade_date"] = pd.to_datetime(df["trade_date"])
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
            "detail": _schema_check_detail("_catalog", int(swapped_catalog)),
        }
    )
    checks.append(
        {
            "name": "delivery_schema",
            "status": "ok" if swapped_delivery == 0 else "error",
            "detail": _schema_check_detail("_delivery", int(swapped_delivery)),
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


def _default_pattern_backtest_dates() -> tuple[datetime.date, datetime.date]:
    """Default research date window for pattern backtests."""
    to_date = datetime.fromisoformat(db_paths.research_static_end_date()).date()
    from_date = to_date.replace(year=max(to_date.year - 5, 2000))
    return from_date, to_date


def _format_pattern_bundle_option(row: pd.Series) -> str:
    generated = str(row.get("generated_at") or "")[:19].replace("T", " ")
    return (
        f"{row.get('bundle_name', 'bundle')} | "
        f"{generated or 'n/a'} | "
        f"events={int(row.get('event_count', 0) or 0)} | "
        f"trades={int(row.get('trade_count', 0) or 0)}"
    )


def _render_pattern_chart(chart_path: str) -> None:
    path = Path(chart_path)
    if not path.exists():
        st.warning(f"Chart file is missing: {chart_path}")
        return
    try:
        html_payload = path.read_text(encoding="utf-8")
    except Exception as exc:
        st.warning(f"Could not read chart HTML: {exc}")
        return
    components.html(html_payload, height=780, scrolling=True)


def build_pattern_chart_index(events_df: pd.DataFrame, chart_paths: list[str]) -> pd.DataFrame:
    """Build a chart-selection index keyed by event id / chart path."""
    rows: list[dict[str, str]] = []
    existing_chart_lookup = {Path(chart_path).stem: str(chart_path) for chart_path in chart_paths}
    event_lookup = (
        events_df.set_index("event_id").to_dict(orient="index")
        if events_df is not None and not events_df.empty and "event_id" in events_df.columns
        else {}
    )
    for event_id, event_row in event_lookup.items():
        chart_path = existing_chart_lookup.get(str(event_id), "")
        event_row = event_lookup.get(event_id, {})
        symbol_id = str(event_row.get("symbol_id", ""))
        pattern_type = str(event_row.get("pattern_type", ""))
        breakout_date = str(event_row.get("breakout_date", ""))
        label_parts = [part for part in [symbol_id, pattern_type, breakout_date] if part]
        label = " | ".join(label_parts) if label_parts else event_id
        rows.append(
            {
                "event_id": str(event_id),
                "symbol_id": symbol_id,
                "pattern_type": pattern_type,
                "breakout_date": breakout_date,
                "breakout_year": breakout_date[:4] if breakout_date else "",
                "chart_path": str(chart_path),
                "chart_label": label,
                "chart_exists": bool(chart_path),
            }
        )
    return pd.DataFrame(rows)


def _set_pattern_drilldown(
    *,
    bundle_dir: str,
    chart_path: str | None = None,
) -> None:
    """Update in-page pattern state without browser navigation."""
    st.session_state.pattern_backtest_bundle_dir = bundle_dir
    if chart_path is not None:
        st.session_state.pattern_selected_chart = str(chart_path)
    else:
        st.session_state.pattern_selected_chart = ""


def build_pattern_browser_rows(
    events_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    chart_index_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build a simple event browser with year, stock, type, comment, and chart path."""
    if chart_index_df is None or chart_index_df.empty:
        return pd.DataFrame(
            columns=[
                "event_id",
                "symbol_id",
                "pattern_type",
                "breakout_date",
                "breakout_year",
                "comment",
                "chart_path",
            ]
        )

    browser = chart_index_df.copy()
    if events_df is not None and not events_df.empty and "event_id" in events_df.columns:
        event_cols = [
            col
            for col in [
                "event_id",
                "breakout_level",
                "invalidation_price",
                "cup_depth_pct",
                "width_bars",
                "handle_depth_pct",
                "symmetry_ratio",
                "breakout_volume_ratio",
                "volume_dry_up",
            ]
            if col in events_df.columns
        ]
        browser = browser.merge(events_df[event_cols], on="event_id", how="left")
    if trades_df is not None and not trades_df.empty and "event_id" in trades_df.columns:
        trade_cols = [
            col
            for col in ["event_id", "net_return", "r_multiple", "exit_reason"]
            if col in trades_df.columns
        ]
        browser = browser.merge(trades_df[trade_cols], on="event_id", how="left")

    def _comment(row: pd.Series) -> str:
        parts: list[str] = []
        breakout_date = str(row.get("breakout_date") or "")
        if breakout_date:
            parts.append(f"breakout {breakout_date}")
        if pd.notna(row.get("cup_depth_pct")):
            parts.append(f"depth {float(row['cup_depth_pct']):.1f}%")
        if pd.notna(row.get("width_bars")):
            parts.append(f"width {int(float(row['width_bars']))} bars")
        if pd.notna(row.get("net_return")):
            parts.append(f"net {float(row['net_return']) * 100:.1f}%")
        elif pd.notna(row.get("r_multiple")):
            parts.append(f"R {float(row['r_multiple']):.2f}")
        if row.get("exit_reason"):
            parts.append(str(row["exit_reason"]))
        return " | ".join(parts[:4])

    browser.loc[:, "breakout_year"] = browser["breakout_year"].replace("", pd.NA)
    if browser["breakout_year"].isna().any() and "breakout_date" in browser.columns:
        browser.loc[:, "breakout_year"] = browser["breakout_year"].fillna(
            pd.to_datetime(browser["breakout_date"], errors="coerce").dt.year.astype("Int64").astype(str)
        )
    browser.loc[:, "comment"] = browser.apply(_comment, axis=1)
    browser = browser.sort_values(["breakout_year", "breakout_date", "symbol_id"], ascending=[False, False, True])
    return browser.reset_index(drop=True)


def _coerce_pattern_config(raw: dict | None) -> PatternBacktestConfig:
    payload = dict(raw or {})
    if "symbols" in payload and isinstance(payload["symbols"], list):
        payload["symbols"] = tuple(str(item) for item in payload["symbols"])
    if "event_horizons" in payload and isinstance(payload["event_horizons"], list):
        payload["event_horizons"] = tuple(int(item) for item in payload["event_horizons"])
    return PatternBacktestConfig(**payload)


def _pattern_stock_button_label(row: pd.Series, *, selected: bool = False) -> str:
    """Build a compact stock button label."""
    symbol = str(row.get("symbol_id") or "")
    return f"{symbol}{' *' if selected else ''}".strip()


def _pattern_quality_badge(row: pd.Series) -> str:
    """Classify a breakout event as strong, borderline, or failed."""
    net_return = pd.to_numeric(pd.Series([row.get("net_return")]), errors="coerce").iloc[0]
    breakout_volume_ratio = pd.to_numeric(pd.Series([row.get("breakout_volume_ratio")]), errors="coerce").iloc[0]
    cup_depth_pct = pd.to_numeric(pd.Series([row.get("cup_depth_pct")]), errors="coerce").iloc[0]
    handle_depth_pct = pd.to_numeric(pd.Series([row.get("handle_depth_pct")]), errors="coerce").iloc[0]
    symmetry_ratio = pd.to_numeric(pd.Series([row.get("symmetry_ratio")]), errors="coerce").iloc[0]
    width_bars = pd.to_numeric(pd.Series([row.get("width_bars")]), errors="coerce").iloc[0]
    volume_dry_up = bool(row.get("volume_dry_up"))
    exit_reason = str(row.get("exit_reason") or "").strip().lower()
    pattern_type = str(row.get("pattern_type") or "").strip().lower()

    if exit_reason == "stop" or (pd.notna(net_return) and float(net_return) < 0):
        return "FAILED"

    strong_common = (
        pd.notna(breakout_volume_ratio)
        and float(breakout_volume_ratio) >= 1.8
        and pd.notna(cup_depth_pct)
        and 18.0 <= float(cup_depth_pct) <= 32.0
        and pd.notna(width_bars)
        and float(width_bars) >= 20.0
        and volume_dry_up
    )
    if pattern_type == "cup_handle":
        if strong_common and pd.notna(handle_depth_pct) and float(handle_depth_pct) <= 8.0:
            return "STRONG"
    elif pattern_type == "round_bottom":
        if strong_common and pd.notna(symmetry_ratio) and 0.75 <= float(symmetry_ratio) <= 1.35:
            return "STRONG"

    return "BORDERLINE"


def _pattern_quality_badge_html(row: pd.Series) -> str:
    """Render a compact colored quality badge."""
    badge = _pattern_quality_badge(row)
    color_map = {
        "STRONG": ("#14532d", "#86efac", "#dcfce7"),
        "BORDERLINE": ("#78350f", "#fcd34d", "#fef3c7"),
        "FAILED": ("#7f1d1d", "#fca5a5", "#fee2e2"),
    }
    bg, border, text = color_map.get(badge, ("#1f2937", "#cbd5e1", "#f8fafc"))
    return (
        f"<span style=\"display:inline-block;padding:0.08rem 0.38rem;border-radius:999px;"
        f"font-size:0.68rem;font-weight:700;letter-spacing:0.02em;background:{bg};"
        f"border:1px solid {border};color:{text};\">{badge}</span>"
    )


def _render_pattern_backtests_tab() -> None:
    """Render the research-only pattern backtest UI."""
    st.subheader("🧩 Pattern Backtests")
    st.caption(
        "Run the research backtest, then browse breakout events by year on the left and view the marked chart on the right."
    )

    if "pattern_backtest_bundle_dir" not in st.session_state:
        st.session_state.pattern_backtest_bundle_dir = None
    if "pattern_selected_chart" not in st.session_state:
        st.session_state.pattern_selected_chart = ""

    default_from_date, default_to_date = _default_pattern_backtest_dates()
    with st.container(border=True):
        st.markdown("**Run New Pattern Backtest**")
        run_col1, run_col2, run_col3 = st.columns([1, 1, 1.25])
        with run_col1:
            from_date = st.date_input(
                "From Date",
                value=default_from_date,
                key="pattern_backtest_from_date",
            )
        with run_col2:
            to_date = st.date_input(
                "To Date",
                value=default_to_date,
                key="pattern_backtest_to_date",
            )
        with run_col3:
            symbol_input = st.text_input(
                "Symbols (optional, comma-separated)",
                value="",
                key="pattern_backtest_symbols",
                help="Leave blank to scan the full NSE research universe.",
            )

        config_col1, config_col2, config_col3 = st.columns(3)
        with config_col1:
            bandwidth = st.slider(
                "Kernel Bandwidth",
                min_value=1.0,
                max_value=6.0,
                value=3.0,
                step=0.5,
                key="pattern_backtest_bandwidth",
            )
        with config_col2:
            prominence = st.slider(
                "Extrema Prominence",
                min_value=0.005,
                max_value=0.05,
                value=0.02,
                step=0.005,
                key="pattern_backtest_prominence",
            )
        with config_col3:
            volume_ratio = st.slider(
                "Breakout Volume Ratio",
                min_value=1.0,
                max_value=3.0,
                value=1.5,
                step=0.1,
                key="pattern_backtest_volume_ratio",
            )
        precompute_all_charts = st.checkbox(
            "Precompute all charts",
            value=False,
            key="pattern_backtest_precompute_all_charts",
            help="If enabled, the backtest writes one chart HTML per detected breakout event.",
        )

        if st.button("Run Pattern Backtest", type="primary", use_container_width=True):
            selected_symbols = [item.strip().upper() for item in symbol_input.split(",") if item.strip()]
            config = PatternBacktestConfig(
                exchange="NSE",
                symbols=tuple(selected_symbols),
                bandwidth=float(bandwidth),
                extrema_prominence=float(prominence),
                breakout_volume_ratio_min=float(volume_ratio),
            )
            with st.spinner("Running research pattern backtest..."):
                result = run_pattern_backtest(
                    project_root=PROJECT_ROOT,
                    from_date=from_date.isoformat(),
                    to_date=to_date.isoformat(),
                    exchange="NSE",
                    symbols=selected_symbols,
                    config=config,
                    precompute_all_charts=bool(precompute_all_charts),
                )
            first_chart = result["paths"]["charts"][0] if result["paths"].get("charts") else None
            _set_pattern_drilldown(
                bundle_dir=result["paths"]["bundle_dir"],
                chart_path=first_chart,
            )
            load_pattern_backtest_bundle.clear()
            list_pattern_backtest_bundles.clear()
            st.success(
                f"Pattern backtest finished with {len(result['events'])} events and {len(result['trades'])} trades."
            )

    bundles_df = list_pattern_backtest_bundles(RESEARCH_REPORTS_DIR, max_bundles=30)
    if bundles_df.empty:
        st.info("No pattern backtest bundles found yet. Run one above to generate the first visual review set.")
        return

    bundle_options = {
        _format_pattern_bundle_option(row): row["bundle_dir"]
        for _, row in bundles_df.iterrows()
    }
    default_bundle_dir = st.session_state.pattern_backtest_bundle_dir
    default_label = next(
        (label for label, value in bundle_options.items() if value == default_bundle_dir),
        next(iter(bundle_options.keys())),
    )
    selected_bundle_label = st.selectbox(
        "Saved Backtest Bundles",
        options=list(bundle_options.keys()),
        index=list(bundle_options.keys()).index(default_label),
        key="pattern_backtest_bundle_selector",
    )
    selected_bundle_dir = bundle_options[selected_bundle_label]
    if st.session_state.pattern_backtest_bundle_dir != selected_bundle_dir:
        _set_pattern_drilldown(bundle_dir=selected_bundle_dir)

    bundle = load_pattern_backtest_bundle(selected_bundle_dir)
    summary_json = bundle.get("summary_json", {}) or {}
    summary_df = bundle.get("summary_df", pd.DataFrame())
    events_df = bundle.get("events_df", pd.DataFrame())
    trades_df = bundle.get("trades_df", pd.DataFrame())
    yearly_df = bundle.get("yearly_df", pd.DataFrame())
    chart_paths = bundle.get("chart_paths", [])
    chart_index_df = build_pattern_chart_index(events_df, chart_paths)
    browser_df = build_pattern_browser_rows(events_df, trades_df, chart_index_df)

    metric_cols = st.columns(5)
    metric_cols[0].metric("Bundle", Path(selected_bundle_dir).name)
    metric_cols[1].metric("From", summary_json.get("from_date", "—"))
    metric_cols[2].metric("To", summary_json.get("to_date", "—"))
    metric_cols[3].metric("Events", f"{len(events_df):,}")
    metric_cols[4].metric("Trades", f"{len(trades_df):,}")
    st.caption(f"Bundle path: `{selected_bundle_dir}`")
    if summary_json.get("precompute_all_charts"):
        st.caption("Chart mode: precomputed for all events")
    else:
        st.caption("Chart mode: generated on demand when you open a breakout")

    left_col, right_col = st.columns([1.05, 1.45], gap="large")
    with left_col:
        st.markdown("**Pattern Summary**")
        if summary_df.empty:
            st.info("No summary rows available for this bundle.")
        else:
            st.dataframe(
                reorder_columns(
                    summary_df,
                    ["pattern_type", "signal_count", "trade_count", "confirmation_rate", "avg_net_return", "expectancy_r"],
                ),
                use_container_width=True,
                hide_index=True,
                height=200,
            )

        st.markdown("**Yearly Breakdown**")
        if yearly_df.empty:
            st.info("No yearly breakdown available.")
        else:
            st.dataframe(
                reorder_columns(
                    yearly_df,
                    ["breakout_year", "pattern_type", "signal_count", "avg_net_return", "avg_r_multiple", "stop_out_rate"],
                ),
                use_container_width=True,
                hide_index=True,
                height=200,
            )

        st.markdown("**Breakout Browser**")
        if browser_df.empty:
            st.info("No breakout charts are available for this bundle.")
        else:
            st.markdown(
                """
                <style>
                .pattern-browser-note {
                    font-size: 0.74rem;
                    color: #94a3b8;
                    margin-top: -0.2rem;
                    margin-bottom: 0.35rem;
                }
                .pattern-browser-comment {
                    font-size: 0.76rem;
                    color: #cbd5e1;
                    line-height: 1.05rem;
                    padding-top: 0.18rem;
                }
                .pattern-browser-type {
                    font-size: 0.75rem;
                    color: #e2e8f0;
                    line-height: 1.0rem;
                    padding-top: 0.22rem;
                    white-space: nowrap;
                }
                </style>
                """,
                unsafe_allow_html=True,
            )
            st.markdown(
                "<div class='pattern-browser-note'>Badges show only pattern quality: STRONG, BORDERLINE, or FAILED.</div>",
                unsafe_allow_html=True,
            )
            years = [str(year) for year in browser_df["breakout_year"].dropna().astype(str).unique().tolist() if str(year).strip()]
            if not years:
                years = ["All"]
            year_tabs = st.tabs(years)
            for tab_idx, year in enumerate(years):
                with year_tabs[tab_idx]:
                    year_df = browser_df[browser_df["breakout_year"].astype(str) == str(year)].copy()
                    if year_df.empty:
                        st.info("No breakout events in this year.")
                        continue
                    header_cols = st.columns([1.2, 1.0, 0.95, 2.7], gap="small")
                    header_cols[0].markdown("**Stock**")
                    header_cols[1].markdown("**Quality**")
                    header_cols[2].markdown("**Type**")
                    header_cols[3].markdown("**Comment**")
                    for row_idx, row in year_df.iterrows():
                        cols = st.columns([1.2, 1.0, 0.95, 2.7], gap="small")
                        with cols[0]:
                            expected_chart_path = str(Path(selected_bundle_dir) / "charts" / f"{row['event_id']}.html")
                            current_chart_path = str(row.get("chart_path") or expected_chart_path)
                            is_selected = current_chart_path == st.session_state.pattern_selected_chart
                            button_label = _pattern_stock_button_label(row, selected=is_selected)
                            if st.button(
                                button_label,
                                key=f"pattern_browser_stock_{year}_{row_idx}",
                                use_container_width=True,
                                type="tertiary",
                            ):
                                _set_pattern_drilldown(
                                    bundle_dir=selected_bundle_dir,
                                    chart_path=current_chart_path,
                                )
                                st.rerun()
                        with cols[1]:
                            st.markdown(_pattern_quality_badge_html(row), unsafe_allow_html=True)
                        with cols[2]:
                            st.markdown(
                                f"<div class='pattern-browser-type'>{str(row.get('pattern_type') or '')}</div>",
                                unsafe_allow_html=True,
                            )
                        with cols[3]:
                            st.markdown(
                                f"<div class='pattern-browser-comment'>{str(row.get('comment') or '')}</div>",
                                unsafe_allow_html=True,
                            )

    with right_col:
        st.markdown("**Visual Review**")
        if browser_df.empty:
            st.info("Run a backtest first to browse charts.")
        else:
            chart_options = [str(Path(selected_bundle_dir) / "charts" / f"{event_id}.html") for event_id in browser_df["event_id"].astype(str).tolist()]
            selected_chart = (
                st.session_state.pattern_selected_chart
                if st.session_state.pattern_selected_chart in chart_options
                else chart_options[0]
            )
            selected_event_id = Path(selected_chart).stem
            selected_row = browser_df[browser_df["event_id"].astype(str) == selected_event_id]
            if not selected_row.empty:
                selected_meta = selected_row.iloc[0]
                st.markdown(
                    f"**{selected_meta.get('symbol_id', '')}**  |  "
                    f"`{selected_meta.get('pattern_type', '')}`  |  "
                    f"`{selected_meta.get('breakout_date', '')}`"
                )
                st.caption(str(selected_meta.get("comment") or ""))

            selected_chart = st.selectbox(
                "Chart",
                options=chart_options,
                index=chart_options.index(selected_chart),
                format_func=lambda value: (
                    browser_df.loc[browser_df["event_id"].astype(str) == Path(value).stem, "chart_label"].iloc[0]
                    if not browser_df.loc[browser_df["event_id"].astype(str) == Path(value).stem].empty
                    else Path(value).stem
                ),
                key="pattern_backtest_chart_select",
            )
            st.session_state.pattern_selected_chart = selected_chart
            if not Path(selected_chart).exists() and not selected_row.empty:
                event_record = events_df[events_df["event_id"].astype(str) == selected_event_id]
                if not event_record.empty:
                    with st.spinner("Generating chart on demand..."):
                        generated_chart = ensure_pattern_event_chart(
                            project_root=PROJECT_ROOT,
                            bundle_dir=selected_bundle_dir,
                            event_row=event_record.iloc[0],
                            config=_coerce_pattern_config(summary_json.get("config")),
                            from_date=str(summary_json.get("from_date") or ""),
                            to_date=str(summary_json.get("to_date") or ""),
                            exchange=str(summary_json.get("exchange") or "NSE"),
                        )
                    st.session_state.pattern_selected_chart = generated_chart
                    selected_chart = generated_chart
                    load_pattern_backtest_bundle.clear()
            _render_pattern_chart(selected_chart)
            st.caption(f"Chart file: `{selected_chart}`")


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
    pattern_path = rank_dir / "pattern_scan.csv"
    stock_scan_path = rank_dir / "stock_scan.csv"
    sector_path = rank_dir / "sector_dashboard.csv"

    breakout_df = pd.read_csv(breakout_path) if breakout_path.exists() else pd.DataFrame()
    pattern_df = pd.read_csv(pattern_path) if pattern_path.exists() else pd.DataFrame()
    stock_scan_df = pd.read_csv(stock_scan_path) if stock_scan_path.exists() else pd.DataFrame()
    sector_df = pd.read_csv(sector_path) if sector_path.exists() else pd.DataFrame()

    top_sector = None
    if not sector_df.empty:
        sector_col = "Sector" if "Sector" in sector_df.columns else sector_df.columns[0]
        top_sector = sector_df.iloc[0].get(sector_col)

    ranked_leaders = (
        stock_scan_df.loc[pd.to_numeric(stock_scan_df.get("rank"), errors="coerce").notna()].head(10).to_dict(orient="records")
        if not stock_scan_df.empty
        else []
    )
    pattern_discoveries = (
        stock_scan_df.loc[
            stock_scan_df.get("discovered_by_pattern_scan", pd.Series(False, index=stock_scan_df.index))
            .fillna(False)
            .astype(bool)
        ]
        .head(10)
        .to_dict(orient="records")
        if not stock_scan_df.empty
        else []
    )
    breakout_candidates = (
        stock_scan_df.loc[
            pd.to_numeric(stock_scan_df.get("rank"), errors="coerce").isna()
            & stock_scan_df.get("breakout_positive", pd.Series(False, index=stock_scan_df.index)).fillna(False).astype(bool)
        ]
        .head(10)
        .to_dict(orient="records")
        if not stock_scan_df.empty
        else []
    )

    return {
        "summary": {
            "run_id": ranked_path.parts[-4],
            "ranked_count": int(len(ranked_df)),
            "breakout_count": int(len(breakout_df)),
            "pattern_count": int(len(pattern_df)),
            "stock_scan_count": int(len(stock_scan_df)),
            "sector_count": int(len(sector_df)),
            "top_symbol": ranked_df.iloc[0].get("symbol_id") if not ranked_df.empty else None,
            "top_sector": top_sector,
            "ranked_universe_covers_stock_scan": bool(
                not stock_scan_df.empty
                and "symbol_id" in ranked_df.columns
                and "symbol_id" in stock_scan_df.columns
                and set(stock_scan_df["symbol_id"].astype(str)).issubset(set(ranked_df["symbol_id"].astype(str)))
            ),
            "ranked_universe_stock_scan_coverage_pct": (
                round(
                    (
                        len(
                            set(stock_scan_df["symbol_id"].astype(str))
                            & set(ranked_df["symbol_id"].astype(str))
                        )
                        / len(set(stock_scan_df["symbol_id"].astype(str)))
                    )
                    * 100.0,
                    2,
                )
                if not stock_scan_df.empty and "symbol_id" in ranked_df.columns and "symbol_id" in stock_scan_df.columns
                else 0.0
            ),
            "discovery_visibility_reason": (
                "ranked_universe_covers_stock_scan"
                if (
                    not stock_scan_df.empty
                    and "symbol_id" in ranked_df.columns
                    and "symbol_id" in stock_scan_df.columns
                    and set(stock_scan_df["symbol_id"].astype(str)).issubset(set(ranked_df["symbol_id"].astype(str)))
                    and not pattern_discoveries
                    and not breakout_candidates
                )
                else None
            ),
            "discovery_visibility_note": (
                "No non-ranked pattern discoveries or breakout candidates are shown because the ranked universe already covers the full stock-scan symbol set for this run."
                if (
                    not stock_scan_df.empty
                    and "symbol_id" in ranked_df.columns
                    and "symbol_id" in stock_scan_df.columns
                    and set(stock_scan_df["symbol_id"].astype(str)).issubset(set(ranked_df["symbol_id"].astype(str)))
                    and not pattern_discoveries
                    and not breakout_candidates
                )
                else None
            ),
        },
        "ranked_signals": ranked_df.head(10).to_dict(orient="records"),
        "breakout_scan": breakout_df.head(10).to_dict(orient="records"),
        "pattern_scan": pattern_df.head(10).to_dict(orient="records"),
        "stock_scan": stock_scan_df.head(10).to_dict(orient="records"),
        "ranked_leaders": ranked_leaders,
        "pattern_discoveries": pattern_discoveries,
        "breakout_candidates": breakout_candidates,
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
        df.loc[:, "prediction_date"] = pd.to_datetime(df["prediction_date"])
    return df


def load_shadow_period_summary(grain: str, horizon: int, periods: int = 12) -> pd.DataFrame:
    """Load weekly/monthly shadow-monitor comparison summaries."""
    registry = RegistryStore(PROJECT_ROOT)
    rows = registry.get_shadow_period_summary(grain=grain, horizon=horizon, periods=periods)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df.loc[:, "period_start"] = pd.to_datetime(df["period_start"])
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


def _extract_selected_rows(selection_event: object) -> list[int]:
    """Handle Streamlit dataframe selection payloads across versions."""
    if selection_event is None:
        return []
    selection = getattr(selection_event, "selection", None)
    if selection is not None:
        rows = getattr(selection, "rows", None)
        if rows is not None:
            return list(rows)
        if isinstance(selection, dict):
            return list((selection.get("rows") or []))
    if isinstance(selection_event, dict):
        selection = selection_event.get("selection") or {}
        return list(selection.get("rows") or [])
    return []


def render_selectable_grid(
    df: pd.DataFrame,
    *,
    key: str,
    height: int,
    column_config: dict[str, object] | None = None,
    hide_index: bool = True,
) -> pd.Series | None:
    """Render an interactive dataframe grid and return the selected row when supported."""
    if df is None or df.empty:
        return None
    try:
        selection_event = st.dataframe(
            df,
            use_container_width=True,
            hide_index=hide_index,
            height=height,
            column_config=column_config or {},
            on_select="rerun",
            selection_mode="single-row",
            key=key,
        )
        selected_rows = _extract_selected_rows(selection_event)
        if selected_rows:
            selected_idx = int(selected_rows[0])
            if 0 <= selected_idx < len(df):
                return df.iloc[selected_idx]
    except TypeError:
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=hide_index,
            height=height,
            column_config=column_config or {},
        )
    return None


def render_breakout_setup_detail(
    breakout_row: pd.Series | None,
    breakout_df: pd.DataFrame,
    *,
    signal_date: str | None,
    title: str,
) -> None:
    """Render a compact breakout detail inspector for a selected grid row."""
    if breakout_row is None or breakout_df is None or breakout_df.empty:
        return
    evidence_frame = build_breakout_evidence_frame(breakout_df, signal_date=signal_date)
    if evidence_frame.empty:
        return
    mask = evidence_frame["symbol_id"].astype(str) == str(breakout_row.get("symbol_id", ""))
    if "setup_family" in evidence_frame.columns and "setup_family" in breakout_row.index:
        mask &= evidence_frame["setup_family"].astype(str) == str(breakout_row.get("setup_family", ""))
    selected_evidence = evidence_frame.loc[mask].head(1)
    if selected_evidence.empty:
        selected_evidence = evidence_frame.loc[evidence_frame["symbol_id"].astype(str) == str(breakout_row.get("symbol_id", ""))].head(1)
    if selected_evidence.empty:
        return
    row = selected_evidence.iloc[0]
    verdict_color = str(row.get("verdict_color", "#334155"))
    badge_html = (
        f"<span style='background:{verdict_color};color:white;padding:2px 8px;"
        "border-radius:999px;font-size:0.74rem;font-weight:700;'>"
        f"{row.get('verdict', 'Unknown')}</span>"
    )
    with st.container(border=True):
        st.markdown(f"**{title}**")
        st.markdown(
            f"**{row.get('symbol_id', '—')}**  \n{badge_html}",
            unsafe_allow_html=True,
        )
        st.caption(
            f"{row.get('breakout_type', '—')} | "
            f"Setup {pd.to_numeric(row.get('setup_quality'), errors='coerce'):.1f} | "
            f"State {row.get('breakout_state', '—')}"
        )
        st.caption(
            f"Base {int(pd.to_numeric(row.get('base_length_days'), errors='coerce') or 0)}D | "
            f"Vol x {pd.to_numeric(row.get('volume_ratio'), errors='coerce'):.2f} | "
            f"Contraction {pd.to_numeric(row.get('contraction_pct'), errors='coerce'):.1f}%"
        )
        st.caption(
            f"Regime {row.get('market_regime', 'N/A')} / {row.get('market_bias', 'N/A')} | "
            f"Tier {row.get('candidate_tier', 'N/A')}"
        )
        trend_reasons = str(row.get("symbol_trend_reasons", "") or "").strip()
        if trend_reasons:
            st.caption(f"Trend reasons: {trend_reasons}")
        filter_reason = str(row.get("filter_reason", "") or "").strip()
        if filter_reason:
            st.caption(f"Filter reason: {filter_reason}")


def _build_tradingview_link(symbol: object) -> str:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return ""
    encoded = quote(sym, safe="._-")
    return f"https://www.tradingview.com/chart/?symbol=NSE%3A{encoded}"


def _with_symbol_hyperlink(df: pd.DataFrame, *, symbol_col: str) -> pd.DataFrame:
    if df is None or df.empty or symbol_col not in df.columns:
        return df
    out = df.copy()
    out.loc[:, symbol_col] = out[symbol_col].map(_build_tradingview_link)
    return out


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


def _query_param_value(name: str) -> str:
    """Read query parameter as a plain string."""
    value = st.query_params.get(name, "")
    if isinstance(value, list):
        return str(value[0]) if value else ""
    return str(value or "")


def _with_unique_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe with guaranteed-unique column names."""
    if df is None or df.empty:
        return df
    cols = list(df.columns)
    if not pd.Index(cols).has_duplicates:
        return df
    seen: dict[str, int] = {}
    unique_cols: list[str] = []
    for col in cols:
        key = str(col)
        count = seen.get(key, 0)
        if count == 0:
            unique_cols.append(key)
        else:
            unique_cols.append(f"{key}_{count + 1}")
        seen[key] = count + 1
    out = df.copy()
    out.columns = unique_cols
    return out


def _normalize_text_key(value: str) -> str:
    """Normalize labels for loose text matching across sector taxonomies."""
    return "".join(ch for ch in str(value).lower().strip() if ch.isalnum())


def _sector_match_mask(df: pd.DataFrame, sector_col: str, group_col: str, target_raw: str) -> pd.Series:
    """Build loose+exact mask across sector and industry-group labels."""
    target = str(target_raw).strip().lower()
    target_key = _normalize_text_key(target_raw)

    sector_text = df[sector_col].astype(str).str.lower()
    group_text = df[group_col].astype(str).str.lower()
    sector_key = df[sector_col].astype(str).map(_normalize_text_key)
    group_key = df[group_col].astype(str).map(_normalize_text_key)

    exact_sector = sector_text == target
    exact_group = group_text == target
    contains_sector = sector_text.str.contains(target, regex=False, na=False) if target else pd.Series(False, index=df.index)
    contains_group = group_text.str.contains(target, regex=False, na=False) if target else pd.Series(False, index=df.index)
    loose_sector = sector_key.str.contains(target_key, regex=False, na=False) if target_key else pd.Series(False, index=df.index)
    loose_group = group_key.str.contains(target_key, regex=False, na=False) if target_key else pd.Series(False, index=df.index)
    return exact_sector | exact_group | contains_sector | contains_group | loose_sector | loose_group


def _label_match_mask(series: pd.Series, target_raw: str) -> pd.Series:
    """Match a label series to the selected drilldown text using the same loose rules."""
    target = str(target_raw).strip().lower()
    target_key = _normalize_text_key(target_raw)
    text = series.fillna("").astype(str).str.lower()
    text_key = series.fillna("").astype(str).map(_normalize_text_key)
    exact = text == target
    contains = text.str.contains(target, regex=False, na=False) if target else pd.Series(False, index=series.index)
    loose = text_key.str.contains(target_key, regex=False, na=False) if target_key else pd.Series(False, index=series.index)
    return exact | contains | loose


@st.cache_data(show_spinner=False, ttl=60 * 10)
def load_symbol_sector_details() -> pd.DataFrame:
    """Load symbol-to-sector lookup from master DB."""
    try:
        conn = sqlite3.connect(MASTER_DB)
        try:
            df = pd.read_sql_query(
                """
                SELECT
                    "Symbol" AS symbol_id,
                    "Name" AS company_name,
                    "Sector" AS sector_name,
                    "Industry Group" AS industry_group
                FROM stock_details
                WHERE "Symbol" IS NOT NULL
                """,
                conn,
            )
        finally:
            conn.close()
        if df.empty:
            return df
        df.loc[:, "symbol_id"] = df["symbol_id"].astype(str).str.upper()
        df.loc[:, "sector_name"] = df["sector_name"].fillna("").astype(str).str.strip()
        df.loc[:, "industry_group"] = df["industry_group"].fillna("").astype(str).str.strip()
        return df
    except Exception:
        return pd.DataFrame(columns=["symbol_id", "company_name", "sector_name", "industry_group"])


def resolve_drilldown_label_kind(label: str) -> str:
    """Classify a drilldown label as Sector, Industry Group, or Mixed."""
    details = load_symbol_sector_details()
    if details.empty:
        return "Sector / Industry"

    sector_hits = _label_match_mask(details["sector_name"], label).sum()
    group_hits = _label_match_mask(details["industry_group"], label).sum()
    if sector_hits and not group_hits:
        return "Sector"
    if group_hits and not sector_hits:
        return "Industry Group"
    if group_hits and sector_hits:
        return "Sector / Industry"
    return "Sector / Industry"


@st.cache_data(show_spinner=False, ttl=60 * 10)
def get_sector_dropdown_options() -> List[str]:
    """Return sector labels for drilldown selector."""
    details = load_symbol_sector_details()
    if details.empty:
        return []
    labels = pd.concat(
        [
            details["sector_name"].fillna("").astype(str).str.strip(),
            details["industry_group"].fillna("").astype(str).str.strip(),
        ],
        ignore_index=True,
    )
    labels = labels[labels != ""]
    if labels.empty:
        return []
    unique = sorted(set(labels.tolist()), key=lambda x: x.lower())
    return unique


def build_sector_universe_frame(rank_df: pd.DataFrame, sector_name: str) -> tuple[pd.DataFrame, bool]:
    """Build full-universe company table for a selected sector.

    Returns `(frame, used_fallback_all)` where fallback means no sector match was found,
    so the full stock universe is returned in descending score order.
    """
    lookup = load_symbol_sector_details().copy()
    if lookup.empty:
        return pd.DataFrame(), False
    lookup = _with_unique_column_names(lookup)
    for col in ("sector_name", "industry_group", "company_name", "symbol_id"):
        if col not in lookup.columns:
            lookup.loc[:, col] = ""
    lookup.loc[:, "symbol_id"] = lookup["symbol_id"].astype(str).str.upper()

    mask = _sector_match_mask(lookup, "sector_name", "industry_group", sector_name)
    filtered_lookup = lookup[mask].copy()
    used_fallback_all = False
    if filtered_lookup.empty:
        filtered_lookup = lookup.copy()
        used_fallback_all = True

    # Optional rank context: left join so unranked sector stocks are still shown.
    rank_base = pd.DataFrame()
    base = pd.DataFrame()
    if rank_df is not None and not rank_df.empty:
        base = normalize_rank_df(rank_df).copy()
    if not base.empty and "symbol_id" in base.columns:
        rank_base = _with_unique_column_names(base.copy())
        rank_base.loc[:, "symbol_id"] = rank_base["symbol_id"].astype(str).str.upper()
        score_col = "custom_score" if "custom_score" in rank_base.columns else "composite_score"
        if score_col in rank_base.columns:
            rank_base = rank_base.sort_values(score_col, ascending=False, na_position="last").reset_index(drop=True)
            rank_base.loc[:, "overall_rank"] = np.arange(1, len(rank_base) + 1)
        rank_cols_preferred = [
            "symbol_id",
            "overall_rank",
            "custom_score",
            "composite_score",
            "close",
            "rel_strength_score",
            "vol_intensity_score",
            "trend_score_score",
            "prox_high_score",
            "delivery_pct_score",
            "sector_strength_score",
        ]
        rank_cols = [col for col in rank_cols_preferred if col in rank_base.columns]
        if rank_cols:
            rank_base = rank_base.loc[:, rank_cols].drop_duplicates(subset=["symbol_id"], keep="first")
        else:
            rank_base = pd.DataFrame(columns=["symbol_id"])

    out = filtered_lookup.merge(rank_base, on="symbol_id", how="left", suffixes=("", "_rank"))
    out = out.loc[:, ~pd.Index(out.columns).duplicated(keep="first")]
    score_col = "custom_score" if "custom_score" in out.columns else "composite_score"
    if score_col in out.columns:
        out = out.sort_values([score_col, "symbol_id"], ascending=[False, True], na_position="last")
    else:
        out = out.sort_values("symbol_id")

    out.reset_index(drop=True, inplace=True)
    if score_col in out.columns:
        ranked_mask = pd.to_numeric(out[score_col], errors="coerce").notna()
        out.insert(0, "ranked_flag", np.where(ranked_mask, "Yes", "No"))
        sector_rank = pd.Series(pd.NA, index=out.index, dtype="Int64")
        sector_rank.loc[ranked_mask] = np.arange(1, int(ranked_mask.sum()) + 1)
        out.insert(1, "sector_rank", sector_rank)
    else:
        out.insert(0, "ranked_flag", "No")
        out.insert(1, "sector_rank", pd.Series([pd.NA] * len(out), dtype="Int64"))
    return out, used_fallback_all


def render_sector_drilldown_page(sector_name: str, rank_source_df: pd.DataFrame) -> None:
    """Render sector-level ranked company drilldown page."""
    label_kind = resolve_drilldown_label_kind(sector_name)
    st.title(f"🏭 {label_kind} Drilldown: {sector_name}")
    options = get_sector_dropdown_options()
    c1, c2, c3 = st.columns([1, 4, 3])
    with c1:
        if st.button("← Back", use_container_width=True):
            st.query_params.clear()
            st.rerun()
    with c2:
        st.caption(
            f"Showing full universe stocks for the selected {label_kind.lower()}, "
            "ordered by descending ranking score."
        )
    with c3:
        if options:
            current_label = str(sector_name).strip()
            default_idx = options.index(current_label) if current_label in options else 0
            selected_sector = st.selectbox(
                "Sector / Industry Group",
                options=options,
                index=default_idx,
                key="sector_drilldown_selector",
            )
            if selected_sector != current_label:
                st.query_params["view"] = "sector"
                st.query_params["sector"] = selected_sector
                st.rerun()

    sector_ranked, used_fallback_all = build_sector_universe_frame(rank_source_df, sector_name)
    if sector_ranked.empty:
        st.warning("No companies found for this sector in the master universe.")
        return
    if used_fallback_all:
        st.info(
            "No direct sector match found in master mapping for this label. "
            "Showing full stock universe in descending ranking score."
        )

    score_col = "custom_score" if "custom_score" in sector_ranked.columns else "composite_score"
    metric_cols = st.columns(4)
    with metric_cols[0]:
        st.metric("Companies", f"{len(sector_ranked):,}")
    with metric_cols[1]:
        st.metric("Top Symbol", str(sector_ranked.iloc[0].get("symbol_id", "—")))
    with metric_cols[2]:
        top_score = pd.to_numeric(sector_ranked.iloc[0].get(score_col), errors="coerce")
        st.metric("Top Score", f"{float(top_score):.2f}" if pd.notna(top_score) else "—")
    with metric_cols[3]:
        median_score = pd.to_numeric(sector_ranked[score_col], errors="coerce").median() if score_col in sector_ranked.columns else np.nan
        st.metric("Median Score", f"{float(median_score):.2f}" if pd.notna(median_score) else "—")

    sector_history_df = load_sector_history_for_sectors(
        PIPELINE_RUNS_DIR,
        (sector_name,),
        max_runs=30,
    )
    if not sector_history_df.empty:
        history_ordered = sector_history_df.sort_values(["run_order", "run_id"]).copy()
        latest_rs = pd.to_numeric(history_ordered["rs_value"], errors="coerce").dropna()
        latest_momentum = pd.to_numeric(history_ordered["momentum"], errors="coerce").dropna()
        rs_trend_payload = build_value_sparkline_payload(
            history_ordered,
            key_col="sector_name",
            value_col="rs_value",
            max_points=12,
            higher_is_better=True,
        ).get(sector_name, {})
        rank_trend_payload = build_value_sparkline_payload(
            history_ordered,
            key_col="sector_name",
            value_col="rank_position",
            max_points=12,
            higher_is_better=False,
        ).get(sector_name, {})
        latest_rank = pd.to_numeric(history_ordered["rank_position"], errors="coerce").dropna()
        trend_cols = st.columns(5)
        with trend_cols[0]:
            st.metric("Sector RS", f"{float(latest_rs.iloc[-1]):.3f}" if not latest_rs.empty else "—")
        with trend_cols[1]:
            delta_value = rs_trend_payload.get("delta_value")
            st.metric("Δ RS", f"{float(delta_value):+.3f}" if delta_value is not None else "—")
        with trend_cols[2]:
            st.metric("RS Trend", str(rs_trend_payload.get("trend", "Flat")))
        with trend_cols[3]:
            st.metric("Sector Rank", f"{int(latest_rank.iloc[-1])}" if not latest_rank.empty else "—")
        with trend_cols[4]:
            delta_rank = rank_trend_payload.get("delta_value")
            if delta_rank is None:
                st.metric("Δ Sector Rank", "—")
            else:
                st.metric("Δ Sector Rank", f"{int(round(delta_rank)):+d}")

        chart_df = history_ordered.copy()
        chart_df.loc[:, "run_label"] = chart_df["run_date"].dt.strftime("%Y-%m-%d").fillna(chart_df["run_id"])
        fig_sector = go.Figure()
        fig_sector.add_trace(
            go.Scatter(
                x=chart_df["run_label"],
                y=chart_df["rs_value"],
                mode="lines+markers",
                name="Sector RS",
                line=dict(width=2, color="#0ea5e9"),
                marker=dict(size=6),
            )
        )
        if not latest_rank.empty:
            fig_sector.add_trace(
                go.Scatter(
                    x=chart_df["run_label"],
                    y=chart_df["rank_position"],
                    mode="lines+markers",
                    name="Sector Rank",
                    line=dict(width=1.8, color="#7c3aed"),
                    marker=dict(size=5),
                    yaxis="y3",
                )
            )
        if not latest_momentum.empty:
            fig_sector.add_trace(
                go.Scatter(
                    x=chart_df["run_label"],
                    y=chart_df["momentum"],
                    mode="lines",
                    name="Momentum",
                    line=dict(width=1.5, color="#f97316", dash="dot"),
                    yaxis="y2",
                )
            )
            fig_sector.update_layout(
                yaxis2=dict(
                    title="Momentum",
                    overlaying="y",
                    side="right",
                    showgrid=False,
                )
            )
        if not latest_rank.empty:
            fig_sector.update_layout(
                yaxis3=dict(
                    title="Sector Rank",
                    overlaying="y",
                    side="right",
                    anchor="free",
                    position=0.94,
                    autorange="reversed",
                    showgrid=False,
                )
            )
        fig_sector.update_layout(
            height=280,
            margin=dict(l=20, r=20, t=10, b=20),
            xaxis_title="Run",
            yaxis_title="Sector RS",
            legend_title="Metric",
        )
        st.markdown(f"**{label_kind} Trendline**")
        st.plotly_chart(fig_sector, use_container_width=True)
    else:
        drilldown_history_df = load_drilldown_history_for_symbols(
            PIPELINE_RUNS_DIR,
            tuple(sector_ranked["symbol_id"].astype(str).tolist()),
            max_runs=30,
        )
        if not drilldown_history_df.empty:
            trend_payload = build_value_sparkline_payload(
                drilldown_history_df.assign(group_label=sector_name),
                key_col="group_label",
                value_col="median_score",
                max_points=12,
                higher_is_better=True,
            ).get(sector_name, {})
            rank_payload = build_value_sparkline_payload(
                drilldown_history_df.assign(group_label=sector_name),
                key_col="group_label",
                value_col="best_rank",
                max_points=12,
                higher_is_better=False,
            ).get(sector_name, {})
            latest_row = drilldown_history_df.iloc[-1]
            fallback_cols = st.columns(5)
            with fallback_cols[0]:
                st.metric("Median Score", f"{float(latest_row.get('median_score', 0.0)):.2f}")
            with fallback_cols[1]:
                delta_value = trend_payload.get("delta_value")
                st.metric("Δ Median Score", f"{float(delta_value):+.2f}" if delta_value is not None else "—")
            with fallback_cols[2]:
                st.metric("Score Trend", str(trend_payload.get("trend", "Flat")))
            with fallback_cols[3]:
                best_rank = latest_row.get("best_rank")
                st.metric("Best Rank", f"{int(best_rank)}" if pd.notna(best_rank) else "—")
            with fallback_cols[4]:
                delta_rank = rank_payload.get("delta_value")
                st.metric("Δ Best Rank", f"{int(round(delta_rank)):+d}" if delta_rank is not None else "—")

            chart_df = drilldown_history_df.copy()
            chart_df.loc[:, "run_label"] = chart_df["run_date"].dt.strftime("%Y-%m-%d").fillna(chart_df["run_id"])
            fig_group = go.Figure()
            fig_group.add_trace(
                go.Scatter(
                    x=chart_df["run_label"],
                    y=chart_df["median_score"],
                    mode="lines+markers",
                    name="Median Score",
                    line=dict(width=2, color="#0ea5e9"),
                    marker=dict(size=6),
                )
            )
            fig_group.add_trace(
                go.Scatter(
                    x=chart_df["run_label"],
                    y=chart_df["best_rank"],
                    mode="lines+markers",
                    name="Best Rank",
                    line=dict(width=1.8, color="#7c3aed"),
                    marker=dict(size=5),
                    yaxis="y2",
                )
            )
            fig_group.update_layout(
                yaxis2=dict(
                    title="Best Rank",
                    overlaying="y",
                    side="right",
                    autorange="reversed",
                    showgrid=False,
                ),
                height=280,
                margin=dict(l=20, r=20, t=10, b=20),
                xaxis_title="Run",
                yaxis_title="Median Score",
                legend_title="Metric",
            )
            st.markdown(f"**{label_kind} Trendline**")
            st.plotly_chart(fig_group, use_container_width=True)

    trust_df = load_symbol_trust_snapshot(PROJECT_ROOT, sector_ranked["symbol_id"].astype(str).tolist())
    if not trust_df.empty:
        trust_df = trust_df.rename(
            columns={
                "provider": "trust_provider",
                "validation_status": "trust_status",
                "is_quarantined": "trust_quarantined",
            }
        )
        trust_df.loc[:, "symbol_id"] = trust_df["symbol_id"].astype(str).str.upper()
        sector_ranked.loc[:, "symbol_id"] = sector_ranked["symbol_id"].astype(str).str.upper()
        sector_ranked = sector_ranked.merge(
            trust_df[
                [
                    "symbol_id",
                    "trust_provider",
                    "trust_status",
                    "validated_against",
                    "repair_batch_id",
                    "latest_trade_date",
                    "trust_quarantined",
                ]
            ],
            on="symbol_id",
            how="left",
        )

    display_cols = [
        "overall_rank",
        "sector_rank",
        "ranked_flag",
        "symbol_id",
        "company_name",
        "sector_name",
        "industry_group",
        score_col,
        "close",
        "rel_strength_score",
        "vol_intensity_score",
        "trend_score_score",
        "prox_high_score",
        "delivery_pct_score",
        "sector_strength_score",
        "trust_provider",
        "trust_status",
        "trust_quarantined",
        "latest_trade_date",
    ]
    sector_ranked = sector_ranked.loc[:, ~pd.Index(sector_ranked.columns).duplicated(keep="first")]
    keep_cols = [col for col in display_cols if col in sector_ranked.columns]
    display_df = sector_ranked.loc[:, keep_cols]
    renamed = display_df.rename(
        columns={
            "overall_rank": "Overall Rank",
            "sector_rank": "Sector Rank",
            "ranked_flag": "Ranked",
            "symbol_id": "Symbol",
            "company_name": "Company",
            "sector_name": "Sector",
            "industry_group": "Industry Group",
            "composite_score": "Score",
            "custom_score": "Score",
            "close": "Price",
            "rel_strength_score": "RS",
            "vol_intensity_score": "Vol",
            "trend_score_score": "Trend",
            "prox_high_score": "Highs",
            "delivery_pct_score": "Delivery",
            "sector_strength_score": "Sector",
            "trust_provider": "Provider",
            "trust_status": "Trust Status",
            "trust_quarantined": "Quarantined",
            "latest_trade_date": "Latest Trade Date",
        }
    )
    renamed = _with_unique_column_names(renamed)
    st.dataframe(renamed, use_container_width=True, hide_index=True, height=720)


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
            df.loc[:, "date"] = pd.to_datetime(df["date"])
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
                    result.loc[:, "timestamp"] = pd.to_datetime(result["timestamp"])
                    result.loc[:, "timestamp"] = result["timestamp"].dt.normalize()
                    result.set_index("timestamp", inplace=True)
                    features[feat] = result.copy()
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
            result.loc[:, "timestamp"] = pd.to_datetime(result["timestamp"])
            result.loc[:, "timestamp"] = result["timestamp"].dt.normalize()
            result.set_index("timestamp", inplace=True)
            features["bb"] = result.copy()

    for feat in per_symbol_features:
        path = os.path.join(FEATURE_STORE, feat, exchange, f"{symbol}.parquet")
        if os.path.exists(path):
            df = pd.read_parquet(path)
            if "timestamp" in df.columns:
                df.loc[:, "timestamp"] = pd.to_datetime(df["timestamp"])
                df.loc[:, "timestamp"] = df["timestamp"].dt.normalize()
                df.set_index("timestamp", inplace=True)
            features[feat] = df.copy()

    return features


def resolve_sector_drilldown_rank_source(
    latest_rank_frames: Dict[str, pd.DataFrame],
    session_rank_df: pd.DataFrame | None,
    dashboard_payload: Dict[str, object] | None,
) -> pd.DataFrame:
    """Prefer the full rank artifact over cached/session summary tables for sector drilldown."""
    latest_rank_df = normalize_rank_df(latest_rank_frames.get("ranked_signals", pd.DataFrame()))
    if not latest_rank_df.empty:
        return latest_rank_df

    session_df = normalize_rank_df(session_rank_df if session_rank_df is not None else pd.DataFrame())
    if not session_df.empty:
        return session_df

    if dashboard_payload:
        return normalize_rank_df(pd.DataFrame(dashboard_payload.get("ranked_signals", [])))
    return pd.DataFrame()


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
    df.loc[:, "custom_score"] = (
        df["rel_strength_score"] * (w_rs / total)
        + df["vol_intensity_score"] * (w_vol / total)
        + df["trend_score_score"] * (w_trend / total)
        + df["prox_high_score"] * (w_high / total)
    )
    df = df.sort_values("custom_score", ascending=False)
    return df.reset_index(drop=True)


def _pattern_overlay_color(state: str) -> str:
    state_key = str(state or "").strip().lower()
    if state_key == "confirmed":
        return "#22c55e"
    if state_key == "watchlist":
        return "#f59e0b"
    return "#94a3b8"


def _parse_pattern_json_list(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    if isinstance(value, str) and not value.strip():
        return []
    if pd.isna(value):
        return []
    try:
        return list(json.loads(str(value)))
    except Exception:
        return []


def _build_pattern_overlay_option_map(pattern_df: pd.DataFrame, symbol: str) -> dict[str, str | None]:
    options: dict[str, str | None] = {"None": None}
    if pattern_df is None or pattern_df.empty or "symbol_id" not in pattern_df.columns:
        return options
    subset = pattern_df[pattern_df["symbol_id"].astype(str).str.upper() == str(symbol).upper()].copy()
    if subset.empty:
        return options
    if "pattern_state" in subset.columns:
        subset.loc[:, "_state_order"] = np.where(subset["pattern_state"].astype(str) == "confirmed", 0, 1)
    else:
        subset.loc[:, "_state_order"] = 1
    subset.loc[:, "_score_order"] = pd.to_numeric(subset.get("pattern_score"), errors="coerce").fillna(0.0)
    subset = subset.sort_values(["_state_order", "_score_order", "signal_date"], ascending=[True, False, False])
    for _, row in subset.iterrows():
        score_value = pd.to_numeric(pd.Series([row.get("pattern_score")]), errors="coerce").iloc[0]
        label = (
            f"{str(row.get('pattern_family') or '').replace('_', ' ')}"
            f" | {str(row.get('pattern_state') or '')}"
            f" | {str(row.get('signal_date') or '')}"
            f" | score {0.0 if pd.isna(score_value) else float(score_value):.1f}"
        )
        options[label] = str(row.get("signal_id") or "")
    return options


def _add_pattern_overlay_to_figure(fig: go.Figure, ohlcv: pd.DataFrame, pattern_row: pd.Series | None) -> go.Figure:
    if pattern_row is None or ohlcv.empty:
        return fig

    color = _pattern_overlay_color(str(pattern_row.get("pattern_state") or ""))
    smoothed = kernel_smooth(ohlcv["close"], bandwidth=3.0)
    fig.add_trace(
        go.Scatter(
            x=ohlcv.index,
            y=smoothed,
            name="Pattern Smoothed",
            line=dict(color=color, width=2, dash="dash"),
            opacity=0.85,
        ),
        row=1,
        col=1,
    )

    pivot_dates = pd.to_datetime(_parse_pattern_json_list(pattern_row.get("pivot_dates")), errors="coerce")
    pivot_prices = pd.to_numeric(pd.Series(_parse_pattern_json_list(pattern_row.get("pivot_prices"))), errors="coerce")
    pivot_labels = [str(value) for value in _parse_pattern_json_list(pattern_row.get("pivot_labels"))]
    if len(pivot_dates) and len(pivot_prices):
        fig.add_trace(
            go.Scatter(
                x=pivot_dates,
                y=pivot_prices,
                name="Pattern Pivots",
                mode="markers+text",
                text=pivot_labels[: len(pivot_prices)],
                textposition="top center",
                marker=dict(size=9, color=color, line=dict(width=1, color="#111827")),
                textfont=dict(size=10, color=color),
            ),
            row=1,
            col=1,
        )

    breakout_level = pd.to_numeric(pd.Series([pattern_row.get("breakout_level")]), errors="coerce").iloc[0]
    invalidation_price = pd.to_numeric(pd.Series([pattern_row.get("invalidation_price")]), errors="coerce").iloc[0]
    if pd.notna(breakout_level):
        fig.add_hline(
            y=float(breakout_level),
            line_dash="dash",
            line_color=color,
            annotation_text="Breakout" if str(pattern_row.get("pattern_state") or "") == "confirmed" else "Trigger",
            row=1,
            col=1,
        )
    if pd.notna(invalidation_price):
        fig.add_hline(
            y=float(invalidation_price),
            line_dash="dot",
            line_color="#ef4444",
            annotation_text="Invalidation",
            row=1,
            col=1,
        )

    score = pd.to_numeric(pd.Series([pattern_row.get("pattern_score")]), errors="coerce").iloc[0]
    badge_text = (
        f"{str(pattern_row.get('pattern_family') or '').replace('_', ' ').title()} | "
        f"{str(pattern_row.get('pattern_state') or '').upper()} | "
        f"Score {float(score) if pd.notna(score) else 0.0:.1f}"
    )
    fig.add_annotation(
        x=0.01,
        y=0.99,
        xref="paper",
        yref="paper",
        text=badge_text,
        showarrow=False,
        bgcolor="rgba(15,23,42,0.88)",
        bordercolor=color,
        borderwidth=1,
        font=dict(color="#f8fafc", size=12),
        xanchor="left",
        yanchor="top",
    )
    return fig


def plot_candlestick_with_features(
    ohlcv: pd.DataFrame,
    features: Dict[str, pd.DataFrame],
    symbol: str,
    pattern_row: pd.Series | None = None,
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
    return _add_pattern_overlay_to_figure(fig, ohlcv, pattern_row)


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


def _clear_portfolio_caches() -> None:
    load_trade_report.clear()


def _get_paper_execution_service() -> ExecutionService:
    store = ExecutionStore(PROJECT_ROOT)
    adapter = PaperExecutionAdapter()
    return ExecutionService(
        store,
        adapter,
        default_order_type="MARKET",
        default_product_type="CNC",
        default_validity="DAY",
    )


def _suggest_position_size(symbol_id: str, *, exchange: str = "NSE", capital: float = 1_000_000.0) -> dict:
    try:
        risk_manager = RiskManager(
            ohlcv_db_path=OHLCV_DB,
            feature_store_dir=FEATURE_STORE,
        )
        return risk_manager.compute_position_size(
            symbol_id,
            exchange=exchange,
            capital=float(capital),
            regime="TREND",
            regime_multiplier=1.0,
        )
    except Exception:
        return {}


def _execute_portfolio_buy(
    *,
    candidate: dict,
    price: float,
    quantity: int,
    thesis: str,
    setup_note: str,
    tags: str,
) -> dict:
    symbol_id = str(candidate.get("symbol_id") or "").strip().upper()
    exchange = str(candidate.get("exchange") or "NSE").strip().upper()
    service = _get_paper_execution_service()
    intent = OrderIntent(
        symbol_id=symbol_id,
        exchange=exchange,
        quantity=int(quantity),
        side="BUY",
        requested_price=float(price),
        product_type="CNC",
        metadata={
            "strategy": "portfolio_manual_buy",
            "thesis": thesis,
            "setup_note": setup_note,
            "tags": tags,
            "source": "portfolio_tab",
            "composite_score": candidate.get("composite_score"),
            "breakout_score": candidate.get("breakout_score"),
            "breakout_state": candidate.get("breakout_state"),
            "candidate_tier": candidate.get("candidate_tier"),
        },
    )
    result = service.submit_order(intent, market_price=float(price))
    save_trade_journal_note(
        PROJECT_ROOT,
        trade_ref=open_position_trade_ref(symbol_id, exchange),
        symbol_id=symbol_id,
        exchange=exchange,
        thesis=thesis,
        setup_note=setup_note,
        tags=tags,
        metadata={"source": "portfolio_tab", "action": "buy"},
    )
    _clear_portfolio_caches()
    return result


def _execute_portfolio_sell(
    *,
    position: dict,
    price: float,
    quantity: int,
    exit_note: str,
) -> dict:
    symbol_id = str(position.get("symbol_id") or "").strip().upper()
    exchange = str(position.get("exchange") or "NSE").strip().upper()
    service = _get_paper_execution_service()
    intent = OrderIntent(
        symbol_id=symbol_id,
        exchange=exchange,
        quantity=int(quantity),
        side="SELL",
        requested_price=float(price),
        product_type="CNC",
        metadata={
            "strategy": "portfolio_manual_sell",
            "exit_note": exit_note,
            "source": "portfolio_tab",
        },
    )
    result = service.submit_order(intent, market_price=float(price))
    fills = result.get("fills", []) if isinstance(result, dict) else []
    if fills:
        save_trade_journal_note(
            PROJECT_ROOT,
            trade_ref=closed_trade_ref(str(fills[0].get("fill_id") or "")),
            symbol_id=symbol_id,
            exchange=exchange,
            exit_note=exit_note,
            metadata={"source": "portfolio_tab", "action": "sell"},
        )
    _clear_portfolio_caches()
    return result


def _render_portfolio_workspace(
    *,
    rank_df: pd.DataFrame | None,
    latest_rank_frames: Dict[str, pd.DataFrame],
) -> None:
    st.subheader("💼 Portfolio Workspace")

    workspace = load_portfolio_workspace_report(
        PROJECT_ROOT,
        ranked_df=rank_df,
        breakout_df=latest_rank_frames.get("breakout_scan", pd.DataFrame()),
    )
    summary = workspace.get("summary", {}) or {}
    candidates = workspace.get("candidates", pd.DataFrame())
    open_positions_df = workspace.get("open_positions", pd.DataFrame())
    closed_trades_df = workspace.get("closed_trades", pd.DataFrame())
    journal_df = workspace.get("journal", pd.DataFrame())
    realized_curve = workspace.get("realized_curve", pd.DataFrame())

    summary_cols = st.columns(7)
    summary_cols[0].metric("Open Positions", int(summary.get("open_positions", 0) or 0))
    summary_cols[1].metric("Invested", f"₹{float(summary.get('invested_capital', 0.0) or 0.0):,.0f}")
    summary_cols[2].metric("Market Value", f"₹{float(summary.get('market_value', 0.0) or 0.0):,.0f}")
    summary_cols[3].metric("Realized P&L", f"₹{float(summary.get('realized_pnl', 0.0) or 0.0):,.0f}")
    summary_cols[4].metric("Unrealized P&L", f"₹{float(summary.get('unrealized_pnl', 0.0) or 0.0):,.0f}")
    summary_cols[5].metric("Total P&L", f"₹{float(summary.get('total_pnl', 0.0) or 0.0):,.0f}")
    summary_cols[6].metric("Win Rate", f"{float(summary.get('win_rate', 0.0) or 0.0) * 100:.1f}%")

    tab_filter, tab_current, tab_journal = st.tabs(
        ["Filter & Add", "Current Portfolio", "Performance & Journal"]
    )

    with tab_filter:
        st.markdown("**Screen, review, and add paper positions**")
        if candidates is None or candidates.empty:
            st.info("No ranking candidates available yet. Run the pipeline or refresh ranking first.")
        else:
            working = candidates.copy()
            working.loc[:, "sector_name"] = working.get("sector_name", "").fillna("").astype(str)
            working.loc[:, "company_name"] = working.get("company_name", "").fillna("").astype(str)
            working.loc[:, "breakout_state"] = working.get("breakout_state", "").fillna("").astype(str)
            working.loc[:, "candidate_tier"] = working.get("candidate_tier", "").fillna("").astype(str)

            filter_cols = st.columns(4)
            sectors = sorted([value for value in working["sector_name"].dropna().unique().tolist() if str(value).strip()])
            selected_sectors = filter_cols[0].multiselect("Sector", sectors)
            top_n = int(filter_cols[1].number_input("Top N Rank", min_value=5, max_value=max(5, len(working)), value=min(100, len(working)), step=5))
            breakout_only = filter_cols[2].toggle("Breakout Only", value=False)
            search_text = filter_cols[3].text_input("Search Symbol / Company", value="").strip().lower()

            filter_cols2 = st.columns(4)
            breakout_states = sorted([value for value in working["breakout_state"].dropna().unique().tolist() if str(value).strip()])
            selected_breakout_states = filter_cols2[0].multiselect("Breakout State", breakout_states)
            selected_tiers = filter_cols2[1].multiselect("Candidate Tier", ["A", "B", "C"])
            min_composite = float(filter_cols2[2].number_input("Min Composite Score", value=0.0, step=1.0))
            min_breakout = float(filter_cols2[3].number_input("Min Breakout Score", value=0.0, step=1.0))

            filtered = working.copy()
            if selected_sectors:
                filtered = filtered[filtered["sector_name"].isin(selected_sectors)]
            if breakout_only:
                filtered = filtered[filtered["has_breakout"].fillna(False)]
            if selected_breakout_states:
                filtered = filtered[filtered["breakout_state"].isin(selected_breakout_states)]
            if selected_tiers:
                filtered = filtered[filtered["candidate_tier"].isin(selected_tiers)]
            if "rank_position" in filtered.columns:
                filtered = filtered[pd.to_numeric(filtered["rank_position"], errors="coerce").fillna(999999) <= top_n]
            filtered = filtered[pd.to_numeric(filtered["composite_score"], errors="coerce").fillna(0.0) >= min_composite]
            filtered = filtered[pd.to_numeric(filtered["breakout_score"], errors="coerce").fillna(0.0) >= min_breakout]
            if search_text:
                filtered = filtered[
                    filtered["symbol_id"].astype(str).str.lower().str.contains(search_text, regex=False)
                    | filtered["company_name"].astype(str).str.lower().str.contains(search_text, regex=False)
                ]

            st.caption(f"{len(filtered)} candidates after filters")

            display = filtered.copy()
            display.loc[:, "Select"] = False
            screener_cols = [
                "Select",
                "symbol_id",
                "company_name",
                "sector_name",
                "rank_position",
                "composite_score",
                "breakout_tag",
                "breakout_state",
                "candidate_tier",
                "breakout_score",
                "close",
                "tradingview_url",
            ]
            screener_cols = [column for column in screener_cols if column in display.columns]
            edited = st.data_editor(
                display[screener_cols].head(250),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Select": st.column_config.CheckboxColumn("Pick", default=False),
                    "symbol_id": st.column_config.TextColumn("Symbol"),
                    "company_name": st.column_config.TextColumn("Company"),
                    "sector_name": st.column_config.TextColumn("Sector"),
                    "rank_position": st.column_config.NumberColumn("Rank", format="%d"),
                    "composite_score": st.column_config.NumberColumn("Composite", format="%.2f"),
                    "breakout_tag": st.column_config.TextColumn("Breakout"),
                    "breakout_state": st.column_config.TextColumn("State"),
                    "candidate_tier": st.column_config.TextColumn("Tier"),
                    "breakout_score": st.column_config.NumberColumn("Breakout Score", format="%.2f"),
                    "close": st.column_config.NumberColumn("Close", format="%.2f"),
                    "tradingview_url": st.column_config.LinkColumn("TradingView", display_text="Open"),
                },
                disabled=[column for column in screener_cols if column != "Select"],
                key="portfolio_candidate_editor",
            )

            picked = edited[edited["Select"] == True] if "Select" in edited.columns else pd.DataFrame()
            if len(picked) > 1:
                st.warning("Select one stock at a time for the trade ticket. Using the first selected row.")
            selected_symbol = str(picked.iloc[0]["symbol_id"]) if not picked.empty else ""

            if selected_symbol:
                candidate_row = filtered[filtered["symbol_id"].astype(str) == selected_symbol].head(1)
                if not candidate_row.empty:
                    candidate = candidate_row.iloc[0].to_dict()
                    st.markdown(f"**Trade Ticket: {selected_symbol}**")
                    risk_hint = _suggest_position_size(
                        selected_symbol,
                        exchange=str(candidate.get("exchange") or "NSE"),
                        capital=1_000_000.0,
                    )
                    ticket_cols = st.columns(4)
                    default_price = float(candidate.get("close") or 0.0)
                    suggested_qty = int(risk_hint.get("shares") or 0)
                    with st.form(f"portfolio_buy_form_{selected_symbol}", clear_on_submit=False):
                        buy_price = ticket_cols[0].number_input("Price", min_value=0.0, value=max(default_price, 0.0), step=0.05, format="%.2f")
                        buy_qty = ticket_cols[1].number_input("Quantity", min_value=1, value=max(suggested_qty, 1), step=1)
                        ticket_cols[2].metric("Suggested Qty", suggested_qty)
                        ticket_cols[3].metric("Risk Stop", f"₹{float(risk_hint.get('stop_loss', 0.0) or 0.0):,.2f}")
                        thesis = st.text_input("Thesis", value=str(candidate.get("thesis") or ""))
                        setup_note = st.text_area("Setup Note", value=str(candidate.get("symbol_trend_reasons") or ""))
                        tags = st.text_input("Tags", value=str(candidate.get("candidate_tier") or ""))
                        submitted = st.form_submit_button("Add to Portfolio", use_container_width=True)
                    if submitted:
                        result = _execute_portfolio_buy(
                            candidate=candidate,
                            price=float(buy_price),
                            quantity=int(buy_qty),
                            thesis=thesis,
                            setup_note=setup_note,
                            tags=tags,
                        )
                        st.success(
                            f"Paper BUY executed for {selected_symbol}: {int(buy_qty)} shares at about ₹{float(buy_price):,.2f}."
                        )
                        st.caption(f"Order status: {result.get('status', 'unknown')}")
                        st.rerun()

    with tab_current:
        st.markdown("**Live paper holdings with broker-style action grid**")
        if open_positions_df is None or open_positions_df.empty:
            st.info("No open paper positions recorded yet.")
        else:
            row_selected_symbol = _render_portfolio_holdings_broker_table(open_positions_df)
            if row_selected_symbol:
                st.session_state.portfolio_selected_symbol = row_selected_symbol

            symbols = open_positions_df["symbol_id"].astype(str).tolist()
            if st.session_state.portfolio_selected_symbol not in symbols:
                st.session_state.portfolio_selected_symbol = symbols[0] if symbols else None

            default_index = symbols.index(st.session_state.portfolio_selected_symbol) if st.session_state.portfolio_selected_symbol in symbols else 0
            selected_holding = st.selectbox(
                "Active Sell Ticket",
                symbols,
                index=default_index,
                format_func=lambda symbol: f"{symbol} | {open_positions_df.loc[open_positions_df['symbol_id'].astype(str) == symbol, 'sell_reason'].iloc[0]}",
            )
            st.session_state.portfolio_selected_symbol = selected_holding

            position_row = open_positions_df[open_positions_df["symbol_id"].astype(str) == str(selected_holding)].head(1)
            if not position_row.empty:
                position = position_row.iloc[0].to_dict()
                st.markdown("**Sell Ticket**")
                manage_cols = st.columns(4)
                manage_cols[0].metric("Qty", int(position.get("quantity", 0) or 0))
                manage_cols[1].metric("Avg Entry", f"₹{float(position.get('avg_entry_price', 0.0) or 0.0):,.2f}")
                manage_cols[2].metric("Current", f"₹{float(position.get('current_price', 0.0) or 0.0):,.2f}")
                manage_cols[3].metric("Suggestion", str(position.get("sell_suggestion") or "HOLD"))

                with st.form(f"portfolio_sell_form_{selected_holding}", clear_on_submit=False):
                    sell_mode = st.radio("Sell Type", ["Full", "Partial"], horizontal=True)
                    max_qty = int(position.get("quantity") or 0)
                    default_qty = max_qty if sell_mode == "Full" else max(1, max_qty // 2)
                    sell_qty = st.number_input("Sell Quantity", min_value=1, max_value=max_qty, value=default_qty, step=1)
                    sell_price = st.number_input(
                        "Sell Price",
                        min_value=0.0,
                        value=float(position.get("current_price") or position.get("avg_entry_price") or 0.0),
                        step=0.05,
                        format="%.2f",
                    )
                    exit_note = st.text_area("Exit Note", value=str(position.get("sell_reason") or ""))
                    sell_submitted = st.form_submit_button("Execute Sell", use_container_width=True)
                if sell_submitted:
                    result = _execute_portfolio_sell(
                        position=position,
                        price=float(sell_price),
                        quantity=int(sell_qty),
                        exit_note=exit_note,
                    )
                    st.success(
                        f"Paper SELL executed for {selected_holding}: {int(sell_qty)} shares at about ₹{float(sell_price):,.2f}."
                    )
                    st.caption(f"Order status: {result.get('status', 'unknown')}")
                    st.rerun()

    with tab_journal:
        st.markdown("**Performance history and editable trade journal**")
        if realized_curve is not None and not realized_curve.empty:
            fig_realized = go.Figure()
            fig_realized.add_trace(
                go.Scatter(
                    x=realized_curve["filled_at"],
                    y=realized_curve["cumulative_realized_pnl"],
                    mode="lines+markers",
                    name="Cumulative Realized P&L",
                    line=dict(color="#22c55e", width=2),
                )
            )
            fig_realized.update_layout(
                height=320,
                margin=dict(l=20, r=20, t=20, b=20),
                yaxis_title="P&L (₹)",
                xaxis_title="Trade Date",
            )
            st.plotly_chart(fig_realized, use_container_width=True)
        else:
            st.info("Realized performance curve will appear after closed trades exist.")

        perf_cols = st.columns(2)
        with perf_cols[0]:
            st.markdown("**Closed Trades**")
            if closed_trades_df is None or closed_trades_df.empty:
                st.info("No closed trades recorded yet.")
            else:
                closed_cols = [
                    "symbol_id",
                    "sector_name",
                    "closed_quantity",
                    "entry_avg_price",
                    "exit_price",
                    "realized_pnl",
                    "return_pct",
                    "filled_at",
                ]
                closed_cols = [column for column in closed_cols if column in closed_trades_df.columns]
                st.dataframe(closed_trades_df[closed_cols], use_container_width=True, hide_index=True, height=280)
        with perf_cols[1]:
            st.markdown("**Trade Journal**")
            if journal_df is None or journal_df.empty:
                st.info("Journal entries appear after buys or sells are recorded.")
            else:
                journal_cols = [
                    "trade_ref",
                    "journal_status",
                    "symbol_id",
                    "sector_name",
                    "qty",
                    "pnl_value",
                    "thesis",
                    "setup_note",
                    "exit_note",
                    "lesson_learned",
                    "tags",
                ]
                journal_cols = [column for column in journal_cols if column in journal_df.columns]
                st.dataframe(journal_df[journal_cols], use_container_width=True, hide_index=True, height=280)

                journal_options = journal_df["trade_ref"].astype(str).tolist()
                selected_trade_ref = st.selectbox("Edit Journal Entry", journal_options)
                note_row = journal_df[journal_df["trade_ref"].astype(str) == str(selected_trade_ref)].head(1)
                if not note_row.empty:
                    note = note_row.iloc[0].to_dict()
                    with st.form(f"journal_edit_form_{selected_trade_ref}", clear_on_submit=False):
                        thesis = st.text_input("Thesis", value=str(note.get("thesis") or ""))
                        setup_note = st.text_area("Setup Note", value=str(note.get("setup_note") or ""))
                        exit_note = st.text_area("Exit Note", value=str(note.get("exit_note") or ""))
                        lesson_learned = st.text_area("Lesson Learned", value=str(note.get("lesson_learned") or ""))
                        tags = st.text_input("Tags", value=str(note.get("tags") or ""))
                        note_saved = st.form_submit_button("Save Journal Note", use_container_width=True)
                    if note_saved:
                        save_trade_journal_note(
                            PROJECT_ROOT,
                            trade_ref=str(selected_trade_ref),
                            symbol_id=str(note.get("symbol_id") or ""),
                            exchange=str(note.get("exchange") or "NSE"),
                            thesis=thesis,
                            setup_note=setup_note,
                            exit_note=exit_note,
                            lesson_learned=lesson_learned,
                            tags=tags,
                            metadata={"source": "portfolio_tab", "action": "journal_edit"},
                        )
                        _clear_portfolio_caches()
                        st.success("Journal note saved.")
                        st.rerun()


def _portfolio_badge_html(value: object, *, tone: str = "slate") -> str:
    text = str(value or "—").strip() or "—"
    return f"<span class='broker-badge broker-badge-{tone}'>{text}</span>"


def _portfolio_pnl_html(value: object) -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return "<div class='broker-cell-value broker-neutral'>—</div>"
    tone = "broker-positive" if float(number) >= 0 else "broker-negative"
    prefix = "+" if float(number) > 0 else ""
    return f"<div class='broker-cell-value {tone}'>{prefix}{float(number):,.2f}</div>"


def _portfolio_percent_html(value: object) -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return "<div class='broker-cell-value broker-neutral'>—</div>"
    tone = "broker-positive" if float(number) >= 0 else "broker-negative"
    prefix = "+" if float(number) > 0 else ""
    return f"<div class='broker-cell-value {tone}'>{prefix}{float(number):,.2f}%</div>"


def _render_portfolio_holdings_broker_table(open_positions_df: pd.DataFrame) -> str | None:
    header_cols = st.columns([1.4, 1.8, 0.75, 1.0, 1.0, 1.0, 1.0, 0.95, 1.0, 1.0, 0.9, 0.8], gap="small")
    headers = [
        "Symbol",
        "Sector / Signal",
        "Qty",
        "Avg",
        "LTP",
        "Value",
        "P&L",
        "Return",
        "Rank",
        "Breakout",
        "Signal",
        "Action",
    ]
    for column, label in zip(header_cols, headers):
        with column:
            st.markdown(f"<div class='broker-header-cell'>{label}</div>", unsafe_allow_html=True)

    selected_symbol: str | None = None
    for row in open_positions_df.to_dict(orient="records"):
        symbol_id = str(row.get("symbol_id") or "").strip()
        tv_link = str(row.get("tradingview_url") or _build_tradingview_link(symbol_id))
        sector_name = str(row.get("sector_name") or "—").strip() or "—"
        breakout_state = str(row.get("breakout_state") or "—").strip() or "—"
        candidate_tier = str(row.get("candidate_tier") or "—").strip() or "—"
        suggestion = str(row.get("sell_suggestion") or "HOLD").strip().upper()
        suggestion_tone = "emerald" if suggestion == "HOLD" else "amber"
        tier_tone = "emerald" if candidate_tier == "A" else "amber" if candidate_tier == "B" else "rose"
        breakout_tone = "emerald" if breakout_state == "qualified" else "amber" if breakout_state == "watchlist" else "rose"

        row_cols = st.columns([1.4, 1.8, 0.75, 1.0, 1.0, 1.0, 1.0, 0.95, 1.0, 1.0, 0.9, 0.8], gap="small")
        with row_cols[0]:
            st.markdown(
                (
                    "<div class='broker-cell'>"
                    f"<div class='broker-cell-title'><a href='{tv_link}' target='_blank'>{symbol_id}</a></div>"
                    f"<div class='broker-cell-sub'>{str(row.get('company_name') or '').strip() or 'TradingView linked'}</div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
        with row_cols[1]:
            st.markdown(
                (
                    "<div class='broker-cell'>"
                    f"<div class='broker-cell-title'>{sector_name}</div>"
                    f"<div class='broker-cell-sub'>{str(row.get('sell_reason') or '').strip() or 'rank_and_breakout_intact'}</div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
        with row_cols[2]:
            st.markdown(f"<div class='broker-cell-value'>{int(row.get('quantity') or 0)}</div>", unsafe_allow_html=True)
        with row_cols[3]:
            st.markdown(f"<div class='broker-cell-value'>{float(row.get('avg_entry_price') or 0.0):,.2f}</div>", unsafe_allow_html=True)
        with row_cols[4]:
            st.markdown(f"<div class='broker-cell-value'>{float(row.get('current_price') or 0.0):,.2f}</div>", unsafe_allow_html=True)
        with row_cols[5]:
            st.markdown(f"<div class='broker-cell-value'>{float(row.get('market_value') or 0.0):,.0f}</div>", unsafe_allow_html=True)
        with row_cols[6]:
            st.markdown(_portfolio_pnl_html(row.get("unrealized_pnl")), unsafe_allow_html=True)
        with row_cols[7]:
            st.markdown(_portfolio_percent_html(row.get("return_pct")), unsafe_allow_html=True)
        with row_cols[8]:
            st.markdown(f"<div class='broker-cell-value'>#{int(row.get('rank_position') or 0) if str(row.get('rank_position') or '').strip() else '—'}</div>", unsafe_allow_html=True)
        with row_cols[9]:
            st.markdown(
                (
                    "<div class='broker-cell'>"
                    f"{_portfolio_badge_html(breakout_state, tone=breakout_tone)}"
                    f"<div class='broker-inline-gap'></div>{_portfolio_badge_html(candidate_tier, tone=tier_tone)}"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
        with row_cols[10]:
            st.markdown(
                (
                    "<div class='broker-cell'>"
                    f"{_portfolio_badge_html(suggestion, tone=suggestion_tone)}"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
        with row_cols[11]:
            if st.button("Sell", key=f"portfolio_sell_row_{symbol_id}", use_container_width=True):
                selected_symbol = symbol_id

    return selected_symbol


def main():
    st.set_page_config(
        page_title="AI Trading Command Center",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.html("""
    <style>
    .stMainBlockContainer {padding-top: 0.64rem; padding-bottom: 0.78rem; max-width: 106rem;}
    [data-testid="stMetric"] {padding-top: 0.12rem; padding-bottom: 0.12rem;}
    [data-testid="stMetricValue"] {font-size: 1.22rem !important; line-height: 1.16 !important;}
    [data-testid="stMetricLabel"] {font-size: 0.82rem !important; line-height: 1.18 !important;}
    .stock-row:hover > div {background: rgba(0,176,246,0.08);}
    section[data-testid="stSidebar"] > div {padding-top: 0.56rem;}
    .workspace-nav-label {
      margin: 0 0 0.2rem 0;
      font-size: 0.73rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: #94a3b8;
      font-weight: 700;
    }
    .workspace-nav-row {
      padding-top: 0.08rem;
      padding-bottom: 0.34rem;
      margin-bottom: 0.36rem;
    }
    div[data-testid="stButtonGroup"] [role="radiogroup"] {
      gap: 0.45rem;
      flex-wrap: wrap;
      row-gap: 0.5rem;
    }
    div[data-testid="stButtonGroup"] button[data-testid^="stBaseButton-segmented_control"] {
      min-height: 2.42rem;
      border-radius: 12px !important;
      font-size: 0.95rem;
      font-weight: 600;
      border: 1px solid rgba(148, 163, 184, 0.4) !important;
      background: rgba(15, 23, 42, 0.04) !important;
    }
    div[data-testid="stButtonGroup"] button[data-testid="stBaseButton-segmented_controlActive"] {
      border-color: rgba(255, 75, 75, 0.92) !important;
      background: rgba(255, 75, 75, 0.96) !important;
      color: #ffffff !important;
    }
    div[data-testid="stDataFrame"] [role="gridcell"] {padding-top: 0.23rem; padding-bottom: 0.23rem;}
    .stMarkdown h1, .stMarkdown h2, .stMarkdown h3, .stMarkdown h4, .stMarkdown h5 {margin-top: 0.34rem !important; margin-bottom: 0.52rem !important;}
    div[data-testid="stExpander"] {margin-top: 0.26rem; margin-bottom: 0.26rem;}
    div[data-testid="stButton"] button {
      min-height: 2.42rem;
      font-size: 0.95rem;
      font-weight: 600;
    }
        .broker-header-cell {
          font-size: 0.68rem;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          color: #8fa3b8;
          padding: 0.15rem 0.1rem 0.55rem 0.1rem;
          font-weight: 700;
        }
        .broker-cell {
          background: linear-gradient(180deg, rgba(12, 18, 28, 0.96), rgba(20, 28, 40, 0.96));
          border: 1px solid rgba(71, 85, 105, 0.42);
          border-radius: 14px;
          min-height: 58px;
          padding: 0.6rem 0.72rem;
          display: flex;
          flex-direction: column;
          justify-content: center;
          box-shadow: inset 0 1px 0 rgba(148, 163, 184, 0.06);
        }
        .broker-cell-title {
          color: #f8fafc;
          font-size: 0.94rem;
          font-weight: 700;
          line-height: 1.15;
        }
        .broker-cell-title a {
          color: #f8fafc;
          text-decoration: none;
        }
        .broker-cell-title a:hover {
          color: #38bdf8;
        }
        .broker-cell-sub {
          color: #94a3b8;
          font-size: 0.73rem;
          margin-top: 0.2rem;
          line-height: 1.18;
        }
        .broker-cell-value {
          background: linear-gradient(180deg, rgba(12, 18, 28, 0.96), rgba(20, 28, 40, 0.96));
          border: 1px solid rgba(71, 85, 105, 0.42);
          border-radius: 14px;
          min-height: 58px;
          padding: 0.6rem 0.72rem;
          display: flex;
          align-items: center;
          justify-content: flex-end;
          color: #e2e8f0;
          font-size: 0.92rem;
          font-weight: 700;
          box-shadow: inset 0 1px 0 rgba(148, 163, 184, 0.06);
        }
        .broker-positive { color: #22c55e; }
        .broker-negative { color: #f97316; }
        .broker-neutral { color: #cbd5e1; }
        .broker-badge {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          padding: 0.18rem 0.5rem;
          border-radius: 999px;
          font-size: 0.67rem;
          font-weight: 700;
          letter-spacing: 0.03em;
          white-space: nowrap;
        }
        .broker-badge-emerald {
          background: rgba(34, 197, 94, 0.16);
          color: #86efac;
          border: 1px solid rgba(34, 197, 94, 0.28);
        }
        .broker-badge-amber {
          background: rgba(245, 158, 11, 0.16);
          color: #fcd34d;
          border: 1px solid rgba(245, 158, 11, 0.28);
        }
        .broker-badge-rose {
          background: rgba(244, 63, 94, 0.16);
          color: #fda4af;
          border: 1px solid rgba(244, 63, 94, 0.28);
        }
        .broker-badge-slate {
          background: rgba(148, 163, 184, 0.12);
          color: #cbd5e1;
          border: 1px solid rgba(148, 163, 184, 0.2);
        }
        .broker-inline-gap {
          height: 0.35rem;
        }
        div[data-testid="stButton"] button[kind="secondary"] {
          border-radius: 12px;
          border: 1px solid rgba(148, 163, 184, 0.4);
          background: rgba(15, 23, 42, 0.04);
        }
    @media (max-width: 1200px) {
      .stMainBlockContainer {max-width: 100%;}
    }
    </style>
    """)

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
    if "portfolio_selected_symbol" not in st.session_state:
        st.session_state.portfolio_selected_symbol = None

    # Prefer full ranked_signals.csv over limited dashboard_payload.json
    latest_rank_frames = load_latest_rank_frames(PROJECT_ROOT)
    ranked_signals_df = latest_rank_frames.get("ranked_signals", pd.DataFrame())

    # DEBUG: Add visible debug info at top of page
    st.sidebar.write(f"DEBUG: ranked_signals = {len(ranked_signals_df)} rows")

    # Build dashboard_payload from full ranked_signals.csv
    if not ranked_signals_df.empty:
        dashboard_payload = {
            "ranked_signals": ranked_signals_df.to_dict(orient="records"),
            "breakout_scan": latest_rank_frames.get("breakout_scan", pd.DataFrame()).to_dict(orient="records"),
            "pattern_scan": latest_rank_frames.get("pattern_scan", pd.DataFrame()).to_dict(orient="records"),
            "stock_scan": latest_rank_frames.get("stock_scan", pd.DataFrame()).to_dict(orient="records"),
            "sector_dashboard": latest_rank_frames.get("sector_dashboard", pd.DataFrame()).to_dict(orient="records"),
            "summary": {"ranked_count": len(ranked_signals_df)},
        }
    else:
        dashboard_payload = load_latest_dashboard_payload() or load_latest_rank_fallback()

    dashboard_health = get_dashboard_health(dashboard_payload)
    ops_snapshot = load_ops_health_snapshot(PROJECT_ROOT)
    data_trust_snapshot = load_data_trust_snapshot(PROJECT_ROOT)
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
            stage2_filter_enabled = st.toggle(
                "Stage-2 uptrend only",
                value=False,
                key="stage2_filter_enabled",
            )
            stage2_min_score = st.slider(
                "Stage-2 min score",
                0.0,
                100.0,
                70.0,
                1.0,
                key="stage2_min_score",
            )

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
                source_label = st.radio(
                    "Breadth Source",
                    options=["Operational (live)", "Research (static)"],
                    index=0,
                    horizontal=True,
                    key="breadth_history_source",
                )
                source_key = "operational" if source_label.startswith("Operational") else "research"
                breadth_df = load_breadth_history(data_source=source_key)
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
                    if source_key == "operational":
                        st.caption(
                            "Operational universe breadth using NSE symbols with enough history for each SMA window."
                        )
                    else:
                        st.caption(
                            "Research universe breadth (static snapshot) using NSE symbols with enough history for each SMA window."
                        )
            except Exception as e:
                st.warning(f"Could not load breadth history: {e}")

        st.divider()
        st.caption(f"Last refresh: {st.session_state.last_refresh or 'Never'}")

    requested_view = _query_param_value("view").strip().lower()
    requested_sector = unquote_plus(_query_param_value("sector").strip())
    if requested_view == "sector" and requested_sector:
        rank_source_df = resolve_sector_drilldown_rank_source(
            latest_rank_frames,
            st.session_state.rank_df,
            dashboard_payload,
        )
        render_sector_drilldown_page(requested_sector, rank_source_df)
        return

    render_ops_health_ribbon(
        ops_snapshot,
        dashboard_health=dashboard_health,
        data_trust_snapshot=data_trust_snapshot,
    )

    workspace_options = [
        "🧭 Pipeline",
        "📋 Overview",
        "🏆 Ranking",
        "🧩 Patterns",
        "📈 Chart",
        "🧠 ML",
        "💼 Portfolio",
    ]
    if (
        "workspace_nav" not in st.session_state
        or st.session_state.workspace_nav not in workspace_options
    ):
        st.session_state.workspace_nav = "🧭 Pipeline"
    st.markdown("<div class='workspace-nav-row'>", unsafe_allow_html=True)
    st.markdown("<div class='workspace-nav-label'>Workspace</div>", unsafe_allow_html=True)
    selected_workspace = st.segmented_control(
        "Workspace",
        workspace_options,
        selection_mode="single",
        default=st.session_state.workspace_nav,
        key="workspace_nav",
        width="stretch",
        label_visibility="collapsed",
    )
    st.markdown("</div>", unsafe_allow_html=True)
    if selected_workspace not in workspace_options:
        selected_workspace = st.session_state.workspace_nav

    if selected_workspace == "🧭 Pipeline":
        latest_provider_stats = data_trust_snapshot.get("latest_provider_stats", {}) or {}
        latest_repair_run = data_trust_snapshot.get("latest_repair_run") or {}

        if not dashboard_payload:
            st.info("No dashboard payload found yet. Run the rank stage first.")
        else:
            summary = dashboard_payload.get("summary", {})
            pipeline_breakout_df = latest_rank_frames.get("breakout_scan", pd.DataFrame())
            pipeline_pattern_df = latest_rank_frames.get("pattern_scan", pd.DataFrame())
            pipeline_sector_df = latest_rank_frames.get("sector_dashboard", pd.DataFrame())
            pipeline_stock_scan_df = latest_rank_frames.get("stock_scan", pd.DataFrame())
            if pipeline_breakout_df.empty:
                pipeline_breakout_df = pd.DataFrame(dashboard_payload.get("breakout_scan", []))
            if pipeline_pattern_df.empty:
                pipeline_pattern_df = pd.DataFrame(dashboard_payload.get("pattern_scan", []))
            if pipeline_sector_df.empty:
                pipeline_sector_df = pd.DataFrame(dashboard_payload.get("sector_dashboard", []))
            if pipeline_stock_scan_df.empty:
                pipeline_stock_scan_df = pd.DataFrame(dashboard_payload.get("stock_scan", []))
            pipeline_ranked_leaders_df = pd.DataFrame(dashboard_payload.get("ranked_leaders", []))
            pipeline_pattern_discoveries_df = pd.DataFrame(dashboard_payload.get("pattern_discoveries", []))
            pipeline_breakout_candidates_df = pd.DataFrame(dashboard_payload.get("breakout_candidates", []))
            if pipeline_ranked_leaders_df.empty and not pipeline_stock_scan_df.empty:
                pipeline_ranked_leaders_df = pipeline_stock_scan_df.loc[
                    pd.to_numeric(pipeline_stock_scan_df.get("rank"), errors="coerce").notna()
                ].copy()
            if pipeline_pattern_discoveries_df.empty and not pipeline_stock_scan_df.empty:
                pipeline_pattern_discoveries_df = pipeline_stock_scan_df.loc[
                    pipeline_stock_scan_df.get(
                        "discovered_by_pattern_scan",
                        pd.Series(False, index=pipeline_stock_scan_df.index),
                    ).fillna(False).astype(bool)
                ].copy()
            if pipeline_breakout_candidates_df.empty and not pipeline_stock_scan_df.empty:
                pipeline_breakout_candidates_df = pipeline_stock_scan_df.loc[
                    pd.to_numeric(pipeline_stock_scan_df.get("rank"), errors="coerce").isna()
                    & pipeline_stock_scan_df.get(
                        "breakout_positive",
                        pd.Series(False, index=pipeline_stock_scan_df.index),
                    ).fillna(False).astype(bool)
                ].copy()
            task_status_map = dashboard_payload.get("task_status", {}) or {}
            col_left, col_right = st.columns([1, 1])
            with col_left:
                st.markdown("**Top Ranked Signals**")
                ranked_df = normalize_rank_df(pd.DataFrame(dashboard_payload.get("ranked_signals", [])))
                if ranked_df.empty:
                    st.info("No ranked signals in payload.")
                else:
                    pipeline_history_df = load_rank_history_for_symbols(
                        PIPELINE_RUNS_DIR,
                        tuple(ranked_df["symbol_id"].astype(str).tolist()),
                        max_runs=40,
                    )
                    pipeline_sparkline_payload = build_rank_sparkline_payload(
                        pipeline_history_df, max_points=10
                    )
                    ranked_display = enrich_ranked_table_with_context(
                        ranked_df,
                        weights=build_dashboard_weights(),
                        sparkline_payload=pipeline_sparkline_payload,
                        symbol_col="symbol_id",
                    )
                    ranked_display = reorder_columns(
                        ranked_display,
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
                            "Top Driver",
                            "Rank Trend",
                            "Δ Rank",
                            "Rank History",
                        ],
                    )
                    ranked_display = _with_symbol_hyperlink(ranked_display, symbol_col="symbol_id")
                    st.dataframe(
                        ranked_display,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                                "symbol_id": st.column_config.LinkColumn(
                                    "Symbol",
                                    help="Open symbol in TradingView.",
                                    display_text=r".*symbol=NSE(?:%3A|:)([^&]+).*",
                                ),
                            "Rank History": st.column_config.LineChartColumn(
                                "Rank History",
                                help="Recent rank trend across pipeline runs.",
                            )
                        },
                    )

                st.markdown("**Breakout Monitor**")
                show_filtered_pipeline = st.toggle(
                    "Show Filtered Rows",
                    value=False,
                    key="pipeline_breakout_hide_filtered",
                )
                breakout_monitor_df = pipeline_breakout_df.copy()
                if (
                    not show_filtered_pipeline
                    and not breakout_monitor_df.empty
                    and "breakout_state" in breakout_monitor_df.columns
                ):
                    breakout_monitor_df = breakout_monitor_df[
                        ~breakout_monitor_df["breakout_state"].astype(str).str.startswith("filtered_")
                    ].copy()

                if breakout_monitor_df.empty:
                    if pipeline_breakout_df.empty:
                        st.info("No breakout candidates in payload.")
                    else:
                        st.info("All breakout candidates are filtered by regime.")
                else:
                    breakout_monitor_display = reorder_columns(
                        breakout_monitor_df,
                        [
                            "symbol_id",
                            "sector",
                            "taxonomy_family",
                            "setup_family",
                            "breakout_score",
                            "breakout_rank",
                            "breakout_state",
                            "candidate_tier",
                            "symbol_trend_reasons",
                            "filter_reason",
                            "execution_label",
                            "market_bias",
                            "market_regime",
                            "setup_quality",
                        ],
                    )
                    breakout_monitor_display = _with_symbol_hyperlink(
                        breakout_monitor_display, symbol_col="symbol_id"
                    )
                    breakout_monitor_head = breakout_monitor_df.head(20).reset_index(drop=True)
                    breakout_monitor_head_display = breakout_monitor_display.head(20).reset_index(drop=True)
                    selected_breakout = render_selectable_grid(
                        breakout_monitor_head_display,
                        key="pipeline_breakout_grid",
                        height=360,
                        column_config={
                            "symbol_id": st.column_config.LinkColumn(
                                "Symbol",
                                help="Open symbol in TradingView.",
                                display_text=r".*symbol=NSE(?:%3A|:)([^&]+).*",
                            )
                        },
                    )
                    if selected_breakout is not None:
                        selected_idx = int(selected_breakout.name)
                        if 0 <= selected_idx < len(breakout_monitor_head):
                            render_breakout_setup_detail(
                                breakout_monitor_head.iloc[selected_idx],
                                breakout_monitor_df,
                                signal_date=summary.get("run_date"),
                                title="Selected Breakout Setup",
                            )

                st.markdown("**Pattern Monitor**")
                pattern_task_status = (task_status_map.get("pattern_scan", {}) or {}).get("status", "")
                pattern_task_detail = (task_status_map.get("pattern_scan", {}) or {}).get("detail", "")
                if pattern_task_status in {"failed", "timed_out"}:
                    st.warning(f"Pattern Scan: {pattern_task_status}. {pattern_task_detail}")
                elif pipeline_pattern_df.empty:
                    if pattern_task_status == "completed_empty":
                        st.info("Pattern Scan completed with no current operational pattern signals for this run.")
                    elif pattern_task_status == "skipped":
                        st.info(f"Pattern Scan skipped. {pattern_task_detail}")
                    else:
                        st.info("No operational pattern signals in payload.")
                else:
                    pattern_monitor_display = reorder_columns(
                        pipeline_pattern_df,
                        [
                            "symbol_id",
                            "pattern_family",
                            "pattern_state",
                            "pattern_lifecycle_state",
                            "pattern_operational_tier",
                            "pattern_score",
                            "pattern_priority_score",
                            "pattern_priority_rank",
                            "pattern_rank",
                            "setup_quality",
                            "breakout_level",
                            "invalidation_price",
                            "signal_date",
                            "volume_zscore_20",
                            "volume_zscore_50",
                        ],
                    )
                    pattern_monitor_display = _with_symbol_hyperlink(pattern_monitor_display, symbol_col="symbol_id")
                    st.dataframe(
                        pattern_monitor_display.head(20),
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "symbol_id": st.column_config.LinkColumn(
                                "Symbol",
                                help="Open symbol in TradingView.",
                                display_text=r".*symbol=NSE(?:%3A|:)([^&]+).*",
                            )
                        },
                    )
            with col_right:
                sector_dashboard_display = pipeline_sector_df.copy()
                st.caption("Click a sector name to open the sector drilldown page.")

                sector_history_df = load_sector_history_for_sectors(
                    PIPELINE_RUNS_DIR,
                    tuple(pipeline_sector_df["Sector"].astype(str).tolist()) if not pipeline_sector_df.empty and "Sector" in pipeline_sector_df.columns else (),
                    max_runs=30,
                )
                sector_rs_payload = build_value_sparkline_payload(
                    sector_history_df,
                    key_col="sector_name",
                    value_col="rs_value",
                    max_points=10,
                    higher_is_better=True,
                )
                sector_rank_payload = build_value_sparkline_payload(
                    sector_history_df,
                    key_col="sector_name",
                    value_col="rank_position",
                    max_points=10,
                    higher_is_better=False,
                )
                if not sector_dashboard_display.empty and (sector_rs_payload or sector_rank_payload):
                    if "Sector" in sector_dashboard_display.columns:
                        sector_dashboard_display.loc[:, "RS Trend"] = sector_dashboard_display["Sector"].astype(str).map(
                            lambda sector: sector_rs_payload.get(str(sector), {}).get("trend", "Flat")
                        )
                        sector_dashboard_display.loc[:, "Δ RS"] = sector_dashboard_display["Sector"].astype(str).map(
                            lambda sector: sector_rs_payload.get(str(sector), {}).get("delta_value", 0.0)
                        )
                        sector_dashboard_display.loc[:, "RS History"] = sector_dashboard_display["Sector"].astype(str).map(
                            lambda sector: sector_rs_payload.get(str(sector), {}).get("sparkline", [np.nan])
                        )
                        sector_dashboard_display.loc[:, "Sector Rank Trend"] = sector_dashboard_display["Sector"].astype(str).map(
                            lambda sector: sector_rank_payload.get(str(sector), {}).get("trend", "Flat")
                        )
                        sector_dashboard_display.loc[:, "Δ Sector Rank"] = sector_dashboard_display["Sector"].astype(str).map(
                            lambda sector: sector_rank_payload.get(str(sector), {}).get("delta_value", 0.0)
                        )
                        sector_dashboard_display.loc[:, "Sector Rank History"] = sector_dashboard_display["Sector"].astype(str).map(
                            lambda sector: sector_rank_payload.get(str(sector), {}).get("sparkline", [np.nan])
                        )
                st.markdown("**Sector Dashboard**")
                if sector_dashboard_display.empty:
                    render_sector_dashboard_links_table(sector_dashboard_display)
                else:
                    sector_display_df = reorder_columns(
                        sector_dashboard_display.loc[:, ~sector_dashboard_display.columns.duplicated()].copy(),
                        [
                            "Sector",
                            "RS",
                            "Momentum",
                            "Quadrant",
                            "RS_rank",
                            "RS Trend",
                            "Δ RS",
                            "RS History",
                            "Sector Rank Trend",
                            "Δ Sector Rank",
                            "Sector Rank History",
                            "Top Stocks",
                        ],
                    )
                    if "Sector" in sector_display_df.columns:
                        sector_names = sector_display_df.loc[:, sector_display_df.columns == "Sector"].iloc[:, 0].astype(str)
                        sector_display_df = sector_display_df.loc[:, sector_display_df.columns != "Sector"].copy()
                        sector_display_df.insert(
                            0,
                            "Sector Link",
                            sector_names.map(lambda sector: f"?view=sector&sector={quote(str(sector), safe='')}"),
                        )
                    st.dataframe(
                        sector_display_df,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Sector Link": st.column_config.LinkColumn(
                                "Sector",
                                help="Open the sector drilldown page.",
                                display_text=r".*sector=([^&]+).*",
                            ),
                            "RS History": st.column_config.LineChartColumn(
                                "RS History",
                                help="Recent sector RS across pipeline runs.",
                            ),
                            "Sector Rank History": st.column_config.LineChartColumn(
                                "Sector Rank History",
                                help="Recent sector rank across pipeline runs. Lower is better.",
                            ),
                        },
                        height=320,
                    )

                st.markdown("**Stock Scan**")
                if pipeline_stock_scan_df.empty:
                    st.info("No stock scan rows in payload.")
                else:
                    stock_scan_df = reorder_columns(
                        pipeline_stock_scan_df,
                        [
                            "Symbol",
                            "symbol_id",
                            "rank",
                            "composite_score",
                            "category",
                            "score",
                            "sector",
                            "why",
                            "pattern_positive",
                            "breakout_positive",
                            "discovered_by_pattern_scan",
                        ],
                    )
                    stock_scan_df = _with_symbol_hyperlink(stock_scan_df, symbol_col="symbol_id")
                    st.dataframe(stock_scan_df, use_container_width=True, hide_index=True)

                st.markdown("**Ranked Leaders**")
                if pipeline_ranked_leaders_df.empty:
                    st.info("No ranked leaders available.")
                else:
                    ranked_leaders_display = reorder_columns(
                        pipeline_ranked_leaders_df,
                        ["symbol_id", "rank", "composite_score", "exchange", "close", "rel_strength_score", "stage2_score"],
                    )
                    ranked_leaders_display = _with_symbol_hyperlink(ranked_leaders_display, symbol_col="symbol_id")
                    st.dataframe(ranked_leaders_display.head(10), use_container_width=True, hide_index=True)

                st.markdown("**Pattern Discoveries**")
                if pipeline_pattern_discoveries_df.empty:
                    discovery_visibility_note = str(summary.get("discovery_visibility_note") or "").strip()
                    if discovery_visibility_note:
                        st.info(discovery_visibility_note)
                    st.info("No pattern discoveries outside the ranked universe.")
                else:
                    pattern_discoveries_display = reorder_columns(
                        pipeline_pattern_discoveries_df,
                        [
                            "symbol_id",
                            "pattern_positive",
                            "discovered_by_pattern_scan",
                            "pattern_family",
                            "pattern_state",
                            "pattern_lifecycle_state",
                            "pattern_operational_tier",
                            "pattern_priority_score",
                            "pattern_priority_rank",
                            "stage2_score",
                            "rel_strength_score",
                        ],
                    )
                    pattern_discoveries_display = _with_symbol_hyperlink(pattern_discoveries_display, symbol_col="symbol_id")
                    st.dataframe(pattern_discoveries_display.head(10), use_container_width=True, hide_index=True)

                st.markdown("**Breakout Candidates**")
                if pipeline_breakout_candidates_df.empty:
                    discovery_visibility_note = str(summary.get("discovery_visibility_note") or "").strip()
                    if discovery_visibility_note:
                        st.info(discovery_visibility_note)
                    st.info("No non-ranked breakout candidates available.")
                else:
                    breakout_candidates_display = reorder_columns(
                        pipeline_breakout_candidates_df,
                        [
                            "symbol_id",
                            "breakout_positive",
                            "breakout_state",
                            "breakout_score",
                            "stage2_score",
                            "rel_strength_score",
                        ],
                    )
                    breakout_candidates_display = _with_symbol_hyperlink(breakout_candidates_display, symbol_col="symbol_id")
                    st.dataframe(breakout_candidates_display.head(10), use_container_width=True, hide_index=True)

                warnings = dashboard_payload.get("warnings", [])
                if warnings:
                    st.markdown("**Warnings**")
                    for warning in warnings:
                        st.warning(warning)

                with st.expander("🩺 Self Check", expanded=False):
                    checks_df = pd.DataFrame(dashboard_health.get("checks", []))
                    if not checks_df.empty:
                        st.dataframe(checks_df, use_container_width=True, hide_index=True)
                    else:
                        st.info("No health checks available.")

                with st.expander("🛡️ Data Trust", expanded=False):
                    provider_mix = latest_provider_stats.get("counts", {})
                    trust_summary_cols = st.columns(3)
                    with trust_summary_cols[0]:
                        st.caption("Latest Provider Mix")
                        st.json(provider_mix or {})
                    with trust_summary_cols[1]:
                        st.caption("Quarantined Dates")
                        st.write(data_trust_snapshot.get("active_quarantined_dates", []) or ["None"])
                    with trust_summary_cols[2]:
                        st.caption("Latest Repair Batch")
                        st.json(latest_repair_run or {})
                    st.caption(f"Payload: `{dashboard_payload.get('_artifact_path', 'n/a')}`")

    if selected_workspace == "📋 Overview":
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
        source_options = ["Operational (live)", "Research (static)"]
        controls = st.columns([1.6, 1.2, 1, 1, 1], gap="small")
        with controls[0]:
            breadth_source_label = st.selectbox(
                "Breadth Source",
                options=source_options,
                index=0,
                key="breadth_source",
            )
        breadth_source_key = "operational" if breadth_source_label.startswith("Operational") else "research"
        min_year, max_year = get_breadth_year_bounds(data_source=breadth_source_key)
        year_options = list(range(min_year, max_year + 1))
        default_from_year = min_year if min_year in year_options else year_options[0]
        with controls[1]:
            from_year = st.selectbox(
                "From Year",
                options=year_options,
                index=year_options.index(default_from_year),
                key="breadth_from_year",
            )
        with controls[2]:
            show_20 = st.checkbox("20 SMA", value=True, key="breadth_show_20")
        with controls[3]:
            show_50 = st.checkbox("50 SMA", value=True, key="breadth_show_50")
        with controls[4]:
            show_200 = st.checkbox("200 SMA", value=True, key="breadth_show_200")

        try:
            breadth_df = load_breadth_history(
                start_date=f"{from_year}-01-01",
                data_source=breadth_source_key,
            )
            if breadth_df.empty:
                st.info("No breadth history available.")
            else:
                latest_breadth = breadth_df.iloc[-1]
                m_cols = st.columns(4)
                enabled_count = sum([show_20, show_50, show_200])
                with m_cols[0]:
                    st.metric("From Year", str(from_year), f"{enabled_count} indicators")
                with m_cols[1]:
                    st.metric("Eligible 200 SMA Universe", f"{int(latest_breadth['eligible_200']):,}")
                with m_cols[2]:
                    st.metric("% > 20 SMA", f"{latest_breadth['pct_above_20']:.2f}%")
                with m_cols[3]:
                    st.metric("% > 50 SMA", f"{latest_breadth['pct_above_50']:.2f}%")

                if show_200:
                    st.caption(f"% > 200 SMA: {latest_breadth['pct_above_200']:.2f}%")

                fig_breadth_overview = go.Figure()
                if show_20:
                    fig_breadth_overview.add_trace(
                        go.Scatter(
                            x=breadth_df["trade_date"],
                            y=breadth_df["pct_above_20"],
                            mode="lines",
                            name="% Above 20 SMA",
                            line=dict(color="#2563eb", width=1.6),
                        )
                    )
                if show_50:
                    fig_breadth_overview.add_trace(
                        go.Scatter(
                            x=breadth_df["trade_date"],
                            y=breadth_df["pct_above_50"],
                            mode="lines",
                            name="% Above 50 SMA",
                            line=dict(color="#14b8a6", width=1.6),
                        )
                    )
                if show_200:
                    fig_breadth_overview.add_trace(
                        go.Scatter(
                            x=breadth_df["trade_date"],
                            y=breadth_df["pct_above_200"],
                            mode="lines",
                            name="% Above 200 SMA",
                            line=dict(color="#dc2626", width=1.8),
                        )
                    )

                if not any([show_20, show_50, show_200]):
                    st.info("Enable at least one indicator (20/50/200) to render the chart.")
                else:
                    fig_breadth_overview.add_hline(y=60, line_dash="dash", line_color="#16a34a", opacity=0.5)
                    fig_breadth_overview.add_hline(y=40, line_dash="dash", line_color="#ea580c", opacity=0.5)
                    fig_breadth_overview.update_layout(
                        height=360,
                        margin=dict(l=20, r=20, t=20, b=20),
                        yaxis_title="% of Stocks Above SMA",
                        xaxis_title="Date",
                        legend_title="Breadth",
                    )
                    st.plotly_chart(fig_breadth_overview, use_container_width=True)
                st.caption(
                    "Source: Operational (live rolling store)." if breadth_source_key == "operational"
                    else "Source: Research (static store, typically up to prior year end)."
                )
        except Exception as e:
            st.warning(f"Could not render long-term breadth chart: {e}")

        st.divider()
        st.subheader("🧭 Sector Rotation Heatmap")
        render_sector_rotation_heatmap(
            latest_rank_frames.get("sector_dashboard", pd.DataFrame()),
            stock_scan_df=latest_rank_frames.get("stock_scan", pd.DataFrame()),
            chart_key="overview-sector-rotation-heatmap",
        )

        st.divider()

        col_left, col_right = st.columns([1, 1], gap="small")

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
                            height=380,
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
                    height=380,
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

    if selected_workspace == "🏆 Ranking":
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
            score_col = "composite_score"
            stage2_has_uptrend = "is_stage2_uptrend" in rank_df.columns
            stage2_has_score = "stage2_score" in rank_df.columns
            stage2_has_label = "stage2_label" in rank_df.columns
            stage2_has_bonus = "stage2_score_bonus" in rank_df.columns

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
            stage2_gate_unavailable = False
            if st.session_state.get("stage2_filter_enabled", False):
                if stage2_has_uptrend:
                    uptrend_series = rank_df["is_stage2_uptrend"]
                    if pd.api.types.is_bool_dtype(uptrend_series):
                        gate = uptrend_series.fillna(False)
                    elif pd.api.types.is_numeric_dtype(uptrend_series):
                        gate = pd.to_numeric(uptrend_series, errors="coerce").fillna(0).astype(float) > 0
                    else:
                        gate = (
                            uptrend_series.astype(str)
                            .str.strip()
                            .str.lower()
                            .isin({"1", "true", "t", "yes", "y", "uptrend"})
                        )
                    rank_df = rank_df[gate]
                else:
                    stage2_gate_unavailable = True

                if stage2_has_score:
                    stage2_scores = pd.to_numeric(rank_df["stage2_score"], errors="coerce")
                    rank_df = rank_df[stage2_scores >= float(st.session_state.get("stage2_min_score", 70.0))]
                else:
                    stage2_gate_unavailable = True

            rank_df = rank_df.reset_index(drop=True)
            top_df = rank_df.head(top_n_default).copy()
            top_df.index = range(1, len(top_df) + 1)
            top_df.index.name = "Rank"
            history_df = load_rank_history_for_symbols(
                PIPELINE_RUNS_DIR,
                tuple(top_df["symbol_id"].astype(str).tolist()),
                max_runs=50,
            )
            sparkline_payload = build_rank_sparkline_payload(history_df, max_points=12)
            top_df = enrich_ranked_table_with_context(
                top_df,
                weights=weights_dict,
                sparkline_payload=sparkline_payload,
                symbol_col="symbol_id",
            )

            summary_cols = st.columns(5)
            score_series = pd.to_numeric(top_df.get(score_col), errors="coerce")
            delivery_series = (
                pd.to_numeric(top_df["delivery_pct_score"], errors="coerce")
                if "delivery_pct_score" in top_df.columns
                else pd.Series(dtype=float)
            )
            breakout_frame = latest_rank_frames.get("breakout_scan", pd.DataFrame())
            if breakout_frame.empty and dashboard_payload:
                breakout_frame = pd.DataFrame(dashboard_payload.get("breakout_scan", []))
            summary_cols[0].metric("Rows Shown", f"{len(top_df):,}")
            summary_cols[1].metric(
                "Median Score",
                "—" if score_series.empty else f"{score_series.median():.1f}",
            )
            improving_count = int((top_df.get("Rank Trend", pd.Series(dtype=str)) == "Improving").sum())
            summary_cols[2].metric("Improving", str(improving_count))
            summary_cols[3].metric(
                "Avg Delivery",
                "—" if delivery_series.empty else f"{delivery_series.mean():.1f}",
            )
            breakout_count = len(breakout_frame)
            summary_cols[4].metric("Breakout Candidates", str(breakout_count))
            if st.session_state.get("stage2_filter_enabled", False) and stage2_gate_unavailable:
                st.warning("Stage-2 filter requested, but Stage-2 columns are missing in this ranking dataset.")
            elif st.session_state.get("stage2_filter_enabled", False):
                st.caption(
                    f"Stage-2 filter active: uptrend only, min score {float(st.session_state.get('stage2_min_score', 70.0)):.0f}"
                )

            stage2_summary_df = pd.DataFrame()
            if stage2_has_label:
                label_counts = (
                    top_df["stage2_label"]
                    .fillna("unknown")
                    .astype(str)
                    .str.strip()
                    .replace("", "unknown")
                    .str.lower()
                    .value_counts()
                )
                stage2_summary_df = pd.DataFrame(
                    [{"metric": "stage2_label", "name": str(label), "value": int(count)} for label, count in label_counts.items()]
                )

            table_col, insight_col = st.columns([1.75, 1.05], gap="small")
            with table_col:
                cols_show = [
                    "symbol_id",
                    "close",
                    score_col,
                    "stage2_score",
                    "stage2_score_bonus",
                    "is_stage2_uptrend",
                    "stage2_label",
                    "Top Driver",
                    "Rank Trend",
                    "Δ Rank",
                    "Rank History",
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
                    "stage2_score": "S2 Score",
                    "stage2_score_bonus": "S2 Bonus",
                    "is_stage2_uptrend": "S2 Uptrend",
                    "stage2_label": "S2 Label",
                }
                display_df = top_df[cols_show].rename(columns=rename_cols)
                display_df = _with_symbol_hyperlink(display_df, symbol_col="Symbol")
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=False,
                    height=560,
                    column_config={
                        "Symbol": st.column_config.LinkColumn(
                            "Symbol",
                            help="Open symbol in TradingView.",
                            display_text=r".*symbol=NSE(?:%3A|:)([^&]+).*",
                        ),
                        "Rank History": st.column_config.LineChartColumn(
                            "Rank History",
                            help="Recent rank trajectory across pipeline runs (lower is better).",
                        ),
                        "Δ Rank": st.column_config.NumberColumn(
                            "Δ Rank",
                            help="Positive means improving rank vs earliest sparkline point.",
                        ),
                    },
                )
                if stage2_has_uptrend or stage2_has_score or stage2_has_label or stage2_has_bonus:
                    st.markdown("**Stage-2 Summary**")
                    s2_cols = st.columns(3)
                    if stage2_has_uptrend:
                        uptrend_count = int(
                            pd.Series(top_df["is_stage2_uptrend"])
                            .astype(str)
                            .str.strip()
                            .str.lower()
                            .isin({"1", "true", "t", "yes", "y", "uptrend"})
                            .sum()
                        )
                        s2_cols[0].metric("S2 Uptrend", f"{uptrend_count}/{len(top_df)}")
                    if stage2_has_score:
                        s2_score = pd.to_numeric(top_df["stage2_score"], errors="coerce")
                        s2_cols[1].metric("S2 Avg Score", "—" if s2_score.dropna().empty else f"{s2_score.mean():.1f}")
                        s2_cols[2].metric("S2 Max Score", "—" if s2_score.dropna().empty else f"{s2_score.max():.1f}")
                    elif stage2_has_bonus:
                        s2_bonus = pd.to_numeric(top_df["stage2_score_bonus"], errors="coerce")
                        s2_cols[1].metric("S2 Avg Bonus", "—" if s2_bonus.dropna().empty else f"{s2_bonus.mean():.1f}")
                        s2_cols[2].metric("S2 Max Bonus", "—" if s2_bonus.dropna().empty else f"{s2_bonus.max():.1f}")
                    if not stage2_summary_df.empty:
                        st.dataframe(stage2_summary_df, use_container_width=True, hide_index=True, height=180)
                else:
                    st.caption("Stage-2 fields are not present in this rank artifact yet (only base ranking factors are available).")

            with insight_col:
                ranked_symbols = top_df["symbol_id"].astype(str).tolist()
                options = [""] + ranked_symbols
                default_symbol = st.session_state.selected_symbol if st.session_state.selected_symbol in ranked_symbols else ""
                default_index = options.index(default_symbol) if default_symbol in options else 0
                selected = st.selectbox(
                    "Focus Symbol",
                    options=options,
                    index=default_index,
                    label_visibility="visible",
                )
                if selected:
                    st.session_state.selected_symbol = selected
                    selected_row = top_df[top_df["symbol_id"] == selected]
                    if not selected_row.empty:
                        row = selected_row.iloc[0]
                        st.metric(
                            "Focus Rank",
                            f"{int(row.name)}",
                            f"{row.get('Rank Trend', 'Flat')} | Δ {int(row.get('Δ Rank', 0)):+d}",
                        )
                        render_factor_attribution_widget(
                            row,
                            weights=weights_dict,
                            title=f"Factor Attribution — {selected}",
                            show_table=False,
                            chart_key=f"ranking_focus_factor_{selected}",
                        )
                        render_symbol_rank_history(
                            history_df,
                            selected,
                            chart_key=f"ranking_focus_rank_{selected}",
                        )
                else:
                    st.info("Select a symbol to inspect live factor attribution and rank path.")

                st.markdown("**Breakout Evidence (Top Signals)**")
                ranking_breakout_df = latest_rank_frames.get("breakout_scan", pd.DataFrame())
                ranking_pattern_df = latest_rank_frames.get("pattern_scan", pd.DataFrame())
                if ranking_breakout_df.empty and dashboard_payload:
                    ranking_breakout_df = pd.DataFrame(dashboard_payload.get("breakout_scan", []))
                if ranking_pattern_df.empty and dashboard_payload:
                    ranking_pattern_df = pd.DataFrame(dashboard_payload.get("pattern_scan", []))
                show_filtered_ranking = st.toggle(
                    "Show Filtered Rows (Ranking)",
                    value=False,
                    key="ranking_breakout_hide_filtered",
                )
                if (
                    not show_filtered_ranking
                    and not ranking_breakout_df.empty
                    and "breakout_state" in ranking_breakout_df.columns
                ):
                    ranking_breakout_df = ranking_breakout_df[
                        ~ranking_breakout_df["breakout_state"].astype(str).str.startswith("filtered_")
                    ].copy()
                if not ranking_breakout_df.empty:
                    ranking_breakout_display = reorder_columns(
                        ranking_breakout_df,
                        [
                            "symbol_id",
                            "taxonomy_family",
                            "breakout_score",
                            "breakout_rank",
                            "breakout_state",
                            "candidate_tier",
                            "symbol_trend_reasons",
                            "filter_reason",
                        ],
                    )
                    ranking_breakout_display = _with_symbol_hyperlink(
                        ranking_breakout_display,
                        symbol_col="symbol_id",
                    )
                    ranking_breakout_head = ranking_breakout_df.head(8).reset_index(drop=True)
                    ranking_breakout_display_head = ranking_breakout_display.head(8).reset_index(drop=True)
                    selected_ranking_breakout = render_selectable_grid(
                        ranking_breakout_display_head,
                        key="ranking_breakout_grid",
                        height=220,
                        column_config={
                            "symbol_id": st.column_config.LinkColumn(
                                "Symbol",
                                help="Open symbol in TradingView.",
                                display_text=r".*symbol=NSE(?:%3A|:)([^&]+).*",
                            )
                        },
                    )
                    if selected_ranking_breakout is not None:
                        selected_idx = int(selected_ranking_breakout.name)
                        if 0 <= selected_idx < len(ranking_breakout_head):
                            render_breakout_setup_detail(
                                ranking_breakout_head.iloc[selected_idx],
                                ranking_breakout_df,
                                signal_date=(dashboard_payload or {}).get("summary", {}).get("run_date"),
                                title="Ranking Breakout Setup",
                            )
                st.markdown("**Pattern Evidence (Top Signals)**")
                if not ranking_pattern_df.empty and not top_df.empty and "symbol_id" in ranking_pattern_df.columns:
                    ranking_pattern_df = ranking_pattern_df[
                        ranking_pattern_df["symbol_id"].astype(str).isin(set(top_df["symbol_id"].astype(str)))
                    ].copy()
                if not ranking_pattern_df.empty:
                    ranking_pattern_display = reorder_columns(
                        ranking_pattern_df,
                        [
                            "symbol_id",
                            "pattern_family",
                            "pattern_state",
                            "pattern_score",
                            "pattern_rank",
                            "setup_quality",
                            "signal_date",
                        ],
                    )
                    ranking_pattern_display = _with_symbol_hyperlink(
                        ranking_pattern_display,
                        symbol_col="symbol_id",
                    )
                    st.dataframe(
                        ranking_pattern_display.head(8),
                        use_container_width=True,
                        hide_index=True,
                        height=220,
                        column_config={
                            "symbol_id": st.column_config.LinkColumn(
                                "Symbol",
                                help="Open symbol in TradingView.",
                                display_text=r".*symbol=NSE(?:%3A|:)([^&]+).*",
                            )
                        },
                    )
                else:
                    st.info("No top-ranked pattern signals available.")

            st.markdown("**Universe Factor Contribution Map**")
            render_factor_attribution_widget(
                top_df,
                weights=weights_dict,
                title="Top-Ranked Factor Mix",
                max_symbols=min(16, len(top_df)),
                show_table=False,
                chart_key="ranking_universe_factor_map",
            )

        else:
            st.info(
                "No ranking data available. Click 'Refresh Rankings' in the sidebar."
            )

    if selected_workspace == "🧩 Patterns":
        _render_pattern_backtests_tab()

    if selected_workspace == "📈 Chart":
        symbol = st.session_state.selected_symbol

        if not symbol:
            st.info("Select a stock from the Ranking tab or search below.")
            symbol = (
                st.text_input("Or enter symbol manually (e.g. RELIANCE):", "")
                .strip()
                .upper()
            )

        if symbol:
            pattern_frame = latest_rank_frames.get("pattern_scan", pd.DataFrame())
            if pattern_frame.empty and dashboard_payload:
                pattern_frame = pd.DataFrame(dashboard_payload.get("pattern_scan", []))
            col1, col2 = st.columns([1, 3])
            symbol_trust_df = load_symbol_trust_snapshot(PROJECT_ROOT, [symbol])
            symbol_trust = symbol_trust_df.iloc[0].to_dict() if not symbol_trust_df.empty else {}
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
                    st.metric("Provider", str(symbol_trust.get("provider", "n/a")))
                    st.metric("Trust", str(symbol_trust.get("validation_status", "n/a")))
                    if bool(symbol_trust.get("is_quarantined")):
                        st.error("This symbol currently has active quarantine rows in operational OHLC.")

                    features = load_features(symbol)
                    if not features:
                        st.warning(
                            "No feature data found. Run feature computation first."
                        )
                    pattern_option_map = _build_pattern_overlay_option_map(pattern_frame, symbol)
                    selected_pattern_label = st.selectbox(
                        "Pattern Overlay",
                        options=list(pattern_option_map.keys()),
                        index=0,
                        key=f"chart_pattern_overlay_{symbol}",
                    )
                else:
                    st.warning("No OHLCV data for this symbol.")

            with col2:
                if not ohlcv.empty:
                    features = load_features(symbol)
                    pattern_row = None
                    selected_signal_id = pattern_option_map.get(selected_pattern_label) if "pattern_option_map" in locals() else None
                    if selected_signal_id and pattern_frame is not None and not pattern_frame.empty and "signal_id" in pattern_frame.columns:
                        matched = pattern_frame[pattern_frame["signal_id"].astype(str) == str(selected_signal_id)]
                        if not matched.empty:
                            pattern_row = matched.iloc[0]
                    fig = plot_candlestick_with_features(ohlcv, features, symbol, pattern_row=pattern_row)
                    st.plotly_chart(fig, use_container_width=True)
                    if pattern_row is not None:
                        st.caption(
                            f"Pattern overlay: {str(pattern_row.get('pattern_family') or '').replace('_', ' ')} | "
                            f"{str(pattern_row.get('pattern_state') or '').upper()} | "
                            f"score {float(pd.to_numeric(pd.Series([pattern_row.get('pattern_score')]), errors='coerce').iloc[0] or 0.0):.1f}"
                        )
                    st.markdown("**Rank History Sparkline**")
                    symbol_history_df = load_rank_history_for_symbols(
                        PIPELINE_RUNS_DIR,
                        (symbol,),
                        max_runs=50,
                    )
                    render_symbol_rank_history(
                        symbol_history_df,
                        symbol,
                        chart_key=f"chart_rank_history_{symbol}",
                    )

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
                            render_factor_attribution_widget(
                                row,
                                weights=build_dashboard_weights(),
                                title=f"Factor Attribution — {symbol}",
                                show_table=True,
                                chart_key=f"chart_factor_attr_{symbol}",
                            )
                else:
                    st.info(f"No data for {symbol}. Try another symbol.")

    if selected_workspace == "🧠 ML":
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
                metadata_summary.loc[:, "Value"] = metadata_summary["Value"].astype(str)
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
                        height=420,
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
                "No shadow-monitor predictions recorded yet. Run `python -m ai_trading_system.research.shadow_monitor` "
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
            display_overlay_df.loc[:, "prediction_date"] = display_overlay_df["prediction_date"].dt.date.astype(str)
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
                        weekly_display.loc[:, "period_start"] = weekly_display["period_start"].dt.date.astype(str)
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
                        monthly_display.loc[:, "period_start"] = monthly_display["period_start"].dt.date.astype(str)
                        st.dataframe(monthly_display, use_container_width=True, hide_index=True, height=240)

    if selected_workspace == "💼 Portfolio":
        _render_portfolio_workspace(
            rank_df=st.session_state.rank_df,
            latest_rank_frames=latest_rank_frames,
        )


if __name__ == "__main__":
    main()
