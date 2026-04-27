"""Query/data-access helpers for research and portfolio dashboard data."""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

import duckdb
import pandas as pd
from ai_trading_system.analytics.data_trust import load_data_trust_summary, load_symbol_trust_state
from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.domains.execution.store import ExecutionStore


STAGE_NAMES = ("ingest", "features", "rank", "execute", "publish")


def _cache_data(*_args: object, **_kwargs: object):
    """No-op cache decorator retained for call-site compatibility."""
    def _decorator(func):
        return func

    return _decorator


def open_position_trade_ref(symbol_id: str, exchange: str = "NSE") -> str:
    """Stable journal reference for an active position."""
    return f"open:{str(exchange or 'NSE').upper()}:{str(symbol_id or '').upper()}"


def closed_trade_ref(fill_id: str) -> str:
    """Stable journal reference for a realized trade row."""
    return f"closed:{str(fill_id or '').strip()}"


def _safe_list_trade_notes(store: ExecutionStore) -> list[dict]:
    """Compatibility wrapper for older ExecutionStore instances."""
    method = getattr(store, "list_trade_notes", None)
    if callable(method):
        try:
            rows = method()
            if isinstance(rows, list):
                return rows
        except Exception:
            return []
    return []


def _safe_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _parse_run_id_from_path(path: Path) -> str:
    parts = path.parts
    if len(parts) < 5:
        return ""
    try:
        return parts[-4]
    except Exception:
        return ""


def _run_date_from_run_id(run_id: str) -> str | None:
    match = re.match(r"^(?:pipeline|ui)-(\d{4}-\d{2}-\d{2})-", str(run_id))
    return match.group(1) if match else None


def _normalize_lookup_key(value: object) -> str:
    return str(value or "").strip().lower()


def _get_pipeline_runs_dir(project_root: str) -> Path:
    paths = get_domain_paths(project_root, "operational")
    return Path(paths.pipeline_runs_dir)


def _build_live_sector_dashboard_frame(project_root: str) -> pd.DataFrame:
    """Recompute the sector dashboard from the latest operational sector RS features."""
    from ai_trading_system.domains.ranking.sector_dashboard import (
        build_dashboard,
        compute_sector_momentum,
        load_sector_rs,
    )

    sector_rs = load_sector_rs()
    if sector_rs.empty:
        return pd.DataFrame()
    return build_dashboard(
        sector_rs,
        compute_sector_momentum(sector_rs, days=20),
    ).reset_index()


def _should_refresh_sector_dashboard(
    project_root: str,
    rank_dir: Path | None,
    sector_dashboard_path: Path | None,
) -> bool:
    """Return True when live sector features are newer than the persisted rank artifact."""
    paths = get_domain_paths(project_root, "operational")
    sector_rs_path = Path(paths.feature_store_dir) / "all_symbols" / "sector_rs.parquet"
    if not sector_rs_path.exists():
        return False
    if sector_dashboard_path is None or not sector_dashboard_path.exists() or rank_dir is None:
        return True
    try:
        return sector_rs_path.stat().st_mtime > sector_dashboard_path.stat().st_mtime
    except OSError:
        return False


def _load_latest_payload_path(project_root: str) -> Path | None:
    runs_dir = _get_pipeline_runs_dir(project_root)
    if not runs_dir.exists():
        return None

    candidates = sorted(
        runs_dir.glob("*/rank/attempt_*/dashboard_payload.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return None

    control_plane_db = Path(project_root) / "data" / "control_plane.duckdb"
    run_metadata: dict[str, dict] = {}
    if control_plane_db.exists():
        conn = duckdb.connect(str(control_plane_db), read_only=True)
        try:
            rows = conn.execute("SELECT run_id, metadata_json FROM pipeline_run").fetchall()
            for run_id, metadata_json in rows:
                try:
                    run_metadata[str(run_id)] = json.loads(metadata_json) if metadata_json else {}
                except Exception:
                    run_metadata[str(run_id)] = {}
        finally:
            conn.close()

    def _is_live_operational_payload(path: Path) -> bool:
        run_id = _parse_run_id_from_path(path)
        metadata = run_metadata.get(run_id, {})
        params = metadata.get("params", {}) if isinstance(metadata, dict) else {}
        if params.get("smoke") is True:
            return False
        if params.get("canary") is True:
            return False
        return True

    for candidate in candidates:
        if _is_live_operational_payload(candidate):
            return candidate
    return candidates[0]


@_cache_data(show_spinner=False, ttl=60 * 3)
def load_latest_rank_frames(project_root: str) -> Dict[str, pd.DataFrame]:
    """Load latest rank-stage CSV artifacts without importing ai_trading_system.ui.execution_api.services package."""
    payload_path = _load_latest_payload_path(project_root)
    rank_dir: Path | None = payload_path.parent if payload_path is not None else None

    frame_names = {
        "ranked_signals": "ranked_signals.csv",
        "breakout_scan": "breakout_scan.csv",
        "pattern_scan": "pattern_scan.csv",
        "stock_scan": "stock_scan.csv",
        "sector_dashboard": "sector_dashboard.csv",
    }
    if rank_dir is None:
        runs_dir = _get_pipeline_runs_dir(project_root)
        ranked_candidates = sorted(
            runs_dir.glob("*/rank/attempt_*/ranked_signals.csv"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        rank_dir = ranked_candidates[0].parent if ranked_candidates else None

    frames: Dict[str, pd.DataFrame] = {}
    if rank_dir is None:
        return {key: pd.DataFrame() for key in frame_names}
    for key, filename in frame_names.items():
        path = rank_dir / filename
        if not path.exists():
            frames[key] = pd.DataFrame()
            continue
        try:
            frames[key] = pd.read_csv(path)
        except Exception:
            frames[key] = pd.DataFrame()

    sector_dashboard_path = rank_dir / frame_names["sector_dashboard"]
    if _should_refresh_sector_dashboard(project_root, rank_dir, sector_dashboard_path):
        try:
            frames["sector_dashboard"] = _build_live_sector_dashboard_frame(project_root)
        except Exception:
            pass
    return frames


@_cache_data(show_spinner=False, ttl=60 * 5)
def load_recent_rank_paths(pipeline_runs_dir: str, max_runs: int = 40) -> List[str]:
    """Return recent ranked_signals artifact paths (one per run)."""
    runs_dir = Path(pipeline_runs_dir)
    if not runs_dir.exists():
        return []

    candidates = sorted(
        runs_dir.glob("*/rank/attempt_*/ranked_signals.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return []

    selected: list[str] = []
    seen_runs: set[str] = set()
    for path in candidates:
        run_id = _parse_run_id_from_path(path)
        if not run_id or run_id in seen_runs:
            continue
        selected.append(path.as_posix())
        seen_runs.add(run_id)
        if len(selected) >= max_runs:
            break
    return selected


@_cache_data(show_spinner=False, ttl=60 * 5)
def load_recent_sector_paths(pipeline_runs_dir: str, max_runs: int = 40) -> List[str]:
    """Return recent sector_dashboard artifact paths (one per run)."""
    runs_dir = Path(pipeline_runs_dir)
    if not runs_dir.exists():
        return []

    candidates = sorted(
        runs_dir.glob("*/rank/attempt_*/sector_dashboard.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return []

    selected: list[str] = []
    seen_runs: set[str] = set()
    for path in candidates:
        run_id = _parse_run_id_from_path(path)
        if not run_id or run_id in seen_runs:
            continue
        selected.append(path.as_posix())
        seen_runs.add(run_id)
        if len(selected) >= max_runs:
            break
    return selected


@_cache_data(show_spinner=False, ttl=60 * 5)
def list_pattern_backtest_bundles(reports_dir: str, max_bundles: int = 20) -> pd.DataFrame:
    """List recent pattern-backtest research bundles."""
    base = Path(reports_dir) / "pattern_backtests"
    if not base.exists():
        return pd.DataFrame(
            columns=[
                "bundle_name",
                "bundle_dir",
                "generated_at",
                "from_date",
                "to_date",
                "event_count",
                "trade_count",
                "chart_count",
            ]
        )

    rows: list[dict[str, Any]] = []
    candidates = sorted(
        base.glob("*/summary.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in candidates[:max_bundles]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        artifact_map = payload.get("artifacts", {}) if isinstance(payload, dict) else {}
        event_path = Path(str(artifact_map.get("pattern_events", "")))
        trade_path = Path(str(artifact_map.get("pattern_trades", "")))
        event_count = 0
        trade_count = 0
        try:
            if event_path.exists():
                event_count = max(0, len(pd.read_csv(event_path)))
            if trade_path.exists():
                trade_count = max(0, len(pd.read_csv(trade_path)))
        except Exception:
            pass
        rows.append(
            {
                "bundle_name": path.parent.name,
                "bundle_dir": str(path.parent),
                "generated_at": payload.get("generated_at"),
                "from_date": payload.get("from_date"),
                "to_date": payload.get("to_date"),
                "event_count": event_count,
                "trade_count": trade_count,
                "chart_count": len(artifact_map.get("charts", []) or []),
            }
        )
    return pd.DataFrame(rows)


@_cache_data(show_spinner=False, ttl=60 * 5)
def load_pattern_backtest_bundle(bundle_dir: str) -> Dict[str, Any]:
    """Load one pattern-backtest research bundle and its artifacts."""
    base = Path(bundle_dir)
    summary_json = base / "summary.json"
    if not summary_json.exists():
        return {
            "summary_json": {},
            "summary_df": pd.DataFrame(),
            "events_df": pd.DataFrame(),
            "trades_df": pd.DataFrame(),
            "yearly_df": pd.DataFrame(),
            "chart_paths": [],
        }

    payload = json.loads(summary_json.read_text(encoding="utf-8"))
    artifact_map = payload.get("artifacts", {}) if isinstance(payload, dict) else {}

    def _read_csv(path_value: Any) -> pd.DataFrame:
        path = Path(str(path_value or ""))
        if not path.exists():
            return pd.DataFrame()
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()

    return {
        "summary_json": payload,
        "summary_df": _read_csv(artifact_map.get("summary_csv")),
        "events_df": _read_csv(artifact_map.get("pattern_events")),
        "trades_df": _read_csv(artifact_map.get("pattern_trades")),
        "yearly_df": _read_csv(artifact_map.get("yearly_breakdown_csv")),
        "chart_paths": [path for path in (artifact_map.get("charts", []) or []) if Path(path).exists()],
    }


@_cache_data(show_spinner=False, ttl=60 * 5)
def load_rank_history_for_symbols(
    pipeline_runs_dir: str,
    symbols: Iterable[str],
    max_runs: int = 40,
) -> pd.DataFrame:
    """Load rank-position history for the requested symbols across recent rank runs."""
    symbol_list = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    if not symbol_list:
        return pd.DataFrame(
            columns=["run_id", "symbol_id", "composite_score", "rank_position", "run_order", "run_date"]
        )

    path_strings = load_recent_rank_paths(pipeline_runs_dir, max_runs=max_runs)
    if not path_strings:
        return pd.DataFrame(
            columns=["run_id", "symbol_id", "composite_score", "rank_position", "run_order", "run_date"]
        )

    path_sql = "[" + ", ".join(f"'{_safe_sql_literal(path)}'" for path in path_strings) + "]"
    symbols_sql = "(" + ", ".join(f"'{_safe_sql_literal(symbol)}'" for symbol in symbol_list) + ")"
    query = f"""
        WITH raw AS (
            SELECT
                filename,
                symbol_id,
                composite_score
            FROM read_csv_auto({path_sql}, filename = true)
            WHERE symbol_id IN {symbols_sql}
        ),
        ranked AS (
            SELECT
                regexp_extract(filename, '/pipeline_runs/([^/]+)/rank/', 1) AS run_id,
                symbol_id,
                composite_score,
                ROW_NUMBER() OVER (
                    PARTITION BY regexp_extract(filename, '/pipeline_runs/([^/]+)/rank/', 1)
                    ORDER BY composite_score DESC NULLS LAST
                ) AS rank_position
            FROM raw
        )
        SELECT run_id, symbol_id, composite_score, rank_position
        FROM ranked
        WHERE run_id IS NOT NULL AND run_id <> ''
    """

    conn = duckdb.connect()
    try:
        history_df = conn.execute(query).fetchdf()
    finally:
        conn.close()

    if history_df.empty:
        return pd.DataFrame(
            columns=["run_id", "symbol_id", "composite_score", "rank_position", "run_order", "run_date"]
        )

    run_id_to_order: dict[str, int] = {}
    for idx, path_string in enumerate(reversed(path_strings)):
        run_id = _parse_run_id_from_path(Path(path_string))
        if run_id and run_id not in run_id_to_order:
            run_id_to_order[run_id] = idx

    history_df.loc[:, "symbol_id"] = history_df["symbol_id"].astype(str).str.upper()
    history_df.loc[:, "run_order"] = history_df["run_id"].map(run_id_to_order).fillna(-1).astype(int)
    history_df.loc[:, "run_date"] = history_df["run_id"].map(_run_date_from_run_id)
    history_df.loc[:, "run_date"] = pd.to_datetime(history_df["run_date"], errors="coerce")
    history_df = history_df.sort_values(["run_order", "run_id", "rank_position"]).reset_index(drop=True).copy()
    return history_df


@_cache_data(show_spinner=False, ttl=60 * 5)
def load_sector_history_for_sectors(
    pipeline_runs_dir: str,
    sectors: Iterable[str],
    max_runs: int = 40,
) -> pd.DataFrame:
    """Load sector RS history across recent rank runs for the requested sectors."""
    sector_list = [str(sector).strip() for sector in sectors if str(sector).strip()]
    if not sector_list:
        return pd.DataFrame(
            columns=["run_id", "sector_name", "rs_value", "momentum", "rank_position", "run_order", "run_date"]
        )

    target_keys = {_normalize_lookup_key(sector) for sector in sector_list}
    path_strings = load_recent_sector_paths(pipeline_runs_dir, max_runs=max_runs)
    if not path_strings:
        return pd.DataFrame(
            columns=["run_id", "sector_name", "rs_value", "momentum", "rank_position", "run_order", "run_date"]
        )

    run_id_to_order: dict[str, int] = {}
    for idx, path_string in enumerate(reversed(path_strings)):
        run_id = _parse_run_id_from_path(Path(path_string))
        if run_id and run_id not in run_id_to_order:
            run_id_to_order[run_id] = idx

    rows: list[dict[str, object]] = []
    for path_string in path_strings:
        path = Path(path_string)
        run_id = _parse_run_id_from_path(path)
        if not run_id or not path.exists():
            continue
        try:
            frame = pd.read_csv(path)
        except Exception:
            continue
        if frame.empty:
            continue

        sector_col = "Sector" if "Sector" in frame.columns else None
        if sector_col is None:
            continue
        rs_col = "RS" if "RS" in frame.columns else None
        momentum_col = "Momentum" if "Momentum" in frame.columns else None
        rank_col = "RS_rank" if "RS_rank" in frame.columns else None

        working = frame.copy()
        working.loc[:, "sector_name"] = working[sector_col].astype(str).str.strip()
        working = working.loc[working["sector_name"].map(_normalize_lookup_key).isin(target_keys)].copy()
        if working.empty:
            continue

        if rank_col is None:
            sort_col = rs_col if rs_col else sector_col
            ascending = False if rs_col else True
            working = working.sort_values(sort_col, ascending=ascending, na_position="last").reset_index(drop=True).copy()
            working.loc[:, "rank_position"] = working.index + 1
        else:
            working.loc[:, "rank_position"] = pd.to_numeric(working[rank_col], errors="coerce")

        for _, row in working.iterrows():
            rows.append(
                {
                    "run_id": run_id,
                    "sector_name": str(row.get("sector_name", "")).strip(),
                    "rs_value": pd.to_numeric(row.get(rs_col), errors="coerce") if rs_col else pd.NA,
                    "momentum": pd.to_numeric(row.get(momentum_col), errors="coerce") if momentum_col else pd.NA,
                    "rank_position": pd.to_numeric(row.get("rank_position"), errors="coerce"),
                    "run_order": run_id_to_order.get(run_id, -1),
                    "run_date": pd.to_datetime(_run_date_from_run_id(run_id), errors="coerce"),
                }
            )

    history_df = pd.DataFrame(rows)
    if history_df.empty:
        return pd.DataFrame(
            columns=["run_id", "sector_name", "rs_value", "momentum", "rank_position", "run_order", "run_date"]
        )
    history_df.loc[:, "sector_name"] = history_df["sector_name"].astype(str)
    history_df = history_df.sort_values(["sector_name", "run_order", "run_id"]).reset_index(drop=True).copy()
    dated_history = history_df[history_df["run_date"].notna()].copy()
    undated_history = history_df[history_df["run_date"].isna()].copy()
    if not dated_history.empty:
        dated_history = dated_history.drop_duplicates(
            subset=["sector_name", "run_date"],
            keep="last",
        )
    history_df = pd.concat([dated_history, undated_history], ignore_index=True)
    history_df = history_df.sort_values(["run_order", "run_id", "sector_name"]).reset_index(drop=True)
    return history_df


@_cache_data(show_spinner=False, ttl=60 * 5)
def load_drilldown_history_for_symbols(
    pipeline_runs_dir: str,
    symbols: Iterable[str],
    max_runs: int = 40,
) -> pd.DataFrame:
    """Aggregate recent rank history into drilldown-level trends for a symbol basket."""
    history_df = load_rank_history_for_symbols(
        pipeline_runs_dir,
        symbols,
        max_runs=max_runs,
    )
    if history_df.empty:
        return pd.DataFrame(
            columns=[
                "run_id",
                "run_order",
                "run_date",
                "symbol_count",
                "median_score",
                "top_score",
                "best_rank",
                "avg_rank",
            ]
        )

    aggregated = (
        history_df.groupby(["run_id", "run_order", "run_date"], dropna=False)
        .agg(
            symbol_count=("symbol_id", "nunique"),
            median_score=("composite_score", "median"),
            top_score=("composite_score", "max"),
            best_rank=("rank_position", "min"),
            avg_rank=("rank_position", "mean"),
        )
        .reset_index()
        .sort_values(["run_order", "run_id"])
        .reset_index(drop=True)
    )
    return aggregated


@_cache_data(show_spinner=False, ttl=60 * 5)
def load_ops_health_snapshot(
    project_root: str,
    stale_threshold_hours: dict[str, float] | None = None,
) -> Dict[str, object]:
    """Read control-plane stage freshness + DQ summary for top-of-page ribbon."""
    thresholds = stale_threshold_hours or {
        "ingest": 36.0,
        "features": 36.0,
        "rank": 24.0,
        "execute": 24.0,
        "publish": 48.0,
    }
    db_path = Path(project_root) / "data" / "control_plane.duckdb"
    if not db_path.exists():
        return {"available": False, "error": "control_plane.duckdb missing"}

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        stage_rows = conn.execute(
            """
            WITH latest AS (
                SELECT
                    stage_name,
                    run_id,
                    started_at,
                    ended_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY stage_name
                        ORDER BY COALESCE(ended_at, started_at) DESC
                    ) AS rn
                FROM pipeline_stage_run
                WHERE status = 'completed'
                  AND stage_name IN ('ingest', 'features', 'rank', 'execute', 'publish')
            )
            SELECT stage_name, run_id, started_at, ended_at
            FROM latest
            WHERE rn = 1
            """
        ).fetchall()
    finally:
        conn.close()

    now_utc = datetime.utcnow()
    stages: dict[str, dict[str, object]] = {}
    stale_stages: list[str] = []
    for stage_name in STAGE_NAMES:
        stages[stage_name] = {
            "stage_name": stage_name,
            "run_id": None,
            "ended_at": None,
            "age_hours": None,
            "stale": True,
        }

    for stage_name, run_id, started_at, ended_at in stage_rows:
        end_ts = ended_at or started_at
        if end_ts is None:
            continue
        age_hours = max((now_utc - end_ts).total_seconds() / 3600.0, 0.0)
        stale = age_hours > float(thresholds.get(stage_name, 24.0))
        stages[stage_name] = {
            "stage_name": stage_name,
            "run_id": run_id,
            "ended_at": end_ts,
            "age_hours": round(age_hours, 2),
            "stale": stale,
        }
        if stale:
            stale_stages.append(stage_name)

    latest_rank_run = stages.get("rank", {}).get("run_id")
    dq_summary = {"run_id": latest_rank_run, "failed_by_severity": {}, "total_failed": 0}
    if latest_rank_run:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            dq_rows = conn.execute(
                """
                SELECT severity, status, COUNT(*) AS cnt
                FROM dq_result
                WHERE run_id = ?
                GROUP BY severity, status
                """,
                [latest_rank_run],
            ).fetchall()
        finally:
            conn.close()

        failed_by_severity: dict[str, int] = {}
        total_failed = 0
        for severity, status, cnt in dq_rows:
            if status != "failed":
                continue
            failed_by_severity[str(severity)] = int(cnt)
            total_failed += int(cnt)
        dq_summary = {
            "run_id": latest_rank_run,
            "failed_by_severity": failed_by_severity,
            "total_failed": total_failed,
        }

    return {
        "available": True,
        "stages": stages,
        "stale_stages": stale_stages,
        "dq_summary": dq_summary,
        "generated_at": now_utc.isoformat(),
    }


@_cache_data(show_spinner=False, ttl=60)
def load_data_trust_snapshot(project_root: str) -> Dict[str, object]:
    """Load current operational data trust summary plus latest repair metadata."""
    paths = get_domain_paths(project_root, "operational")
    summary = load_data_trust_summary(paths.ohlcv_db_path)
    registry = RegistryStore(project_root)
    summary["latest_repair_run"] = registry.get_latest_data_repair_run("NSE")
    return summary


@_cache_data(show_spinner=False, ttl=60)
def load_symbol_trust_snapshot(project_root: str, symbols: Iterable[str]) -> pd.DataFrame:
    """Load latest trust state for a set of symbols."""
    paths = get_domain_paths(project_root, "operational")
    return load_symbol_trust_state(paths.ohlcv_db_path, symbols)


@_cache_data(show_spinner=False, ttl=60)
def load_trade_report(project_root: str) -> Dict[str, object]:
    """Build a paper-trading P&L snapshot for the unified dashboard."""
    store = ExecutionStore(project_root)
    fills = pd.DataFrame(store.list_fills())
    if fills.empty:
        return {
            "summary": {
                "open_positions": 0,
                "closed_trade_count": 0,
                "win_rate": 0.0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "total_pnl": 0.0,
            },
            "open_positions": pd.DataFrame(),
            "closed_trades": pd.DataFrame(),
            "fills": pd.DataFrame(),
        }

    fills.loc[:, "filled_at"] = pd.to_datetime(fills["filled_at"], errors="coerce")
    fills = fills.sort_values(["filled_at", "fill_id"]).reset_index(drop=True).copy()
    latest_prices = _load_latest_prices(project_root)
    notes_lookup = {
        str(row.get("trade_ref")): row
        for row in _safe_list_trade_notes(store)
        if str(row.get("trade_ref") or "").strip()
    }

    open_rows: list[dict] = []
    closed_rows: list[dict] = []
    for (symbol_id, exchange), group in fills.groupby(["symbol_id", "exchange"], sort=True):
        quantity = 0
        avg_cost = 0.0
        last_buy_metadata: dict[str, Any] = {}
        last_buy_filled_at = None
        for row in group.to_dict(orient="records"):
            fill_qty = int(row.get("quantity") or 0)
            fill_price = float(row.get("price") or 0.0)
            side = str(row.get("side", "BUY")).upper()
            filled_at = row.get("filled_at")
            fill_id = str(row.get("fill_id") or "")
            fill_metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            if side == "BUY":
                new_qty = quantity + fill_qty
                if new_qty > 0:
                    avg_cost = ((quantity * avg_cost) + (fill_qty * fill_price)) / new_qty
                quantity = new_qty
                last_buy_metadata = fill_metadata or {}
                last_buy_filled_at = filled_at
            else:
                closed_qty = min(quantity, fill_qty)
                realized_pnl = (fill_price - avg_cost) * closed_qty
                trade_ref = closed_trade_ref(fill_id)
                note_row = notes_lookup.get(trade_ref, {})
                closed_rows.append(
                    {
                        "trade_ref": trade_ref,
                        "symbol_id": symbol_id,
                        "exchange": exchange,
                        "closed_quantity": closed_qty,
                        "entry_avg_price": round(avg_cost, 4),
                        "exit_price": round(fill_price, 4),
                        "realized_pnl": round(realized_pnl, 2),
                        "entry_date": last_buy_filled_at.isoformat() if pd.notna(last_buy_filled_at) else None,
                        "filled_at": filled_at.isoformat() if pd.notna(filled_at) else None,
                        "status": "win" if realized_pnl > 0 else "loss" if realized_pnl < 0 else "flat",
                        "strategy": (fill_metadata or {}).get("strategy") or last_buy_metadata.get("strategy"),
                        "thesis": note_row.get("thesis") or last_buy_metadata.get("thesis") or "",
                        "setup_note": note_row.get("setup_note") or last_buy_metadata.get("setup_note") or "",
                        "exit_note": note_row.get("exit_note") or (fill_metadata or {}).get("exit_note") or "",
                        "lesson_learned": note_row.get("lesson_learned") or "",
                        "tags": note_row.get("tags") or last_buy_metadata.get("tags") or "",
                    }
                )
                quantity = max(0, quantity - fill_qty)
                if quantity == 0:
                    avg_cost = 0.0

        if quantity > 0:
            current_price = latest_prices.get((str(symbol_id), str(exchange)))
            unrealized = ((current_price - avg_cost) * quantity) if current_price is not None else None
            market_value = (current_price * quantity) if current_price is not None else None
            trade_ref = open_position_trade_ref(str(symbol_id), str(exchange))
            note_row = notes_lookup.get(trade_ref, {})
            open_rows.append(
                {
                    "trade_ref": trade_ref,
                    "symbol_id": symbol_id,
                    "exchange": exchange,
                    "quantity": int(quantity),
                    "avg_entry_price": round(avg_cost, 4),
                    "current_price": round(float(current_price), 4) if current_price is not None else None,
                    "market_value": round(float(market_value), 2) if market_value is not None else None,
                    "unrealized_pnl": round(float(unrealized), 2) if unrealized is not None else None,
                    "return_pct": round(((current_price / avg_cost) - 1) * 100, 2) if current_price is not None and avg_cost else None,
                    "entry_date": last_buy_filled_at.isoformat() if pd.notna(last_buy_filled_at) else None,
                    "strategy": last_buy_metadata.get("strategy"),
                    "thesis": note_row.get("thesis") or last_buy_metadata.get("thesis") or "",
                    "setup_note": note_row.get("setup_note") or last_buy_metadata.get("setup_note") or "",
                    "exit_note": note_row.get("exit_note") or "",
                    "lesson_learned": note_row.get("lesson_learned") or "",
                    "tags": note_row.get("tags") or last_buy_metadata.get("tags") or "",
                }
            )

    open_df = pd.DataFrame(open_rows).sort_values("unrealized_pnl", ascending=False) if open_rows else pd.DataFrame()
    closed_df = pd.DataFrame(closed_rows).sort_values("filled_at", ascending=False) if closed_rows else pd.DataFrame()
    realized_pnl = float(pd.to_numeric(closed_df["realized_pnl"], errors="coerce").fillna(0.0).sum()) if not closed_df.empty else 0.0
    unrealized_pnl = float(pd.to_numeric(open_df["unrealized_pnl"], errors="coerce").fillna(0.0).sum()) if not open_df.empty else 0.0
    win_rate = (
        float((closed_df["realized_pnl"] > 0).sum()) / float(len(closed_df))
        if not closed_df.empty
        else 0.0
    )
    return {
        "summary": {
            "open_positions": int(len(open_df)),
            "closed_trade_count": int(len(closed_df)),
            "win_rate": round(win_rate, 4),
            "realized_pnl": round(realized_pnl, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "total_pnl": round(realized_pnl + unrealized_pnl, 2),
            "invested_capital": round(
                float(
                    (
                        pd.to_numeric(open_df["avg_entry_price"], errors="coerce").fillna(0.0)
                        * pd.to_numeric(open_df["quantity"], errors="coerce").fillna(0.0)
                    ).sum()
                ),
                2,
            ) if not open_df.empty else 0.0,
            "market_value": round(float(pd.to_numeric(open_df["market_value"], errors="coerce").fillna(0.0).sum()), 2) if not open_df.empty else 0.0,
        },
        "open_positions": open_df,
        "closed_trades": closed_df,
        "fills": fills,
    }


@_cache_data(show_spinner=False, ttl=60 * 10)
def load_portfolio_symbol_details(project_root: str) -> pd.DataFrame:
    paths = get_domain_paths(project_root, "operational")
    db_path = Path(paths.master_db_path)
    if not db_path.exists() or db_path.stat().st_size == 0:
        return pd.DataFrame(columns=["symbol_id", "company_name", "sector_name", "industry_group"])
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            try:
                frame = pd.read_sql_query(
                    """
                    SELECT
                        symbol_id,
                        symbol_name AS company_name,
                        sector AS sector_name
                    FROM symbols
                    WHERE symbol_id IS NOT NULL
                    """,
                    conn,
                )
            except Exception:
                frame = pd.read_sql_query(
                    """
                    SELECT
                        Symbol AS symbol_id,
                        Name AS company_name,
                        Sector AS sector_name,
                        "Industry Group" AS industry_group
                    FROM stock_details
                    WHERE Symbol IS NOT NULL
                    """,
                    conn,
                )
        finally:
            conn.close()
    except Exception:
        return pd.DataFrame(columns=["symbol_id", "company_name", "sector_name", "industry_group"])
    if frame.empty:
        return frame
    frame.loc[:, "symbol_id"] = frame["symbol_id"].astype(str).str.upper().str.strip()
    for column in ("company_name", "sector_name", "industry_group"):
        if column not in frame.columns:
            frame.loc[:, column] = ""
        frame.loc[:, column] = frame[column].fillna("").astype(str).str.strip()
    return frame


def build_portfolio_candidate_frame(
    project_root: str,
    *,
    ranked_df: pd.DataFrame | None = None,
    breakout_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    frames = load_latest_rank_frames(project_root)
    ranked = ranked_df.copy() if isinstance(ranked_df, pd.DataFrame) and not ranked_df.empty else frames.get("ranked_signals", pd.DataFrame()).copy()
    breakout = breakout_df.copy() if isinstance(breakout_df, pd.DataFrame) and not breakout_df.empty else frames.get("breakout_scan", pd.DataFrame()).copy()

    if ranked is None or ranked.empty:
        return pd.DataFrame()

    ranked.loc[:, "symbol_id"] = ranked["symbol_id"].astype(str).str.upper().str.strip()
    if "rank_position" not in ranked.columns:
        if "composite_score" in ranked.columns:
            ranked = ranked.sort_values("composite_score", ascending=False).reset_index(drop=True).copy()
            ranked.loc[:, "rank_position"] = ranked.index + 1
        else:
            ranked.loc[:, "rank_position"] = range(1, len(ranked) + 1)

    details = load_portfolio_symbol_details(project_root)
    if not details.empty:
        ranked = ranked.merge(details, on="symbol_id", how="left").copy()
    else:
        ranked.loc[:, "company_name"] = ranked.get("company_name", "")
        ranked.loc[:, "industry_group"] = ranked.get("industry_group", "")

    if "company_name_x" in ranked.columns or "company_name_y" in ranked.columns:
        ranked.loc[:, "company_name"] = (
            ranked.get("company_name_y")
            .combine_first(ranked.get("company_name_x"))
            .fillna(ranked.get("company_name", ""))
        )
    if "industry_group_x" in ranked.columns or "industry_group_y" in ranked.columns:
        ranked.loc[:, "industry_group"] = (
            ranked.get("industry_group_y")
            .combine_first(ranked.get("industry_group_x"))
            .fillna(ranked.get("industry_group", ""))
        )

    sector_primary = None
    if "sector_name_y" in ranked.columns:
        sector_primary = ranked["sector_name_y"]
    if "sector_name_x" in ranked.columns:
        sector_primary = ranked["sector_name_x"] if sector_primary is None else sector_primary.combine_first(ranked["sector_name_x"])
    if "sector_name" in ranked.columns and sector_primary is not None:
        sector_primary = sector_primary.combine_first(ranked["sector_name"])
    elif "sector_name" in ranked.columns:
        sector_primary = ranked["sector_name"]
    if sector_primary is None:
        sector_primary = ranked.get("sector", "")
    elif "sector" in ranked.columns:
        sector_primary = sector_primary.combine_first(ranked["sector"])
    ranked.loc[:, "sector_name"] = pd.Series(sector_primary, index=ranked.index).fillna("").astype(str).str.strip()
    ranked.loc[:, "company_name"] = ranked.get("company_name", "").fillna("").astype(str).str.strip()
    ranked.loc[:, "industry_group"] = ranked.get("industry_group", "").fillna("").astype(str).str.strip()

    if breakout is not None and not breakout.empty and "symbol_id" in breakout.columns:
        breakout.loc[:, "symbol_id"] = breakout["symbol_id"].astype(str).str.upper().str.strip()
        keep_cols = [
            "symbol_id",
            "breakout_tag",
            "breakout_state",
            "candidate_tier",
            "breakout_score",
            "breakout_rank",
            "symbol_trend_reasons",
            "filter_reason",
        ]
        keep_cols = [column for column in keep_cols if column in breakout.columns]
        ranked = ranked.merge(
            breakout[keep_cols].drop_duplicates(subset=["symbol_id"]),
            on="symbol_id",
            how="left",
        ).copy()

    for column in (
        "breakout_tag",
        "breakout_state",
        "candidate_tier",
        "breakout_score",
        "breakout_rank",
        "symbol_trend_reasons",
        "filter_reason",
    ):
        if column not in ranked.columns:
            ranked.loc[:, column] = ""

    ranked.loc[:, "has_breakout"] = ranked["breakout_tag"].fillna("").astype(str).ne("")
    ranked.loc[:, "tradingview_url"] = ranked["symbol_id"].map(
        lambda value: f"https://www.tradingview.com/chart/?symbol=NSE%3A{value}"
    )
    if "close" in ranked.columns:
        ranked.loc[:, "close"] = pd.to_numeric(ranked["close"], errors="coerce")
    ranked.loc[:, "composite_score"] = pd.to_numeric(ranked.get("composite_score"), errors="coerce")
    ranked.loc[:, "breakout_score"] = pd.to_numeric(ranked.get("breakout_score"), errors="coerce")
    return ranked.sort_values(["rank_position", "composite_score"], ascending=[True, False], na_position="last").reset_index(drop=True).copy()


def load_portfolio_workspace_report(
    project_root: str,
    *,
    ranked_df: pd.DataFrame | None = None,
    breakout_df: pd.DataFrame | None = None,
    top_rank_limit: int = 25,
) -> Dict[str, object]:
    trade_report = load_trade_report(project_root)
    candidates = build_portfolio_candidate_frame(project_root, ranked_df=ranked_df, breakout_df=breakout_df)
    candidate_lookup = (
        candidates.drop_duplicates(subset=["symbol_id"]).set_index("symbol_id").to_dict(orient="index")
        if not candidates.empty and "symbol_id" in candidates.columns
        else {}
    )

    open_df = trade_report.get("open_positions", pd.DataFrame()).copy()
    closed_df = trade_report.get("closed_trades", pd.DataFrame()).copy()

    if not open_df.empty:
        enriched_rows: list[dict[str, Any]] = []
        for row in open_df.to_dict(orient="records"):
            symbol_id = str(row.get("symbol_id") or "").strip().upper()
            candidate = candidate_lookup.get(symbol_id, {})
            reasons: list[str] = []
            suggestion = "HOLD"

            rank_position = candidate.get("rank_position")
            breakout_state = str(candidate.get("breakout_state") or "")
            candidate_tier = str(candidate.get("candidate_tier") or "")

            try:
                if rank_position is None or int(rank_position) > int(top_rank_limit):
                    suggestion = "REVIEW"
                    reasons.append("rank_fell_below_target")
            except Exception:
                suggestion = "REVIEW"
                reasons.append("rank_missing")

            if breakout_state.startswith("filtered_"):
                suggestion = "REVIEW"
                reasons.append(str(breakout_state))
            if candidate_tier == "C":
                suggestion = "REVIEW"
                reasons.append("tier_c")
            if not candidate:
                suggestion = "REVIEW"
                reasons.append("not_in_latest_ranked_universe")

            row.update(
                {
                    "company_name": candidate.get("company_name", ""),
                    "sector_name": candidate.get("sector_name", ""),
                    "industry_group": candidate.get("industry_group", ""),
                    "rank_position": candidate.get("rank_position"),
                    "composite_score": candidate.get("composite_score"),
                    "breakout_tag": candidate.get("breakout_tag", ""),
                    "breakout_state": candidate.get("breakout_state", ""),
                    "candidate_tier": candidate.get("candidate_tier", ""),
                    "breakout_score": candidate.get("breakout_score"),
                    "sell_suggestion": suggestion,
                    "sell_reason": ", ".join(reasons) if reasons else "rank_and_breakout_intact",
                    "tradingview_url": candidate.get("tradingview_url", f"https://www.tradingview.com/chart/?symbol=NSE%3A{symbol_id}"),
                    "invested_capital": round(float(row.get("avg_entry_price") or 0.0) * float(row.get("quantity") or 0.0), 2),
                }
            )
            enriched_rows.append(row)
        open_df = pd.DataFrame(enriched_rows).sort_values("unrealized_pnl", ascending=False, na_position="last").reset_index(drop=True).copy()

    if not closed_df.empty:
        closed_rows: list[dict[str, Any]] = []
        for row in closed_df.to_dict(orient="records"):
            symbol_id = str(row.get("symbol_id") or "").strip().upper()
            candidate = candidate_lookup.get(symbol_id, {})
            row.update(
                {
                    "company_name": candidate.get("company_name", ""),
                    "sector_name": candidate.get("sector_name", ""),
                    "industry_group": candidate.get("industry_group", ""),
                    "return_pct": round(
                        ((float(row.get("exit_price") or 0.0) / float(row.get("entry_avg_price") or 1.0)) - 1.0) * 100.0,
                        2,
                    ) if row.get("entry_avg_price") not in (None, 0, 0.0) else None,
                }
            )
            closed_rows.append(row)
        closed_df = pd.DataFrame(closed_rows).sort_values("filled_at", ascending=False, na_position="last").reset_index(drop=True).copy()

    journal_frames: list[pd.DataFrame] = []
    if not open_df.empty:
        open_journal = open_df.copy()
        open_journal.loc[:, "journal_status"] = "OPEN"
        open_journal.loc[:, "pnl_value"] = open_journal.get("unrealized_pnl")
        open_journal.loc[:, "qty"] = open_journal.get("quantity")
        journal_frames.append(open_journal)
    if not closed_df.empty:
        closed_journal = closed_df.copy()
        closed_journal.loc[:, "journal_status"] = "CLOSED"
        closed_journal.loc[:, "pnl_value"] = closed_journal.get("realized_pnl")
        closed_journal.loc[:, "qty"] = closed_journal.get("closed_quantity")
        journal_frames.append(closed_journal)
    journal_df = pd.concat(journal_frames, ignore_index=True, sort=False) if journal_frames else pd.DataFrame()
    if not journal_df.empty:
        timestamp_col = "filled_at" if "filled_at" in journal_df.columns else "entry_date"
        journal_df = journal_df.sort_values(timestamp_col, ascending=False, na_position="last").reset_index(drop=True).copy()

    realized_curve = pd.DataFrame()
    if not closed_df.empty and "filled_at" in closed_df.columns and "realized_pnl" in closed_df.columns:
        realized_curve = closed_df.copy()
        realized_curve.loc[:, "filled_at"] = pd.to_datetime(realized_curve["filled_at"], errors="coerce")
        realized_curve = realized_curve.dropna(subset=["filled_at"]).sort_values("filled_at").reset_index(drop=True).copy()
        if not realized_curve.empty:
            realized_curve.loc[:, "cumulative_realized_pnl"] = realized_curve["realized_pnl"].cumsum()

    return {
        "summary": trade_report.get("summary", {}),
        "candidates": candidates,
        "open_positions": open_df,
        "closed_trades": closed_df,
        "journal": journal_df,
        "fills": trade_report.get("fills", pd.DataFrame()),
        "realized_curve": realized_curve,
    }


def save_trade_journal_note(
    project_root: str,
    *,
    trade_ref: str,
    symbol_id: str | None = None,
    exchange: str | None = None,
    thesis: str | None = None,
    setup_note: str | None = None,
    exit_note: str | None = None,
    lesson_learned: str | None = None,
    tags: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    store = ExecutionStore(project_root)
    method = getattr(store, "upsert_trade_note", None)
    if not callable(method):
        return
    method(
        trade_ref=trade_ref,
        symbol_id=symbol_id,
        exchange=exchange,
        thesis=thesis,
        setup_note=setup_note,
        exit_note=exit_note,
        lesson_learned=lesson_learned,
        tags=tags,
        metadata=metadata,
    )


def _load_latest_prices(project_root: str) -> dict[tuple[str, str], float]:
    paths = get_domain_paths(project_root, "operational")
    db_path = Path(paths.ohlcv_db_path)
    if not db_path.exists():
        return {}
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT symbol_id, exchange, close
            FROM (
                SELECT
                    symbol_id,
                    exchange,
                    close,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol_id, exchange
                        ORDER BY timestamp DESC
                    ) AS rn
                FROM _catalog
            )
            WHERE rn = 1
            """
        ).fetchall()
    finally:
        conn.close()
    return {
        (str(symbol_id), str(exchange)): float(close)
        for symbol_id, exchange, close in rows
        if close is not None
    }
