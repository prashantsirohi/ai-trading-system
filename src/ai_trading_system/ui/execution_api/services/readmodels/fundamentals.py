"""Readmodels for fundamentals pipeline artifacts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from pandas.errors import EmptyDataError

from ai_trading_system.domains.fundamentals.analytical_store import default_fundamentals_duckdb_path
from ai_trading_system.domains.fundamentals.insight_readmodels import (
    COMPOUNDER_PRIORITY,
    GREAT_RESULT_PRIORITY,
    TURNAROUND_PRIORITY,
)
from ai_trading_system.domains.fundamentals.presentation_payloads import (
    DEFAULT_PUBLISH_UNIVERSE_ID,
    build_fundamental_dashboard_payload,
)
from ai_trading_system.platform.db.paths import get_domain_paths


def get_latest_fundamentals(project_root: Path, *, limit: int = 25) -> dict[str, Any]:
    summary_path = _latest_summary_from_registry(project_root) or _latest_summary_from_disk(project_root)
    if summary_path is None:
        return {
            "snapshot_date": None,
            "stale_days": None,
            "tier_counts": {},
            "top_watchlist": [],
            "summary": {},
            "source_path": None,
            "generated_at": None,
            "industry_status": None,
            "industry_snapshot_date": None,
            "industry_label_counts": {},
            "industry_rows_scored": None,
            "industry_trend_status": None,
            "industry_trend_label_counts": {},
        }

    summary = _read_json(summary_path)
    watchlist_path = summary_path.parent / "watchlist_candidates.csv"
    top_watchlist = _read_top_watchlist(watchlist_path, limit=limit)
    generated_at = summary.get("generated_at") or _mtime_iso(summary_path)
    return {
        "snapshot_date": summary.get("snapshot_date"),
        "stale_days": summary.get("stale_days"),
        "tier_counts": summary.get("tier_counts") or {},
        "top_watchlist": top_watchlist,
        "summary": summary,
        "source_path": str(summary_path),
        "generated_at": generated_at,
        "industry_status": summary.get("industry_status"),
        "industry_snapshot_date": summary.get("industry_snapshot_date"),
        "industry_label_counts": summary.get("industry_label_counts") or {},
        "industry_rows_scored": summary.get("industry_rows_scored"),
        "industry_trend_status": summary.get("industry_trend_status"),
        "industry_trend_label_counts": summary.get("industry_trend_label_counts") or {},
    }


def get_fundamentals_dashboard(project_root: Path) -> dict[str, Any]:
    payload_path = _latest_artifact_from_registry(project_root, "fundamental_dashboard_payload") or _latest_payload_from_disk(project_root)
    if payload_path is None:
        live = _build_live_dashboard_payload(project_root)
        if live is not None:
            return live
        latest = get_latest_fundamentals(project_root, limit=10)
        return _empty_dashboard_payload(source_path=latest.get("source_path"), summary=latest.get("summary") or {})
    payload = _read_json(payload_path)
    return {
        "summary": payload.get("summary") or {},
        "valuation_chart": payload.get("valuation_chart") or payload.get("valuation_cycle") or [],
        "great_results_top": payload.get("great_results_top") or payload.get("top_great_results") or [],
        "turnarounds_top": payload.get("turnarounds_top") or payload.get("top_turnarounds") or [],
        "compounders_top": payload.get("compounders_top") or payload.get("top_compounders") or [],
        "sector_earnings_top": payload.get("sector_earnings_top") or payload.get("sector_earnings_leadership") or [],
        "universe": payload.get("universe") or {},
        "run_date": payload.get("run_date"),
        "source_path": str(payload_path),
    }


def _empty_dashboard_payload(*, source_path: str | None, summary: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "summary": summary or {},
        "valuation_chart": [],
        "great_results_top": [],
        "turnarounds_top": [],
        "compounders_top": [],
        "sector_earnings_top": [],
        "source_path": source_path,
    }


def _build_live_dashboard_payload(project_root: Path) -> dict[str, Any] | None:
    fundamentals_db = default_fundamentals_duckdb_path(project_root)
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    if not fundamentals_db.exists():
        return None
    universe = _read_table(fundamentals_db, "universe_valuation_daily")
    cycle = _read_table(fundamentals_db, "valuation_cycle_features")
    tags = _read_table(fundamentals_db, "company_insight_tags")
    sectors = _read_table(fundamentals_db, "sector_earnings_leadership")
    if universe.empty and cycle.empty and tags.empty and sectors.empty:
        return None
    great = _curated_tag_rows(tags, GREAT_RESULT_PRIORITY, score_name="great_result_score", limit=10)
    turnarounds = _curated_tag_rows(tags, TURNAROUND_PRIORITY, score_name="turnaround_score", limit=10)
    compounders = _curated_tag_rows(tags, COMPOUNDER_PRIORITY, score_name="compounder_score", limit=20)
    sector_top = _latest_by_date(sectors, "report_date")
    if not sector_top.empty and "sector_fundamental_score" in sector_top.columns:
        sector_top = sector_top.sort_values("sector_fundamental_score", ascending=False, na_position="last").head(20)
    payload = build_fundamental_dashboard_payload(
        great_results=great,
        turnarounds=turnarounds,
        compounders=compounders,
        sector_earnings=sector_top,
        universe_valuation=universe,
        valuation_cycle=cycle,
        universe_id=DEFAULT_PUBLISH_UNIVERSE_ID,
        years=5,
    )
    payload["source_path"] = str(fundamentals_db)
    if paths.ohlcv_db_path.exists():
        payload["ohlcv_db_path"] = str(paths.ohlcv_db_path)
    return payload


def _read_table(db_path: Path, table_name: str) -> pd.DataFrame:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        exists = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchone()[0]
        if not exists:
            return pd.DataFrame()
        return conn.execute(f"SELECT * FROM {table_name}").df()
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def _curated_tag_rows(tags: pd.DataFrame, priority: dict[str, int], *, score_name: str, limit: int) -> pd.DataFrame:
    if tags.empty or "insight_type" not in tags.columns:
        return pd.DataFrame()
    frame = tags.loc[tags["insight_type"].astype(str).isin(priority)].copy()
    if frame.empty:
        return pd.DataFrame()
    frame = _latest_by_date(frame, "report_date")
    frame.loc[:, "_priority"] = frame["insight_type"].astype(str).map(priority).fillna(99).astype(int)
    frame.loc[:, "insight_score"] = pd.to_numeric(frame.get("insight_score"), errors="coerce")
    frame.loc[:, score_name] = frame["insight_score"]
    evidence = frame.get("evidence_json", pd.Series("", index=frame.index)).map(_parse_evidence)
    frame.loc[:, "evidence"] = evidence.map(lambda item: item.get("note") or "")
    for key in ("sales_yoy_growth", "profit_yoy_growth", "profit_qoq_growth", "opm_yoy_change", "net_profit_cr"):
        if key not in frame.columns:
            frame.loc[:, key] = evidence.map(lambda item, metric=key: item.get(metric))
    frame = frame.sort_values(["_priority", "insight_score", "symbol"], ascending=[True, False, True], na_position="last")
    if "symbol" in frame.columns:
        frame = frame.drop_duplicates("symbol", keep="first")
    return frame.head(limit).reset_index(drop=True)


def _latest_by_date(frame: pd.DataFrame, date_col: str) -> pd.DataFrame:
    if frame.empty or date_col not in frame.columns:
        return frame
    out = frame.copy()
    out.loc[:, date_col] = pd.to_datetime(out[date_col], errors="coerce")
    latest = out[date_col].max()
    if pd.isna(latest):
        return out
    return out.loc[out[date_col].eq(latest)].reset_index(drop=True)


def _parse_evidence(value: Any) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {"note": value}
    return parsed if isinstance(parsed, dict) else {}


def _latest_summary_from_registry(project_root: Path) -> Path | None:
    return _latest_artifact_from_registry(project_root, "fundamental_summary")


def _latest_artifact_from_registry(project_root: Path, artifact_type: str) -> Path | None:
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    db_path = paths.root_dir / "control_plane.duckdb"
    if not db_path.exists():
        return None
    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        row = conn.execute(
            """
            SELECT a.uri
            FROM pipeline_artifact a
            JOIN pipeline_run r ON r.run_id = a.run_id
            WHERE a.stage_name = 'fundamentals'
              AND a.artifact_type = ?
            ORDER BY r.started_at DESC NULLS LAST, a.created_at DESC NULLS LAST
            LIMIT 1
            """,
            [artifact_type],
        ).fetchone()
    except Exception:
        return None
    finally:
        if "conn" in locals():
            conn.close()
    if not row:
        return None
    path = Path(str(row[0]))
    return path if path.exists() else None


def _latest_summary_from_disk(project_root: Path) -> Path | None:
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    candidates = list(paths.pipeline_runs_dir.glob("*/fundamentals/attempt_*/fundamental_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _latest_payload_from_disk(project_root: Path) -> Path | None:
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    candidates = list(paths.pipeline_runs_dir.glob("*/fundamentals/attempt_*/fundamental_dashboard_payload.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_top_watchlist(path: Path, *, limit: int) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    try:
        frame = pd.read_csv(path)
    except (EmptyDataError, OSError):
        return []
    if frame.empty:
        return []
    bucket = frame.get("watchlist_bucket", pd.Series("", index=frame.index)).astype(str)
    priority = bucket.ne("ADD_TO_WATCHLIST").astype(int)
    score = pd.to_numeric(frame.get("final_watchlist_score", pd.Series(0, index=frame.index)), errors="coerce").fillna(0)
    ordered = frame.assign(_priority=priority, _score=score).sort_values(
        ["_priority", "_score"],
        ascending=[True, False],
        kind="stable",
    )
    return ordered.drop(columns=["_priority", "_score"], errors="ignore").head(int(limit)).to_dict(orient="records")


def _mtime_iso(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
