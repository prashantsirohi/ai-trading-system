"""Read model for the stock investigator endpoint."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from math import isfinite
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.domains.investigator.payload import build_investigator_payload
from ai_trading_system.platform.db.paths import get_domain_paths, resolve_artifact_path


def get_investigator_pattern_history(
    symbol_id: str,
    lookback_days: int,
    as_of: str | date | datetime | None = None,
    *,
    project_root: Path | str | None = None,
) -> dict[str, Any]:
    symbol = str(symbol_id or "").strip().upper()
    lookback = max(0, int(lookback_days))
    if not symbol:
        return {"symbol_id": symbol, "lookback_days": lookback, "as_of": _json_safe(as_of), "history": []}
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    db_path = paths.root_dir / "control_plane.duckdb"
    if not db_path.exists():
        return {"symbol_id": symbol, "lookback_days": lookback, "as_of": _json_safe(as_of), "history": []}
    conn: duckdb.DuckDBPyConnection | None = None
    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        as_of_date = _coerce_date(as_of)
        if as_of_date is None:
            row = conn.execute(
                "SELECT MAX(trade_date) FROM investigator_pattern_scan WHERE UPPER(symbol_id) = ?",
                [symbol],
            ).fetchone()
            as_of_date = _coerce_date(row[0] if row else None)
        if as_of_date is None:
            return {"symbol_id": symbol, "lookback_days": lookback, "as_of": None, "history": []}
        from_date = as_of_date - timedelta(days=lookback)
        frame = conn.execute(
            """
            SELECT
                run_id,
                attempt_number,
                artifact_uri,
                trade_date,
                symbol_id,
                pattern_family,
                pattern_state,
                pattern_lifecycle_state,
                pattern_score,
                setup_quality,
                stage2_score,
                stage2_label,
                breakout_level,
                watchlist_trigger_level,
                invalidation_price,
                is_strong_volume_confirmation,
                is_combined_volume_confirmation,
                breakout_volume_ratio,
                s1_promotion_state,
                promotion_reason,
                trigger_reason,
                investigator_status,
                investigator_verdict,
                investigator_final_score,
                source_investigator,
                source_ranked
            FROM investigator_pattern_scan
            WHERE UPPER(symbol_id) = ?
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date DESC, run_id DESC, attempt_number DESC
            """,
            [symbol, from_date, as_of_date],
        ).fetchdf()
    except Exception:
        return {"symbol_id": symbol, "lookback_days": lookback, "as_of": _json_safe(as_of), "history": []}
    finally:
        if conn is not None:
            conn.close()
    return {
        "symbol_id": symbol,
        "lookback_days": lookback,
        "as_of": as_of_date.isoformat(),
        "history": _records(frame),
    }


def get_investigator_snapshot(project_root: Path) -> dict[str, Any]:
    artifacts = _latest_artifacts(project_root)
    frames = {
        "today_gainers": _read_csv(artifacts.get("daily_gainer_log")),
        "scores": _read_csv(artifacts.get("investigator_scores")),
        "repeat_tracker": _read_csv(artifacts.get("repeat_tracker")),
        "trap_log": _read_csv(artifacts.get("trap_log")),
        "active_watchlist": _read_csv(artifacts.get("active_watchlist")),
        "archive": _read_csv(artifacts.get("archived_investigator")),
        "investigator_pattern_scan": _read_csv(artifacts.get("investigator_pattern_scan")),
        "investigator_early_accumulation": _read_csv(artifacts.get("investigator_early_accumulation")),
        "final_3q_gate": _read_csv(artifacts.get("final_3q_gate")),
        "investigator_performance_summary": _read_csv(artifacts.get("investigator_performance_summary")),
    }
    summary = _read_json(artifacts.get("investigator_summary"))
    scores = frames["scores"]
    high = (
        scores.loc[scores["verdict"].astype(str).eq("HIGH_CONVICTION")].copy()
        if not scores.empty and "verdict" in scores.columns
        else pd.DataFrame()
    )
    archive = frames["archive"]
    previous_summary = _read_json(_previous_artifact_from_disk(project_root, "investigator_summary", artifacts.get("investigator_summary")))
    payload = _read_json(artifacts.get("investigator_payload"))
    if not payload:
        payload = build_investigator_payload(
            run_id=str(summary.get("run_id") or _run_id_from_artifact(artifacts.get("investigator_summary")) or ""),
            run_date=str(summary.get("run_date") or ""),
            summary=summary,
            today_gainers=frames["today_gainers"],
            scores=scores,
            repeat_tracker=frames["repeat_tracker"],
            active_watchlist=frames["active_watchlist"],
            trap_log=frames["trap_log"],
            archive=archive,
            final_3q_gate=frames["final_3q_gate"],
            investigator_pattern_scan=frames["investigator_pattern_scan"],
            investigator_early_accumulation=frames["investigator_early_accumulation"],
            previous_summary=previous_summary,
            stage_status=_stage_status_from_artifacts(artifacts),
        )
    else:
        payload = dict(payload)
        if not payload.get("summary_deltas"):
            payload["summary_deltas"] = _summary_deltas_from_payload(payload, previous_summary)
        payload.setdefault("stage_status", _stage_status_from_artifacts(artifacts))
        payload.setdefault("final_3q_gate", _records(frames["final_3q_gate"], limit=50))
        payload.setdefault("investigator_early_accumulation", _records(_sort_early_accumulation(frames["investigator_early_accumulation"]), limit=100))
        payload.setdefault("summary", {})
        if isinstance(payload["summary"], dict):
            payload["summary"].setdefault("investigator_early_accumulation_count", int(len(frames["investigator_early_accumulation"])))
        payload.setdefault("performance_summary", _read_json(artifacts.get("investigator_performance_summary_json")))
        payload.setdefault("threshold_recommendations", _read_json(artifacts.get("investigator_threshold_recommendations")))

    compatible = {
        "summary": summary,
        "today_gainers": _records(frames["today_gainers"]),
        "high_conviction": _records(high),
        "repeat_tracker": _records(_sort_repeat_tracker(frames["repeat_tracker"])),
        "trap_log": _records(frames["trap_log"]),
        "active_watchlist": _records(_sort_active_watchlist(frames["active_watchlist"])),
        "investigator_pattern_scan": _records(frames["investigator_pattern_scan"]),
        "investigator_early_accumulation": _records(_sort_early_accumulation(frames["investigator_early_accumulation"]), limit=100),
        "final_3q_gate": _records(frames["final_3q_gate"], limit=100),
        "investigator_performance_summary": _records(frames["investigator_performance_summary"], limit=200),
        "investigator_performance_summary_json": _read_json(artifacts.get("investigator_performance_summary_json")),
        "investigator_threshold_recommendations": _read_json(artifacts.get("investigator_threshold_recommendations")),
        "archive_summary": {
            "count": int(len(archive)),
            "by_reason": _counts(archive, "drop_reason"),
            "rows": _records(archive, limit=100),
        },
        "source_artifacts": {key: str(value) for key, value in artifacts.items() if value is not None},
    }
    return _json_safe({**compatible, **payload, "raw_summary": summary, "decision_payload": payload})


def _latest_artifacts(project_root: Path) -> dict[str, Path | None]:
    out: dict[str, Path | None] = {}
    for artifact_type in (
        "daily_gainer_log",
        "investigator_scores",
        "repeat_tracker",
        "active_watchlist",
        "trap_log",
        "archived_investigator",
        "investigator_pattern_scan",
        "investigator_early_accumulation",
        "final_3q_gate",
        "investigator_performance_summary",
        "investigator_summary",
        "investigator_payload",
        "investigator_performance_summary_json",
        "investigator_threshold_recommendations",
    ):
        out[artifact_type] = _latest_artifact_from_registry(project_root, artifact_type) or _latest_artifact_from_disk(
            project_root,
            artifact_type,
        )
    return out


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
            WHERE a.stage_name = 'investigator'
              AND a.artifact_type = ?
              AND r.status = 'completed'
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
    path = resolve_artifact_path(str(row[0]), project_root=project_root)
    return path if path.exists() else None


def _latest_artifact_from_disk(project_root: Path, artifact_type: str) -> Path | None:
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    filename = _artifact_filename(artifact_type)
    candidates = list(paths.pipeline_runs_dir.glob(f"*/investigator/attempt_*/{filename}"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _previous_artifact_from_disk(project_root: Path, artifact_type: str, current: Path | None) -> Path | None:
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    filename = _artifact_filename(artifact_type)
    candidates = sorted(
        (path for path in paths.pipeline_runs_dir.glob(f"*/investigator/attempt_*/{filename}") if path != current),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _read_csv(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _artifact_filename(artifact_type: str) -> str:
    if artifact_type == "investigator_performance_summary_json":
        return "investigator_performance_summary.json"
    if artifact_type in {
        "investigator_summary",
        "investigator_payload",
        "investigator_threshold_recommendations",
    }:
        return f"{artifact_type}.json"
    return f"{artifact_type}.csv"


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _run_id_from_artifact(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        return path.parts[-4]
    except IndexError:
        return ""


def _stage_status_from_artifacts(artifacts: dict[str, Path | None]) -> dict[str, str]:
    return {
        "rank": "completed",
        "investigator": "completed" if artifacts.get("investigator_summary") else "missing",
        "publish": "unknown",
    }


def _summary_deltas_from_payload(payload: dict[str, Any], previous_summary: dict[str, Any]) -> dict[str, int]:
    summary = payload.get("summary", {})
    if not isinstance(summary, dict):
        return {}
    out: dict[str, int] = {}
    for key, value in summary.items():
        if key == "trap_rate":
            continue
        try:
            out[str(key)] = int(value) - int(previous_summary.get(key, previous_summary.get(_legacy_summary_key(str(key)), 0)) or 0)
        except (TypeError, ValueError):
            continue
    return out


def _legacy_summary_key(key: str) -> str:
    return {
        "total_intake": "total_intake_count",
        "total_intake_count": "daily_gainer_count",
        "daily_gainers": "daily_gainer_count",
        "active_queue": "active_count",
        "high_conviction": "high_conviction_count",
        "traps": "trap_count",
        "archived": "archived_count",
    }.get(key, key)


def _records(frame: pd.DataFrame, limit: int | None = None) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    safe = frame.head(limit).copy() if limit else frame.copy()
    safe = safe.loc[:, ~safe.columns.duplicated()].copy()
    safe = safe.where(safe.notna(), None)
    return [_json_safe(row) for row in safe.to_dict(orient="records")]


def _counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if frame.empty or column not in frame.columns:
        return {}
    return {str(key): int(value) for key, value in frame[column].fillna("").astype(str).value_counts().to_dict().items()}


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if value is pd.NA or value is pd.NaT:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, float) and not isfinite(value):
        return None
    return value


def _coerce_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _sort_repeat_tracker(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    safe = frame.copy()
    priority_sort = (
        safe.get("high_priority_repeat", pd.Series(False, index=safe.index))
        .fillna(False)
        .astype(str)
        .str.lower()
        .isin({"true", "1", "yes"})
        .astype(int)
    )
    for column in ("repeat_score", "appearance_count_20d", "price_progression_pct"):
        safe.loc[:, column] = pd.to_numeric(safe[column], errors="coerce") if column in safe.columns else pd.NA
    safe = safe.assign(_priority_sort=priority_sort)
    safe = safe.sort_values(
        ["_priority_sort", "repeat_score", "appearance_count_20d", "price_progression_pct", "symbol_id"],
        ascending=[False, False, False, False, True],
        na_position="last",
        kind="stable",
    )
    return safe.drop(columns=["_priority_sort"]).reset_index(drop=True)


def _sort_active_watchlist(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    safe = frame.copy()
    verdict_order = {
        "HIGH_CONVICTION": 0,
        "MEDIUM_CONVICTION": 1,
        "WATCH_ONLY": 2,
        "NOISE_TRAP": 3,
    }
    verdict_sort = safe.get("verdict", pd.Series("", index=safe.index)).fillna("").astype(str).str.upper().map(verdict_order).fillna(99)
    for column in ("score_current", "score_peak", "appearance_count_20d"):
        safe.loc[:, column] = pd.to_numeric(safe[column], errors="coerce") if column in safe.columns else pd.NA
    safe = safe.assign(_verdict_sort=verdict_sort)
    safe = safe.sort_values(
        ["_verdict_sort", "score_current", "score_peak", "appearance_count_20d", "symbol_id"],
        ascending=[True, False, False, False, True],
        na_position="last",
        kind="stable",
    )
    return safe.drop(columns=["_verdict_sort"]).reset_index(drop=True)


def _sort_early_accumulation(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    safe = frame.copy()
    for column in ("early_accumulation_rank", "early_accumulation_score"):
        safe.loc[:, column] = pd.to_numeric(safe[column], errors="coerce") if column in safe.columns else pd.NA
    symbol = safe.get("symbol_id", safe.get("symbol", pd.Series("", index=safe.index))).fillna("").astype(str)
    safe = safe.assign(_symbol_sort=symbol)
    safe = safe.sort_values(
        ["early_accumulation_rank", "early_accumulation_score", "_symbol_sort"],
        ascending=[True, False, True],
        na_position="last",
        kind="stable",
    )
    return safe.drop(columns=["_symbol_sort"]).reset_index(drop=True)
