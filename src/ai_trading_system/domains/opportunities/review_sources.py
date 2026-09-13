"""Read-only, cutoff-bound inputs for the final shadow review projection."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import hashlib
import json
import sqlite3
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.domains.opportunities.stage_governance import (
    resolve_stage_observation_payloads,
)

SOURCE_TYPES = {
    "rank": ("rank", "ranked_universe"),
    "stage": ("weekly_stage", "weekly_stock_stage_universe"),
    "pattern": ("pattern_lane_scan", "pattern_lane_assessments"),
    "signals": ("pattern_lane_scan", "pattern_lane_scan"),
    "fundamental": ("fundamental_discovery", "fundamental_thesis_universe"),
    "positions": ("scan_router", "active_position_coverage"),
}


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def frame_digest(frame: pd.DataFrame) -> str:
    return digest_bytes(
        frame.to_json(orient="split", date_format="iso", double_precision=15).encode()
    )


def utc(value: Any) -> datetime:
    result = pd.Timestamp(value)
    if pd.isna(result):
        raise ValueError("Missing availability timestamp")
    if result.tzinfo is None:
        result = result.tz_localize("UTC")
    return result.to_pydatetime().astimezone(timezone.utc)


@dataclass
class ReviewSource:
    frame: pd.DataFrame = field(default_factory=pd.DataFrame)
    metadata: dict[str, Any] = field(default_factory=dict)
    issue: str | None = "SOURCE_MISSING"


@dataclass
class ReviewSourceBundle:
    session: date
    decision_at: datetime
    sources: dict[str, ReviewSource]
    market: pd.DataFrame
    master: pd.DataFrame
    holidays: tuple[date, ...]
    indices: pd.DataFrame
    quarantine: pd.DataFrame
    governed: dict[tuple[str, str], dict[str, Any]]
    governance_issue: str | None
    dq_rows: list[dict[str, Any]]
    snapshot_hashes: dict[str, str]
    snapshot_issues: tuple[str, ...] = ()


def _path(root: Path, uri: str, run_id: str, stage: str, attempt: int) -> Path:
    # Registered URIs in a copied control plane still name the original root.
    # Resolve only the corresponding copied attempt; never fall back to that URI.
    name = Path(uri).name
    relative = Path("pipeline_runs") / run_id / stage / f"attempt_{attempt}" / name
    path = root / relative
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("Artifact escapes the selected runtime root")
    return path


def _read_source(
    conn: Any,
    root: Path,
    run_id: str,
    stage: str,
    kind: str,
    cutoff: datetime,
    selected: Any = None,
) -> ReviewSource:
    params: list[Any] = [stage, kind]
    scope = "a.run_id = ?"
    params.append(run_id)
    if selected is not None:
        parts = Path(selected.uri).parts
        if len(parts) < 4 or parts[-3] != stage:
            return ReviewSource(issue="SOURCE_ATTEMPT_PATH_MISMATCH")
        scope = "a.run_id = ? AND a.content_hash = ?"
        params = [stage, kind, parts[-4], selected.content_hash]
        if selected.attempt_number is not None:
            scope += " AND a.attempt_number = ?"
            params.append(selected.attempt_number)
    rows = conn.execute(
        f"""
        SELECT a.run_id,a.attempt_number,a.uri,a.content_hash,a.row_count,
               a.created_at,a.promoted_at,s.ended_at,r.run_date
        FROM pipeline_artifact a
        JOIN pipeline_stage_run s USING(run_id,stage_name,attempt_number)
        JOIN pipeline_run r USING(run_id)
        WHERE a.stage_name=? AND a.artifact_type=? AND {scope}
          AND s.status='completed' AND a.lifecycle_status='promoted'
        ORDER BY a.created_at DESC,a.attempt_number DESC
    """,
        params,
    ).fetchall()
    if not rows:
        return ReviewSource(issue="NO_COMPLETED_PROMOTED_SOURCE")
    # A later selected correction is not replaced by a silently older attempt.
    row = rows[0]
    metadata = dict(
        zip(
            (
                "run_id",
                "attempt",
                "registered_uri",
                "content_hash",
                "row_count",
                "created_at",
                "promoted_at",
                "stage_ended_at",
                "session",
            ),
            row,
        )
    )
    try:
        available = max(utc(row[i]) for i in (5, 6, 7))
        metadata["available_at"] = available
        if available > cutoff:
            return ReviewSource(metadata=metadata, issue="SOURCE_AFTER_CUTOFF")
        path = _path(root, row[2], row[0], stage, row[1])
        data = path.read_bytes()
        if not row[3] or digest_bytes(data) != row[3]:
            return ReviewSource(metadata=metadata, issue="ARTIFACT_HASH_MISMATCH")
        import io

        frame = pd.read_csv(io.BytesIO(data), keep_default_na=False)
        if row[4] is None or len(frame) != row[4]:
            return ReviewSource(metadata=metadata, issue="ARTIFACT_ROW_COUNT_MISMATCH")
        metadata["local_uri"] = str(path)
        return ReviewSource(frame, metadata, None)
    except (OSError, ValueError, pd.errors.ParserError) as exc:
        return ReviewSource(
            metadata=metadata, issue=f"SOURCE_UNREADABLE:{type(exc).__name__}"
        )


def load_review_sources(
    *,
    data_root: Path,
    run_id: str,
    session: date,
    decision_at: datetime,
    selected_artifacts: dict | None = None,
    ohlcv_db_path: Path | None = None,
    control_plane_db_path: Path | None = None,
    master_db_path: Path | None = None,
) -> ReviewSourceBundle:
    """Read a consistent transaction per store. No initializer or writer is used.

    This captures current stored vintages through the specified market session;
    it does not reconstruct overwritten historical OHLCV/master/quarantine rows.
    """
    if decision_at.tzinfo is None:
        raise ValueError("decision_at must be timezone-aware")
    decision_at = utc(decision_at)
    root = Path(data_root).resolve()
    sources: dict[str, ReviewSource] = {}
    selected_artifacts = selected_artifacts or {}
    with duckdb.connect(
        str(control_plane_db_path or root / "control_plane.duckdb"), read_only=True
    ) as conn:
        conn.execute("BEGIN TRANSACTION")
        for key, (stage, kind) in SOURCE_TYPES.items():
            sources[key] = _read_source(
                conn,
                root,
                run_id,
                stage,
                kind,
                decision_at,
                selected_artifacts.get(stage, {}).get(kind),
            )
        dq_rows = (
            conn.execute(
                """SELECT stage_name,rule_id,status,band,failed_count,created_at
            FROM dq_result WHERE run_id=? AND created_at<=?
            QUALIFY row_number() OVER(PARTITION BY stage_name,rule_id ORDER BY created_at DESC)=1
            ORDER BY stage_name,rule_id""",
                [run_id, decision_at.replace(tzinfo=None)],
            )
            .fetchdf()
            .to_dict("records")
        )
        governance_issue = None
        try:
            governed_rows = resolve_stage_observation_payloads(
                conn,
                scope="STOCK",
                table="weekly_stock_stage_history",
                as_of=session.isoformat(),
                available_at=decision_at,
                entity_columns=("exchange", "symbol_id"),
                clauses=[
                    "CAST(as_of AS DATE)<=?",
                    "source_week_end<=?",
                    "classifier_version=?",
                ],
                params=[session, session, "weekly-stage-v2"],
            )
        except (duckdb.Error, ValueError, RuntimeError):
            governed_rows = []
            governance_issue = "GOVERNED_STAGE_RESOLUTION_FAILED"
        governed = {(str(r["exchange"]), str(r["symbol_id"])): r for r in governed_rows}
    start = session - timedelta(days=800)
    with duckdb.connect(
        str(ohlcv_db_path or root / "ohlcv.duckdb"), read_only=True
    ) as conn:
        conn.execute("BEGIN TRANSACTION")
        market = conn.execute(
            """SELECT symbol_id,exchange,timestamp,open,high,low,close,volume,
                adjusted_open,adjusted_high,adjusted_low,adjusted_close,adjustment_factor,
                adjusted_at,ingestion_ts,provider,validation_status,provider_discrepancy_flag
            FROM _catalog WHERE exchange IN (?,?) AND CAST(timestamp AS DATE) BETWEEN ? AND ?
              AND NOT COALESCE(is_benchmark,FALSE)
            ORDER BY exchange,symbol_id,timestamp""",
            ["NSE", "BSE", start, session],
        ).fetchdf()
        indices = conn.execute(
            """SELECT index_code,date,provider,validated_at FROM _index_catalog
            WHERE index_code=? AND date BETWEEN ? AND ? ORDER BY date""",
            ["NIFTY_50", start, session],
        ).fetchdf()
        quarantine = conn.execute(
            """SELECT symbol_id,exchange,trade_date,reason,status,created_at,resolved_at
            FROM _catalog_quarantine WHERE trade_date BETWEEN ? AND ?
            ORDER BY exchange,symbol_id,trade_date,reason""",
            [start, session],
        ).fetchdf()
    issues = []
    with sqlite3.connect(
        (master_db_path or root / "masterdata.db").resolve().as_uri() + "?mode=ro",
        uri=True,
    ) as conn:
        conn.execute("BEGIN")
        master = pd.read_sql_query(
            "SELECT symbol_id,exchange,isin,instrument_type,last_updated FROM symbols ORDER BY exchange,symbol_id",
            conn,
        )
        try:
            holidays = tuple(
                date.fromisoformat(str(r[0])[:10])
                for r in conn.execute("SELECT date FROM nse_holidays ORDER BY date")
            )
        except sqlite3.Error:
            holidays = ()
            issues.append("NSE_HOLIDAY_SOURCE_MISSING")
    snapshots = {
        name: frame_digest(frame)
        for name, frame in (
            ("market", market),
            ("master", master),
            ("indices", indices),
            ("quarantine", quarantine),
        )
    }
    snapshots["holidays"] = digest_bytes(json.dumps(holidays, default=str).encode())
    snapshots["governance"] = digest_bytes(
        json.dumps(governed_rows, default=str, sort_keys=True).encode()
    )
    snapshots["dq"] = digest_bytes(
        json.dumps(dq_rows, default=str, sort_keys=True).encode()
    )
    return ReviewSourceBundle(
        session,
        decision_at,
        sources,
        market,
        master,
        holidays,
        indices,
        quarantine,
        governed,
        governance_issue,
        dq_rows,
        snapshots,
        tuple(issues),
    )
