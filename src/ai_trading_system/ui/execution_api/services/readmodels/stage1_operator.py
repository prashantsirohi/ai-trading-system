"""Read-only Stage-1 operator model for the web console."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths


ACTIVE_STATES = {
    "BASE_BUILDING",
    "ACCUMULATING",
    "LATE_STAGE1",
    "BREAKOUT_READY",
    "PROMOTION_PENDING",
    "REGRESSED",
    "STALE_BASE",
}
TERMINAL_STATES = {"INVALIDATED", "ARCHIVED"}
LIFECYCLE_ORDER = {
    "PROMOTION_PENDING": 0,
    "BREAKOUT_READY": 1,
    "LATE_STAGE1": 2,
    "ACCUMULATING": 3,
    "BASE_BUILDING": 4,
    "REGRESSED": 5,
    "STALE_BASE": 6,
    "INVALIDATED": 7,
    "ARCHIVED": 8,
}
PRIORITY_ORDER = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}


@dataclass(frozen=True)
class Stage1OperatorPolicy:
    late_stage_rank_cutoff: int = 20
    late_stage_pivot_distance_pct: float = 5.0
    major_progression_score_delta: float = 8.0


POLICY = Stage1OperatorPolicy()
ALLOWED_SORTS = {
    "operator_priority",
    "stage1_emerging_rank",
    "stage1_maturity_score",
    "stage1_score_delta_5d",
    "stage1_score_delta_20d",
    "emerging_rank_improvement_20d",
    "distance_to_pivot_pct",
    "stage1_days_in_lifecycle_state",
}


def get_stage1_context_by_symbol(project_root: Path | str) -> dict[str, dict[str, Any]]:
    """Return the latest derived context for enriching other read models."""
    rows, _ = _latest_rows(project_root)
    return {str(row.get("symbol_id") or "").upper(): row for row in rows}


def get_stage1_current(
    project_root: Path | str,
    *,
    lifecycle_state: str | None = None,
    operator_status: str | None = None,
    operator_priority: str | None = None,
    sector: str | None = None,
    golden_cross_status: str | None = None,
    pattern_promotion_state: str | None = None,
    promotion_eligibility: bool | None = None,
    search: str | None = None,
    include_blocked: bool = False,
    limit: int = 100,
    offset: int = 0,
    sort_by: str | None = None,
    sort_direction: str = "asc",
) -> dict[str, Any]:
    rows, as_of = _latest_rows(project_root)
    filtered = [r for r in rows if r.get("stage1_lifecycle_state") in ACTIVE_STATES]
    if not include_blocked:
        filtered = [r for r in filtered if r["operator_status"] != "BLOCKED"]
    filters = {
        "stage1_lifecycle_state": lifecycle_state,
        "operator_status": operator_status,
        "operator_priority": operator_priority,
        "sector": sector,
        "golden_cross_status": golden_cross_status,
        "pattern_promotion_state": pattern_promotion_state,
    }
    for key, wanted in filters.items():
        if wanted:
            filtered = [
                r
                for r in filtered
                if str(r.get(key) or "").upper() == str(wanted).upper()
            ]
    if promotion_eligibility is not None:
        filtered = [
            r
            for r in filtered
            if _truth(r.get("promotion_eligibility")) is promotion_eligibility
        ]
    if search:
        needle = search.strip().upper()
        filtered = [
            r for r in filtered if needle in str(r.get("symbol_id") or "").upper()
        ]
    total = len(filtered)
    filtered = _sort_rows(filtered, sort_by, sort_direction)
    safe_limit, safe_offset = min(max(int(limit), 1), 500), max(int(offset), 0)
    return {
        "as_of": as_of,
        "total": total,
        "limit": safe_limit,
        "offset": safe_offset,
        "rows": filtered[safe_offset : safe_offset + safe_limit],
    }


def get_stage1_summary(project_root: Path | str) -> dict[str, Any]:
    rows, as_of = _latest_rows(project_root)
    active = [
        r
        for r in rows
        if r.get("stage1_lifecycle_state") in ACTIVE_STATES
        and r["operator_status"] != "BLOCKED"
    ]
    transitions = (
        get_stage1_transitions(project_root, trade_date=as_of, limit=1000)["rows"]
        if as_of
        else []
    )
    counts = {
        state: sum(r.get("stage1_lifecycle_state") == state for r in rows)
        for state in ACTIVE_STATES | TERMINAL_STATES
    }
    progress_states = {
        "ACCUMULATING",
        "LATE_STAGE1",
        "BREAKOUT_READY",
        "PROMOTION_PENDING",
    }
    progressions = [
        r
        for r in transitions
        if r.get("to_lifecycle_state") in progress_states
        and r.get("from_lifecycle_state")
    ]
    discoveries = [r for r in transitions if not r.get("from_lifecycle_state")]
    regressions = [r for r in transitions if r.get("to_lifecycle_state") == "REGRESSED"]

    def top(field: str, reverse: bool, n: int = 5) -> list[dict[str, Any]]:
        valid = [r for r in active if _number(r.get(field)) is not None]
        return sorted(valid, key=lambda r: _number(r.get(field)) or 0, reverse=reverse)[
            :n
        ]

    return {
        "as_of": as_of,
        "active_count": len(active),
        "base_building_count": counts["BASE_BUILDING"],
        "accumulating_count": counts["ACCUMULATING"],
        "late_stage1_count": counts["LATE_STAGE1"],
        "breakout_ready_count": counts["BREAKOUT_READY"],
        "promotion_pending_count": counts["PROMOTION_PENDING"],
        "regressed_count": counts["REGRESSED"],
        "stale_count": counts["STALE_BASE"],
        "invalidated_today": counts["INVALIDATED"],
        "new_discoveries_today": len(discoveries),
        "progressions_today": len(progressions),
        "regressions_today": len(regressions),
        "top_emerging_candidates": top("stage1_emerging_rank", False),
        "top_score_improvers": top("stage1_score_delta_20d", True),
        "top_rank_improvers": top("emerging_rank_improvement_20d", True),
    }


def get_stage1_transitions(
    project_root: Path | str, *, trade_date: str | None = None, limit: int = 200
) -> dict[str, Any]:
    frame, as_of = _table_frame(
        project_root, "investigator_stage1_transition", trade_date
    )
    if frame.empty:
        return {"as_of": as_of, "rows": []}
    frame = _dedupe_transitions(frame)
    frame = frame.sort_values(
        ["trade_date", "created_at"] if "created_at" in frame else ["trade_date"],
        ascending=False,
    )
    return {"as_of": as_of, "rows": _records(frame.head(min(max(limit, 1), 1000)))}


def get_stage1_exits(
    project_root: Path | str, *, trade_date: str | None = None, limit: int = 200
) -> dict[str, Any]:
    rows, as_of = _latest_rows(project_root, trade_date=trade_date)
    exits = [
        r
        for r in rows
        if r.get("stage1_lifecycle_state")
        in TERMINAL_STATES | {"REGRESSED", "STALE_BASE"}
    ]
    return {
        "as_of": as_of,
        "rows": _sort_rows(exits, None, "asc")[: min(max(limit, 1), 1000)],
    }


def get_stage1_detail(
    symbol_id: str, lookback_days: int, project_root: Path | str
) -> dict[str, Any]:
    symbol = str(symbol_id or "").strip().upper()
    if not symbol:
        return {
            "symbol_id": symbol,
            "current": None,
            "state": [],
            "transitions": [],
            "histories": {},
        }
    state, _ = _table_frame(
        project_root,
        "investigator_stage1_state",
        None,
        symbol=symbol,
        latest_only=False,
    )
    transitions, _ = _table_frame(
        project_root,
        "investigator_stage1_transition",
        None,
        symbol=symbol,
        latest_only=False,
    )
    transitions = _dedupe_transitions(transitions)
    if not state.empty and "trade_date" in state:
        cutoff = pd.to_datetime(state["trade_date"].max()) - timedelta(
            days=max(0, lookback_days)
        )
        state = state[pd.to_datetime(state["trade_date"]) >= cutoff].sort_values(
            "trade_date", ascending=False
        )
    if not transitions.empty and "trade_date" in transitions and not state.empty:
        transitions = transitions[
            pd.to_datetime(transitions["trade_date"]) >= cutoff
        ].sort_values("trade_date", ascending=False)
    state_rows = [_derive_operator(row) for row in _records(state)]
    transition_rows = _records(transitions)
    current = state_rows[0] if state_rows else None
    chronological = list(reversed(state_rows))
    return {
        "symbol_id": symbol,
        "current": current,
        "state": state_rows,
        "transitions": transition_rows,
        "histories": {
            "score": [
                {"date": r.get("trade_date"), "value": r.get("stage1_maturity_score")}
                for r in chronological
            ],
            "rank": [
                {"date": r.get("trade_date"), "value": r.get("stage1_emerging_rank")}
                for r in chronological
            ],
            "golden_cross": [
                {"date": r.get("trade_date"), "value": r.get("golden_cross_status")}
                for r in chronological
            ],
            "pattern_promotion": [
                {"date": r.get("trade_date"), "value": r.get("pattern_promotion_state")}
                for r in chronological
            ],
        },
    }


def _latest_rows(
    project_root: Path | str, trade_date: str | None = None
) -> tuple[list[dict[str, Any]], str | None]:
    state, as_of = _table_frame(project_root, "investigator_stage1_state", trade_date)
    if state.empty:
        return [], as_of
    scores, _ = _table_frame(project_root, "investigator_scores", as_of)
    score_fields = [
        "symbol_id",
        "sector",
        "close",
        "price_structure_score",
        "volume_delivery_score",
        "sector_support_score",
        "buyer_fingerprint_score",
        "ranking_overlay_score",
        "final_score",
        "verdict",
    ]
    if not scores.empty:
        available = [c for c in score_fields if c in scores]
        state = state.merge(
            scores[available].drop_duplicates("symbol_id", keep="last"),
            on="symbol_id",
            how="left",
            suffixes=("", "_investigator"),
        )
    transitions, _ = _table_frame(
        project_root, "investigator_stage1_transition", None, latest_only=False
    )
    transitions = _dedupe_transitions(transitions)
    latest_transition: dict[str, dict[str, Any]] = {}
    for row in _records(
        transitions.sort_values("trade_date") if not transitions.empty else transitions
    ):
        latest_transition[str(row.get("symbol_id"))] = row
    rows = []
    for row in _records(state):
        transition = latest_transition.get(str(row.get("symbol_id")), {})
        row["latest_transition"] = transition or None
        row["latest_transition_type"] = transition.get("transition_type")
        row["latest_transition_summary"] = transition.get("transition_summary")
        rows.append(_derive_operator(row))
    return rows, as_of


def _table_frame(
    project_root: Path | str,
    table: str,
    trade_date: str | None,
    *,
    symbol: str | None = None,
    latest_only: bool = True,
) -> tuple[pd.DataFrame, str | None]:
    allowed = {
        "investigator_stage1_state",
        "investigator_stage1_transition",
        "investigator_scores",
    }
    if table not in allowed:
        raise ValueError("unsupported table")
    db = (
        get_domain_paths(project_root=project_root, data_domain="operational").root_dir
        / "control_plane.duckdb"
    )
    if not db.exists():
        return pd.DataFrame(), trade_date
    try:
        with duckdb.connect(str(db), read_only=True) as conn:
            tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
            if table not in tables:
                return pd.DataFrame(), trade_date
            as_of = trade_date
            if latest_only and not as_of:
                result = conn.execute(
                    f"SELECT MAX(trade_date) FROM {table}"
                ).fetchone()  # trusted constant
                as_of = str(result[0]) if result and result[0] else None
            clauses, params = [], []
            if latest_only and as_of:
                clauses.append("trade_date = CAST(? AS DATE)")
                params.append(as_of)
            if symbol:
                clauses.append("UPPER(symbol_id) = ?")
                params.append(symbol)
            where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
            frame = conn.execute(
                f"SELECT * FROM {table}{where}", params
            ).fetchdf()  # trusted table/clauses
            return frame, as_of
    except Exception:
        return pd.DataFrame(), trade_date


def _dedupe_transitions(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    keys = [
        column
        for column in (
            "symbol_id",
            "trade_date",
            "from_lifecycle_state",
            "to_lifecycle_state",
            "transition_type",
        )
        if column in frame.columns
    ]
    if not keys:
        return frame.copy()
    order = [column for column in ("attempt_number", "created_at") if column in frame.columns]
    safe = frame.sort_values(order, na_position="first", kind="stable") if order else frame.copy()
    return safe.drop_duplicates(keys, keep="last").reset_index(drop=True)


def _derive_operator(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    lifecycle = str(out.get("stage1_lifecycle_state") or "").upper()
    evaluation = str(out.get("stage1_evaluation_status") or "").upper()
    eligible = _truth(out.get("stage1_eligible"))
    promotion = _truth(out.get("promotion_eligibility"))
    rank = _number(out.get("stage1_emerging_rank"))
    pivot = _number(out.get("distance_to_pivot_pct"))
    qualifying_late = (
        lifecycle == "LATE_STAGE1"
        and promotion
        and rank is not None
        and rank <= POLICY.late_stage_rank_cutoff
        and pivot is not None
        and pivot <= POLICY.late_stage_pivot_distance_pct
    )
    if evaluation == "DATA_PENDING":
        status = "DATA_PENDING"
    elif not eligible or evaluation == "STRUCTURALLY_BLOCKED":
        status = "BLOCKED"
    elif lifecycle == "REGRESSED":
        status = "REGRESSED"
    elif lifecycle == "STALE_BASE":
        status = "STALE"
    elif lifecycle == "PROMOTION_PENDING":
        status = "ACT_NOW"
    elif lifecycle == "BREAKOUT_READY" or qualifying_late:
        status = "WATCH_CLOSELY"
    elif lifecycle in {"ACCUMULATING", "LATE_STAGE1"}:
        status = "DEVELOPING"
    else:
        status = "MONITOR"
    action = {
        "DATA_PENDING": "REVIEW_DATA",
        "BLOCKED": "NO_ACTION",
        "REGRESSED": "REVIEW_REGRESSION",
        "STALE": "NO_ACTION",
        "ACT_NOW": "CHECK_BREAKOUT",
        "WATCH_CLOSELY": "WATCH_CLOSELY",
        "DEVELOPING": "MONITOR",
        "MONITOR": "NO_ACTION",
    }[status]
    advanced_previous = str(out.get("stage1_previous_lifecycle_state") or "") in {
        "LATE_STAGE1",
        "BREAKOUT_READY",
        "PROMOTION_PENDING",
    }
    if lifecycle == "PROMOTION_PENDING":
        priority = "CRITICAL"
    elif (
        lifecycle == "BREAKOUT_READY"
        or qualifying_late
        or (status in {"REGRESSED", "DATA_PENDING"} and advanced_previous)
    ):
        priority = "HIGH"
    elif (
        lifecycle in {"LATE_STAGE1", "ACCUMULATING", "REGRESSED"}
        or status == "DATA_PENDING"
    ):
        priority = "MEDIUM"
    else:
        priority = "LOW"
    out.update(
        {
            "operator_status": status,
            "operator_action": action,
            "operator_priority": priority,
            "operator_reason": _operator_reason(out, status),
            "operator_queue_eligible": priority in {"CRITICAL", "HIGH"}
            or status in {"REGRESSED", "DATA_PENDING"},
        }
    )
    return out


def _operator_reason(row: dict[str, Any], status: str) -> str:
    if status == "DATA_PENDING":
        return "Evaluation paused because Stage-1 inputs are incomplete"
    if status == "BLOCKED":
        codes = _codes(row.get("stage1_block_reasons"))
        return (
            f"Stage-1 evaluation blocked: {', '.join(codes[:2]).replace('_', ' ').lower()}"
            if codes
            else "Stage-1 structural eligibility is not met"
        )
    lifecycle = (
        str(row.get("stage1_lifecycle_state") or "Stage-1").replace("_", " ").title()
    )
    facts: list[str] = []
    score = _number(row.get("stage1_score_delta_20d"))
    rank = _number(row.get("emerging_rank_improvement_20d"))
    pivot = _number(row.get("distance_to_pivot_pct"))
    gc = str(row.get("golden_cross_status") or "").upper()
    pattern = str(row.get("pattern_promotion_state") or "").upper()
    if score is not None and abs(score) >= 1:
        facts.append(f"score {'+' if score > 0 else ''}{score:.1f} over 20D")
    if rank is not None and abs(rank) >= 1:
        facts.append(
            f"rank {'improved' if rank > 0 else 'weakened'} {abs(rank):.0f} over 20D"
        )
    if pivot is not None:
        facts.append(f"{pivot:.1f}% from pivot")
    if gc in {"APPROACHING", "IMMINENT", "CROSSED_RECENTLY"}:
        facts.append(f"Golden Cross {gc.replace('_', ' ').lower()}")
    if pattern in {"CONFIRMED", "BREAKOUT_ATTEMPT", "PENDING_3D"}:
        facts.append(f"pattern {pattern.replace('_', ' ').lower()}")
    lead = "Regressed" if status == "REGRESSED" else lifecycle
    return f"{lead}: {', '.join(facts[:3])}" if facts else lead


def _sort_rows(
    rows: list[dict[str, Any]], sort_by: str | None, direction: str
) -> list[dict[str, Any]]:
    reverse = direction.lower() == "desc"
    if sort_by in ALLOWED_SORTS:
        if sort_by == "operator_priority":
            return sorted(
                rows,
                key=lambda r: (
                    PRIORITY_ORDER.get(str(r.get(sort_by)), 99),
                    _number(r.get("stage1_emerging_rank")) or 10**9,
                ),
                reverse=reverse,
            )
        return sorted(
            rows,
            key=lambda r: (
                _number(r.get(sort_by)) is None,
                _number(r.get(sort_by)) or 0,
            ),
            reverse=reverse,
        )
    return sorted(
        rows,
        key=lambda r: (
            LIFECYCLE_ORDER.get(str(r.get("stage1_lifecycle_state")), 99),
            _number(r.get("stage1_emerging_rank")) or 10**9,
        ),
    )


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    safe = (
        frame.loc[:, ~frame.columns.duplicated()]
        .copy()
        .astype(object)
        .where(pd.notna(frame), None)
    )
    return [
        {str(k): _json_safe(v) for k, v in row.items()}
        for row in safe.to_dict(orient="records")
    ]


def _json_safe(value: Any) -> Any:
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, TypeError):
            pass
    return value


def _truth(value: Any) -> bool:
    return value is True or str(value or "").strip().lower() in {
        "true",
        "1",
        "yes",
        "y",
    }


def _number(value: Any) -> float | None:
    try:
        result = float(value)
        return result if pd.notna(result) else None
    except (TypeError, ValueError):
        return None


def _codes(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    try:
        return [str(v) for v in json.loads(value or "[]")]
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
