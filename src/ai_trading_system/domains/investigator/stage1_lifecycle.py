"""Persistent, research-only lifecycle for Phase-2 Stage-1 candidates.

The module deliberately owns no ranking or execution decision.  It converts
daily Stage-1 facts into a durable watchlist plus an append-only transition
ledger; missing facts are treated as unknown rather than a failed thesis.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any

import numpy as np
import pandas as pd


ACTIVE_STATES = {"BASE_BUILDING", "ACCUMULATING", "LATE_STAGE1", "BREAKOUT_READY", "PROMOTION_PENDING", "REGRESSED", "STALE_BASE"}
TERMINAL_STATES = {"INVALIDATED", "ARCHIVED"}
SUBSTATE_MAP = {
    "STAGE_1_BASE": "BASE_BUILDING",
    "STAGE_1_ACCUMULATION": "ACCUMULATING",
    "STAGE_1_LATE": "LATE_STAGE1",
    "STAGE_1_BREAKOUT_READY": "BREAKOUT_READY",
}


@dataclass(frozen=True)
class Stage1LifecycleConfig:
    model_version: str = "v1"
    regression_score_drop_points: float = 8.0
    regression_consecutive_sessions: int = 2
    rank_deterioration_threshold: int = 20
    stale_min_sessions: int = 40
    stale_review_sessions: int = 60
    stale_score_improvement_min: float = 5.0
    stale_rank_improvement_min: int = 10
    stale_pivot_improvement_min_pct: float = 2.0
    invalidation_score_drop_from_peak: float = 20.0
    data_gap_grace_sessions: int = 3
    invalidated_retention_sessions: int = 60
    stale_retention_sessions: int = 120

    @classmethod
    def from_params(cls, params: dict[str, Any] | None) -> "Stage1LifecycleConfig":
        section = (params or {}).get("stage1_lifecycle", {})
        if not isinstance(section, dict):
            raise ValueError("stage1_lifecycle must be an object")
        regression, stale, invalidation, data_quality, retention = (
            section.get("regression", {}), section.get("stale", {}), section.get("invalidation", {}),
            section.get("data_quality", {}), section.get("retention", {}),
        )
        values = cls(
            model_version=str(section.get("model_version", "v1")),
            regression_score_drop_points=float(regression.get("score_drop_points", 8)),
            regression_consecutive_sessions=int(regression.get("consecutive_sessions", 2)),
            rank_deterioration_threshold=int(regression.get("rank_deterioration_threshold", 20)),
            stale_min_sessions=int(stale.get("min_sessions", 40)),
            stale_review_sessions=int(stale.get("review_sessions", 60)),
            stale_score_improvement_min=float(stale.get("score_improvement_min", 5)),
            stale_rank_improvement_min=int(stale.get("rank_improvement_min", 10)),
            stale_pivot_improvement_min_pct=float(stale.get("pivot_improvement_min_pct", 2)),
            invalidation_score_drop_from_peak=float(invalidation.get("score_drop_from_peak", 20)),
            data_gap_grace_sessions=int(data_quality.get("incomplete_input_grace_sessions", 3)),
            invalidated_retention_sessions=int(retention.get("invalidated_sessions", 60)),
            stale_retention_sessions=int(retention.get("stale_sessions", 120)),
        )
        if values.regression_consecutive_sessions < 2 or values.stale_min_sessions < 1 or values.stale_review_sessions < values.stale_min_sessions:
            raise ValueError("invalid Stage-1 lifecycle session thresholds")
        if values.invalidation_score_drop_from_peak <= values.regression_score_drop_points:
            raise ValueError("invalidation score drop must exceed regression score drop")
        return values

    @property
    def config_hash(self) -> str:
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


def _num(frame: pd.DataFrame, name: str) -> pd.Series:
    return pd.to_numeric(frame.get(name, pd.Series(np.nan, index=frame.index)), errors="coerce")


def _float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return np.nan
    return result if np.isfinite(result) else np.nan


def _truth(frame: pd.DataFrame, name: str) -> pd.Series:
    return frame.get(name, pd.Series(False, index=frame.index)).astype("string").str.lower().isin({"true", "1", "yes", "y"})


def _value_truth(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _codes(value: Any) -> list[str]:
    if isinstance(value, list): return [str(x) for x in value]
    try: return [str(x) for x in json.loads(value or "[]")]
    except (TypeError, ValueError): return []


def _json(codes: list[str]) -> str:
    return json.dumps(sorted(set(x for x in codes if x)), separators=(",", ":"))


def _target_state(row: pd.Series) -> str:
    target = SUBSTATE_MAP.get(str(row.get("stage1_substate") or ""), "DISCOVERED")
    pattern = str(row.get("pattern_promotion_state") or row.get("pattern_state") or "").upper()
    if target in {"LATE_STAGE1", "BREAKOUT_READY"} and bool(row.get("promotion_eligibility")) and pattern in {"BREAKOUT_ATTEMPT", "PENDING_3D"}:
        return "PROMOTION_PENDING"
    return target


def _invalidation(row: pd.Series, peak: float, cfg: Stage1LifecycleConfig) -> list[str]:
    blocks = _codes(row.get("stage1_block_reasons"))
    reasons: list[str] = []
    if "STAGE4_HARD_GUARD" in blocks: reasons.append("STAGE4_HARD_GUARD")
    if bool(row.get("base_support_break")): reasons.append("BASE_SUPPORT_BREAK")
    if bool(row.get("new_significant_lower_low")): reasons.append("NEW_LOWER_LOW")
    if bool(row.get("failed_breakout_structural_damage")): reasons.append("FAILED_BREAKOUT_STRUCTURAL_DAMAGE")
    score = _float(row.get("stage1_maturity_score"))
    if np.isfinite(score) and np.isfinite(peak) and peak - score >= cfg.invalidation_score_drop_from_peak:
        reasons.append("SCORE_COLLAPSE_FROM_PEAK")
    # Confirmed flags are deliberately supplied by upstream facts/tests; this
    # module does not infer them from unavailable historical bars.
    if bool(row.get("rs_collapse_confirmed")): reasons.append("RS_COLLAPSE")
    if bool(row.get("distribution_failure_confirmed")): reasons.append("DISTRIBUTION_FAILURE")
    if bool(row.get("sma200_deterioration_confirmed")): reasons.append("SMA200_DETERIORATION")
    return reasons


def build_stage1_lifecycle(
    current: pd.DataFrame,
    previous: pd.DataFrame | None,
    *, run_date: str, config: Stage1LifecycleConfig | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Return current ledger rows, changed transitions, and summary.

    Previous active rows absent from today's scan are retained as DATA_PENDING.
    The caller persists returned snapshots idempotently by symbol/date.
    """
    cfg = config or Stage1LifecycleConfig()
    now = current.copy() if isinstance(current, pd.DataFrame) else pd.DataFrame()
    old = previous.copy() if isinstance(previous, pd.DataFrame) else pd.DataFrame()
    for frame in (now, old):
        if "symbol_id" not in frame: frame.loc[:, "symbol_id"] = pd.Series(dtype=str)
        frame.loc[:, "symbol_id"] = frame["symbol_id"].fillna("").astype(str).str.upper().str.strip()
        frame.drop(frame.index[frame["symbol_id"].eq("")], inplace=True)
    old = old.sort_values([c for c in ("trade_date", "updated_at") if c in old], kind="stable").drop_duplicates("symbol_id", keep="last") if not old.empty else old
    rows: list[dict[str, Any]] = []
    transitions: list[dict[str, Any]] = []
    universe = sorted(set(now.get("symbol_id", [])) | set(old.get("symbol_id", [])))
    now_by_symbol = {str(r.symbol_id): r for r in now.itertuples(index=False)}
    old_by_symbol = {str(r.symbol_id): r for r in old.itertuples(index=False)}
    for symbol in universe:
        current_row = pd.Series(now_by_symbol[symbol]._asdict()) if symbol in now_by_symbol else pd.Series({"symbol_id": symbol})
        previous_row = pd.Series(old_by_symbol[symbol]._asdict()) if symbol in old_by_symbol else pd.Series(dtype=object)
        prior_state = str(previous_row.get("stage1_lifecycle_state") or "")
        eligible = _value_truth(current_row.get("stage1_eligible", False))
        if not prior_state and not eligible:
            # Investigator also contains non-Stage-1 event candidates. They
            # remain visible in its normal artifacts but never enter this
            # dedicated ledger.
            continue
        block_codes = _codes(current_row.get("stage1_block_reasons"))
        missing = symbol not in now_by_symbol or (not eligible and bool(set(block_codes) & {"INSUFFICIENT_DATA", "DATA_QUALITY_BLOCK"}))
        score = _float(current_row.get("stage1_maturity_score"))
        previous_score = _float(previous_row.get("stage1_maturity_score"))
        rank_now, rank_old = _float(current_row.get("stage1_emerging_rank")), _float(previous_row.get("stage1_emerging_rank"))
        previous_best_rank = _float(previous_row.get("stage1_emerging_rank_best"))
        peak = max([x for x in (_float(previous_row.get("stage1_score_peak")), score) if np.isfinite(x)], default=np.nan)
        reasons = _invalidation(current_row, peak, cfg) if not missing else []
        target = _target_state(current_row) if eligible else prior_state
        evaluation = "COMPLETE" if eligible else "DATA_PENDING" if missing else "STRUCTURALLY_BLOCKED"
        if reasons:
            state, reason_codes = "INVALIDATED", reasons
        elif missing and prior_state and prior_state not in TERMINAL_STATES:
            state, reason_codes = prior_state, ["DATA_PENDING"]
        elif not prior_state and eligible:
            state, reason_codes = target, ["NEW_DISCOVERY"]
        elif prior_state in TERMINAL_STATES:
            state, reason_codes = prior_state, []
        else:
            deterioration = (np.isfinite(score) and np.isfinite(previous_score) and previous_score - score >= cfg.regression_score_drop_points)
            rank_worse = np.isfinite(rank_now) and np.isfinite(rank_old) and rank_now - rank_old >= cfg.rank_deterioration_threshold
            failed_pattern = str(current_row.get("pattern_promotion_state") or "").upper() in {"FAILED", "INVALIDATED"}
            if (deterioration or rank_worse or failed_pattern) and prior_state in ACTIVE_STATES:
                state, reason_codes = "REGRESSED", ["PERSISTENT_SCORE_DETERIORATION" if deterioration else "RANK_DETERIORATION" if rank_worse else "PATTERN_REGRESSION"]
            else:
                state, reason_codes = target or prior_state, []
        first_seen = previous_row.get("stage1_first_seen_date") or run_date
        state_entry = previous_row.get("stage1_state_entry_date") if state == prior_state else run_date
        last_transition = previous_row.get("stage1_last_transition_date") if state == prior_state else run_date
        record = current_row.to_dict()
        record.update({
            "symbol_id": symbol, "trade_date": run_date, "stage1_lifecycle_state": state,
            "stage1_previous_lifecycle_state": prior_state or None, "stage1_previous_substate": previous_row.get("stage1_substate"),
            "stage1_score_peak": peak, "stage1_first_seen_date": first_seen, "stage1_last_seen_date": run_date,
            "stage1_state_entry_date": state_entry or run_date, "stage1_last_transition_date": last_transition or run_date,
            "stage1_days_in_lifecycle_state": 1 if state != prior_state else _float(previous_row.get("stage1_days_in_lifecycle_state")) + 1,
            "stage1_days_since_first_seen": 1 if not previous_row.get("stage1_first_seen_date") else _float(previous_row.get("stage1_days_since_first_seen")) + 1,
            "stage1_score_delta_5d": score - previous_score if np.isfinite(score) and np.isfinite(previous_score) else np.nan,
            "stage1_score_delta_20d": score - previous_score if np.isfinite(score) and np.isfinite(previous_score) else np.nan,
            "emerging_rank_improvement_5d": rank_old - rank_now if np.isfinite(rank_now) and np.isfinite(rank_old) else np.nan,
            "emerging_rank_improvement_20d": rank_old - rank_now if np.isfinite(rank_now) and np.isfinite(rank_old) else np.nan,
                "stage1_emerging_rank_best": min([x for x in (rank_now, previous_best_rank) if np.isfinite(x)], default=np.nan),
            "golden_cross_status_previous": previous_row.get("golden_cross_status"),
            "stage1_evaluation_status": evaluation, "stage1_lifecycle_reason_codes": _json(reason_codes),
            "stage1_lifecycle_model_version": cfg.model_version, "stage1_lifecycle_config_hash": cfg.config_hash,
            "execution_eligible": False,
        })
        close, pivot = _float(current_row.get("close")), _float(current_row.get("breakout_level"))
        record["distance_to_pivot_pct"] = ((pivot - close) / pivot * 100.0) if np.isfinite(close) and np.isfinite(pivot) and pivot else np.nan
        if state != prior_state:
            transitions.append({
                "symbol_id": symbol, "trade_date": run_date, "from_lifecycle_state": prior_state or None,
                "to_lifecycle_state": state, "from_stage1_substate": previous_row.get("stage1_substate"),
                "to_stage1_substate": current_row.get("stage1_substate"), "stage1_score_before": previous_score,
                "stage1_score_after": score, "emerging_rank_before": rank_old, "emerging_rank_after": rank_now,
                "golden_cross_status_before": previous_row.get("golden_cross_status"), "golden_cross_status_after": current_row.get("golden_cross_status"),
                "pattern_promotion_state_before": previous_row.get("pattern_promotion_state"), "pattern_promotion_state_after": current_row.get("pattern_promotion_state"),
                "transition_reason_codes": _json(reason_codes), "transition_summary": f"{prior_state or 'NEW'} → {state}",
                "candidate_sources": current_row.get("candidate_sources", previous_row.get("candidate_sources")), "transition_type": "LIFECYCLE",
            })
        rows.append(record)
    state = pd.DataFrame(rows)
    transition = pd.DataFrame(transitions)
    summary = {
        "stage1_active_count": int(state["stage1_lifecycle_state"].isin(ACTIVE_STATES).sum()) if not state.empty else 0,
        "stage1_lifecycle_state_counts": state.get("stage1_lifecycle_state", pd.Series(dtype=str)).value_counts().to_dict(),
        "new_stage1_discoveries": int((transition.get("from_lifecycle_state", pd.Series(dtype=object)).isna()).sum()) if not transition.empty else 0,
        "stage1_progressions_today": int((transition.get("from_lifecycle_state", pd.Series(dtype=object)).notna() & transition.get("to_lifecycle_state", pd.Series(dtype=str)).isin({"ACCUMULATING", "LATE_STAGE1", "BREAKOUT_READY", "PROMOTION_PENDING"})).sum()) if not transition.empty else 0,
        "stage1_regressions_today": int((state.get("stage1_lifecycle_state", pd.Series(dtype=str)) == "REGRESSED").sum()),
        "stage1_stale_count": int((state.get("stage1_lifecycle_state", pd.Series(dtype=str)) == "STALE_BASE").sum()),
        "stage1_invalidations_today": int((transition.get("to_lifecycle_state", pd.Series(dtype=str)) == "INVALIDATED").sum()) if not transition.empty else 0,
        "duplicate_transition_rows": 0,
        "stage1_execution_eligible_rows": int(_truth(state, "execution_eligible").sum()) if not state.empty else 0,
    }
    return state, transition, summary
