"""Decision-board payload builder for Investigator artifacts."""

from __future__ import annotations

from datetime import date, datetime
from math import isfinite
from typing import Any

import pandas as pd


S1_PATTERN_SETUP_SCORE = {
    "FAILED_S1": 0,
    "S1_BASE_FORMING": 25,
    "S1_ACCUMULATION": 45,
    "S1_NEAR_BREAKOUT": 65,
    "S1_TO_S2_TRANSITION": 80,
    "S2_CONFIRMED": 100,
}


def build_investigator_payload(
    *,
    run_id: str,
    run_date: str,
    summary: dict[str, Any] | None,
    today_gainers: pd.DataFrame,
    scores: pd.DataFrame,
    repeat_tracker: pd.DataFrame,
    active_watchlist: pd.DataFrame,
    trap_log: pd.DataFrame,
    archive: pd.DataFrame,
    final_3q_gate: pd.DataFrame | None = None,
    investigator_pattern_scan: pd.DataFrame | None = None,
    performance_summary: dict[str, Any] | None = None,
    threshold_recommendations: dict[str, Any] | None = None,
    previous_summary: dict[str, Any] | None = None,
    data_trust_status: str = "unknown",
    stage_status: dict[str, str] | None = None,
) -> dict[str, Any]:
    summary = summary or {}
    stage_status = stage_status or {}
    active = _attach_decision_fields(active_watchlist, repeat_tracker)
    traps = _attach_trap_category(trap_log)
    archive_with_traps = _attach_trap_category(archive)
    enriched_scores = _attach_decision_fields(scores, repeat_tracker)
    high = enriched_scores.loc[_text(enriched_scores, "decision_verdict").eq("High Conviction")].copy()

    decision_queue = _records(
        _sort_for_records(active, ["investigator_score", "symbol_id"], [False, True]),
        limit=20,
    )
    closest = _records(
        _sort_for_records(
            enriched_scores.loc[~_text(enriched_scores, "decision_verdict").eq("High Conviction")],
            ["investigator_score", "symbol_id"],
            [False, True],
        ),
        limit=5,
    )
    repeat_quality = _records(_repeat_quality(repeat_tracker), limit=20)
    trap_radar = _trap_radar(pd.concat([traps, archive_with_traps], ignore_index=True))
    archive_today = _records(_archive_today(archive_with_traps, run_date), limit=25)
    evidence = pd.concat([traps, archive_with_traps], ignore_index=True)
    pattern_confirmation = _pattern_confirmation(investigator_pattern_scan)
    decision_summary = _decision_summary(
        summary=summary,
        today_gainers=today_gainers,
        active=active,
        repeat_tracker=repeat_tracker,
        high=high,
        traps=traps,
        archive=archive,
        trap_evidence=evidence,
        run_date=run_date,
    )
    return {
        "run_id": run_id or str(summary.get("run_id") or ""),
        "run_date": run_date or str(summary.get("run_date") or ""),
        "data_trust_status": data_trust_status,
        "stage_status": {
            "rank": stage_status.get("rank", "unknown"),
            "investigator": stage_status.get("investigator", str(summary.get("status") or "unknown")),
            "publish": stage_status.get("publish", "unknown"),
        },
        "summary": decision_summary,
        "pattern_confirmation": pattern_confirmation,
        "summary_deltas": _summary_deltas(decision_summary, previous_summary) if previous_summary else {},
        "decision_queue": decision_queue,
        "final_3q_gate": _records(final_3q_gate if final_3q_gate is not None else pd.DataFrame(), limit=50),
        "performance_summary": performance_summary or {},
        "threshold_recommendations": threshold_recommendations or {},
        "closest_to_high_conviction": [] if len(high) > 0 else closest,
        "repeat_quality": repeat_quality,
        "trap_radar": trap_radar,
        "archive_today": archive_today,
        "charts": {
            "funnel": _funnel(decision_summary),
            "funnel_today": _funnel_today(decision_summary),
            "funnel_window": _funnel_window(decision_summary),
            "repeat_price_scatter": _repeat_price_scatter(active, repeat_tracker),
            "four_week_trend": _four_week_trend(active, traps, archive_with_traps),
            "trend": _trend(active, traps, archive_with_traps),
        },
        "row_details": _row_details(active, repeat_tracker, traps, archive_with_traps),
    }


def _attach_decision_fields(frame: pd.DataFrame, repeat_tracker: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy() if frame is not None else pd.DataFrame()
    if out.empty:
        return out
    if "symbol_id" in out.columns and repeat_tracker is not None and not repeat_tracker.empty and "symbol_id" in repeat_tracker.columns:
        repeat_cols = [
            "symbol_id",
            "repeat_score",
            "appearance_count_20d",
            "price_progression_pct",
            "rank_change_20d",
            "volume_escalation",
            "high_priority_repeat",
            "first_seen_date",
            "last_seen_date",
        ]
        available = [col for col in repeat_cols if col in repeat_tracker.columns]
        out = out.merge(repeat_tracker[available], on="symbol_id", how="left", suffixes=("", "_repeat"))
    out.loc[:, "investigator_score"] = _investigator_score(out)
    out.loc[:, "trap_category"] = _trap_category_series(out)
    out.loc[:, "decision_verdict"] = out.apply(_decision_verdict, axis=1)
    out.loc[:, "decision_reason"] = out.apply(_decision_reason, axis=1)
    out.loc[:, "volume_signal"] = out.apply(_volume_signal, axis=1)
    out.loc[:, "rank_signal"] = out.apply(_rank_signal, axis=1)
    out.loc[:, "setup"] = _setup_series(out)
    return out


def _attach_trap_category(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy() if frame is not None else pd.DataFrame()
    if out.empty:
        return out
    out.loc[:, "trap_category"] = _trap_category_series(out)
    return out


def _investigator_score(frame: pd.DataFrame) -> pd.Series:
    repeat = _clip(_num(frame, "repeat_score"), 0, 100)
    price = _clip((_num(frame, "price_progression_pct").fillna(_num(frame, "price_vs_first_trigger_pct")) + 20) * 2.5, 0, 100)
    rank = _clip(50 - _num(frame, "rank_change_20d") * 1.5, 0, 100)
    volume = _clip(_num(frame, "volume_delivery_score") * 5, 0, 100)
    sector = _clip(_num(frame, "sector_support_score") * 10, 0, 100)
    move_setup = _clip(_num(frame, "trigger_quality_score") * 5, 0, 100)
    pattern_setup = _pattern_setup_score(frame)
    setup = pd.concat([move_setup, pattern_setup], axis=1).max(axis=1)
    trap_penalty = _trap_penalty(frame)
    score = 0.25 * repeat + 0.20 * price + 0.20 * rank + 0.15 * volume + 0.10 * sector + 0.10 * setup - 0.20 * trap_penalty
    return score.clip(lower=0, upper=100).round(1)


def _pattern_setup_score(frame: pd.DataFrame) -> pd.Series:
    if "s1_promotion_state" not in frame.columns:
        return pd.Series(0.0, index=frame.index)
    return (
        frame["s1_promotion_state"]
        .fillna("")
        .astype(str)
        .str.upper()
        .map(S1_PATTERN_SETUP_SCORE)
        .fillna(0.0)
        .astype(float)
    )


def _decision_verdict(row: pd.Series) -> str:
    if _is_truthy(row.get("hard_trap_flag")) or str(row.get("verdict") or "").upper() == "NOISE_TRAP":
        return "Trap Risk"
    score = _float(row.get("investigator_score"))
    if score >= 80:
        return "High Conviction"
    if score >= 65:
        return "Investigate"
    if score >= 50:
        return "Watch"
    if score >= 35:
        return "Archive Candidate"
    return "Avoid"


def _decision_reason(row: pd.Series) -> str:
    if str(row.get("decision_verdict") or "") == "Trap Risk":
        return str(row.get("trap_category") or "Trap evidence")
    repeat = _float(row.get("appearance_count_20d"))
    price = _float(row.get("price_progression_pct", row.get("price_vs_first_trigger_pct")))
    rank = _float(row.get("rank_change_20d"))
    if repeat >= 3 and price > 0 and rank < 0:
        return "Repeat + price holding"
    if repeat >= 3 and rank >= 0:
        return "Repeat but rank slipping"
    if price < 0:
        return "Price below first seen"
    if _is_truthy(row.get("volume_escalation")):
        return "Volume rising"
    return str(row.get("move_tag") or row.get("trigger_reason") or "Needs review").replace("_", " ").title()


def _trap_category_series(frame: pd.DataFrame) -> pd.Series:
    reason = _text(frame, "drop_reason") + " " + _text(frame, "move_tag") + " " + _text(frame, "verdict")
    price = _num(frame, "price_progression_pct").fillna(_num(frame, "price_vs_first_trigger_pct"))
    rank = _num(frame, "rank_change_20d")
    low_delivery = _bool(frame, "low_delivery_flag")
    volume_declining = _bool(frame, "volume_ratio_declining")
    out = pd.Series("Trap evidence", index=frame.index, dtype=object)
    out = out.mask(reason.str.contains("ONE_CANDLE|OPERATOR|SPIKE", case=False, na=False), "One-day spike")
    out = out.mask(price.lt(0).fillna(False), "Price fade")
    out = out.mask(rank.gt(25).fillna(False), "Rank collapse")
    out = out.mask(volume_declining, "Volume not sustaining")
    out = out.mask(low_delivery | reason.str.contains("LOW_DELIVERY|ILLIQUID|LIQUIDITY", case=False, na=False), "Low delivery / liquidity")
    return out


def _trap_penalty(frame: pd.DataFrame) -> pd.Series:
    penalty = pd.Series(0, index=frame.index, dtype=float)
    penalty = penalty.mask(_text(frame, "verdict").str.upper().eq("NOISE_TRAP"), 90)
    penalty = penalty.mask(_bool(frame, "hard_trap_flag"), 100)
    penalty = penalty.mask(_bool(frame, "low_delivery_flag"), penalty.clip(lower=55))
    penalty = penalty.mask(_num(frame, "price_progression_pct").lt(0).fillna(False), penalty.clip(lower=40))
    return penalty


def _decision_summary(
    *,
    summary: dict[str, Any],
    today_gainers: pd.DataFrame,
    active: pd.DataFrame,
    repeat_tracker: pd.DataFrame,
    high: pd.DataFrame,
    traps: pd.DataFrame,
    archive: pd.DataFrame,
    trap_evidence: pd.DataFrame,
    run_date: str,
) -> dict[str, Any]:
    total_intake = int(summary.get("total_intake_count", len(today_gainers)))
    daily = int(summary.get("daily_gainer_count", total_intake))
    weekly = int(summary.get("weekly_gainer_count", 0))
    stealth = int(summary.get("stealth_accumulation_count", 0))
    repeat_ge3 = int((_num(repeat_tracker, "appearance_count_20d") >= 3).sum()) if not repeat_tracker.empty else 0
    improving = int(((_num(repeat_tracker, "appearance_count_20d") >= 3) & (_num(repeat_tracker, "rank_change_20d") < 0) & (_num(repeat_tracker, "price_progression_pct") > 0)).sum()) if not repeat_tracker.empty else 0
    new_candidates = int((_num(active, "appearance_count_20d").fillna(1) <= 1).sum()) if not active.empty else 0
    traps_count = int(summary.get("trap_count", len(traps)))
    evidence_count = int(len(trap_evidence)) if trap_evidence is not None else traps_count
    fresh_trap_today = _fresh_trap_count(trap_evidence, run_date)
    repeat_trap = _repeat_trap_count(trap_evidence)
    return {
        "total_intake": total_intake,
        "total_intake_count": total_intake,
        "daily_gainers": total_intake,
        "daily_gainer_count": daily,
        "weekly_gainer_count": weekly,
        "stealth_accumulation_count": stealth,
        "new_candidates": new_candidates,
        "new_in_window": new_candidates,
        "active_queue": int(summary.get("active_count", len(active))),
        "repeat_ge3": repeat_ge3,
        "improving_repeats": improving,
        "high_conviction": int(summary.get("high_conviction_count", len(high))),
        "trap_rate": round((traps_count / total_intake) if total_intake else 0.0, 3),
        "traps": traps_count,
        "trap_count": traps_count,
        "trap_evidence_count": evidence_count,
        "fresh_trap_today": fresh_trap_today,
        "repeat_trap": repeat_trap,
        "archived": int(summary.get("archived_count", len(archive))),
    }


def _summary_deltas(current: dict[str, Any], previous: dict[str, Any] | None) -> dict[str, int]:
    previous = previous or {}
    return {
        key: int(value) - int(previous.get(key, previous.get(_legacy_summary_key(key), 0)) or 0)
        for key, value in current.items()
        if key != "trap_rate"
    }


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


def _repeat_quality(repeat_tracker: pd.DataFrame) -> pd.DataFrame:
    if repeat_tracker is None or repeat_tracker.empty:
        return pd.DataFrame()
    out = repeat_tracker.copy()
    out.loc[:, "repeat_strength"] = _clip(_num(out, "repeat_score"), 0, 100)
    out.loc[:, "price_sustain"] = _clip((_num(out, "price_progression_pct") + 20) * 2.5, 0, 100)
    out.loc[:, "rank_momentum"] = _clip(50 - _num(out, "rank_change_20d") * 1.5, 0, 100)
    out.loc[:, "volume_confirmation"] = _bool(out, "volume_escalation").astype(int) * 100
    return out.sort_values(["repeat_strength", "price_sustain", "symbol_id"], ascending=[False, False, True], kind="stable")


def _trap_radar(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty or "trap_category" not in frame.columns:
        return []
    rows: list[dict[str, Any]] = []
    for category, group in frame.groupby("trap_category", sort=True):
        examples = [str(value) for value in group.get("symbol_id", pd.Series(dtype=object)).dropna().head(3).tolist()]
        rows.append({"trap_category": str(category), "count": int(len(group)), "examples": examples})
    return sorted(rows, key=lambda row: (-int(row["count"]), str(row["trap_category"])))


def _pattern_confirmation(pattern_scan: pd.DataFrame | None) -> dict[str, Any]:
    frame = pattern_scan.copy() if pattern_scan is not None else pd.DataFrame()
    if frame.empty:
        scanned_symbols = list(getattr(pattern_scan, "attrs", {}).get("scanned_symbols", []) if pattern_scan is not None else [])
        return {
            "scanned_count": len(scanned_symbols),
            "failed_s1": 0,
            "s1_base_forming": 0,
            "s1_accumulation": 0,
            "s1_near_breakout": 0,
            "s1_to_s2_transition": 0,
            "s2_confirmed": 0,
            "top_setups": [],
        }
    state = _text(frame, "s1_promotion_state").str.upper()
    out = frame.copy()
    out.loc[:, "_state_priority"] = state.map(
        {
            "S2_CONFIRMED": 5,
            "S1_TO_S2_TRANSITION": 4,
            "S1_NEAR_BREAKOUT": 3,
            "S1_ACCUMULATION": 2,
            "S1_BASE_FORMING": 1,
            "FAILED_S1": 0,
        }
    ).fillna(0)
    out.loc[:, "_pattern_score_sort"] = _num(out, "pattern_score").fillna(-1)
    out.loc[:, "_setup_quality_sort"] = _num(out, "setup_quality").fillna(-1)
    top_cols = [
        "symbol_id",
        "pattern_family",
        "pattern_state",
        "pattern_score",
        "setup_quality",
        "s1_promotion_state",
        "promotion_reason",
        "trigger_reason",
        "investigator_status",
    ]
    top = out.sort_values(
        ["_state_priority", "_pattern_score_sort", "_setup_quality_sort", "symbol_id"],
        ascending=[False, False, False, True],
        kind="stable",
    )
    top = top[[col for col in top_cols if col in top.columns]]
    return {
        "scanned_count": int(frame.get("symbol_id", pd.Series(dtype=object)).astype(str).str.upper().nunique()),
        "failed_s1": int(state.eq("FAILED_S1").sum()),
        "s1_base_forming": int(state.eq("S1_BASE_FORMING").sum()),
        "s1_accumulation": int(state.eq("S1_ACCUMULATION").sum()),
        "s1_near_breakout": int(state.eq("S1_NEAR_BREAKOUT").sum()),
        "s1_to_s2_transition": int(state.eq("S1_TO_S2_TRANSITION").sum()),
        "s2_confirmed": int(state.eq("S2_CONFIRMED").sum()),
        "top_setups": _records(top, limit=10),
    }


def _archive_today(frame: pd.DataFrame, run_date: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    if "archived_at" not in frame.columns or not run_date:
        return frame
    archived_date = pd.to_datetime(frame["archived_at"], errors="coerce").dt.date.astype(str)
    return frame.loc[archived_date.eq(str(run_date))].copy()


def _funnel(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"key": "intake", "label": "Investigator Intake", "count": int(summary.get("total_intake", summary.get("daily_gainers", 0)))},
        {"key": "active", "label": "Active Queue", "count": int(summary.get("active_queue", 0))},
        {"key": "repeat", "label": "Repeat >=3x", "count": int(summary.get("repeat_ge3", 0))},
        {"key": "improving", "label": "Improving", "count": int(summary.get("improving_repeats", 0))},
        {"key": "high", "label": "High Conviction", "count": int(summary.get("high_conviction", 0))},
        {"key": "traps", "label": "Trap Count", "count": int(summary.get("trap_count", summary.get("traps", 0)))},
        {"key": "archived", "label": "Archived", "count": int(summary.get("archived", 0))},
    ]


def _funnel_today(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"key": "intake", "label": "Investigator Intake (today)", "count": int(summary.get("total_intake", summary.get("daily_gainers", 0)))},
        {"key": "fresh_traps", "label": "Fresh Traps (today)", "count": int(summary.get("fresh_trap_today", 0))},
        {"key": "high", "label": "High Conviction (today)", "count": int(summary.get("high_conviction", 0))},
    ]


def _funnel_window(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"key": "new_window", "label": "New In Window", "count": int(summary.get("new_in_window", summary.get("new_candidates", 0)))},
        {"key": "active", "label": "Active Queue", "count": int(summary.get("active_queue", 0))},
        {"key": "repeat", "label": "Repeat >=3x", "count": int(summary.get("repeat_ge3", 0))},
        {"key": "improving", "label": "Improving", "count": int(summary.get("improving_repeats", 0))},
        {"key": "repeat_trap", "label": "Repeat Trap", "count": int(summary.get("repeat_trap", 0))},
        {"key": "archived", "label": "Archived", "count": int(summary.get("archived", 0))},
    ]


def _repeat_price_scatter(active: pd.DataFrame, repeat_tracker: pd.DataFrame) -> list[dict[str, Any]]:
    source = active if not active.empty else repeat_tracker
    if source.empty:
        return []
    rows = source.copy()
    return _records(rows.sort_values("symbol_id", kind="stable"), limit=100)


def _four_week_trend(active: pd.DataFrame, traps: pd.DataFrame, archive: pd.DataFrame) -> list[dict[str, Any]]:
    frames = [
        (active, "active"),
        (traps, "traps"),
        (archive, "archived"),
    ]
    counts: dict[str, dict[str, int]] = {}
    for frame, key in frames:
        if frame.empty:
            continue
        date_col = "trade_date" if "trade_date" in frame.columns else "last_seen_date"
        if date_col not in frame.columns:
            continue
        dates = pd.to_datetime(frame[date_col], errors="coerce").dropna().dt.to_period("W").astype(str)
        for week, count in dates.value_counts().items():
            counts.setdefault(str(week), {"week": str(week), "active": 0, "traps": 0, "archived": 0})[key] += int(count)
    return [counts[key] for key in sorted(counts)]


def _trend(active: pd.DataFrame, traps: pd.DataFrame, archive: pd.DataFrame) -> list[dict[str, Any]]:
    counts: dict[str, dict[str, int]] = {}

    def add(frame: pd.DataFrame, metric: str, mask: pd.Series | None = None) -> None:
        if frame.empty:
            return
        date_col = _best_date_column(frame)
        if date_col is None:
            return
        source = frame.loc[mask] if mask is not None else frame
        dates = pd.to_datetime(source[date_col], errors="coerce").dropna().dt.date.astype(str)
        for day, count in dates.value_counts().items():
            bucket = counts.setdefault(
                str(day),
                {"date": str(day), "new": 0, "repeat": 0, "improving": 0, "traps": 0, "archived": 0, "high_conviction": 0},
            )
            bucket[metric] += int(count)

    if not active.empty:
        appearances = _num(active, "appearance_count_20d").fillna(1)
        add(active, "new", appearances <= 1)
        add(active, "repeat", appearances >= 2)
        add(active, "improving", (appearances >= 2) & (_num(active, "rank_change_20d") < 0) & (_num(active, "price_progression_pct").fillna(_num(active, "price_vs_first_trigger_pct")) > 0))
        verdict = _text(active, "decision_verdict").str.upper().str.replace(" ", "_", regex=False)
        if not verdict.str.contains("HIGH_CONVICTION").any() and "verdict" in active.columns:
            verdict = _text(active, "verdict").str.upper()
        add(active, "high_conviction", verdict.str.contains("HIGH_CONVICTION", na=False))
    add(traps, "traps")
    add(archive, "archived")
    return [counts[key] for key in sorted(counts)]


def _row_details(active: pd.DataFrame, repeat: pd.DataFrame, traps: pd.DataFrame, archive: pd.DataFrame) -> dict[str, Any]:
    details: dict[str, Any] = {}
    for frame, section in ((active, "summary"), (repeat, "repeat"), (traps, "trap"), (archive, "archive")):
        if frame.empty or "symbol_id" not in frame.columns:
            continue
        for row in _records(frame):
            symbol = str(row.get("symbol_id") or "")
            if not symbol:
                continue
            details.setdefault(symbol, {})[section] = row
    return details


def _setup_series(frame: pd.DataFrame) -> pd.Series:
    if "trigger_reason" in frame.columns:
        return _text(frame, "trigger_reason").str.replace("_", " ", regex=False).str.title()
    return _text(frame, "move_tag").str.replace("_", " ", regex=False).str.title()


def _volume_signal(row: pd.Series) -> str:
    if _is_truthy(row.get("volume_escalation")):
        return "Rising"
    if _float(row.get("volume_ratio_declining")) > 0:
        return "Falling"
    return "Flat"


def _rank_signal(row: pd.Series) -> str:
    change = _float(row.get("rank_change_20d"))
    if change < 0:
        return "Improving"
    if change > 0:
        return "Falling"
    return "Flat"


def _records(frame: pd.DataFrame, limit: int | None = None) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    safe = frame.head(limit).copy() if limit else frame.copy()
    safe = safe.loc[:, ~safe.columns.duplicated()].copy()
    safe = safe.where(safe.notna(), None)
    return [_json_safe(row) for row in safe.to_dict(orient="records")]


def _sort_for_records(frame: pd.DataFrame, columns: list[str], ascending: list[bool]) -> pd.DataFrame:
    if frame.empty:
        return frame
    available = [col for col in columns if col in frame.columns]
    if not available:
        return frame
    directions = [ascending[columns.index(col)] for col in available]
    return frame.sort_values(available, ascending=directions, kind="stable")


def _num(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(float("nan"), index=frame.index, dtype="float")
    return pd.to_numeric(frame[column], errors="coerce")


def _text(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series("", index=frame.index, dtype=object)
    return frame[column].fillna("").astype(str)


def _bool(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    return frame[column].astype("string").fillna("").str.lower().isin({"true", "1", "yes"})


def _fresh_trap_count(frame: pd.DataFrame, run_date: str) -> int:
    if frame.empty or not run_date:
        return 0
    date_cols = [column for column in ("trade_date", "last_seen_date", "archived_at", "created_at", "first_seen_date") if column in frame.columns]
    if not date_cols:
        return 0
    seen = pd.Series(False, index=frame.index)
    for column in date_cols:
        dates = pd.to_datetime(frame[column], errors="coerce").dt.date.astype(str)
        seen = seen | dates.eq(str(run_date))
    return int(seen.sum())


def _repeat_trap_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    appearances = _num(frame, "appearance_count_20d")
    if appearances.notna().any():
        return int(appearances.fillna(0).ge(2).sum())
    symbols = _text(frame, "symbol_id")
    return int(symbols[symbols.ne("")].duplicated(keep=False).sum())


def _best_date_column(frame: pd.DataFrame) -> str | None:
    for column in ("trade_date", "last_seen_date", "archived_at", "created_at", "first_seen_date"):
        if column in frame.columns:
            return column
    return None


def _clip(series: pd.Series, lower: float, upper: float) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).clip(lower=lower, upper=upper)


def _float(value: object) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if pd.isna(out) else out


def _is_truthy(value: object) -> bool:
    return str(value).lower() in {"true", "1", "yes"}


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
