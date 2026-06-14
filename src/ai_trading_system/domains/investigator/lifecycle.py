"""Status ageing and archive decisions for investigator candidates."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd


ACTIVE_STATUSES = {"NEW_TRIGGER", "TRACKING", "ACTIVE_RESEARCH", "HIGH_CONVICTION", "WATCHLIST"}


def apply_lifecycle(scores: pd.DataFrame, repeat_tracker: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if scores.empty:
        return _active_empty(), _archive_empty()
    out = scores.copy()
    if repeat_tracker is not None and not repeat_tracker.empty:
        repeat_cols = [
            "symbol_id",
            "first_seen_date",
            "last_seen_date",
            "days_since_last_seen",
            "appearance_count_20d",
            "score_current",
            "score_peak",
            "rank_current",
            "rank_change_20d",
            "price_progression_pct",
            "volume_escalation",
            "sector_cluster_count",
            "high_priority_repeat",
        ]
        out = out.merge(repeat_tracker[[col for col in repeat_cols if col in repeat_tracker.columns]], on="symbol_id", how="left", suffixes=("", "_repeat"))
    out = _fill_lifecycle_fields(out)
    decisions = out.apply(_decide_row, axis=1, result_type="expand")
    out.loc[:, "status"] = decisions["status"]
    out.loc[:, "drop_reason"] = decisions["drop_reason"]
    out.loc[:, "archived_at"] = decisions["archived_at"]
    archive = out.loc[out["status"].isin({"DROPPED", "ARCHIVED"})].copy()
    active = out.loc[~out["status"].isin({"DROPPED", "ARCHIVED"})].copy()
    return active.reset_index(drop=True), archive.reset_index(drop=True)


def _fill_lifecycle_fields(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    trade_date = pd.to_datetime(out.get("trade_date"), errors="coerce")
    for column, default in (
        ("first_seen_date", trade_date.dt.date.astype(str)),
        ("last_seen_date", trade_date.dt.date.astype(str)),
        ("days_since_last_seen", 0),
        ("appearance_count_20d", 1),
        ("score_current", out.get("final_score", 0)),
        ("score_peak", out.get("final_score", 0)),
        ("rank_current", out.get("rank_position", out.get("composite_score", 0))),
        ("rank_change_20d", 0),
        ("price_progression_pct", 0),
    ):
        if column not in out.columns:
            out.loc[:, column] = default
        else:
            out.loc[:, column] = out[column].fillna(default)
    if "price_vs_first_trigger_pct" not in out.columns:
        out.loc[:, "price_vs_first_trigger_pct"] = out["price_progression_pct"]
    return out


def _decide_row(row: pd.Series) -> dict[str, object]:
    now = datetime.now(timezone.utc).isoformat()
    verdict = str(row.get("verdict") or "")
    long_upper = bool(row.get("long_upper_wick_trap", False))
    low_delivery = bool(row.get("low_delivery_flag", False))
    appearance = int(_num(row.get("appearance_count_20d"), 1))
    rank_score = _num(row.get("composite_score"), 0)
    credible = bool(row.get("credible_trigger", False))
    sector_support = _num(row.get("sector_support_score"), 0) > 0 or bool(row.get("sector_rotation_active", False))
    price_vs_trigger = _num(row.get("price_progression_pct"), 0)
    days = int(_num(row.get("days_since_last_seen"), 0))
    volume_declining = bool(row.get("volume_ratio_declining", False)) and not bool(row.get("volume_escalation", False))
    rank_change = _num(row.get("rank_change_20d"), 0)
    rank_not_improving = rank_change >= 0
    fa_improvement = bool(row.get("fa_improvement", False))
    sector_cluster = bool(row.get("sector_clustering", False)) or _num(row.get("sector_cluster_count"), 0) >= 3
    current_score = _num(row.get("final_score", row.get("score_current")), 0)
    keep_beyond = (
        current_score >= 55
        or appearance >= 3
        or price_vs_trigger > 0
        or rank_change < 0
        or bool(row.get("sector_rotation_active", False))
        or bool(row.get("fa_trigger_confirmed", False))
    )
    if verdict == "NOISE_TRAP":
        return _archive("ARCHIVED", "NOISE_TRAP", now)
    if long_upper:
        return _archive("ARCHIVED", "LONG_UPPER_WICK_TRAP", now)
    if low_delivery and appearance <= 1:
        return _archive("ARCHIVED", "LOW_DELIVERY_NO_REPEAT", now)
    if rank_score < 35 and not credible:
        return _archive("ARCHIVED", "LOW_RANK_NO_NEWS", now)
    if days >= 5 and appearance <= 1 and price_vs_trigger < 0 and not credible and not sector_support:
        return _archive("DROPPED", "ONE_CANDLE_DRAMA", now)
    if days >= 10 and appearance < 2 and volume_declining and rank_not_improving:
        return _archive("DROPPED", "STALE_NO_REPEAT", now)
    if days >= 20 and appearance < 3 and not fa_improvement and not sector_cluster and current_score < 55 and not keep_beyond:
        return _archive("ARCHIVED", "FAILED_FOLLOW_THROUGH", now)
    if verdict == "HIGH_CONVICTION":
        status = "HIGH_CONVICTION"
    elif current_score >= 55 or bool(row.get("high_priority_repeat", False)):
        status = "ACTIVE_RESEARCH"
    elif verdict == "WATCH_ONLY":
        status = "WATCHLIST"
    elif appearance <= 1:
        status = "NEW_TRIGGER"
    else:
        status = "TRACKING"
    return {"status": status, "drop_reason": "", "archived_at": ""}


def _archive(status: str, reason: str, archived_at: str) -> dict[str, object]:
    return {"status": status, "drop_reason": reason, "archived_at": archived_at}


def _num(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return default if pd.isna(out) else out


def _active_empty() -> pd.DataFrame:
    return pd.DataFrame(columns=["symbol_id", "status"])


def _archive_empty() -> pd.DataFrame:
    return pd.DataFrame(columns=["symbol_id", "status", "drop_reason", "archived_at"])
