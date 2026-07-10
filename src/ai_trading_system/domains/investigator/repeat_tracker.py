"""Rolling repeat tracker for stock investigator triggers."""

from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.investigator.candidate_union import is_trigger_observation


def build_repeat_tracker(
    *,
    current_scores: pd.DataFrame,
    historical_daily_log: pd.DataFrame | None = None,
    window_days: int = 20,
) -> pd.DataFrame:
    if current_scores.empty and (historical_daily_log is None or historical_daily_log.empty):
        return _empty()
    history = historical_daily_log.copy() if historical_daily_log is not None else pd.DataFrame()
    current = current_scores.copy()
    if not current.empty:
        current_log = current[
            [
                col
                for col in (
                    "symbol_id",
                    "trade_date",
                    "close",
                    "volume_ratio_20",
                    "volume_ratio_5d",
                    "daily_return_pct",
                    "return_5d",
                    "return_20d",
                    "composite_score",
                    "rank_position",
                    "final_score",
                    "sector",
                    "trigger_reason",
                    "candidate_sources",
                    "primary_candidate_source",
                    "candidate_source_count",
                    "new_candidate_today",
                )
                if col in current.columns
            ]
        ].copy()
        current_log.loc[:, "appeared"] = current_log.get(
            "candidate_sources", pd.Series("", index=current_log.index)
        ).map(is_trigger_observation)
        history = current_log if history.empty else pd.concat([history, current_log], ignore_index=True)
    if history.empty or "symbol_id" not in history.columns:
        return _empty()
    history.loc[:, "trade_date"] = pd.to_datetime(history["trade_date"], errors="coerce")
    history = history.dropna(subset=["symbol_id", "trade_date"])
    derived_appeared = history.get(
        "candidate_sources", pd.Series("", index=history.index)
    ).map(is_trigger_observation)
    if "appeared" not in history.columns:
        history.loc[:, "appeared"] = derived_appeared.astype(bool)
    else:
        history.loc[:, "appeared"] = history["appeared"].where(history["appeared"].notna(), derived_appeared)
        history.loc[:, "appeared"] = history["appeared"].map(lambda value: True if pd.isna(value) else bool(value))
    latest_date = history["trade_date"].max()
    if pd.isna(latest_date):
        return _empty()
    history = history.sort_values(["symbol_id", "trade_date"], kind="stable")
    window = history.loc[history["trade_date"] >= latest_date - pd.Timedelta(days=int(window_days) * 2)].copy()
    window = _attach_sector_peer_counts(window)
    rows: list[dict[str, object]] = []
    for symbol, group in window.groupby("symbol_id", sort=True):
        group = group.sort_values("trade_date", kind="stable")
        observations = group.loc[group["appeared"]].copy()
        if observations.empty:
            observations = group.head(1).copy()
        last = group.iloc[-1]
        first = observations.iloc[0]
        last_observation = observations.iloc[-1]
        rows.append(
            {
                "symbol_id": symbol,
                "first_seen_date": str(first["trade_date"].date()),
                "last_seen_date": str(last_observation["trade_date"].date()),
                "days_since_last_seen": int((latest_date - last_observation["trade_date"]).days),
                "appearance_count_5d": _count_since(group, latest_date, 5),
                "appearance_count_10d": _count_since(group, latest_date, 10),
                "appearance_count_15d": _count_since(group, latest_date, 15),
                "appearance_count_20d": _count_since(group, latest_date, 20),
                "daily_gainer_count_20d": _count_trigger_since(group, latest_date, 20, "DAILY_GAINER"),
                "weekly_gainer_count_20d": _count_trigger_since(group, latest_date, 20, "WEEKLY_GAINER"),
                "stealth_count_20d": _count_trigger_since(group, latest_date, 20, "STEALTH_ACCUMULATION"),
                "avg_volume_ratio": _mean(group.get("volume_ratio_20")),
                "volume_escalation": _is_rising(group.get("volume_ratio_20")),
                "price_progression_pct": _pct_change(first.get("close"), last.get("close")),
                "rank_current": _safe(last.get("rank_position")),
                "rank_change_20d": _rank_change(group),
                "score_current": _safe(last.get("final_score")),
                "score_peak": _safe(pd.to_numeric(group.get("final_score"), errors="coerce").max()) if "final_score" in group.columns else 0.0,
                "sector_cluster_count": int(_safe(last.get("_sector_peer_count"))),
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return _empty()
    out.loc[:, "repeat_score"] = (
        out["appearance_count_20d"].clip(upper=5) * 8
        + out["volume_escalation"].astype(int) * 10
        + pd.to_numeric(out["price_progression_pct"], errors="coerce").gt(0).astype(int) * 10
        + pd.to_numeric(out["rank_change_20d"], errors="coerce").lt(0).astype(int) * 6
    ).clip(upper=100)
    out.loc[:, "high_priority_repeat"] = (
        (out["appearance_count_15d"] >= 3)
        & (pd.to_numeric(out["price_progression_pct"], errors="coerce") > 0)
        & out["volume_escalation"]
        & (pd.to_numeric(out["score_current"], errors="coerce") >= 55)
    )
    return out.sort_values(["repeat_score", "symbol_id"], ascending=[False, True], kind="stable").reset_index(drop=True)


def _count_since(group: pd.DataFrame, latest_date: pd.Timestamp, days: int) -> int:
    appeared = group.get("appeared", pd.Series(True, index=group.index)).map(lambda value: True if pd.isna(value) else bool(value))
    return int(((group["trade_date"] >= latest_date - pd.Timedelta(days=int(days))) & appeared).sum())


def _count_trigger_since(group: pd.DataFrame, latest_date: pd.Timestamp, days: int, trigger_reason: str) -> int:
    if "trigger_reason" not in group.columns:
        return 0
    in_window = group["trade_date"] >= latest_date - pd.Timedelta(days=int(days))
    triggers = group["trigger_reason"].fillna("").astype(str).str.upper().eq(trigger_reason)
    appeared = group.get("appeared", pd.Series(True, index=group.index)).map(lambda value: True if pd.isna(value) else bool(value))
    return int((in_window & triggers & appeared).sum())


def _attach_sector_peer_counts(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out.loc[:, "_sector_peer_count"] = 0
    if out.empty or "sector" not in out.columns:
        return out
    sector = out["sector"].fillna("").astype(str).str.strip().str.upper()
    valid = sector.ne("") & ~sector.isin({"NAN", "NONE", "NULL", "<NA>"})
    if not valid.any():
        return out
    out = out.assign(_sector_norm=sector)
    out.loc[valid, "_sector_peer_count"] = (
        out.loc[valid]
        .groupby(["trade_date", "_sector_norm"], dropna=True)["symbol_id"]
        .transform("nunique")
        .astype(int)
    )
    return out.drop(columns=["_sector_norm"], errors="ignore")


def _is_rising(series: pd.Series | None) -> bool:
    if series is None:
        return False
    values = pd.to_numeric(series, errors="coerce").dropna()
    if len(values) < 2:
        return False
    return bool(values.iloc[-1] > values.iloc[0])


def _mean(series: pd.Series | None) -> float:
    if series is None:
        return 0.0
    value = pd.to_numeric(series, errors="coerce").mean()
    return 0.0 if pd.isna(value) else float(value)


def _pct_change(first: object, last: object) -> float:
    try:
        first_f = float(first)
        last_f = float(last)
    except (TypeError, ValueError):
        return 0.0
    if first_f == 0:
        return 0.0
    return (last_f / first_f - 1.0) * 100.0


def _rank_change(group: pd.DataFrame) -> float:
    if "rank_position" not in group.columns:
        return 0.0
    values = pd.to_numeric(group["rank_position"], errors="coerce").dropna()
    if len(values) < 2:
        return 0.0
    return float(values.iloc[-1] - values.iloc[0])


def _safe(value: object) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if pd.isna(out) else out


def _empty() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol_id",
            "first_seen_date",
            "last_seen_date",
            "days_since_last_seen",
            "appearance_count_5d",
            "appearance_count_10d",
            "appearance_count_15d",
            "appearance_count_20d",
            "daily_gainer_count_20d",
            "weekly_gainer_count_20d",
            "stealth_count_20d",
            "avg_volume_ratio",
            "volume_escalation",
            "price_progression_pct",
            "rank_current",
            "rank_change_20d",
            "score_current",
            "score_peak",
            "sector_cluster_count",
            "repeat_score",
            "high_priority_repeat",
        ]
    )
