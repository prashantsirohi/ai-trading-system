"""Investigator-owned pattern scan for early accumulation candidates."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from ai_trading_system.analytics.patterns import build_pattern_signals
from ai_trading_system.analytics.patterns.data import load_pattern_frame
from ai_trading_system.pipeline.contracts import StageContext


S1_STATE_PRIORITY = {
    "FAILED_S1": 0,
    "S1_BASE_FORMING": 1,
    "S1_ACCUMULATION": 2,
    "S1_NEAR_BREAKOUT": 3,
    "S1_TO_S2_TRANSITION": 4,
    "S2_CONFIRMED": 5,
}


def build_investigator_pattern_scan(
    *,
    context: StageContext,
    active_watchlist: pd.DataFrame,
    ranked_df: pd.DataFrame,
) -> pd.DataFrame:
    """Pattern-analyze active investigator names without Stage 2 prescreening."""

    symbols = _active_symbols(active_watchlist.get("symbol_id", pd.Series(dtype=object)) if active_watchlist is not None else [])
    max_symbols = int(context.params.get("investigator_pattern_max_symbols", 100))
    symbols = symbols[: max(0, max_symbols)]
    if not symbols:
        return _empty()

    lookback_days = int(context.params.get("investigator_pattern_lookback_days", 420))
    exchange = str(context.params.get("investigator_pattern_exchange", context.params.get("exchange", "NSE")) or "NSE")
    data_domain = str(context.params.get("data_domain", "operational") or "operational")
    from_date = (pd.Timestamp(context.run_date) - pd.Timedelta(days=lookback_days)).date().isoformat()
    frame = load_pattern_frame(
        context.project_root,
        from_date=from_date,
        to_date=context.run_date,
        exchange=exchange,
        symbols=symbols,
        data_domain=data_domain,
    )
    if frame.empty:
        return _empty(symbols=symbols)

    patterns = build_pattern_signals(
        project_root=context.project_root,
        signal_date=context.run_date,
        exchange=exchange,
        data_domain=data_domain,
        symbols=symbols,
        frame=frame,
        ranked_df=ranked_df,
        lookback_days=lookback_days,
        pattern_workers=int(context.params.get("investigator_pattern_workers", 1)),
        scan_mode=str(context.params.get("investigator_pattern_scan_mode", "full")),
        stage2_only=False,
        write_pattern_cache=False,
    )
    if patterns.empty:
        return _empty(symbols=symbols)

    out = patterns.copy()
    out.loc[:, "symbol_id"] = out["symbol_id"].astype(str).str.strip().str.upper()
    context_cols = _investigator_context(active_watchlist)
    if not context_cols.empty:
        out = out.merge(context_cols, on="symbol_id", how="left")

    ranked_symbols = set(_active_symbols(ranked_df.get("symbol_id", pd.Series(dtype=object)) if ranked_df is not None else []))
    out.loc[:, "source_investigator"] = True
    out.loc[:, "source_ranked"] = out["symbol_id"].isin(ranked_symbols)

    states = out.apply(_classify_s1_state, axis=1, result_type="expand")
    out.loc[:, "s1_promotion_state"] = states["s1_promotion_state"]
    out.loc[:, "promotion_reason"] = states["promotion_reason"]
    return out.reset_index(drop=True)


def best_pattern_by_symbol(patterns: pd.DataFrame) -> pd.DataFrame:
    """Return the highest-priority investigator pattern row per symbol."""

    if patterns is None or patterns.empty or "symbol_id" not in patterns.columns:
        return pd.DataFrame()
    out = patterns.copy()
    out.loc[:, "_s1_priority"] = out.get("s1_promotion_state", pd.Series("", index=out.index)).map(S1_STATE_PRIORITY).fillna(0)
    out.loc[:, "_pattern_score_sort"] = pd.to_numeric(_series(out, "pattern_score"), errors="coerce").fillna(-1)
    out.loc[:, "_setup_quality_sort"] = pd.to_numeric(_series(out, "setup_quality"), errors="coerce").fillna(-1)
    return (
        out.sort_values(
            ["_s1_priority", "_pattern_score_sort", "_setup_quality_sort", "symbol_id"],
            ascending=[False, False, False, True],
            kind="stable",
        )
        .drop_duplicates(subset=["symbol_id"], keep="first")
        .drop(columns=["_s1_priority", "_pattern_score_sort", "_setup_quality_sort"])
        .reset_index(drop=True)
    )


def _active_symbols(values: Iterable[object]) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for value in values:
        symbol = str(value or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


def _investigator_context(active_watchlist: pd.DataFrame | None) -> pd.DataFrame:
    if active_watchlist is None or active_watchlist.empty or "symbol_id" not in active_watchlist.columns:
        return pd.DataFrame()
    out = active_watchlist.copy()
    out.loc[:, "symbol_id"] = out["symbol_id"].astype(str).str.strip().str.upper()
    rename = {
        "status": "investigator_status",
        "verdict": "investigator_verdict",
        "final_score": "investigator_final_score",
    }
    out = out.rename(columns={old: new for old, new in rename.items() if old in out.columns})
    desired = [
        "symbol_id",
        "trigger_reason",
        "investigator_status",
        "investigator_verdict",
        "investigator_final_score",
        "hard_trap_flag",
        "drop_reason",
        "low_delivery_flag",
        "volume_escalation",
        "volume_ratio_5d",
        "volume_ratio_20",
        "delivery_pct",
        "appearance_count_20d",
        "price_progression_pct",
        "rank_change_20d",
    ]
    available = [col for col in desired if col in out.columns]
    return out[available].drop_duplicates(subset=["symbol_id"], keep="first")


def _classify_s1_state(row: pd.Series) -> dict[str, str]:
    lifecycle = str(row.get("pattern_lifecycle_state") or "").strip().lower()
    pattern_state = str(row.get("pattern_state") or "").strip().lower()
    pattern_score = _num(row.get("pattern_score"))
    setup_quality = _num(row.get("setup_quality"))
    stage2_score = _num(row.get("stage2_score"))
    volume_confirmed = _volume_confirmed(row)
    trap_evidence = _trap_evidence(row)
    if lifecycle == "invalidated" or trap_evidence:
        return {"s1_promotion_state": "FAILED_S1", "promotion_reason": "Pattern invalidated or investigator trap evidence present"}
    if stage2_score >= 70 and "confirmed" in {pattern_state, lifecycle} and volume_confirmed:
        return {"s1_promotion_state": "S2_CONFIRMED", "promotion_reason": "Stage 2 score confirmed with pattern and volume support"}
    if pattern_score >= 70 and volume_confirmed:
        return {"s1_promotion_state": "S1_TO_S2_TRANSITION", "promotion_reason": "High pattern score with volume confirmation"}
    if pattern_score >= 65 or setup_quality >= 60:
        return {"s1_promotion_state": "S1_NEAR_BREAKOUT", "promotion_reason": "Pattern quality near breakout threshold"}
    if 40 <= pattern_score < 65 and _accumulation_improving(row):
        return {"s1_promotion_state": "S1_ACCUMULATION", "promotion_reason": "Base pattern with improving accumulation evidence"}
    return {"s1_promotion_state": "S1_BASE_FORMING", "promotion_reason": "Pattern exists but sponsorship or breakout evidence is still early"}


def _volume_confirmed(row: pd.Series) -> bool:
    return (
        _truthy(row.get("is_strong_volume_confirmation"))
        or _truthy(row.get("is_combined_volume_confirmation"))
        or _num(row.get("breakout_volume_ratio")) >= 1.2
    )


def _accumulation_improving(row: pd.Series) -> bool:
    trigger_reason = str(row.get("trigger_reason") or "").upper()
    trigger_accumulation = trigger_reason in {"WEEKLY_GAINER", "STEALTH_ACCUMULATION"}
    return (
        _truthy(row.get("volume_escalation"))
        or _num(row.get("delivery_pct")) >= 50
        or _num(row.get("volume_ratio_20")) >= 1.2
        or _num(row.get("volume_ratio_5d")) >= 1.2
        or _num(row.get("appearance_count_20d")) >= 2
        or _num(row.get("price_progression_pct")) > 0
        or _num(row.get("rank_change_20d")) < 0
        or trigger_accumulation
    )


def _trap_evidence(row: pd.Series) -> bool:
    text = " ".join(
        str(row.get(col) or "").upper()
        for col in ("drop_reason", "investigator_verdict", "pattern_lifecycle_state")
    )
    low_delivery_failure = (
        _truthy(row.get("low_delivery_flag"))
        and _num(row.get("appearance_count_20d")) <= 1
        and _num(row.get("price_progression_pct")) < 0
    )
    return (
        _truthy(row.get("hard_trap_flag"))
        or low_delivery_failure
        or "TRAP" in text
        or "INVALIDATED" in text
    )


def _truthy(value: object) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _num(value: object) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if pd.isna(out) else out


def _series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series(pd.NA, index=frame.index)


def _empty(*, symbols: list[str] | None = None) -> pd.DataFrame:
    frame = pd.DataFrame(
        columns=[
            "symbol_id",
            "source_investigator",
            "source_ranked",
            "trigger_reason",
            "investigator_status",
            "investigator_verdict",
            "investigator_final_score",
            "s1_promotion_state",
            "promotion_reason",
        ]
    )
    frame.attrs["scanned_symbols"] = symbols or []
    return frame
