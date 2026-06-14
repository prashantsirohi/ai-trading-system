"""Price-structure scoring for investigator candidates."""

from __future__ import annotations

import pandas as pd


def score_price_structure(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if out.empty:
        out["price_structure_score"] = []
        out["hard_trap_flag"] = []
        out["long_upper_wick_trap"] = []
        return out
    high = pd.to_numeric(out.get("high"), errors="coerce")
    low = pd.to_numeric(out.get("low"), errors="coerce")
    open_ = pd.to_numeric(out.get("open"), errors="coerce")
    close = pd.to_numeric(out.get("close"), errors="coerce")
    range_ = (high - low).replace(0, pd.NA)
    body_pct = (close - open_).abs() / range_ * 100.0
    close_position = (close - low) / range_ * 100.0
    upper_wick_pct = (high - close) / range_ * 100.0
    out.loc[:, "body_pct_of_range"] = body_pct
    out.loc[:, "close_position_pct"] = close_position
    out.loc[:, "upper_wick_pct"] = upper_wick_pct
    breakout_bonus = _boolish(out, "breakout_positive") | _boolish(out, "qualified") | _boolish(out, "is_breakout")
    tight_base_bonus = pd.to_numeric(out.get("base_tightness_pct", pd.Series(index=out.index, dtype=float)), errors="coerce").le(6)
    score = (
        body_pct.gt(70).fillna(False).astype(int) * 4
        + close_position.ge(75).fillna(False).astype(int) * 4
        + breakout_bonus.astype(int) * 4
        + tight_base_bonus.fillna(False).astype(int) * 3
    )
    long_upper = upper_wick_pct.ge(45).fillna(False) & close_position.lt(50).fillna(False)
    close_near_low = close_position.lt(35).fillna(False)
    out.loc[:, "price_structure_score"] = score.clip(upper=15)
    out.loc[:, "long_upper_wick_trap"] = long_upper
    out.loc[:, "hard_trap_flag"] = long_upper | close_near_low
    return out


def _boolish(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    value = frame[column]
    if value.dtype == bool:
        return value.fillna(False)
    return value.astype(str).str.lower().isin({"true", "1", "yes", "y", "qualified", "a", "b"})
