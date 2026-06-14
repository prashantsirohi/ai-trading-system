"""Buyer fingerprint scoring."""

from __future__ import annotations

import pandas as pd


def score_buyer_fingerprint(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if out.empty:
        out["buyer_fingerprint_score"] = []
        return out
    delivery = pd.to_numeric(_series(out, "delivery_pct"), errors="coerce")
    price_rise = pd.to_numeric(_series(out, "daily_return_pct"), errors="coerce").gt(0).fillna(False)
    bulk_block = _boolish(out, "bulk_block_buyer") | out.get("move_tag", pd.Series("", index=out.index)).eq("SMART_MONEY_EVENT")
    oi_rising = _boolish(out, "oi_rising")
    low_delivery = delivery.lt(20).fillna(False)
    score = (
        (delivery.gt(50).fillna(False) & price_rise).astype(int) * 7
        + bulk_block.astype(int) * 4
        + (oi_rising & price_rise).astype(int) * 4
        - (low_delivery & price_rise).astype(int) * 5
    )
    out.loc[:, "buyer_fingerprint_score"] = score.clip(lower=0, upper=15)
    return out


def _boolish(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    return frame[column].astype(str).str.lower().isin({"true", "1", "yes", "y"})


def _series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series(pd.NA, index=frame.index)
