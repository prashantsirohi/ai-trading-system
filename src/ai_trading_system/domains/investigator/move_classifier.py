"""Trigger quality tags and scores."""

from __future__ import annotations

import pandas as pd


def classify_move(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if out.empty:
        out["trigger_quality_score"] = []
        out["move_tag"] = []
        out["credible_trigger"] = []
        return out
    event_text = _joined_text(out, ["event_category", "top_event", "news", "trigger_type", "catalyst", "summary"])
    sector_rotation = _boolish(out, "sector_rotation_active") | pd.to_numeric(_series(out, "peer_gainers_count"), errors="coerce").ge(5).fillna(False)
    tags = pd.Series("RANDOM_NOISE", index=out.index, dtype=object)
    tags = tags.mask(event_text.str.contains("earn|result|profit|revenue", case=False, na=False), "EARNINGS_RERATING")
    tags = tags.mask(event_text.str.contains("order|contract|tender", case=False, na=False), "ORDER_WIN")
    tags = tags.mask(sector_rotation, "SECTOR_ROTATION")
    tags = tags.mask(event_text.str.contains("bulk|block|insider|promoter", case=False, na=False), "SMART_MONEY_EVENT")
    low_delivery = out.get("low_delivery_flag", pd.Series(False, index=out.index)).fillna(False).astype(bool)
    tags = tags.mask(low_delivery & tags.eq("RANDOM_NOISE"), "OPERATOR_SPIKE")
    score_map = {
        "EARNINGS_RERATING": 20,
        "ORDER_WIN": 18,
        "SECTOR_ROTATION": 15,
        "SMART_MONEY_EVENT": 16,
        "SHORT_COVERING": 10,
        "OPERATOR_SPIKE": 3,
        "RANDOM_NOISE": 5,
    }
    out.loc[:, "move_tag"] = tags
    out.loc[:, "trigger_quality_score"] = tags.map(score_map).fillna(5).astype(float)
    out.loc[:, "credible_trigger"] = ~tags.isin({"RANDOM_NOISE", "OPERATOR_SPIKE"}) | event_text.str.strip().ne("")
    return out


def _joined_text(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    text = pd.Series("", index=frame.index, dtype=object)
    for column in columns:
        if column in frame.columns:
            text = text + " " + frame[column].fillna("").astype(str)
    return text


def _boolish(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    return frame[column].astype(str).str.lower().isin({"true", "1", "yes", "active", "leading"})


def _series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series(pd.NA, index=frame.index)
