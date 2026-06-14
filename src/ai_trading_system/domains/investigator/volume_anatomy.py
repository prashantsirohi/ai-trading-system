"""Volume and delivery scoring."""

from __future__ import annotations

import pandas as pd


def score_volume_anatomy(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if out.empty:
        out["volume_delivery_score"] = []
        out["volume_unusual"] = []
        out["low_delivery_flag"] = []
        return out
    volume_ratio = pd.to_numeric(_series(out, "volume_ratio_20"), errors="coerce").fillna(0)
    delivery = pd.to_numeric(_series(out, "delivery_pct"), errors="coerce")
    volume_trend_source = _series(out, "volume_trend_10d")
    if volume_trend_source.isna().all():
        volume_trend_source = _series(out, "volume_trend_10d_pct")
    volume_trend = pd.to_numeric(volume_trend_source, errors="coerce")
    score = (
        volume_ratio.gt(2).astype(int) * 6
        + volume_ratio.gt(5).astype(int) * 4
        + delivery.gt(50).fillna(False).astype(int) * 6
        + volume_trend.gt(0).fillna(False).astype(int) * 4
    )
    speculative = delivery.lt(20).fillna(False)
    score = score - speculative.astype(int) * 6
    out.loc[:, "volume_delivery_score"] = score.clip(lower=0, upper=20)
    out.loc[:, "volume_unusual"] = volume_ratio.gt(5)
    out.loc[:, "low_delivery_flag"] = speculative
    out.loc[:, "volume_ratio_declining"] = volume_trend.lt(0).fillna(False)
    return out


def _series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series(pd.NA, index=frame.index)
