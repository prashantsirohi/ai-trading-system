"""Return-quality classification for perf tracker research rows."""

from __future__ import annotations

import pandas as pd

from ai_trading_system.research.perf_tracker.constants import (
    FORWARD_RETURN_ANOMALY_5D_PCT,
    FORWARD_RETURN_ANOMALY_THRESHOLDS,
    FORWARD_RETURN_REVIEW_PCT,
)
from ai_trading_system.research.perf_tracker.forward_returns import FORWARD_HORIZONS


def annotate_return_quality(rows: pd.DataFrame) -> pd.DataFrame:
    """Attach anomaly flags, status, and explainable reasons to tracker rows.

    The tracker keeps raw rows in the table for auditability. Rows with
    implausibly large raw-close forward returns are quarantined from trusted
    research views and tagged with a compact pipe-delimited reason taxonomy.
    """
    if rows is None or rows.empty:
        return rows

    out = rows.copy()
    reasons = pd.Series("", index=out.index, dtype="object")
    any_anomaly = pd.Series(False, index=out.index, dtype="bool")

    for horizon in FORWARD_HORIZONS:
        col = f"fwd_{horizon}d_return"
        if col not in out.columns:
            continue
        values = pd.to_numeric(out[col], errors="coerce").abs()
        threshold = FORWARD_RETURN_ANOMALY_THRESHOLDS.get(horizon, FORWARD_RETURN_REVIEW_PCT)
        mask = (values >= threshold).fillna(False)
        any_anomaly = any_anomaly | mask
        token = f"extreme_fwd_{horizon}d_return"
        reasons = _append_reason(reasons, mask, token)

        review_mask = (values >= FORWARD_RETURN_REVIEW_PCT).fillna(False)
        reasons = _append_reason(reasons, review_mask, "manual_review_extreme_return")

    if "fwd_5d_return" in out.columns:
        r5 = pd.to_numeric(out["fwd_5d_return"], errors="coerce").abs()
        out.loc[:, "fwd_5d_anomaly"] = (r5 >= FORWARD_RETURN_ANOMALY_5D_PCT).fillna(False)
    else:
        out.loc[:, "fwd_5d_anomaly"] = False

    out.loc[:, "fwd_return_anomaly"] = any_anomaly
    out.loc[:, "data_quality_status"] = "trusted"
    out.loc[any_anomaly, "data_quality_status"] = "quarantined"
    out.loc[:, "data_quality_reason"] = reasons.replace("", pd.NA)
    return out


def _append_reason(reasons: pd.Series, mask: pd.Series, token: str) -> pd.Series:
    updated = reasons.copy()
    current = updated.loc[mask].fillna("").astype(str)
    needs_separator = current.ne("")
    updated.loc[mask] = current.where(~needs_separator, current + "|") + token
    return updated


__all__ = ["annotate_return_quality"]
