"""Eligibility filters for rank candidates."""

from __future__ import annotations

import pandas as pd


def apply_rank_eligibility(
    frame: pd.DataFrame,
    *,
    min_price: float = 20.0,
    min_liquidity_score: float = 0.20,
) -> pd.DataFrame:
    """Mark explicit ranking eligibility and rejection reasons."""
    output = frame.copy()
    if output.empty:
        output["eligible_rank"] = pd.Series(dtype=bool)
        output["rejection_reasons"] = pd.Series(dtype=object)
        return output

    output["eligible_rank"] = True
    output["rejection_reasons"] = [[] for _ in range(len(output))]

    if "close" in output.columns:
        low_price = pd.to_numeric(output["close"], errors="coerce") < float(min_price)
        output.loc[low_price, "eligible_rank"] = False
        for idx in output.index[low_price]:
            output.at[idx, "rejection_reasons"] = output.at[idx, "rejection_reasons"] + ["min_price"]

    if "feature_ready" in output.columns:
        not_ready = ~output["feature_ready"].fillna(False)
        output.loc[not_ready, "eligible_rank"] = False
        for idx in output.index[not_ready]:
            output.at[idx, "rejection_reasons"] = output.at[idx, "rejection_reasons"] + ["feature_not_ready"]

    if "liquidity_score" in output.columns:
        illiquid = pd.to_numeric(output["liquidity_score"], errors="coerce") < float(min_liquidity_score)
        output.loc[illiquid, "eligible_rank"] = False
        for idx in output.index[illiquid]:
            output.at[idx, "rejection_reasons"] = output.at[idx, "rejection_reasons"] + ["insufficient_liquidity"]

    return output
