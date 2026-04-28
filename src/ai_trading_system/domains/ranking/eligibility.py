"""Eligibility filters for rank candidates."""

from __future__ import annotations

import pandas as pd


def apply_rank_eligibility(
    frame: pd.DataFrame,
    *,
    min_price: float = 20.0,
    min_liquidity_score: float = 0.20,
    stage2_gate_enabled: bool = False,
    stage2_min_score: float = 70.0,
    weekly_stage_gate_enabled: bool = False,
    weekly_stage_min_confidence: float = 0.6,
) -> pd.DataFrame:
    """Mark explicit ranking eligibility and rejection reasons.

    Parameters
    ----------
    frame:
        DataFrame of rank candidates, one row per symbol.
    min_price:
        Symbols with ``close < min_price`` are marked ineligible.
    min_liquidity_score:
        Symbols with ``liquidity_score < min_liquidity_score`` are marked
        ineligible.
    stage2_gate_enabled:
        When *True* symbols whose ``stage2_score`` falls below
        ``stage2_min_score`` are marked ineligible.  Enabled automatically
        when ``rank_mode == 'stage2_breakout'``.
    stage2_min_score:
        Legacy fallback score threshold when structural Stage 2 columns are
        unavailable.
    weekly_stage_gate_enabled:
        When *True*, symbols whose ``weekly_stage_label`` is not ``S2`` (or
        that have a confirmed snapshot below ``weekly_stage_min_confidence``)
        are marked ineligible.  Symbols with no snapshot pass through so the
        ranker keeps working during backfill catch-up.
    weekly_stage_min_confidence:
        Minimum ``weekly_stage_confidence`` required to accept an S2 label.
    """
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

    # ── Stage 2 gate (optional — activated by stage2_breakout rank mode) ──
    if stage2_gate_enabled:
        if "is_stage2_structural" in output.columns:
            failed_s2 = ~output["is_stage2_structural"].fillna(False).astype(bool)
            default_reason = "non_structural_stage2"
        elif "is_stage2_uptrend" in output.columns:
            failed_s2 = ~output["is_stage2_uptrend"].fillna(False).astype(bool)
            default_reason = "stage2_uptrend_required"
        elif "stage2_score" in output.columns:
            s2 = pd.to_numeric(output["stage2_score"], errors="coerce").fillna(0.0)
            failed_s2 = s2 < float(stage2_min_score)
            default_reason = "stage2_score_below_threshold"
        else:
            failed_s2 = pd.Series(False, index=output.index)
            default_reason = "stage2_gate_unavailable"

        output.loc[failed_s2, "eligible_rank"] = False
        for idx in output.index[failed_s2]:
            fail_reason = ""
            if "stage2_hard_fail_reason" in output.columns and output.at[idx, "stage2_hard_fail_reason"]:
                fail_reason = str(output.at[idx, "stage2_hard_fail_reason"])
            elif "stage2_fail_reason" in output.columns and output.at[idx, "stage2_fail_reason"]:
                fail_reason = str(output.at[idx, "stage2_fail_reason"])
            else:
                fail_reason = default_reason
            output.at[idx, "rejection_reasons"] = output.at[idx, "rejection_reasons"] + [f"stage2:{fail_reason}"]

    # ── Weekly stage gate (higher-timeframe regime check) ─────────────────
    # Column `weekly_stage_label` is joined by StockRanker._apply_weekly_stage_gate
    # before eligibility runs. Symbols with no snapshot (NaN) pass through —
    # the gate only blocks confirmed non-S2 labels above confidence threshold.
    if weekly_stage_gate_enabled and "weekly_stage_label" in output.columns:
        label = output["weekly_stage_label"]
        conf = pd.to_numeric(
            output.get("weekly_stage_confidence", pd.Series(1.0, index=output.index)),
            errors="coerce",
        ).fillna(0.0)

        has_snapshot = label.notna()
        is_s2 = label == "S2"
        confident = conf >= float(weekly_stage_min_confidence)

        # Fail: has a snapshot AND (not S2, or S2 but below confidence).
        failed = has_snapshot & (~is_s2 | ~confident)
        output.loc[failed, "eligible_rank"] = False
        for idx in output.index[failed]:
            stage = str(label.at[idx]) if pd.notna(label.at[idx]) else "UNKNOWN"
            output.at[idx, "rejection_reasons"] = (
                output.at[idx, "rejection_reasons"]
                + [f"weekly_stage:{stage}"]
            )

    return output
