"""Categorize ranking + breakout output into 4 user-facing watchlist buckets.

Phase 5 of the ranking redesign. Ranking finds *regime* (state factors only —
RS, sector, trend, proximity). Breakout layer finds *trigger* (event factors —
volume spike, delivery surge, range expansion, close near high). The publish
layer combines them into a clear taxonomy:

  TRIGGERED_TODAY         — strong regime + breakout + conviction (act today)
  CORE_MOMENTUM           — strong regime, no trigger today (anchor watchlist)
  EARLY_STAGE2            — improving structure, base/setup forming (lead time)
  AVOID_WEAK_CONFIRMATION — high event volume but weak underlying state
                            (likely speculative spike — explicit anti-pattern)

A symbol may match multiple criteria; highest-priority bucket wins
(triggered > early-stage > core > avoid). Symbols matching none are excluded.
"""

from __future__ import annotations

import pandas as pd

# Operational thresholds — kept module-level so they're easy to find/tune.
HIGH_STATE_PERCENTILE = 80.0   # top quintile composite_score qualifies as "strong regime"
WEAK_STATE_PERCENTILE = 50.0   # below median = weak regime (avoid bucket gate)
TRIGGER_CONVICTION_MIN = 50.0  # breakout_level (35) + at least one confirmation (>=15)
HIGH_EVENT_VOLUME = 1.5        # volume vs 20d avg — "today is unusually heavy"
EARLY_STAGE_MAX_BARS = 15      # freshness window for forming stage-2 transition

BUCKET_PRIORITY = (
    "TRIGGERED_TODAY",
    "EARLY_STAGE2",
    "CORE_MOMENTUM",
    "AVOID_WEAK_CONFIRMATION",
)

OUTPUT_COLUMNS = (
    "symbol_id",
    "watchlist_bucket",
    "bucket_reason",
    "composite_score",
    "composite_percentile",
    "conviction_score",
    "stage2_label",
    "is_stage2_candidate",
    "is_stage2_uptrend",
    "bars_in_stage",
    "volume_ratio_20",
    "sector_name",
)


def assign_watchlist_buckets(
    ranked_signals: pd.DataFrame,
    breakout_scan: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Return a DataFrame of symbols categorized into the 4 watchlist buckets.

    Parameters
    ----------
    ranked_signals
        Output of the rank stage. Must contain ``symbol_id`` and a composite
        score column (``composite_score_adjusted`` preferred, falls back to
        ``composite_score``). Stage-2 columns and ``volume_ratio_20`` are used
        when available; their absence simply disables the relevant bucket.
    breakout_scan
        Output of ``scan_breakouts``. Used to source ``conviction_score`` and
        ``volume_ratio_20`` if not already present on ``ranked_signals``.
        Optional — without it, ``TRIGGERED_TODAY`` will be empty.

    Returns
    -------
    DataFrame with one row per categorized symbol, sorted by bucket priority
    then composite percentile descending. Symbols matching no bucket are
    omitted (they are simply outside the watchlist scope).
    """
    if ranked_signals is None or ranked_signals.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    rs = ranked_signals.copy()
    score_col = (
        "composite_score_adjusted"
        if "composite_score_adjusted" in rs.columns
        else "composite_score"
    )
    rs.loc[:, "composite_score"] = pd.to_numeric(rs.get(score_col), errors="coerce")
    rs.loc[:, "composite_percentile"] = rs["composite_score"].rank(pct=True) * 100.0

    # Pull conviction + event-volume from breakout scan when ranking didn't carry them.
    if breakout_scan is not None and not breakout_scan.empty:
        bo_cols = [
            c for c in ("symbol_id", "conviction_score", "volume_ratio_20")
            if c in breakout_scan.columns
        ]
        if "symbol_id" in bo_cols and len(bo_cols) > 1:
            bo = breakout_scan[bo_cols].drop_duplicates("symbol_id", keep="first")
            # Don't clobber existing columns on ranked_signals
            new_cols = [c for c in bo.columns if c == "symbol_id" or c not in rs.columns]
            rs = rs.merge(bo[new_cols], on="symbol_id", how="left")

    # Ensure expected columns exist so downstream comparisons are well-defined.
    for col in ("conviction_score", "volume_ratio_20"):
        if col not in rs.columns:
            rs.loc[:, col] = pd.NA
    for col, default in (
        ("is_stage2_candidate", False),
        ("is_stage2_uptrend", False),
        ("bars_in_stage", pd.NA),
        ("stage2_label", pd.NA),
        ("sector_name", pd.NA),
    ):
        if col not in rs.columns:
            rs.loc[:, col] = default

    # --- Bucket masks ---------------------------------------------------------
    pct = pd.to_numeric(rs["composite_percentile"], errors="coerce")
    high_state = pct >= HIGH_STATE_PERCENTILE
    weak_state = pct < WEAK_STATE_PERCENTILE

    conviction = pd.to_numeric(rs["conviction_score"], errors="coerce").fillna(0.0)
    triggered_mask = high_state & (conviction >= TRIGGER_CONVICTION_MIN)

    is_candidate = rs["is_stage2_candidate"].fillna(False).astype(bool)
    is_uptrend = rs["is_stage2_uptrend"].fillna(False).astype(bool)
    bars = pd.to_numeric(rs["bars_in_stage"], errors="coerce").fillna(999)
    early_stage_mask = is_candidate & (~is_uptrend) & (bars <= EARLY_STAGE_MAX_BARS)

    core_mask = high_state & (~triggered_mask)

    event_vol = pd.to_numeric(rs["volume_ratio_20"], errors="coerce").fillna(0.0)
    avoid_mask = (event_vol >= HIGH_EVENT_VOLUME) & weak_state

    # --- Priority assignment (later writes override earlier ones) -------------
    rs.loc[:, "watchlist_bucket"] = pd.NA
    rs.loc[:, "bucket_reason"] = pd.NA

    rs.loc[avoid_mask, "watchlist_bucket"] = "AVOID_WEAK_CONFIRMATION"
    rs.loc[avoid_mask, "bucket_reason"] = "high event volume with weak rank-state score"

    rs.loc[core_mask, "watchlist_bucket"] = "CORE_MOMENTUM"
    rs.loc[core_mask, "bucket_reason"] = "top-quintile rank-state, no trigger today"

    rs.loc[early_stage_mask, "watchlist_bucket"] = "EARLY_STAGE2"
    rs.loc[early_stage_mask, "bucket_reason"] = "stage-2 candidate forming, fresh transition"

    rs.loc[triggered_mask, "watchlist_bucket"] = "TRIGGERED_TODAY"
    rs.loc[triggered_mask, "bucket_reason"] = "top-quintile rank-state + breakout trigger + conviction"

    out = rs.dropna(subset=["watchlist_bucket"]).copy()
    if out.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    bucket_order = pd.Categorical(
        out["watchlist_bucket"], categories=list(BUCKET_PRIORITY), ordered=True
    )
    out.loc[:, "watchlist_bucket"] = bucket_order
    out = out.sort_values(
        ["watchlist_bucket", "composite_percentile"],
        ascending=[True, False],
    ).reset_index(drop=True)

    available_cols = [c for c in OUTPUT_COLUMNS if c in out.columns]
    return out[available_cols]


def summarize_buckets(buckets_df: pd.DataFrame) -> dict:
    """Per-bucket counts for ``publish_summary.json`` metadata."""
    counts = {key: 0 for key in BUCKET_PRIORITY}
    counts["total"] = 0
    if buckets_df is None or buckets_df.empty:
        return counts
    series = buckets_df["watchlist_bucket"].astype(str).value_counts()
    for key, value in series.items():
        counts[key] = int(value)
    counts["total"] = int(buckets_df.shape[0])
    return counts
