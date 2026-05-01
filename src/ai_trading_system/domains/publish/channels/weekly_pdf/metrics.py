"""Phase 1 metrics: simple slices over a single run's artifacts.

Week-over-week diffs (rank_movers, sector_movers) and breadth artifact
generation are deferred to Phase 2.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


_RANK_DISPLAY_COLS = [
    "symbol_id",
    "sector_name",
    "composite_score",
    "rank_confidence",
    "stage2_label",
    "return_5",
    "return_20",
    "delivery_pct",
    "delivery_pct_imputed",
]


def _select_existing(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    keep = [c for c in cols if c in df.columns]
    return df.loc[:, keep].copy() if keep else pd.DataFrame()


def top_ranked(ranked: pd.DataFrame, n: int = 25) -> pd.DataFrame:
    if ranked.empty or "composite_score" not in ranked.columns:
        return pd.DataFrame()
    out = ranked.sort_values("composite_score", ascending=False).head(n)
    return _select_existing(out, _RANK_DISPLAY_COLS).reset_index(drop=True)


def tier_a_breakouts(breakouts: pd.DataFrame, n: int = 25) -> pd.DataFrame:
    if breakouts.empty or "candidate_tier" not in breakouts.columns:
        return pd.DataFrame()
    tier_a = breakouts[breakouts["candidate_tier"] == "A"].copy()
    cols = [
        "symbol_id",
        "sector_name",
        "breakout_state",
        "pass_count",
        "distance_from_breakout",
        "volume_confirmation",
        "stage2_structural",
    ]
    return _select_existing(tier_a, cols).head(n).reset_index(drop=True)


def tier_b_breakouts(breakouts: pd.DataFrame, n: int = 25) -> pd.DataFrame:
    if breakouts.empty or "candidate_tier" not in breakouts.columns:
        return pd.DataFrame()
    tier_b = breakouts[breakouts["candidate_tier"] == "B"].copy()
    cols = [
        "symbol_id",
        "sector_name",
        "breakout_state",
        "pass_count",
        "distance_from_breakout",
        "volume_confirmation",
    ]
    return _select_existing(tier_b, cols).head(n).reset_index(drop=True)


def top_patterns(patterns: pd.DataFrame, n: int = 25) -> pd.DataFrame:
    if patterns.empty:
        return pd.DataFrame()
    sort_col = "pattern_score" if "pattern_score" in patterns.columns else None
    out = patterns.sort_values(sort_col, ascending=False).head(n) if sort_col else patterns.head(n)
    cols = [
        "symbol_id",
        "sector_name",
        "pattern_family",
        "pattern_state",
        "pattern_priority_tier",
        "pattern_score",
        "breakout_level",
        "volume_ratio_20",
        "stage2_label",
    ]
    return _select_existing(out, cols).reset_index(drop=True)


def sector_leaders(sector_df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    if sector_df.empty:
        return pd.DataFrame()
    sort_col = None
    for candidate in ("RS", "RS_20", "RS_rank"):
        if candidate in sector_df.columns:
            sort_col = candidate
            break
    out = sector_df.sort_values(sort_col, ascending=False).head(n) if sort_col else sector_df.head(n)
    cols = ["Sector", "RS", "RS_20", "RS_50", "Momentum", "Quadrant", "RS_rank"]
    return _select_existing(out, cols).reset_index(drop=True)


def volume_delivery_movers(ranked: pd.DataFrame, n: int = 25) -> pd.DataFrame:
    """Stage1 accumulation filter: meaningful weekly return + high delivery + price above SMA50.

    Uses fields available in ranked_signals; missing columns degrade gracefully.
    """
    if ranked.empty:
        return pd.DataFrame()
    df = ranked.copy()

    mask = pd.Series(True, index=df.index)
    if "return_5" in df.columns:
        mask &= df["return_5"].fillna(0) >= 0.05
    if "delivery_pct" in df.columns:
        mask &= df["delivery_pct"].fillna(0) >= 40.0
    out = df[mask]
    sort_col = "return_5" if "return_5" in out.columns else "composite_score"
    if sort_col in out.columns:
        out = out.sort_values(sort_col, ascending=False)

    cols = [
        "symbol_id",
        "sector_name",
        "return_5",
        "return_20",
        "delivery_pct",
        "delivery_pct_imputed",
        "composite_score",
        "stage2_label",
    ]
    return _select_existing(out, cols).head(n).reset_index(drop=True)


def regime_summary(
    rank_summary: Dict[str, Any],
    dashboard_payload: Dict[str, Any],
    sector_df: pd.DataFrame,
    ranked: pd.DataFrame,
    trust_status_fallback: str = "unknown",
) -> Dict[str, Any]:
    """Compose a one-glance regime panel from existing summaries."""
    summary = (dashboard_payload or {}).get("summary", {}) or {}
    quadrant_counts: Dict[str, int] = {}
    if not sector_df.empty and "Quadrant" in sector_df.columns:
        quadrant_counts = sector_df["Quadrant"].value_counts().to_dict()

    stage2_count = 0
    if not ranked.empty and "stage2_label" in ranked.columns:
        stage2_count = int((ranked["stage2_label"].astype(str).str.lower() == "stage2").sum())

    trust_status = (
        rank_summary.get("data_trust_status")
        or summary.get("data_trust_status")
        or trust_status_fallback
    )

    return {
        "trust_status": trust_status,
        "trust_confidence": rank_summary.get("trust_confidence"),
        "ml_status": rank_summary.get("ml_status"),
        "market_stage": summary.get("market_stage") or rank_summary.get("market_stage"),
        "universe_count": rank_summary.get("symbol_universe_count"),
        "stage2_count": stage2_count,
        "sector_quadrant_counts": quadrant_counts,
    }


def serialize_for_json(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert DataFrames to lists of records for JSON output."""
    serialized: Dict[str, Any] = {}
    for key, value in data.items():
        if isinstance(value, pd.DataFrame):
            serialized[key] = value.to_dict(orient="records")
        else:
            serialized[key] = value
    return serialized
