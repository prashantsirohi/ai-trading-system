"""Metrics: single-run slices and week-over-week diffs.

Phase 1: top-N slices per artifact.
Phase 2: rank/sector movers (vs prior-week snapshot), failed-breakout flagging.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

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


_BREAKOUT_DISPLAY_COLS = [
    "symbol_id",
    "sector",
    "close",
    "prior_range_high",
    "breakout_state",
    "breakout_score",
    "breakout_pct",
    "volume_ratio",
    "near_52w_high_pct",
    "above_sma200",
    "setup_quality",
]


def tier_a_breakouts(breakouts: pd.DataFrame, n: int = 25) -> pd.DataFrame:
    if breakouts.empty or "candidate_tier" not in breakouts.columns:
        return pd.DataFrame()
    tier_a = breakouts[breakouts["candidate_tier"] == "A"].copy()
    if "breakout_score" in tier_a.columns:
        tier_a = tier_a.sort_values("breakout_score", ascending=False)
    return _select_existing(tier_a, _BREAKOUT_DISPLAY_COLS).head(n).reset_index(drop=True)


def tier_b_breakouts(breakouts: pd.DataFrame, n: int = 25) -> pd.DataFrame:
    if breakouts.empty or "candidate_tier" not in breakouts.columns:
        return pd.DataFrame()
    tier_b = breakouts[breakouts["candidate_tier"] == "B"].copy()
    if "breakout_score" in tier_b.columns:
        tier_b = tier_b.sort_values("breakout_score", ascending=False)
    return _select_existing(tier_b, _BREAKOUT_DISPLAY_COLS).head(n).reset_index(drop=True)


def top_patterns(patterns: pd.DataFrame, n: int = 25) -> pd.DataFrame:
    if patterns.empty:
        return pd.DataFrame()
    sort_col = "pattern_score" if "pattern_score" in patterns.columns else None
    out = patterns.sort_values(sort_col, ascending=False).head(n) if sort_col else patterns.head(n)
    cols = [
        "symbol_id",
        "pattern_family",
        "pattern_state",
        "pattern_operational_tier",
        "pattern_score",
        "pattern_priority_score",
        "breakout_level",
        "volume_ratio_20",
        "stage2_label",
        "setup_quality",
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


def _rank_position(df: pd.DataFrame) -> pd.Series:
    """Return rank position by composite_score (1 = highest), aligned to df index."""
    if df.empty or "composite_score" not in df.columns:
        return pd.Series(dtype="float64")
    return df["composite_score"].rank(ascending=False, method="min")


def compute_rank_movers(
    current: pd.DataFrame,
    prior: pd.DataFrame,
    top_n: int = 15,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return (top_improvers, top_decliners) by absolute rank change.

    Both inputs are ranked_signals-style DataFrames; need at least
    `symbol_id` and `composite_score`.
    """
    if current.empty or prior.empty or "composite_score" not in current.columns:
        return pd.DataFrame(), pd.DataFrame()

    cur = current.copy()
    cur.loc[:, "rank_position"] = _rank_position(cur)
    pri = prior[["symbol_id", "composite_score"]].copy() if "composite_score" in prior.columns else pd.DataFrame()
    if pri.empty:
        return pd.DataFrame(), pd.DataFrame()
    pri.loc[:, "prev_rank_position"] = _rank_position(prior)
    pri = pri.rename(columns={"composite_score": "prev_composite_score"})

    merged = cur.merge(pri, on="symbol_id", how="inner").copy()
    if merged.empty:
        return pd.DataFrame(), pd.DataFrame()
    merged.loc[:, "rank_change"] = merged["prev_rank_position"] - merged["rank_position"]
    merged.loc[:, "score_change"] = merged["composite_score"] - merged["prev_composite_score"]

    keep_cols = [
        "symbol_id",
        "sector_name",
        "rank_position",
        "prev_rank_position",
        "rank_change",
        "composite_score",
        "prev_composite_score",
        "score_change",
        "return_5",
        "return_20",
        "delivery_pct",
        "stage2_label",
    ]
    out = _select_existing(merged, keep_cols)

    improvers = out.sort_values("rank_change", ascending=False).head(top_n).reset_index(drop=True)
    decliners = out.sort_values("rank_change", ascending=True).head(top_n).reset_index(drop=True)
    return improvers, decliners


def compute_sector_movers(
    current: pd.DataFrame,
    prior: pd.DataFrame,
    top_n: int = 10,
) -> pd.DataFrame:
    """Diff sector_dashboard rank/RS week-over-week."""
    if current.empty or prior.empty or "Sector" not in current.columns:
        return pd.DataFrame()
    cur = current.copy()
    pri_cols = [c for c in ("Sector", "RS_rank", "RS") if c in prior.columns]
    if "Sector" not in pri_cols:
        return pd.DataFrame()
    pri = prior.loc[:, pri_cols].rename(
        columns={"RS_rank": "prev_RS_rank", "RS": "prev_RS"}
    )
    merged = cur.merge(pri, on="Sector", how="left").copy()
    if "RS_rank" in merged.columns and "prev_RS_rank" in merged.columns:
        merged.loc[:, "rank_change"] = merged["prev_RS_rank"] - merged["RS_rank"]
    if "RS" in merged.columns and "prev_RS" in merged.columns:
        merged.loc[:, "rs_change"] = merged["RS"] - merged["prev_RS"]
    keep = ["Sector", "RS", "prev_RS", "rs_change", "RS_rank", "prev_RS_rank", "rank_change", "Quadrant"]
    out = _select_existing(merged, keep)
    if "rank_change" in out.columns:
        out = out.sort_values("rank_change", ascending=False)
    return out.head(top_n).reset_index(drop=True)


def detect_failed_breakouts(
    current_breakouts: pd.DataFrame,
    prior_breakouts_per_run: Iterable[Tuple[str, pd.DataFrame]],
    current_ranked: pd.DataFrame,
    top_n: int = 25,
) -> pd.DataFrame:
    """Flag symbols that broke out in the lookback window but now sit below the trigger.

    `prior_breakouts_per_run` yields (run_id, breakout_scan_df) for the lookback window.
    A failed breakout is a symbol where any prior run had `breakout_detected=True` and
    the current close is below the corresponding `prior_range_high` trigger.
    """
    if current_ranked.empty or "symbol_id" not in current_ranked.columns:
        return pd.DataFrame()

    triggered: Dict[str, Dict[str, Any]] = {}
    for run_id, df in prior_breakouts_per_run:
        if df is None or df.empty or "breakout_detected" not in df.columns:
            continue
        recent = df[df["breakout_detected"].astype(str).str.lower().isin(["true", "1"])]
        cols_needed = [c for c in ("symbol_id", "prior_range_high", "candidate_tier") if c in recent.columns]
        if "symbol_id" not in cols_needed:
            continue
        for row in recent[cols_needed].itertuples(index=False):
            sym = getattr(row, "symbol_id", None)
            if sym is None:
                continue
            triggered.setdefault(
                str(sym),
                {
                    "trigger_run_id": run_id,
                    "trigger_level": getattr(row, "prior_range_high", None),
                    "trigger_tier": getattr(row, "candidate_tier", None),
                },
            )

    if not triggered:
        return pd.DataFrame()

    cur_close = (
        current_ranked.loc[:, ["symbol_id", "close"]].dropna(subset=["close"])
        if "close" in current_ranked.columns
        else pd.DataFrame()
    )
    if cur_close.empty:
        return pd.DataFrame()

    rows = []
    close_lookup = dict(zip(cur_close["symbol_id"], cur_close["close"]))
    sector_lookup = {}
    if "sector_name" in current_ranked.columns:
        sector_lookup = dict(zip(current_ranked["symbol_id"], current_ranked["sector_name"]))
    for sym, info in triggered.items():
        cur = close_lookup.get(sym)
        trig = info.get("trigger_level")
        if cur is None or trig is None:
            continue
        try:
            cur_f = float(cur)
            trig_f = float(trig)
        except (TypeError, ValueError):
            continue
        if cur_f < trig_f:
            rows.append({
                "symbol_id": sym,
                "sector_name": sector_lookup.get(sym),
                "trigger_run_id": info["trigger_run_id"],
                "trigger_level": trig_f,
                "current_close": cur_f,
                "drop_pct": round((cur_f - trig_f) / trig_f * 100, 2),
                "trigger_tier": info.get("trigger_tier"),
            })

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).sort_values("drop_pct", ascending=True)
    return out.head(top_n).reset_index(drop=True)


def serialize_for_json(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert DataFrames to lists of records for JSON output."""
    serialized: Dict[str, Any] = {}
    for key, value in data.items():
        if isinstance(value, pd.DataFrame):
            serialized[key] = value.to_dict(orient="records")
        else:
            serialized[key] = value
    return serialized
