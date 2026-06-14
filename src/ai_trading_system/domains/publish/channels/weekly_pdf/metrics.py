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

_STAGE2_LABELS = {"stage2", "strong_stage2", "stage1_to_stage2", "stage_2", "stage 2"}


def _normalized_stage2_label(value: Any) -> str:
    return str(value or "").strip().lower()


def _is_stage2_like(value: Any) -> bool:
    return _normalized_stage2_label(value) in _STAGE2_LABELS


def _numeric_series(df: pd.DataFrame, column: str, default: float | None = None) -> pd.Series:
    if column in df.columns:
        return pd.to_numeric(df[column], errors="coerce")
    return pd.Series(default, index=df.index, dtype="float64")


def _symbol_col(df: pd.DataFrame) -> str | None:
    for col in ("symbol", "symbol_id", "ticker"):
        if col in df.columns:
            return col
    return None


def _normalize_symbol_frame(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    col = _symbol_col(out)
    if col is None:
        return out
    out.loc[:, "symbol"] = out[col].astype(str)
    return out


def fmt_signed_int(value: Any) -> str:
    if value is None or pd.isna(value):
        return "—"
    try:
        number = int(round(float(value)))
    except (TypeError, ValueError):
        return str(value)
    if number > 0:
        return f"+{number}"
    return str(number)


def stage2_summary_for_report(ranked: pd.DataFrame) -> dict[str, int]:
    if ranked is None or ranked.empty or "stage2_label" not in ranked.columns:
        return {"stage2_names": 0, "strong_stage2": 0, "transition_stage2": 0, "raw_stage2": 0}
    labels = ranked["stage2_label"].map(_normalized_stage2_label)
    return {
        "stage2_names": int(labels.isin(_STAGE2_LABELS).sum()),
        "strong_stage2": int(labels.eq("strong_stage2").sum()),
        "transition_stage2": int(labels.eq("stage1_to_stage2").sum()),
        "raw_stage2": int(labels.eq("stage2").sum()),
    }


def _select_existing(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    keep = [c for c in cols if c in df.columns]
    return df.loc[:, keep].copy() if keep else pd.DataFrame()


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _with_volume_ratio(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if {"volume", "vol_20_avg"}.issubset(out.columns):
        denom = pd.to_numeric(out["vol_20_avg"], errors="coerce").replace(0, pd.NA)
        out.loc[:, "volume_ratio_20d"] = pd.to_numeric(out["volume"], errors="coerce") / denom
    return out


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


def sector_rotation_summary(sector_rotation: pd.DataFrame, n: int = 30) -> pd.DataFrame:
    """Latest RRG-style sector rotation rows for the weekly report."""
    if sector_rotation is None or sector_rotation.empty:
        return pd.DataFrame()
    df = sector_rotation.copy()
    if "date" in df.columns:
        parsed_date = pd.to_datetime(df["date"], errors="coerce")
        latest = parsed_date.max()
        if pd.notna(latest):
            df = df.loc[parsed_date.eq(latest)].copy()
            parsed_date = parsed_date.loc[df.index]
        df.loc[:, "date"] = parsed_date.dt.date.astype(str)
    if "industry" not in df.columns and "Sector" in df.columns:
        df.loc[:, "industry"] = df["Sector"]
    if "quadrant" not in df.columns and "Quadrant" in df.columns:
        df.loc[:, "quadrant"] = df["Quadrant"]
    if "quadrant" in df.columns:
        order = {"Leading": 0, "Improving": 1, "Weakening": 2, "Lagging": 3}
        df.loc[:, "_quadrant_order"] = df["quadrant"].map(order).fillna(9)
    else:
        df.loc[:, "_quadrant_order"] = 9
    for col in ("rs_ratio", "rs_momentum", "alpha_20d", "alpha_60d", "sector_return_20d"):
        if col in df.columns:
            df.loc[:, col] = pd.to_numeric(df[col], errors="coerce")
    sort_cols = ["_quadrant_order"]
    ascending = [True]
    if "rs_ratio" in df.columns:
        sort_cols.append("rs_ratio")
        ascending.append(False)
    if "rs_momentum" in df.columns:
        sort_cols.append("rs_momentum")
        ascending.append(False)
    out = df.sort_values(sort_cols, ascending=ascending, kind="stable").head(n)
    cols = [
        "date",
        "industry",
        "quadrant",
        "rs_ratio",
        "rs_momentum",
        "sector_return_20d",
        "alpha_20d",
        "alpha_60d",
        "outperformance_bucket",
    ]
    return _select_existing(out, cols).reset_index(drop=True)


def sector_rotation_information(sector_rotation: pd.DataFrame, n: int = 50) -> pd.DataFrame:
    """Operator-friendly sector table with momentum and RS buckets."""
    summary = sector_rotation_summary(sector_rotation, n=n)
    if summary.empty:
        return pd.DataFrame()
    out = summary.copy()
    out.loc[:, "momentum_category"] = out.get("rs_momentum", pd.Series(index=out.index)).map(_momentum_category)
    out.loc[:, "relative_strength_category"] = out.get("rs_ratio", pd.Series(index=out.index)).map(_relative_strength_category)
    cols = [
        "industry",
        "quadrant",
        "momentum_category",
        "relative_strength_category",
        "rs_ratio",
        "rs_momentum",
        "outperformance_bucket",
    ]
    return _select_existing(out, cols).reset_index(drop=True)


def split_stock_rotation(stock_rotation: pd.DataFrame, n: int = 12) -> dict[str, pd.DataFrame]:
    """Split stock rotation candidates into the four RRG quadrants."""
    empty = pd.DataFrame()
    keys = {"improving": empty, "leading": empty, "lagging": empty, "weakening": empty}
    if stock_rotation is None or stock_rotation.empty:
        return keys
    df = stock_rotation.copy()
    if "quadrant" not in df.columns and "Quadrant" in df.columns:
        df.loc[:, "quadrant"] = df["Quadrant"]
    if "symbol" not in df.columns and "symbol_id" in df.columns:
        df.loc[:, "symbol"] = df["symbol_id"]
    sort_col = "rotation_adjusted_score" if "rotation_adjusted_score" in df.columns else "composite_score"
    if sort_col in df.columns:
        df.loc[:, sort_col] = pd.to_numeric(df[sort_col], errors="coerce")
    cols = [
        "symbol",
        "company_name",
        "industry",
        "market_cap",
        "return_1d",
        "return_1w",
        "return_1m",
        "rs_ratio",
        "rs_momentum",
        "quadrant",
        "sector_quadrant",
        "composite_score",
        "rotation_adjusted_score",
        "near_52w_high_pct",
        "delivery_signal",
        "watchlist_candidate",
    ]
    out: dict[str, pd.DataFrame] = {}
    for key, label in (
        ("improving", "Improving"),
        ("leading", "Leading"),
        ("lagging", "Lagging"),
        ("weakening", "Weakening"),
    ):
        frame = df.loc[df["quadrant"].astype(str).str.casefold().eq(label.casefold())].copy()
        if sort_col in frame.columns:
            frame = frame.sort_values(sort_col, ascending=False, na_position="last", kind="stable")
        out[key] = _select_existing(frame, cols).head(n).reset_index(drop=True)
    return out


def accumulation_distribution_tables(accumulation_distribution: pd.DataFrame, n: int = 12) -> dict[str, pd.DataFrame]:
    """Split delivery-based accumulation and distribution signals."""
    empty = pd.DataFrame()
    if accumulation_distribution is None or accumulation_distribution.empty or "delivery_signal" not in accumulation_distribution.columns:
        return {"accumulation": empty, "distribution": empty}
    df = accumulation_distribution.copy()
    score_col = "accumulation_score" if "accumulation_score" in df.columns else None
    if score_col:
        df.loc[:, score_col] = pd.to_numeric(df[score_col], errors="coerce")
    cols = [
        "symbol",
        "date",
        "close",
        "delivery_pct",
        "delivery_pct_z20",
        "volume_z20",
        "price_return_5d",
        "delivery_signal",
        "accumulation_score",
    ]
    result: dict[str, pd.DataFrame] = {}
    for key, label in (("accumulation", "Accumulation"), ("distribution", "Distribution")):
        frame = df.loc[df["delivery_signal"].astype(str).str.casefold().eq(label.casefold())].copy()
        if score_col:
            frame = frame.sort_values(score_col, ascending=False, na_position="last", kind="stable")
        result[key] = _select_existing(frame, cols).head(n).reset_index(drop=True)
    return result


def delivery_trend_summary(accumulation_distribution: pd.DataFrame, n: int = 12) -> pd.DataFrame:
    """Compact latest delivery trend rows ranked by delivery/volume signal strength."""
    if accumulation_distribution is None or accumulation_distribution.empty:
        return pd.DataFrame()
    df = accumulation_distribution.copy()
    for column in ("delivery_pct_z20", "volume_z20", "price_return_5d", "accumulation_score", "delivery_pct"):
        if column in df.columns:
            df.loc[:, column] = pd.to_numeric(df[column], errors="coerce")
    if "delivery_pct_z20" in df.columns:
        df.loc[:, "_signal_abs"] = df["delivery_pct_z20"].abs()
    elif "accumulation_score" in df.columns:
        df.loc[:, "_signal_abs"] = (df["accumulation_score"] - 50).abs()
    else:
        df.loc[:, "_signal_abs"] = 0
    sort_cols = ["_signal_abs"]
    ascending = [False]
    if "accumulation_score" in df.columns:
        sort_cols.append("accumulation_score")
        ascending.append(False)
    df = df.sort_values(sort_cols, ascending=ascending, na_position="last", kind="stable")
    cols = [
        "symbol",
        "date",
        "delivery_signal",
        "delivery_pct",
        "delivery_pct_z20",
        "volume_z20",
        "price_return_5d",
        "accumulation_score",
    ]
    return _select_existing(df, cols).head(n).reset_index(drop=True)


def custom_indices_summary(
    sector_custom_indices: pd.DataFrame,
    sector_rotation: pd.DataFrame,
    n: int = 20,
) -> pd.DataFrame:
    """Latest custom sector index state with sector-vs-benchmark context."""
    if sector_custom_indices is None or sector_custom_indices.empty:
        return pd.DataFrame()
    idx = sector_custom_indices.copy()
    if "date" in idx.columns:
        idx.loc[:, "_parsed_date"] = pd.to_datetime(idx["date"], errors="coerce")
        idx = idx.sort_values(["industry", "_parsed_date"], kind="stable").drop_duplicates("industry", keep="last")
        idx.loc[:, "date"] = idx["_parsed_date"].dt.date.astype(str)
        idx = idx.drop(columns=["_parsed_date"])
    if sector_rotation is not None and not sector_rotation.empty and "industry" in sector_rotation.columns:
        rot = sector_rotation_summary(sector_rotation, n=1000)
        merge_cols = [
            col
            for col in ("industry", "quadrant", "alpha_20d", "alpha_60d", "outperformance_bucket")
            if col in rot.columns
        ]
        if "industry" in merge_cols:
            idx = idx.merge(rot.loc[:, merge_cols].drop_duplicates("industry"), on="industry", how="left")
    for col in ("sector_index", "alpha_20d", "alpha_60d", "constituent_count"):
        if col in idx.columns:
            idx.loc[:, col] = pd.to_numeric(idx[col], errors="coerce")
    sort_col = "alpha_20d" if "alpha_20d" in idx.columns else "sector_index"
    if sort_col in idx.columns:
        idx = idx.sort_values(sort_col, ascending=False, na_position="last", kind="stable")
    cols = [
        "date",
        "industry",
        "sector_index",
        "weighting_method",
        "constituent_count",
        "quadrant",
        "alpha_20d",
        "alpha_60d",
        "outperformance_bucket",
    ]
    return _select_existing(idx, cols).head(n).reset_index(drop=True)


def _momentum_category(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return "Unknown"
    if number >= 105:
        return "Very High Momentum"
    if number >= 101:
        return "High Momentum"
    if number >= 99:
        return "Medium Momentum"
    if number >= 95:
        return "Low Momentum"
    return "Very Low Momentum"


def _relative_strength_category(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return "Unknown"
    if number >= 105:
        return "Very High Relative Strength"
    if number >= 101:
        return "High Relative Strength"
    if number >= 99:
        return "Medium Relative Strength"
    return "Low Relative Strength"


def volume_delivery_movers(ranked: pd.DataFrame, n: int = 25) -> pd.DataFrame:
    """Weekly price movers backed by volume activity and high delivery.

    Uses fields available in ranked_signals; missing columns degrade gracefully.
    """
    if ranked.empty or "return_5" not in ranked.columns:
        return pd.DataFrame()
    df = _with_volume_ratio(ranked)

    mask = pd.Series(True, index=df.index)
    mask &= pd.to_numeric(df["return_5"], errors="coerce").fillna(0) >= 5.0
    if "delivery_pct" in df.columns:
        mask &= pd.to_numeric(df["delivery_pct"], errors="coerce").fillna(0) >= 40.0
    volume_masks = []
    if "volume_zscore_20" in df.columns:
        volume_masks.append(pd.to_numeric(df["volume_zscore_20"], errors="coerce").fillna(0) >= 1.0)
    if "volume_ratio_20d" in df.columns:
        volume_masks.append(pd.to_numeric(df["volume_ratio_20d"], errors="coerce").fillna(0) >= 1.25)
    if volume_masks:
        volume_mask = volume_masks[0]
        for item in volume_masks[1:]:
            volume_mask |= item
        mask &= volume_mask
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
        "volume_zscore_20",
        "volume_ratio_20d",
        "composite_score",
        "stage2_label",
    ]
    return _select_existing(out, cols).head(n).reset_index(drop=True)


def unusual_volume_shockers(ranked: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    """Institutional-buying proxy: unusual volume + high delivery + non-negative 5d price."""
    if ranked.empty:
        return pd.DataFrame()
    df = _with_volume_ratio(ranked)
    if "delivery_pct" not in df.columns:
        return pd.DataFrame()

    mask = pd.to_numeric(df["delivery_pct"], errors="coerce").fillna(0) >= 50.0
    if "return_5" in df.columns:
        mask &= pd.to_numeric(df["return_5"], errors="coerce").fillna(0) >= 0.0
    if "exhaustion_flag" in df.columns:
        mask &= ~df["exhaustion_flag"].map(_truthy).fillna(False)

    volume_masks = []
    if "volume_zscore_20" in df.columns:
        volume_masks.append(pd.to_numeric(df["volume_zscore_20"], errors="coerce").fillna(0) >= 2.0)
    if "volume_ratio_20d" in df.columns:
        volume_masks.append(pd.to_numeric(df["volume_ratio_20d"], errors="coerce").fillna(0) >= 1.75)
    if not volume_masks:
        return pd.DataFrame()
    volume_mask = volume_masks[0]
    for item in volume_masks[1:]:
        volume_mask |= item
    out = df[mask & volume_mask].copy()
    if out.empty:
        return pd.DataFrame()

    sort_cols = [c for c in ("volume_zscore_20", "delivery_pct", "return_5") if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, ascending=False)
    cols = [
        "symbol_id",
        "sector_name",
        "return_5",
        "return_20",
        "delivery_pct",
        "volume_zscore_20",
        "volume_ratio_20d",
        "composite_score",
        "stage2_label",
    ]
    return _select_existing(out, cols).head(n).reset_index(drop=True)


def weekly_price_movers(ranked: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    if ranked.empty or "return_5" not in ranked.columns:
        return pd.DataFrame()
    df = _with_volume_ratio(ranked)
    out = df.sort_values("return_5", ascending=False)
    cols = [
        "symbol_id",
        "sector_name",
        "return_5",
        "return_20",
        "delivery_pct",
        "volume_zscore_20",
        "volume_ratio_20d",
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

    stage2_count = stage2_summary_for_report(ranked)["stage2_names"]

    trust_status = str(
        rank_summary.get("data_trust_status")
        or summary.get("data_trust_status")
        or trust_status_fallback
    )
    trust_confidence = rank_summary.get("trust_confidence")
    trust_headlines: list[str] = []
    trust_score = None
    if isinstance(trust_confidence, dict):
        trust_score = trust_confidence.get("rank_confidence") or trust_confidence.get("provider_confidence")
        quarantined_dates = trust_confidence.get("active_quarantined_dates") or []
        quarantined_symbols = trust_confidence.get("active_quarantined_symbols")
        if quarantined_dates:
            trust_headlines.append(f"{len(quarantined_dates)} quarantined dates")
        if quarantined_symbols:
            trust_headlines.append(f"{quarantined_symbols} quarantined symbols")
        unknown_ratio = trust_confidence.get("unknown_ratio_latest")
        if unknown_ratio:
            trust_headlines.append(f"{float(unknown_ratio) * 100:.1f}% unknown rows latest")
    else:
        trust_score = trust_confidence

    return {
        "trust_status": trust_status,
        "trust_confidence": trust_score,
        "trust_headlines": trust_headlines[:2],
        "trust_details": trust_confidence,
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

    rank_change_num = pd.to_numeric(out["rank_change"], errors="coerce")
    score_change_num = pd.to_numeric(out["score_change"], errors="coerce")

    improvers = (
        out.loc[rank_change_num > 0]
        .assign(_rank_change_num=rank_change_num, _score_change_num=score_change_num)
        .sort_values(["_rank_change_num", "_score_change_num"], ascending=[False, False])
        .drop(columns=["_rank_change_num", "_score_change_num"])
        .head(top_n)
        .reset_index(drop=True)
    )
    decliners = (
        out.loc[rank_change_num < 0]
        .assign(_rank_change_num=rank_change_num, _score_change_num=score_change_num)
        .sort_values(["_rank_change_num", "_score_change_num"], ascending=[True, True])
        .drop(columns=["_rank_change_num", "_score_change_num"])
        .head(top_n)
        .reset_index(drop=True)
    )
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


def best_patterns_by_symbol(patterns: pd.DataFrame, n: int = 25) -> pd.DataFrame:
    if patterns is None or patterns.empty or "symbol_id" not in patterns.columns:
        return pd.DataFrame()
    df = patterns.copy()
    score_col = next(
        (c for c in ("pattern_priority_score", "pattern_score", "setup_quality") if c in df.columns),
        None,
    )
    df.loc[:, "_ranking_score"] = _numeric_series(df, score_col, 0).fillna(0) if score_col else 0
    grouped = df.groupby("symbol_id", dropna=False)
    counts = grouped.size().rename("pattern_count")
    if "pattern_family" in df.columns:
        all_patterns = grouped["pattern_family"].agg(
            lambda s: ", ".join(sorted({str(x) for x in s.dropna() if str(x)}))
        ).rename("all_patterns")
    else:
        all_patterns = pd.Series("", index=counts.index, name="all_patterns")
    best_idx = grouped["_ranking_score"].idxmax()
    best = df.loc[best_idx.dropna()].copy()
    best = best.merge(counts, on="symbol_id", how="left").merge(all_patterns, on="symbol_id", how="left")
    cols = [
        "symbol_id",
        "pattern_family",
        "all_patterns",
        "pattern_count",
        "pattern_state",
        "pattern_operational_tier",
        "pattern_score",
        "pattern_priority_score",
        "breakout_level",
        "volume_ratio_20",
        "stage2_label",
        "setup_quality",
    ]
    out = _select_existing(best.sort_values("_ranking_score", ascending=False), cols)
    return out.head(n).reset_index(drop=True)


def build_executive_decision_panel(
    *,
    ranked: pd.DataFrame,
    watchlist: pd.DataFrame | None = None,
    rank_improvers: pd.DataFrame | None = None,
    rank_decliners: pd.DataFrame | None = None,
    patterns_best: pd.DataFrame | None = None,
    breadth_latest: dict | None = None,
    trust_status: str = "unknown",
) -> dict[str, object]:
    breadth = breadth_latest or {}
    sma20 = _safe_float(breadth.get("pct_above_sma20"))
    sma50 = _safe_float(breadth.get("pct_above_sma50"))
    sma200 = _safe_float(breadth.get("pct_above_sma200"))
    if sma20 is None or sma50 is None:
        risk_label = "UNKNOWN"
        market_message = "Breadth unavailable."
    elif sma20 < 40 and sma50 < 55:
        risk_label = "RISK_OFF"
        market_message = "Short-term breadth weak; avoid chasing."
    elif 40 <= sma20 <= 55 and sma50 > 55:
        risk_label = "NARROWING"
        market_message = "Constructive but narrowing; prefer RS leaders."
    elif sma50 > 65 and (sma200 or 0) > 50:
        risk_label = "RISK_ON"
        market_message = "Broad participation supportive."
    else:
        risk_label = "CAUTIOUS"
        market_message = "Mixed breadth; keep position sizing selective."

    patterns_lookup = _pattern_lookup(patterns_best)
    action_rows = _actionable_from_watchlist(watchlist, patterns_lookup)
    if not action_rows:
        action_rows = _actionable_from_ranked(ranked, patterns_lookup)

    track_rows = _track_rows(ranked, rank_improvers, patterns_best, patterns_lookup)
    avoid_rows = _avoid_rows(rank_decliners)
    quality = (
        "Data quality trusted."
        if str(trust_status).lower() == "trusted"
        else f"Data trust is {trust_status}; verify candidates before execution."
    )
    return {
        "market_message": market_message,
        "risk_label": risk_label,
        "top_actionable": action_rows[:8],
        "track_next": track_rows[:8],
        "avoid_or_reduce": avoid_rows[:8],
        "data_quality_message": quality,
    }


def fund_value_tech_overlap(
    *,
    ranked: pd.DataFrame,
    watchlist: pd.DataFrame | None = None,
    quarterly: pd.DataFrame | None = None,
    valuation: pd.DataFrame | None = None,
    patterns_best: pd.DataFrame | None = None,
    n: int = 20,
) -> pd.DataFrame:
    ranked_n = _normalize_symbol_frame(ranked)
    watch_n = _normalize_symbol_frame(watchlist)
    quarterly_n = _normalize_symbol_frame(quarterly)
    valuation_n = _normalize_symbol_frame(valuation)
    patterns_n = _normalize_symbol_frame(patterns_best)

    if not watch_n.empty and any(c in watch_n.columns for c in ("quarterly_result_score", "watchlist_bucket")):
        base = watch_n.copy()
    elif not ranked_n.empty:
        base = ranked_n.copy()
    else:
        return pd.DataFrame()

    for other, suffix in (
        (ranked_n, "_ranked"),
        (quarterly_n, "_quarterly"),
        (valuation_n, "_valuation"),
        (patterns_n, "_pattern"),
    ):
        if other.empty or "symbol" not in other.columns:
            continue
        add = other.drop_duplicates("symbol")
        add_cols = [c for c in add.columns if c == "symbol" or c not in base.columns]
        base = base.merge(add.loc[:, add_cols], on="symbol", how="left", suffixes=("", suffix))

    q_score = _numeric_series(base, "quarterly_result_score", 50).fillna(50)
    v_score = _numeric_series(base, "valuation_history_score", 50).fillna(50)
    composite = _numeric_series(base, "composite_score", 50).fillna(50)
    delivery = _numeric_series(base, "delivery_pct", 0).fillna(0).clip(0, 100)
    pattern_bonus = pd.Series(50, index=base.index, dtype="float64")
    if "pattern_operational_tier" in base.columns:
        tiers = base["pattern_operational_tier"].astype(str).str.lower()
        pattern_bonus = pattern_bonus.mask(tiers.str.contains("tier_1|tier-1|confirmed", regex=True), 100)
        has_pattern = base.get("pattern_family", pd.Series(pd.NA, index=base.index)).notna()
        pattern_bonus = pattern_bonus.mask(has_pattern & pattern_bonus.lt(100), 75)

    base.loc[:, "overlap_score"] = (
        0.35 * q_score + 0.20 * v_score + 0.25 * composite + 0.10 * delivery + 0.10 * pattern_bonus
    ).round(2)
    buckets = base.get("watchlist_bucket", pd.Series("", index=base.index)).astype(str)
    base.loc[:, "technical_only"] = q_score.eq(50) & ~buckets.str.startswith("F", na=False)

    def _action(row: pd.Series) -> str:
        bucket = str(row.get("watchlist_bucket") or "")
        score = _safe_float(row.get("overlap_score")) or 0
        if bucket.startswith("F4") or score >= 80:
            return "ACTION_CANDIDATE"
        if row.get("technical_only"):
            return "INFO_ONLY"
        if score >= 70:
            return "TRACK_CLOSELY"
        if score >= 60:
            return "WATCHLIST"
        return "INFO_ONLY"

    base.loc[:, "action"] = base.apply(_action, axis=1)
    preferred = q_score.ge(70) | buckets.str.startswith("F", na=False)
    filtered = base.loc[preferred].copy()
    if filtered.empty:
        filtered = base.copy()

    filtered.loc[:, "reason"] = filtered.apply(_overlap_reason, axis=1)
    cols = [
        "symbol",
        "sector_name",
        "overlap_score",
        "action",
        "watchlist_bucket",
        "quarterly_result_score",
        "quarterly_result_bucket",
        "valuation_history_score",
        "valuation_history_bucket",
        "composite_score",
        "stage2_label",
        "return_5",
        "return_20",
        "delivery_pct",
        "pattern_family",
        "pattern_count",
        "technical_only",
        "reason",
    ]
    out = _select_existing(filtered.sort_values("overlap_score", ascending=False), cols)
    return out.head(n).reset_index(drop=True)


def candidate_tracker_weekly_view(tracker: pd.DataFrame, n: int = 20) -> dict[str, pd.DataFrame]:
    if tracker is None or tracker.empty:
        empty = pd.DataFrame()
        return {"strong_improving": empty, "watch_carefully": empty, "deteriorating": empty}
    df = _normalize_symbol_frame(tracker)
    if "current_status" not in df.columns and "status" in df.columns:
        df.loc[:, "current_status"] = df["status"]
    status = df.get("current_status", pd.Series("", index=df.index)).astype(str).str.upper()
    cols = [
        "symbol",
        "current_status",
        "tracking_health_score",
        "return_since_first_seen",
        "drawdown_from_tracking_high",
        "quarterly_result_score",
        "quarterly_result_score_delta",
        "valuation_history_bucket",
        "relative_strength",
        "relative_strength_delta",
        "next_action",
        "status_reason",
    ]
    return {
        "strong_improving": _select_existing(df.loc[status.isin({"STRONG_IMPROVING", "IMPROVING"})], cols).head(n).reset_index(drop=True),
        "watch_carefully": _select_existing(df.loc[status.eq("WATCH_CAREFULLY")], cols).head(n).reset_index(drop=True),
        "deteriorating": _select_existing(
            df.loc[status.isin({"DETERIORATING", "RESULT_FAILURE", "TECHNICAL_FAILURE", "REMOVE_FROM_TRACKING"})],
            cols,
        ).head(n).reset_index(drop=True),
    }


def flag_low_base_fundamentals(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    sales_growth = _first_numeric(out, ("sales_yoy_growth", "sales_yoy_pct"))
    profit_growth = _first_numeric(out, ("profit_yoy_growth", "profit_yoy_pct", "pat_yoy_growth", "pat_yoy_pct"))
    opm_change = _first_numeric(out, ("opm_yoy_change",))
    opm_bps = _first_numeric(out, ("opm_yoy_change_bps",))
    prior_sales = _first_numeric(out, ("prior_sales", "sales_prior", "previous_sales"))
    prior_profit = _first_numeric(out, ("prior_profit", "profit_prior", "previous_profit", "prior_pat"))

    flag = pd.Series(False, index=out.index)
    warnings: list[list[str]] = [[] for _ in range(len(out))]

    def _mark(mask: pd.Series, message: str) -> None:
        nonlocal flag
        mask = mask.fillna(False)
        flag |= mask
        for pos, value in enumerate(mask.tolist()):
            if value:
                warnings[pos].append(message)

    _mark(_growth_spike(sales_growth, ratio_threshold=3.0, percent_threshold=300), "Sales growth may be low-base.")
    _mark(_growth_spike(profit_growth, ratio_threshold=5.0, percent_threshold=500), "Profit growth may be low-base.")
    _mark(opm_change > 10.0, "OPM jump is unusually large.")
    _mark(opm_bps > 1000, "OPM jump exceeds 1000 bps.")
    _mark(prior_sales.notna() & (prior_sales.abs() < 1), "Prior sales base is very small.")
    _mark(prior_profit.notna() & (prior_profit.abs() < 1), "Prior profit was near zero.")
    out.loc[:, "low_base_flag"] = flag
    out.loc[:, "quality_warning"] = [" ".join(items) if items else "" for items in warnings]
    return out


def split_fundamental_results(great_results: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    flagged = flag_low_base_fundamentals(great_results)
    if flagged.empty:
        return pd.DataFrame(), pd.DataFrame()
    sort_col = "insight_score" if "insight_score" in flagged.columns else None
    clean = flagged.loc[~flagged["low_base_flag"]].copy()
    caution = flagged.loc[flagged["low_base_flag"]].copy()
    if sort_col:
        clean = clean.sort_values(sort_col, ascending=False)
        caution = caution.sort_values(sort_col, ascending=False)
    return clean.reset_index(drop=True), caution.reset_index(drop=True)


def split_sector_leadership(sector_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    empty = pd.DataFrame()
    if sector_df is None or sector_df.empty or "Quadrant" not in sector_df.columns:
        return {"fresh_leaders": empty, "improving_sectors": empty, "weakening_leaders": empty, "lagging": empty}
    df = sector_df.copy()
    quad = df["Quadrant"].astype(str).str.lower()
    cols = ["Sector", "RS", "RS_20", "RS_50", "Momentum", "Quadrant", "RS_rank", "rank_change"]
    fresh = df.loc[quad.eq("leading")].copy()
    if "RS_rank" in fresh.columns:
        fresh = fresh.sort_values("RS_rank", ascending=True)
    improving = df.loc[quad.eq("improving")].copy()
    sort_col = "rank_change" if "rank_change" in improving.columns else ("RS" if "RS" in improving.columns else None)
    if sort_col:
        improving = improving.sort_values(sort_col, ascending=False)
    weakening = df.loc[quad.eq("weakening")].copy()
    if "RS_rank" in weakening.columns:
        weakening = weakening.loc[pd.to_numeric(weakening["RS_rank"], errors="coerce") <= 10].sort_values("RS_rank")
    lagging = df.loc[quad.eq("lagging")].copy()
    return {
        "fresh_leaders": _select_existing(fresh, cols).reset_index(drop=True),
        "improving_sectors": _select_existing(improving, cols).reset_index(drop=True),
        "weakening_leaders": _select_existing(weakening, cols).reset_index(drop=True),
        "lagging": _select_existing(lagging, cols).reset_index(drop=True),
    }


def valuation_cycle_interpretation(latest_valuation: pd.DataFrame) -> dict[str, str]:
    if latest_valuation is None or latest_valuation.empty:
        return {"headline": "Valuation data unavailable.", "detail": "", "risk_label": "unknown"}
    row = latest_valuation.iloc[0]
    pe = _safe_float(row.get("pe_ttm"))
    percentile = _safe_float(row.get("pe_percentile_5y"))
    loss_mcap = _safe_float(row.get("loss_mcap_pct"))
    if pe is None:
        return {"headline": "Valuation data unavailable.", "detail": "", "risk_label": "unknown"}
    if pe <= 0 or pe > 150 or (loss_mcap is not None and loss_mcap > 25):
        return {
            "headline": "Universe PE is unreliable due to loss-making/extreme constituents.",
            "detail": "Use percentile and valuation zones cautiously until the loss-making share normalizes.",
            "risk_label": "unreliable",
        }
    if percentile is not None and percentile >= 80:
        label = "expensive"
        headline = "Valuation is expensive versus own history."
    elif percentile is not None and percentile <= 20:
        label = "cheap"
        headline = "Valuation is cheap versus own history."
    else:
        label = "fair"
        headline = "Valuation is fair versus own history."
    detail = ""
    if pe > 50 and percentile is not None and percentile < 50:
        detail = "Absolute PE is high, but own-history percentile is moderate; model classifies valuation as fair vs history."
    return {"headline": headline, "detail": detail, "risk_label": label}


def _safe_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pattern_lookup(patterns_best: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if patterns_best is None or patterns_best.empty or "symbol_id" not in patterns_best.columns:
        return {}
    return {str(row["symbol_id"]): row.to_dict() for _, row in patterns_best.iterrows()}


def _actionable_from_watchlist(watchlist: pd.DataFrame | None, patterns_lookup: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    df = _normalize_symbol_frame(watchlist)
    if df.empty or "watchlist_bucket" not in df.columns:
        return []
    buckets = df["watchlist_bucket"].astype(str)
    chosen = df.loc[buckets.isin({"F4_ACTION_CANDIDATE", "F3_FUND_VALUE_TECH_READY"})].copy()
    score_col = next((c for c in ("final_watchlist_score", "overlap_score", "composite_score") if c in chosen.columns), None)
    if score_col:
        chosen = chosen.sort_values(score_col, ascending=False)
    rows = []
    for _, row in chosen.iterrows():
        sym = str(row.get("symbol") or row.get("symbol_id") or "")
        pat = patterns_lookup.get(sym, {})
        rows.append({
            "symbol": sym,
            "reason": row.get("watchlist_bucket") or "Watchlist candidate",
            "score": row.get(score_col) if score_col else row.get("composite_score"),
            "stage": row.get("stage2_label"),
            "return_5": row.get("return_5"),
            "pattern": pat.get("pattern_family") or row.get("pattern_family"),
            "action": "Review for entry",
        })
    return rows


def _actionable_from_ranked(ranked: pd.DataFrame, patterns_lookup: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    if ranked is None or ranked.empty:
        return []
    df = ranked.copy()
    mask = _numeric_series(df, "composite_score", 0).ge(85)
    if "stage2_label" in df.columns:
        mask &= df["stage2_label"].map(_is_stage2_like)
    if "return_5" in df.columns:
        mask &= _numeric_series(df, "return_5", 0).ge(0)
    if "delivery_pct" in df.columns:
        mask &= _numeric_series(df, "delivery_pct", 0).ge(35)
    chosen = df.loc[mask].sort_values("composite_score", ascending=False) if "composite_score" in df.columns else df.loc[mask]
    rows = []
    for _, row in chosen.iterrows():
        sym = str(row.get("symbol_id") or row.get("symbol") or "")
        pat = patterns_lookup.get(sym, {})
        rows.append({
            "symbol": sym,
            "reason": "High score Stage 2 candidate",
            "score": row.get("composite_score"),
            "stage": row.get("stage2_label"),
            "return_5": row.get("return_5"),
            "pattern": pat.get("pattern_family"),
            "action": "Review for entry",
        })
    return rows


def _track_rows(
    ranked: pd.DataFrame,
    rank_improvers: pd.DataFrame | None,
    patterns_best: pd.DataFrame | None,
    patterns_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if patterns_best is not None and not patterns_best.empty:
        for _, row in patterns_best.head(10).iterrows():
            sym = str(row.get("symbol_id") or "")
            delivery = None
            if ranked is not None and not ranked.empty and "symbol_id" in ranked.columns and "delivery_pct" in ranked.columns:
                match = ranked.loc[ranked["symbol_id"].astype(str).eq(sym)]
                if not match.empty:
                    delivery = match.iloc[0].get("delivery_pct")
            if delivery is None or (_safe_float(delivery) or 0) < 35:
                rows.append({
                    "symbol": sym,
                    "reason": "Pattern setup needs delivery confirmation",
                    "score": row.get("pattern_score") or row.get("pattern_priority_score"),
                    "stage": row.get("stage2_label"),
                    "return_5": None,
                    "pattern": row.get("pattern_family"),
                    "action": "Track next",
                })
    if rank_improvers is not None and not rank_improvers.empty:
        for _, row in rank_improvers.iterrows():
            if (_safe_float(row.get("score_change")) or 0) <= 0:
                sym = str(row.get("symbol_id") or "")
                pat = patterns_lookup.get(sym, {})
                rows.append({
                    "symbol": sym,
                    "reason": "Rank improved but score did not confirm",
                    "score": row.get("composite_score"),
                    "stage": row.get("stage2_label"),
                    "return_5": row.get("return_5"),
                    "pattern": pat.get("pattern_family"),
                    "action": "Track next",
                })
    if ranked is not None and not ranked.empty and "stage2_label" in ranked.columns:
        trans = ranked.loc[ranked["stage2_label"].map(_normalized_stage2_label).eq("stage1_to_stage2")]
        for _, row in trans.head(10).iterrows():
            sym = str(row.get("symbol_id") or "")
            pat = patterns_lookup.get(sym, {})
            rows.append({
                "symbol": sym,
                "reason": "Transitioning into Stage 2",
                "score": row.get("composite_score"),
                "stage": row.get("stage2_label"),
                "return_5": row.get("return_5"),
                "pattern": pat.get("pattern_family"),
                "action": "Track next",
            })
    return _dedupe_decision_rows(rows)


def _avoid_rows(rank_decliners: pd.DataFrame | None) -> list[dict[str, Any]]:
    if rank_decliners is None or rank_decliners.empty:
        return []
    rows = []
    for _, row in rank_decliners.iterrows():
        if (_safe_float(row.get("return_5")) or 0) < 0:
            rows.append({
                "symbol": str(row.get("symbol_id") or ""),
                "reason": "Rank deterioration with negative 5d return",
                "score": row.get("composite_score"),
                "stage": row.get("stage2_label"),
                "return_5": row.get("return_5"),
                "pattern": None,
                "action": "Avoid or reduce",
            })
    return rows


def _dedupe_decision_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    seen = set()
    for row in rows:
        sym = row.get("symbol")
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(row)
    return out


def _overlap_reason(row: pd.Series) -> str:
    if row.get("technical_only"):
        return "Technical candidate; fundamental tracking not available."
    parts = []
    bucket = row.get("watchlist_bucket")
    if bucket:
        parts.append(str(bucket))
    if row.get("quarterly_result_score") is not None and pd.notna(row.get("quarterly_result_score")):
        parts.append("result support")
    if row.get("valuation_history_score") is not None and pd.notna(row.get("valuation_history_score")):
        parts.append("valuation support")
    if row.get("pattern_family"):
        parts.append("technical pattern")
    return ", ".join(parts) or "Composite technical/fundamental overlap."


def _first_numeric(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.Series:
    for col in cols:
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce")
    return pd.Series(float("nan"), index=df.index, dtype="float64")


def _growth_spike(series: pd.Series, *, ratio_threshold: float, percent_threshold: float) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    max_abs = values.abs().max(skipna=True)
    if pd.notna(max_abs) and max_abs > 20:
        return values > percent_threshold
    return values > ratio_threshold


def serialize_for_json(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert DataFrames to lists of records for JSON output."""
    serialized: Dict[str, Any] = {}
    for key, value in data.items():
        if isinstance(value, pd.DataFrame):
            serialized[key] = value.to_dict(orient="records")
        else:
            serialized[key] = value
    return serialized
