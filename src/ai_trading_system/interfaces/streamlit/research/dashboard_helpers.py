"""Pure transformation helpers for research dashboard widgets."""

from __future__ import annotations

from typing import Dict, Iterable, Tuple

import numpy as np
import pandas as pd


FACTOR_SPECS: list[dict[str, str]] = [
    {
        "label": "Relative Strength",
        "score_col": "rel_strength_score",
        "raw_col": "rel_strength",
        "weight_key": "relative_strength",
    },
    {
        "label": "Volume Intensity",
        "score_col": "vol_intensity_score",
        "raw_col": "vol_intensity",
        "weight_key": "volume_intensity",
    },
    {
        "label": "Trend Persistence",
        "score_col": "trend_score_score",
        "raw_col": "trend_score",
        "weight_key": "trend_persistence",
    },
    {
        "label": "Proximity to Highs",
        "score_col": "prox_high_score",
        "raw_col": "prox_high",
        "weight_key": "proximity_highs",
    },
    {
        "label": "Delivery %",
        "score_col": "delivery_pct_score",
        "raw_col": "delivery_pct",
        "weight_key": "delivery_pct",
    },
    {
        "label": "Sector Strength",
        "score_col": "sector_strength_score",
        "raw_col": "sector_rs_value",
        "weight_key": "sector_strength",
    },
]


def _to_float(value: object, default: float = np.nan) -> float:
    try:
        out = float(value)  # type: ignore[arg-type]
        if np.isfinite(out):
            return out
        return default
    except Exception:
        return default


def _find_column(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    """Resolve a column from candidate aliases (case-insensitive)."""
    lookup = {str(col).strip().lower(): str(col) for col in df.columns}
    for candidate in candidates:
        resolved = lookup.get(str(candidate).strip().lower())
        if resolved:
            return resolved
    return None


def build_factor_attribution_frame(row: pd.Series, weights: Dict[str, float]) -> pd.DataFrame:
    """Build normalized and weighted factor contribution frame for one symbol."""
    records: list[dict[str, object]] = []
    for spec in FACTOR_SPECS:
        score_val = _to_float(row.get(spec["score_col"]), default=0.0)
        weight_val = _to_float(weights.get(spec["weight_key"]), default=0.0)
        raw_val = row.get(spec["raw_col"], np.nan)
        if spec["label"] == "Sector Strength":
            sector_rs = _to_float(row.get("sector_rs_value"))
            stock_vs = _to_float(row.get("stock_vs_sector_value"))
            if np.isfinite(sector_rs) and np.isfinite(stock_vs):
                raw_val = f"sector_rs={sector_rs:.2f}, stock_vs_sector={stock_vs:.2f}"

        records.append(
            {
                "factor": spec["label"],
                "raw_metric": raw_val,
                "normalized_score": round(score_val, 2),
                "weight_pct": round(weight_val * 100.0, 2),
                "contribution_points": round(score_val * weight_val, 3),
            }
        )

    frame = pd.DataFrame(records)
    total = float(frame["contribution_points"].sum()) if not frame.empty else 0.0
    frame["contribution_pct"] = (
        (frame["contribution_points"] / total) * 100.0 if total > 0 else 0.0
    ).round(2)
    return frame


def build_universe_factor_contributions(
    ranked_df: pd.DataFrame,
    weights: Dict[str, float],
    max_symbols: int = 12,
) -> pd.DataFrame:
    """Build long-form contribution records for a compact universe stacked bar."""
    if ranked_df is None or ranked_df.empty:
        return pd.DataFrame(columns=["symbol_id", "factor", "contribution_points"])

    rows = ranked_df.head(max_symbols)
    records: list[dict[str, object]] = []
    for _, row in rows.iterrows():
        symbol = str(row.get("symbol_id", "UNKNOWN"))
        factor_frame = build_factor_attribution_frame(row, weights)
        for _, rec in factor_frame.iterrows():
            records.append(
                {
                    "symbol_id": symbol,
                    "factor": rec["factor"],
                    "contribution_points": float(rec["contribution_points"]),
                }
            )
    return pd.DataFrame(records)


def top_factor_driver(row: pd.Series, weights: Dict[str, float]) -> str:
    """Return the most influential factor label for one symbol."""
    factor_df = build_factor_attribution_frame(row, weights)
    if factor_df.empty:
        return "N/A"
    top_row = factor_df.sort_values("contribution_points", ascending=False).iloc[0]
    return f"{top_row['factor']} ({float(top_row['contribution_pct']):.0f}%)"


def enrich_ranked_table_with_context(
    ranked_df: pd.DataFrame,
    weights: Dict[str, float],
    sparkline_payload: dict[str, dict[str, object]] | None = None,
    symbol_col: str = "symbol_id",
) -> pd.DataFrame:
    """Add sparkline and top-driver context columns to a ranked table."""
    if ranked_df is None or ranked_df.empty:
        return pd.DataFrame() if ranked_df is None else ranked_df.copy()

    payload = sparkline_payload or {}
    out = ranked_df.copy()
    if symbol_col not in out.columns:
        return out

    symbol_series = out[symbol_col].astype(str).str.upper()
    out["Rank History"] = symbol_series.map(lambda symbol: payload.get(symbol, {}).get("sparkline", [np.nan]))
    out["Rank Trend"] = symbol_series.map(lambda symbol: payload.get(symbol, {}).get("trend", "Flat"))
    out["Δ Rank"] = symbol_series.map(lambda symbol: payload.get(symbol, {}).get("delta_rank", 0))
    out["Top Driver"] = out.apply(lambda row: top_factor_driver(row, weights), axis=1)
    return out


def classify_rank_trend(rank_series: Iterable[float]) -> str:
    """Classify rank movement from oldest->latest (lower rank number is better)."""
    values = [float(v) for v in rank_series if pd.notna(v)]
    if len(values) < 2:
        return "Flat"

    change = values[0] - values[-1]
    if change >= 3:
        return "Improving"
    if change <= -3:
        return "Weakening"
    return "Flat"


def build_rank_sparkline_payload(
    history_df: pd.DataFrame,
    max_points: int = 12,
) -> dict[str, dict[str, object]]:
    """Aggregate history rows into sparkline arrays + trend labels by symbol."""
    if history_df is None or history_df.empty:
        return {}

    payload: dict[str, dict[str, object]] = {}
    for symbol, grp in history_df.groupby("symbol_id", sort=False):
        ordered = grp.sort_values(["run_order", "run_id"])
        ranks = ordered["rank_position"].dropna().astype(float).tolist()
        if not ranks:
            continue
        sparkline = [int(v) for v in ranks[-max_points:]]
        trend = classify_rank_trend(sparkline)
        delta = sparkline[0] - sparkline[-1] if len(sparkline) > 1 else 0
        payload[str(symbol)] = {
            "sparkline": sparkline,
            "trend": trend,
            "delta_rank": int(delta),
            "latest_rank": int(sparkline[-1]),
        }
    return payload


def build_value_sparkline_payload(
    history_df: pd.DataFrame,
    *,
    key_col: str,
    value_col: str,
    max_points: int = 12,
    higher_is_better: bool = True,
) -> dict[str, dict[str, object]]:
    """Aggregate generic time-series history rows into sparkline payload by key."""
    if history_df is None or history_df.empty:
        return {}

    payload: dict[str, dict[str, object]] = {}
    for key, grp in history_df.groupby(key_col, sort=False):
        ordered = grp.sort_values(["run_order", "run_id"])
        values = pd.to_numeric(ordered[value_col], errors="coerce").dropna().astype(float).tolist()
        if not values:
            continue
        sparkline = [round(v, 4) for v in values[-max_points:]]
        delta = sparkline[-1] - sparkline[0] if len(sparkline) > 1 else 0.0
        if higher_is_better:
            trend = "Improving" if delta > 0.01 else "Weakening" if delta < -0.01 else "Flat"
        else:
            trend = "Improving" if delta < -0.01 else "Weakening" if delta > 0.01 else "Flat"
        payload[str(key)] = {
            "sparkline": sparkline,
            "trend": trend,
            "delta_value": round(delta, 4),
            "latest_value": round(sparkline[-1], 4),
        }
    return payload


def prepare_sector_rotation_frame(
    sector_df: pd.DataFrame,
    stock_scan_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Prepare sortable sector-rotation frame with optional derived breadth."""
    if sector_df is None or sector_df.empty:
        return pd.DataFrame()

    prepared = sector_df.copy()
    canonical_aliases = {
        "Sector": ("Sector", "sector", "sector_name", "industry", "Industry"),
        "RS": ("RS", "rs", "relative_strength", "sector_rs"),
        "RS_20": ("RS_20", "rs_20", "rs20", "relative_strength_20"),
        "RS_50": ("RS_50", "rs_50", "rs50", "relative_strength_50"),
        "RS_100": ("RS_100", "rs_100", "rs100", "relative_strength_100"),
        "Momentum": ("Momentum", "momentum", "mom", "momentum_score"),
        "RS_rank": ("RS_rank", "rs_rank", "rank", "sector_rank"),
        "RS_rank_pct": ("RS_rank_pct", "rs_rank_pct", "rank_pct"),
        "Momentum_rank": ("Momentum_rank", "momentum_rank", "mom_rank"),
        "Momentum_rank_pct": ("Momentum_rank_pct", "momentum_rank_pct", "mom_rank_pct"),
        "Quadrant": ("Quadrant", "quadrant", "state"),
    }
    rename_map: dict[str, str] = {}
    for canonical, aliases in canonical_aliases.items():
        if canonical in prepared.columns:
            continue
        resolved = _find_column(prepared, aliases)
        if resolved and resolved != canonical:
            rename_map[resolved] = canonical
    if rename_map:
        prepared = prepared.rename(columns=rename_map)

    if "Sector" in prepared.columns:
        prepared = prepared[prepared["Sector"].notna()].copy()
        prepared["Sector"] = prepared["Sector"].astype(str).str.strip()
        prepared = prepared[
            ~prepared["Sector"].str.lower().isin({"", "nan", "none", "null", "na"})
        ]
        prepared = prepared.drop_duplicates(subset=["Sector"], keep="last")
    numeric_cols = [
        "RS",
        "RS_20",
        "RS_50",
        "RS_100",
        "Momentum",
        "RS_rank",
        "RS_rank_pct",
        "Momentum_rank",
        "Momentum_rank_pct",
    ]
    for column in numeric_cols:
        if column in prepared.columns:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce")

    if "RS" in prepared.columns and "RS_20" in prepared.columns:
        prepared["rs_change_20"] = prepared["RS"] - prepared["RS_20"]
    if "RS" in prepared.columns and "RS_50" in prepared.columns:
        prepared["rs_change_50"] = prepared["RS"] - prepared["RS_50"]

    if stock_scan_df is not None and not stock_scan_df.empty:
        scan = stock_scan_df.copy()
        scan_sector_col = _find_column(scan, ("sector", "Sector", "sector_name"))
        scan_category_col = _find_column(scan, ("category", "Category", "signal", "action"))
        if scan_sector_col and scan_category_col:
            scan["sector"] = scan[scan_sector_col].astype(str).str.strip()
            scan["is_buy"] = scan[scan_category_col].astype(str).str.upper().isin({"BUY", "STRONG BUY"})
            breadth = (
                scan.groupby("sector", dropna=False)["is_buy"]
                .agg(["sum", "count"])
                .rename(columns={"sum": "buy_count", "count": "scan_count"})
                .reset_index()
            )
            breadth["breadth_buy_pct"] = (breadth["buy_count"] / breadth["scan_count"] * 100.0).round(2)

            sector_name_col = "Sector" if "Sector" in prepared.columns else None
            if sector_name_col is not None:
                breadth["sector"] = breadth["sector"].astype(str).str.strip()
                prepared = prepared.merge(
                    breadth[["sector", "breadth_buy_pct"]],
                    left_on=sector_name_col,
                    right_on="sector",
                    how="left",
                ).drop(columns=["sector"], errors="ignore")

    sort_col = None
    for candidate in ("RS_rank", "RS", "Momentum"):
        if candidate in prepared.columns:
            sort_col = candidate
            break
    if sort_col is not None:
        ascending = sort_col == "RS_rank"
        prepared = prepared.sort_values(sort_col, ascending=ascending, na_position="last")
    else:
        prepared = prepared.reset_index(drop=True)
    return prepared


def classify_breakout_verdict(row: pd.Series) -> Tuple[str, str]:
    """Classify breakout quality into badge verdict."""
    breakout_pct = _to_float(row.get("breakout_pct"), default=np.nan)
    vol_mult = _to_float(row.get("volume_ratio"), default=np.nan)
    adx = _to_float(row.get("adx_14"), default=np.nan)
    dist_52w = _to_float(row.get("near_52w_high_pct"), default=np.nan)

    if (
        (np.isfinite(breakout_pct) and breakout_pct > 3.5)
        or (np.isfinite(dist_52w) and dist_52w > 15.0)
        or (np.isfinite(vol_mult) and vol_mult < 1.1)
    ):
        return ("Risky", "#dc2626")

    if (
        np.isfinite(breakout_pct)
        and np.isfinite(vol_mult)
        and np.isfinite(adx)
        and breakout_pct <= 0.8
        and vol_mult >= 1.3
        and adx >= 18.0
    ):
        return ("Early", "#2563eb")

    if (
        np.isfinite(breakout_pct)
        and np.isfinite(vol_mult)
        and np.isfinite(adx)
        and np.isfinite(dist_52w)
        and breakout_pct <= 2.2
        and vol_mult >= 1.5
        and adx >= 20.0
        and dist_52w <= 12.0
    ):
        return ("Confirmed", "#16a34a")

    if (
        np.isfinite(breakout_pct)
        and np.isfinite(vol_mult)
        and np.isfinite(dist_52w)
        and breakout_pct <= 4.0
        and vol_mult >= 1.2
        and dist_52w <= 10.0
    ):
        return ("Extended", "#ea580c")

    return ("Risky", "#dc2626")


def build_breakout_evidence_frame(
    breakout_df: pd.DataFrame,
    signal_date: str | None = None,
) -> pd.DataFrame:
    """Prepare breakout evidence rows with verdict badge columns."""
    if breakout_df is None or breakout_df.empty:
        return pd.DataFrame()

    evidence = breakout_df.copy()
    family_to_base_len = {
        "base_breakout": 30,
        "contraction_breakout": 60,
        "supertrend_flip_breakout": 20,
        "resistance_breakout_50d": 50,
        "high_52w_breakout": 252,
        "consolidation_breakout": 60,
        "volatility_expansion_breakout": 20,
    }
    evidence["base_length_days"] = evidence["setup_family"].map(family_to_base_len).fillna(20).astype(int)
    evidence["contraction_pct"] = (
        (1.0 - pd.to_numeric(evidence.get("contraction_ratio"), errors="coerce")) * 100.0
    ).round(2)
    evidence["breakout_type"] = (
        evidence.get("setup_family", pd.Series(index=evidence.index, dtype=object))
        .astype(str)
        .str.replace("_", " ")
        .str.title()
    )
    evidence["signal_date"] = signal_date

    verdicts = evidence.apply(classify_breakout_verdict, axis=1, result_type="expand")
    evidence["verdict"] = verdicts[0]
    evidence["verdict_color"] = verdicts[1]
    return evidence
