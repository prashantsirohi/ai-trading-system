"""Research workflow helpers for LightGBM horizon training and evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from analytics.lightgbm_engine import LightGBMAlphaEngine
from analytics.ranker import StockRanker


@dataclass(frozen=True)
class WalkForwardFoldResult:
    horizon: int
    validation_year: int
    train_rows: int
    validation_rows: int
    validation_auc: float
    ml_precision_at_10pct: float
    tech_precision_at_10pct: float
    blend_precision_at_10pct: float
    ml_avg_return_top_10pct: float
    tech_avg_return_top_10pct: float
    blend_avg_return_top_10pct: float


def add_technical_baseline_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Add technical and blended research scores using production ranker weights."""
    enriched = df.copy()
    weights = StockRanker.WEIGHTS

    sector_strength = (
        enriched.get("sector_rs_pct", 50.0).fillna(50.0) * 0.6
        + enriched.get("stock_vs_sector_pct", 50.0).fillna(50.0) * 0.4
    )
    enriched["technical_score"] = (
        enriched.get("rel_strength_pct", 50.0).fillna(50.0) * weights["relative_strength"]
        + enriched.get("vol_intensity_pct", 50.0).fillna(50.0) * weights["volume_intensity"]
        + enriched.get("trend_score_pct", 50.0).fillna(50.0) * weights["trend_persistence"]
        + enriched.get("prox_high_pct", 50.0).fillna(50.0) * weights["proximity_highs"]
        + enriched.get("delivery_pct_pct", 50.0).fillna(50.0) * weights["delivery_pct"]
        + sector_strength * weights["sector_strength"]
    )
    return enriched


def _top_decile_by_date(df: pd.DataFrame, score_col: str, target_col: str, return_col: str) -> Dict[str, float]:
    scored = df.copy()
    scored = scored.sort_values(["timestamp", score_col], ascending=[True, False])
    scored["_top_n"] = scored.groupby("timestamp")["symbol_id"].transform(
        lambda series: max(1, int(np.ceil(len(series) * 0.1)))
    )
    scored["_rank_in_day"] = scored.groupby("timestamp").cumcount() + 1
    selected = scored[scored["_rank_in_day"] <= scored["_top_n"]].drop(
        columns=["_top_n", "_rank_in_day"]
    )
    if selected.empty:
        return {"precision_at_10pct": 0.0, "avg_return_top_10pct": 0.0}
    return {
        "precision_at_10pct": float(selected[target_col].mean()),
        "avg_return_top_10pct": float(selected[return_col].mean()),
    }


def walk_forward_compare(
    dataset_df: pd.DataFrame,
    *,
    engine: LightGBMAlphaEngine,
    horizon: int,
    min_train_years: int = 5,
    blend_weight_technical: float = 0.75,
    blend_weight_ml: float = 0.25,
) -> Dict[str, Any]:
    """Run yearly walk-forward comparison: ML vs technical vs blended."""
    df = add_technical_baseline_scores(dataset_df)
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["year"] = df["timestamp"].dt.year
    target_col = f"target_{horizon}d"
    return_col = f"return_{horizon}d"
    years = sorted(df["year"].dropna().unique().tolist())
    if len(years) <= min_train_years:
        return {"folds": [], "summary": {"error": "Not enough yearly coverage for walk-forward validation."}}

    fold_results: List[WalkForwardFoldResult] = []
    for validation_year in years[min_train_years:]:
        train_df = df[df["year"] < validation_year].copy()
        valid_df = df[df["year"] == validation_year].copy()
        if train_df.empty or valid_df.empty:
            continue

        model, _ = engine.train(
            train_df,
            horizon=horizon,
            validation_fraction=0.2,
            show_progress=False,
        )
        eval_metrics = engine.evaluate_frame(valid_df, model=model, horizon=horizon)
        scored_valid = engine.score_frame(valid_df, model=model, horizon=horizon)
        scored_valid["ml_score_pct"] = scored_valid["probability"].rank(pct=True) * 100
        scored_valid["blended_score"] = (
            scored_valid["technical_score"] * blend_weight_technical
            + scored_valid["ml_score_pct"] * blend_weight_ml
        )

        ml_bucket = _top_decile_by_date(scored_valid, "probability", target_col, return_col)
        tech_bucket = _top_decile_by_date(scored_valid, "technical_score", target_col, return_col)
        blend_bucket = _top_decile_by_date(scored_valid, "blended_score", target_col, return_col)

        fold_results.append(
            WalkForwardFoldResult(
                horizon=horizon,
                validation_year=int(validation_year),
                train_rows=int(len(train_df)),
                validation_rows=int(len(valid_df)),
                validation_auc=float(eval_metrics.get("validation_auc", 0.0)),
                ml_precision_at_10pct=float(ml_bucket["precision_at_10pct"]),
                tech_precision_at_10pct=float(tech_bucket["precision_at_10pct"]),
                blend_precision_at_10pct=float(blend_bucket["precision_at_10pct"]),
                ml_avg_return_top_10pct=float(ml_bucket["avg_return_top_10pct"]),
                tech_avg_return_top_10pct=float(tech_bucket["avg_return_top_10pct"]),
                blend_avg_return_top_10pct=float(blend_bucket["avg_return_top_10pct"]),
            )
        )

    folds_df = pd.DataFrame([fold.__dict__ for fold in fold_results])
    if folds_df.empty:
        return {"folds": [], "summary": {"error": "No walk-forward folds were produced."}}

    summary = {
        "horizon": horizon,
        "fold_count": int(len(folds_df)),
        "avg_validation_auc": float(folds_df["validation_auc"].mean()),
        "avg_ml_precision_at_10pct": float(folds_df["ml_precision_at_10pct"].mean()),
        "avg_tech_precision_at_10pct": float(folds_df["tech_precision_at_10pct"].mean()),
        "avg_blend_precision_at_10pct": float(folds_df["blend_precision_at_10pct"].mean()),
        "avg_ml_return_top_10pct": float(folds_df["ml_avg_return_top_10pct"].mean()),
        "avg_tech_return_top_10pct": float(folds_df["tech_avg_return_top_10pct"].mean()),
        "avg_blend_return_top_10pct": float(folds_df["blend_avg_return_top_10pct"].mean()),
    }
    return {"folds": folds_df.to_dict(orient="records"), "summary": summary}
