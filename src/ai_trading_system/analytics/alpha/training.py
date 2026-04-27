"""Walk-forward training and evaluation utilities for alpha models."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from ai_trading_system.analytics.ranker import StockRanker


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
    enriched.loc[:, "technical_score"] = (
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
    scored = scored.sort_values(["timestamp", score_col], ascending=[True, False]).copy()
    scored.loc[:, "_top_n"] = scored.groupby("timestamp")["symbol_id"].transform(
        lambda series: max(1, int(np.ceil(len(series) * 0.1)))
    )
    scored.loc[:, "_rank_in_day"] = scored.groupby("timestamp").cumcount() + 1
    selected = scored.loc[scored["_rank_in_day"] <= scored["_top_n"]].drop(
        columns=["_top_n", "_rank_in_day"]
    ).copy()
    if selected.empty:
        return {"precision_at_10pct": 0.0, "avg_return_top_10pct": 0.0}
    return {
        "precision_at_10pct": float(selected[target_col].mean()),
        "avg_return_top_10pct": float(selected[return_col].mean()),
    }


def walk_forward_compare(
    dataset_df: pd.DataFrame,
    *,
    engine,
    horizon: int,
    min_train_years: int = 5,
    blend_weight_technical: float = 0.75,
    blend_weight_ml: float = 0.25,
) -> Dict[str, Any]:
    """Run yearly walk-forward comparison: ML vs technical vs blended."""
    required_methods = ("train", "evaluate_frame", "score_frame")
    if any(not hasattr(engine, method_name) for method_name in required_methods):
        return {
            "folds": [],
            "summary": {
                "error": f"Engine {type(engine).__name__} does not support walk-forward evaluation.",
            },
        }

    df = add_technical_baseline_scores(dataset_df)
    df = df.copy()
    df.loc[:, "timestamp"] = pd.to_datetime(df["timestamp"])
    df.loc[:, "year"] = df["timestamp"].dt.year
    target_col = f"target_{horizon}d"
    return_col = f"return_{horizon}d"
    years = sorted(df["year"].dropna().unique().tolist())
    if len(years) <= min_train_years:
        return {
            "folds": [],
            "summary": {"error": "Not enough yearly coverage for walk-forward validation."},
        }

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
        scored_valid = engine.score_frame(valid_df, model=model, horizon=horizon).copy()
        scored_valid.loc[:, "ml_score_pct"] = scored_valid["probability"].rank(pct=True) * 100
        scored_valid.loc[:, "blended_score"] = (
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

    if not fold_results:
        return {"folds": [], "summary": {"error": "No walk-forward folds were produced."}}

    folds_df = pd.DataFrame([asdict(fold) for fold in fold_results])
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


def train_and_register_model(
    *,
    engine,
    registry,
    training_df: pd.DataFrame,
    dataset_meta: Dict[str, Any],
    horizon: int,
    model_name: str,
    model_version: str,
    progress_interval: int = 25,
    min_train_years: int = 5,
    extra_metadata: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Train a model, run standard evaluation, and register artifacts."""
    model, metadata = engine.train(
        training_df,
        horizon=horizon,
        validation_start=dataset_meta.get("validation_start"),
        validation_fraction=dataset_meta.get("validation_fraction", 0.2),
        show_progress=getattr(engine, "engine_name", "") == "lightgbm",
        progress_interval=progress_interval,
    )

    evaluation = {}
    if hasattr(engine, "evaluate"):
        evaluation = engine.evaluate(
            training_df,
            model=model,
            horizon=horizon,
            validation_start=dataset_meta.get("validation_start"),
            validation_fraction=dataset_meta.get("validation_fraction", 0.2),
        )

    walkforward = walk_forward_compare(
        training_df,
        engine=engine,
        horizon=horizon,
        min_train_years=min_train_years,
    )

    artifact_path = Path(engine.save_model(model, horizon=horizon))
    target_artifact_path = artifact_path.with_name(f"{model_name}_{model_version}{artifact_path.suffix}")
    if artifact_path != target_artifact_path:
        artifact_path.replace(target_artifact_path)
        artifact_path = target_artifact_path

    feature_schema_hash = dataset_meta.get("feature_schema_hash") or hashlib.sha256(
        ",".join(sorted(training_df.columns)).encode("utf-8")
    ).hexdigest()
    model_id = registry.register_model(
        model_name=model_name,
        model_version=model_version,
        artifact_uri=str(artifact_path),
        feature_schema_hash=feature_schema_hash,
        train_snapshot_ref=dataset_meta["dataset_ref"],
        approval_status="pending",
        metadata={
            "engine": getattr(engine, "engine_name", "unknown"),
            "dataset_ref": dataset_meta["dataset_ref"],
            "dataset_uri": dataset_meta.get("dataset_uri"),
            "evaluation": evaluation,
            "walkforward_summary": walkforward.get("summary", {}),
            "horizon": horizon,
            **(extra_metadata or {}),
        },
    )

    metadata_path = artifact_path.with_suffix(".metadata.json")
    metadata_payload = {
        "model_id": model_id,
        "engine": getattr(engine, "engine_name", "unknown"),
        "model_name": model_name,
        "model_version": model_version,
        "horizon": horizon,
        "training_rows": int(len(training_df)),
        "training_symbols": int(training_df["symbol_id"].nunique()),
        "feature_count": int(len(getattr(engine, "_feature_cols")(training_df))),
        "dataset_ref": dataset_meta["dataset_ref"],
        "dataset_uri": dataset_meta.get("dataset_uri"),
        "prepared_dataset": True,
        "dataset_metadata": dataset_meta,
        "evaluation": evaluation,
        "walkforward": walkforward,
        **metadata,
        **(extra_metadata or {}),
    }
    metadata_path.write_text(json.dumps(metadata_payload, indent=2), encoding="utf-8")

    metrics_to_record: Dict[str, float] = {}
    for key in (
        "validation_auc",
        "precision_at_10pct",
        "avg_return_top_10pct",
        "baseline_positive_rate",
    ):
        value = evaluation.get(key)
        if value is not None:
            metrics_to_record[key] = float(value)

    walkforward_summary = walkforward.get("summary", {})
    for key in (
        "avg_validation_auc",
        "avg_ml_precision_at_10pct",
        "avg_tech_precision_at_10pct",
        "avg_blend_precision_at_10pct",
        "avg_ml_return_top_10pct",
        "avg_tech_return_top_10pct",
        "avg_blend_return_top_10pct",
    ):
        value = walkforward_summary.get(key)
        if value is not None:
            metrics_to_record[f"walkforward_{key}"] = float(value)

    if metrics_to_record:
        registry.record_model_eval(
            model_id,
            metrics_to_record,
            dataset_ref=dataset_meta["dataset_ref"],
            notes=f"{getattr(engine, 'engine_name', 'model')} training+walkforward evaluation",
        )

    return {
        "model_id": model_id,
        "artifact_uri": str(artifact_path),
        "metadata_uri": str(metadata_path),
        "evaluation": evaluation,
        "walkforward": walkforward,
    }
