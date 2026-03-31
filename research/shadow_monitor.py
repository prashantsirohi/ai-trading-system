"""Run ML-vs-technical shadow monitoring on the current operational universe."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from analytics.lightgbm_engine import LightGBMAlphaEngine
from analytics.registry import RegistryStore
from analytics.shadow_monitor import (
    build_shadow_overlay,
    compute_matured_outcomes,
    find_latest_model_metadata,
    load_operational_price_history,
    overlay_rows_for_registry,
    prepare_current_universe_dataset,
    prepare_shadow_history_dataset,
)
from utils.data_domains import ensure_domain_layout
from utils.logger import logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Record current-universe ML shadow monitoring results")
    parser.add_argument("--prediction-date", help="Optional explicit prediction date (YYYY-MM-DD)")
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--lookback-days", type=int, default=420)
    parser.add_argument("--backfill-days", type=int, default=0)
    parser.add_argument("--technical-weight", type=float, default=0.75)
    parser.add_argument("--ml-weight", type=float, default=0.25)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[1]
    operational_paths = ensure_domain_layout(project_root=project_root, data_domain="operational")
    research_paths = ensure_domain_layout(project_root=project_root, data_domain="research")

    _, model_5d_meta = find_latest_model_metadata(research_paths.model_dir, horizon=5)
    _, model_20d_meta = find_latest_model_metadata(research_paths.model_dir, horizon=20)

    scorer = LightGBMAlphaEngine(
        ohlcv_db_path=str(operational_paths.ohlcv_db_path),
        feature_store_dir=str(operational_paths.feature_store_dir),
        model_dir=str(research_paths.model_dir),
        data_domain="operational",
    )
    model_5d = scorer.load_model_from_uri(model_5d_meta["_model_path"])
    model_20d = scorer.load_model_from_uri(model_20d_meta["_model_path"])

    if args.backfill_days > 0:
        latest_df, prediction_ts = prepare_current_universe_dataset(
            project_root=project_root,
            prediction_date=args.prediction_date,
            exchange=args.exchange,
            lookback_days=args.lookback_days,
        )
        backfill_start = (prediction_ts - pd.Timedelta(days=int(args.backfill_days))).date().isoformat()
        history_df = prepare_shadow_history_dataset(
            project_root=project_root,
            from_prediction_date=backfill_start,
            to_prediction_date=prediction_ts.date().isoformat(),
            exchange=args.exchange,
            lookback_days=args.lookback_days,
        )
        prediction_frames = {
            pd.Timestamp(date): frame.copy()
            for date, frame in history_df.groupby(history_df["timestamp"].dt.normalize())
        }
    else:
        latest_df, prediction_ts = prepare_current_universe_dataset(
            project_root=project_root,
            prediction_date=args.prediction_date,
            exchange=args.exchange,
            lookback_days=args.lookback_days,
        )
        prediction_frames = {prediction_ts: latest_df.copy()}

    latest_overlay_df = pd.DataFrame()

    reports_dir = research_paths.reports_dir
    reports_dir.mkdir(parents=True, exist_ok=True)
    latest_overlay_path = reports_dir / "ml_rank_overlay.csv"
    dated_overlay_path = reports_dir / f"ml_rank_overlay_{prediction_ts.date().isoformat()}.csv"

    registry = RegistryStore(project_root)
    inserted_predictions = 0
    for prediction_day, frame in sorted(prediction_frames.items()):
        overlay_df = build_shadow_overlay(
            frame,
            scorer=scorer,
            model_5d=model_5d,
            model_20d=model_20d,
            technical_weight=args.technical_weight,
            ml_weight=args.ml_weight,
        )
        overlay_metadata = {
            "prediction_date": prediction_day.date().isoformat(),
            "technical_weight": args.technical_weight,
            "ml_weight": args.ml_weight,
            "exchange": args.exchange,
            "model_5d_path": model_5d_meta["_model_path"],
            "model_20d_path": model_20d_meta["_model_path"],
            "model_5d_metadata": model_5d_meta["_metadata_path"],
            "model_20d_metadata": model_20d_meta["_metadata_path"],
        }
        prediction_rows = overlay_rows_for_registry(overlay_df, metadata=overlay_metadata)
        artifact_uri = None
        if prediction_day.normalize() == prediction_ts.normalize():
            latest_overlay_df = overlay_df.copy()
            latest_overlay_df.to_csv(latest_overlay_path, index=False)
            latest_overlay_df.to_csv(dated_overlay_path, index=False)
            artifact_uri = str(latest_overlay_path)
        inserted_predictions += registry.replace_shadow_predictions(
            prediction_day.date().isoformat(),
            prediction_rows,
            artifact_uri=artifact_uri,
        )

    matured_counts: dict[int, int] = {}
    for horizon in (5, 20):
        pending = registry.get_unscored_shadow_predictions(horizon)
        if not pending:
            matured_counts[horizon] = 0
            continue
        from_date = min(row["prediction_date"] for row in pending)
        price_history = load_operational_price_history(
            ohlcv_db_path=operational_paths.ohlcv_db_path,
            exchange=args.exchange,
            from_date=from_date,
        )
        outcome_rows = compute_matured_outcomes(price_history, pending, horizon=horizon)
        matured_counts[horizon] = registry.replace_shadow_outcomes(outcome_rows)

    summary = {
        "prediction_date": prediction_ts.date().isoformat(),
        "prediction_rows": inserted_predictions,
        "matured_outcomes": matured_counts,
        "overlay_uri": str(latest_overlay_path),
        "dated_overlay_uri": str(dated_overlay_path),
        "backfill_days": int(args.backfill_days),
    }
    summary_path = reports_dir / "ml_shadow_monitor_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("Shadow monitor updated: %s", summary)


if __name__ == "__main__":
    main()
