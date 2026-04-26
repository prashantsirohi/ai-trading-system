"""Run ML-vs-technical shadow monitoring on the current operational universe."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from ai_trading_system.analytics.alpha.drift import score_drift_rows
from ai_trading_system.analytics.alpha.policy import evaluate_promotion_candidate
from ai_trading_system.analytics.lightgbm_engine import LightGBMAlphaEngine
from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.analytics.shadow_monitor import (
    build_shadow_overlay,
    compute_matured_outcomes,
    find_latest_model_metadata,
    load_operational_price_history,
    overlay_rows_for_registry,
    prepare_current_universe_dataset,
    prepare_shadow_history_dataset,
)
from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.platform.logging.logger import logger


def compute_spearman_ic(
    frame: pd.DataFrame,
    *,
    prediction_col: str = "probability",
    realized_return_col: str = "realized_return",
    min_observations: int = 5,
) -> float:
    """Compute cross-sectional Spearman IC between predictions and realized returns."""
    if frame is None or frame.empty:
        return float("nan")

    scoped = frame[[prediction_col, realized_return_col]].copy()
    scoped.loc[:, prediction_col] = pd.to_numeric(scoped[prediction_col], errors="coerce")
    scoped.loc[:, realized_return_col] = pd.to_numeric(scoped[realized_return_col], errors="coerce")
    scoped = scoped.dropna(subset=[prediction_col, realized_return_col])
    if len(scoped) < int(min_observations):
        return float("nan")
    if scoped[prediction_col].nunique(dropna=True) < 2 or scoped[realized_return_col].nunique(dropna=True) < 2:
        return float("nan")

    return float(scoped[prediction_col].corr(scoped[realized_return_col], method="spearman"))


def compute_rolling_spearman_ic(
    frame: pd.DataFrame,
    *,
    prediction_col: str = "probability",
    realized_return_col: str = "realized_return",
    prediction_date_col: str = "prediction_date",
    horizon_col: str = "horizon",
    horizons: list[int] | tuple[int, ...] | None = None,
    window: int = 20,
    min_observations: int = 5,
) -> pd.DataFrame:
    """Compute daily cross-sectional IC plus rolling average IC across horizons."""
    if frame is None or frame.empty:
        return pd.DataFrame(
            columns=["prediction_date", "horizon", "observations", "ic_spearman", "rolling_ic_spearman", "window"]
        )

    scoped = frame.copy()
    scoped.loc[:, prediction_date_col] = pd.to_datetime(scoped[prediction_date_col], errors="coerce")
    scoped = scoped.dropna(subset=[prediction_date_col])
    if horizons is not None and horizon_col in scoped.columns:
        scoped = scoped[scoped[horizon_col].isin(list(horizons))]

    if horizon_col not in scoped.columns:
        scoped[horizon_col] = pd.NA

    rows: list[dict] = []
    for (horizon, prediction_date), day_frame in scoped.groupby([horizon_col, prediction_date_col], dropna=False):
        ic_value = compute_spearman_ic(
            day_frame,
            prediction_col=prediction_col,
            realized_return_col=realized_return_col,
            min_observations=min_observations,
        )
        observations = int(
            day_frame[[prediction_col, realized_return_col]]
            .apply(pd.to_numeric, errors="coerce")
            .dropna()
            .shape[0]
        )
        rows.append(
            {
                "prediction_date": prediction_date,
                "horizon": horizon,
                "observations": observations,
                "ic_spearman": ic_value,
            }
        )

    result = pd.DataFrame(rows)
    if result.empty:
        return pd.DataFrame(
            columns=["prediction_date", "horizon", "observations", "ic_spearman", "rolling_ic_spearman", "window"]
        )

    result = result.sort_values(["horizon", "prediction_date"], kind="stable").reset_index(drop=True)
    result.loc[:, "rolling_ic_spearman"] = (
        result.groupby("horizon", dropna=False)["ic_spearman"]
        .transform(lambda series: series.rolling(window=int(window), min_periods=1).mean())
    )
    result.loc[:, "rolling_ic_spearman"] = result["rolling_ic_spearman"].where(
        result.groupby("horizon", dropna=False).cumcount() + 1 >= int(window),
        np.nan,
    )
    result.loc[:, "window"] = int(window)
    return result


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
    project_root = Path(__file__).resolve().parents[3]
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
    inserted_prediction_logs = 0
    drift_metrics_recorded = 0
    gate_results_recorded = 0
    promotion_status: dict[int, str] = {}
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
            "model_5d_id": model_5d_meta.get("model_id"),
            "model_20d_id": model_20d_meta.get("model_id"),
            "model_5d_name": model_5d_meta.get("model_name"),
            "model_20d_name": model_20d_meta.get("model_name"),
            "model_5d_version": model_5d_meta.get("model_version"),
            "model_20d_version": model_20d_meta.get("model_version"),
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
        for horizon, model_meta, probability_col, rank_col in (
            (5, model_5d_meta, "ml_5d_prob", "ml_5d_rank"),
            (20, model_20d_meta, "ml_20d_prob", "ml_20d_rank"),
        ):
            prediction_log_rows = []
            for row in overlay_df.to_dict(orient="records"):
                probability = row.get(probability_col)
                prediction_log_rows.append(
                    {
                        "symbol_id": row["symbol_id"],
                        "exchange": row.get("exchange", args.exchange),
                        "model_id": model_meta.get("model_id"),
                        "model_name": model_meta.get("model_name"),
                        "model_version": model_meta.get("model_version"),
                        "score": probability,
                        "probability": probability,
                        "prediction": int((probability or 0.0) >= 0.5),
                        "rank": row.get(rank_col),
                        "artifact_uri": artifact_uri,
                        "metadata": {
                            "technical_rank": row.get("technical_rank"),
                            "technical_score": row.get("technical_score"),
                            "blend_score": row.get(f"blend_{horizon}d_score"),
                            "blend_rank": row.get(f"blend_{horizon}d_rank"),
                            "top_decile": row.get(f"ml_{horizon}d_top_decile"),
                        },
                    }
                )
            inserted_prediction_logs += registry.replace_prediction_log(
                prediction_day.date().isoformat(),
                prediction_log_rows,
                deployment_mode="shadow_ml",
                horizon=horizon,
                model_id=model_meta.get("model_id"),
                artifact_uri=artifact_uri,
            )
            model_id = model_meta.get("model_id")
            if model_id:
                current_scores = [
                    float(value)
                    for value in overlay_df[probability_col].dropna().tolist()
                ]
                prior_end = (prediction_day - pd.Timedelta(days=1)).date().isoformat()
                prior_start = (prediction_day - pd.Timedelta(days=60)).date().isoformat()
                reference_scores = registry.get_prediction_score_values(
                    model_id=model_id,
                    horizon=horizon,
                    deployment_mode="shadow_ml",
                    from_date=prior_start,
                    to_date=prior_end,
                )
                drift_rows = score_drift_rows(
                    model_id=model_id,
                    deployment_mode="shadow_ml",
                    horizon=horizon,
                    prediction_date=prediction_day.date().isoformat(),
                    current_scores=current_scores,
                    reference_scores=reference_scores,
                )
                if drift_rows:
                    drift_metrics_recorded += registry.record_drift_metrics(drift_rows)

    matured_counts: dict[int, int] = {}
    generic_matured_counts: dict[int, int] = {}
    for horizon in (5, 20):
        pending = registry.get_unscored_shadow_predictions(horizon)
        if not pending:
            matured_counts[horizon] = 0
        else:
            from_date = min(row["prediction_date"] for row in pending)
            price_history = load_operational_price_history(
                ohlcv_db_path=operational_paths.ohlcv_db_path,
                exchange=args.exchange,
                from_date=from_date,
            )
            outcome_rows = compute_matured_outcomes(price_history, pending, horizon=horizon)
            matured_counts[horizon] = registry.replace_shadow_outcomes(outcome_rows)

        generic_pending = registry.get_unscored_prediction_logs(
            horizon,
            deployment_mode="shadow_ml",
        )
        if not generic_pending:
            generic_matured_counts[horizon] = 0
            continue
        generic_from_date = min(row["prediction_date"] for row in generic_pending)
        price_history = load_operational_price_history(
            ohlcv_db_path=operational_paths.ohlcv_db_path,
            exchange=args.exchange,
            from_date=generic_from_date,
        )
        generic_inputs = [
            {
                "prediction_id": row["prediction_log_id"],
                "prediction_date": row["prediction_date"],
                "symbol_id": row["symbol_id"],
                "exchange": row["exchange"],
            }
            for row in generic_pending
        ]
        pending_by_id = {row["prediction_log_id"]: row for row in generic_pending}
        generic_outcomes = compute_matured_outcomes(price_history, generic_inputs, horizon=horizon)
        generic_rows = [
            {
                "prediction_log_id": row["prediction_id"],
                "prediction_date": row["prediction_date"],
                "model_id": pending_by_id.get(row["prediction_id"], {}).get("model_id"),
                "symbol_id": row["symbol_id"],
                "exchange": row["exchange"],
                "deployment_mode": "shadow_ml",
                "horizon": horizon,
                "future_date": row["future_date"],
                "realized_return": row["realized_return"],
                "hit": row["hit"],
            }
            for row in generic_outcomes
        ]
        generic_matured_counts[horizon] = registry.replace_shadow_eval(generic_rows)

    for horizon, model_meta in ((5, model_5d_meta), (20, model_20d_meta)):
        model_id = model_meta.get("model_id")
        if not model_id:
            promotion_status[horizon] = "skipped_missing_model_id"
            continue
        policy_result = evaluate_promotion_candidate(
            registry=registry,
            model_id=model_id,
            horizon=horizon,
            deployment_mode="shadow_ml",
            lookback_days=60,
            as_of_date=prediction_ts.date().isoformat(),
        )
        gate_results_recorded += registry.record_promotion_gate_results(
            model_id,
            policy_result["gate_results"],
        )
        promotion_status[horizon] = policy_result["overall_status"]

    summary = {
        "prediction_date": prediction_ts.date().isoformat(),
        "prediction_rows": inserted_predictions,
        "prediction_log_rows": inserted_prediction_logs,
        "drift_metrics_recorded": drift_metrics_recorded,
        "promotion_gate_results_recorded": gate_results_recorded,
        "promotion_status": promotion_status,
        "matured_outcomes": matured_counts,
        "shadow_eval_rows": generic_matured_counts,
        "overlay_uri": str(latest_overlay_path),
        "dated_overlay_uri": str(dated_overlay_path),
        "backfill_days": int(args.backfill_days),
    }
    summary_path = reports_dir / "ml_shadow_monitor_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("Shadow monitor updated: %s", summary)


if __name__ == "__main__":
    main()
