"""End-to-end LightGBM research workflow: prepare, train, walk-forward, compare, blend."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Dict, List

from analytics.lightgbm_engine import LightGBMAlphaEngine
from analytics.lightgbm_research import walk_forward_compare
from analytics.registry import RegistryStore
from analytics.training_dataset import TrainingDatasetBuilder
from utils.data_domains import ensure_domain_layout, research_static_end_date
from utils.logger import log_context, logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the LightGBM research workflow")
    parser.add_argument("--from-date", default="2015-01-01")
    parser.add_argument("--to-date", help="Defaults to prior year end for research.")
    parser.add_argument("--horizons", default="5,20", help="Comma-separated horizons, e.g. 5,20")
    parser.add_argument("--dataset-prefix", default="lightgbm_workflow")
    parser.add_argument("--model-prefix", default="lightgbm_alpha")
    parser.add_argument("--min-train-years", type=int, default=5)
    parser.add_argument("--progress-interval", type=int, default=25)
    return parser


def _parse_horizons(raw: str) -> List[int]:
    return [int(part.strip()) for part in raw.split(",") if part.strip()]


def _train_and_register(
    *,
    engine: LightGBMAlphaEngine,
    registry: RegistryStore,
    training_df,
    dataset_meta: Dict,
    horizon: int,
    model_name: str,
    model_version: str,
    progress_interval: int,
) -> Dict:
    model, metadata = engine.train(
        training_df,
        horizon=horizon,
        validation_start=dataset_meta.get("validation_start"),
        validation_fraction=dataset_meta.get("validation_fraction", 0.2),
        show_progress=True,
        progress_interval=progress_interval,
    )
    evaluation = engine.evaluate(
        training_df,
        model=model,
        horizon=horizon,
        validation_start=dataset_meta.get("validation_start"),
        validation_fraction=dataset_meta.get("validation_fraction", 0.2),
    )
    artifact_path = Path(engine.save_model(model, horizon=horizon))
    target_artifact_path = artifact_path.with_name(f"{model_name}_{model_version}{artifact_path.suffix}")
    if artifact_path != target_artifact_path:
        artifact_path.replace(target_artifact_path)
        artifact_path = target_artifact_path

    metadata_path = artifact_path.with_suffix(".metadata.json")
    payload = {
        "engine": "lightgbm",
        "horizon": horizon,
        "training_rows": int(len(training_df)),
        "training_symbols": int(training_df["symbol_id"].nunique()),
        "feature_count": int(len(engine._feature_cols(training_df))),
        "dataset_ref": dataset_meta["dataset_ref"],
        "dataset_uri": dataset_meta["dataset_uri"],
        "prepared_dataset": True,
        "dataset_metadata": dataset_meta,
        "evaluation": evaluation,
        **metadata,
    }
    metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    feature_schema_hash = hashlib.sha256(
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
            "engine": "lightgbm",
            "metadata_uri": str(metadata_path),
            "dataset_ref": dataset_meta["dataset_ref"],
            "dataset_uri": dataset_meta["dataset_uri"],
            "evaluation": evaluation,
            "horizon": horizon,
        },
    )
    registry.record_model_eval(
        model_id,
        {
            "validation_auc": evaluation.get("validation_auc", 0.0),
            "precision_at_10pct": evaluation.get("precision_at_10pct", 0.0),
            "avg_return_top_10pct": evaluation.get("avg_return_top_10pct", 0.0),
            "baseline_positive_rate": evaluation.get("baseline_positive_rate", 0.0),
        },
        dataset_ref=dataset_meta["dataset_ref"],
        notes="lightgbm workflow full-train validation",
    )
    return {
        "model_id": model_id,
        "artifact_uri": str(artifact_path),
        "metadata_uri": str(metadata_path),
        "evaluation": evaluation,
    }


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[1]
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    horizons = _parse_horizons(args.horizons)
    to_date = args.to_date or research_static_end_date()

    registry = RegistryStore(project_root)
    builder = TrainingDatasetBuilder(project_root=project_root, data_domain="research")
    engine = LightGBMAlphaEngine(
        ohlcv_db_path=str(paths.ohlcv_db_path),
        feature_store_dir=str(paths.feature_store_dir),
        model_dir=str(paths.model_dir),
        data_domain="research",
    )

    report = {
        "workflow": "lightgbm_research",
        "from_date": args.from_date,
        "to_date": to_date,
        "horizons": horizons,
        "results": [],
    }

    with log_context(run_id="research-lightgbm-workflow", stage_name="workflow"):
        for horizon in horizons:
            dataset_name = f"{args.dataset_prefix}_h{horizon}"
            prepared = builder.prepare(
                engine=engine,
                dataset_name=dataset_name,
                from_date=args.from_date,
                to_date=to_date,
                horizon=horizon,
                validation_fraction=0.2,
            )
            training_df, dataset_meta = TrainingDatasetBuilder.load_prepared_dataset(prepared.dataset_path)
            model_name = f"{args.model_prefix}_{horizon}d"
            model_version = f"{args.from_date}_{to_date}"
            trained = _train_and_register(
                engine=engine,
                registry=registry,
                training_df=training_df,
                dataset_meta=dataset_meta,
                horizon=horizon,
                model_name=model_name,
                model_version=model_version,
                progress_interval=args.progress_interval,
            )
            walkforward = walk_forward_compare(
                training_df,
                engine=engine,
                horizon=horizon,
                min_train_years=args.min_train_years,
            )
            result = {
                "horizon": horizon,
                "dataset_ref": dataset_meta["dataset_ref"],
                "dataset_uri": dataset_meta["dataset_uri"],
                "full_train": trained,
                "walkforward": walkforward,
                "blend_recommendation": {
                    "technical_weight": 0.75,
                    "ml_weight": 0.25,
                },
            }
            report["results"].append(result)
            logger.info(
                "Completed LightGBM workflow horizon=%s full_eval=%s walkforward=%s",
                horizon,
                trained["evaluation"],
                walkforward.get("summary", {}),
            )

    report_path = paths.reports_dir / f"{args.dataset_prefix}_workflow_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info("Workflow report ready: %s", report_path)


if __name__ == "__main__":
    main()
