"""Research training entrypoint using static historical snapshots."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from analytics.ml_engine import AlphaEngine
from analytics.lightgbm_engine import LightGBMAlphaEngine
from analytics.training_dataset import TrainingDatasetBuilder
from analytics.registry import RegistryStore
from utils.data_domains import ensure_domain_layout, research_static_end_date
from utils.logger import log_context, logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research training pipeline")
    parser.add_argument("--from-date", help="Inclusive training start date")
    parser.add_argument("--to-date", help="Inclusive training end date. Defaults to prior year end.")
    parser.add_argument("--horizon", type=int, default=5)
    parser.add_argument("--engine", choices=["xgboost", "lightgbm"], default="xgboost")
    parser.add_argument("--dataset-uri", help="Prepared dataset parquet path to train from")
    parser.add_argument("--model-name", default="alpha_engine")
    parser.add_argument("--model-version", default="dev")
    parser.add_argument("--progress-interval", type=int, default=25)
    return parser


def build_engine(engine_name: str, *, paths) -> AlphaEngine:
    engine_cls = LightGBMAlphaEngine if engine_name == "lightgbm" else AlphaEngine
    return engine_cls(
        ohlcv_db_path=str(paths.ohlcv_db_path),
        feature_store_dir=str(paths.feature_store_dir),
        model_dir=str(paths.model_dir),
        data_domain="research",
    )


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[1]
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    to_date = args.to_date or research_static_end_date()
    from_date = args.from_date or "2015-01-01"

    with log_context(run_id="research-train", stage_name="train"):
        engine = build_engine(args.engine, paths=paths)
        dataset_ref = f"research:{to_date}"
        if args.dataset_uri:
            training_df, dataset_meta = TrainingDatasetBuilder.load_prepared_dataset(args.dataset_uri)
            dataset_ref = dataset_meta.get("dataset_ref", dataset_ref)
            logger.info(
                "Loaded prepared dataset ref=%s rows=%s path=%s",
                dataset_ref,
                len(training_df),
                args.dataset_uri,
            )
        else:
            training_df = engine.prepare_training_data(from_date=from_date, to_date=to_date)
            dataset_meta = {}
        model, metadata = engine.train(
            training_df,
            horizon=args.horizon,
            validation_start=dataset_meta.get("validation_start"),
            validation_fraction=dataset_meta.get("validation_fraction", 0.2),
            show_progress=args.engine == "lightgbm",
            progress_interval=args.progress_interval,
        )
        eval_metrics = {}
        if args.engine == "lightgbm":
            eval_metrics = engine.evaluate(
                training_df,
                model=model,
                horizon=args.horizon,
                validation_start=dataset_meta.get("validation_start"),
                validation_fraction=dataset_meta.get("validation_fraction", 0.2),
            )
        artifact_path = Path(
            engine.save_model(
                model,
                horizon=args.horizon,
            )
        )
        target_artifact_path = artifact_path.with_name(
            f"{args.model_name}_{args.model_version}{artifact_path.suffix}"
        )
        if artifact_path != target_artifact_path:
            artifact_path.replace(target_artifact_path)
            artifact_path = target_artifact_path

        metadata_path = Path(paths.model_dir) / f"{args.model_name}_{args.model_version}.metadata.json"
        metadata_payload = {
            "engine": args.engine,
            "horizon": args.horizon,
            "from_date": from_date,
            "to_date": to_date,
            "training_rows": int(len(training_df)),
            "training_symbols": int(training_df["symbol_id"].nunique()),
            "feature_count": int(len(engine._feature_cols(training_df))),
            "dataset_ref": dataset_ref,
            "dataset_uri": args.dataset_uri,
            "prepared_dataset": bool(args.dataset_uri),
            "dataset_metadata": dataset_meta,
            "evaluation": eval_metrics,
            **metadata,
        }
        metadata_path.write_text(json.dumps(metadata_payload, indent=2), encoding="utf-8")
        feature_schema_hash = hashlib.sha256(
            ",".join(sorted(training_df.columns)).encode("utf-8")
        ).hexdigest()

        registry = RegistryStore(project_root)
        model_id = registry.register_model(
            model_name=args.model_name,
            model_version=args.model_version,
            artifact_uri=str(artifact_path),
            feature_schema_hash=feature_schema_hash,
            train_snapshot_ref=dataset_ref,
            approval_status="pending",
            metadata={
                "engine": args.engine,
                "metadata_uri": str(metadata_path),
                "horizon": args.horizon,
                "training_rows": int(len(training_df)),
                "training_symbols": int(training_df["symbol_id"].nunique()),
                "dataset_ref": dataset_ref,
                "dataset_uri": args.dataset_uri,
                "evaluation": eval_metrics,
            },
        )
        if eval_metrics:
            registry.record_model_eval(
                model_id,
                {
                    "validation_auc": eval_metrics.get("validation_auc", 0.0),
                    "precision_at_10pct": eval_metrics.get("precision_at_10pct", 0.0),
                    "avg_return_top_10pct": eval_metrics.get("avg_return_top_10pct", 0.0),
                    "baseline_positive_rate": eval_metrics.get("baseline_positive_rate", 0.0),
                },
                dataset_ref=dataset_ref,
                notes=f"{args.engine} prepared-dataset validation",
            )
        logger.info(
            "Research training complete model_id=%s engine=%s rows=%s artifact=%s eval=%s",
            model_id,
            args.engine,
            len(training_df),
            artifact_path,
            eval_metrics,
        )


if __name__ == "__main__":
    main()
