"""Research training entrypoint using static historical snapshots."""

from __future__ import annotations

import argparse
from pathlib import Path

from analytics.alpha.dataset_builder import AlphaDatasetBuilder
from analytics.alpha.training import train_and_register_model
from analytics.ml_engine import AlphaEngine
from analytics.lightgbm_engine import LightGBMAlphaEngine
from analytics.registry import RegistryStore
from core.paths import ensure_domain_layout, research_static_end_date
from core.logging import log_context, logger


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
    parser.add_argument("--min-train-years", type=int, default=5)
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
    project_root = Path(__file__).resolve().parents[3]
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    to_date = args.to_date or research_static_end_date()
    from_date = args.from_date or "2015-01-01"

    with log_context(run_id="research-train", stage_name="train"):
        engine = build_engine(args.engine, paths=paths)
        if args.dataset_uri:
            training_df, dataset_meta = AlphaDatasetBuilder.load_prepared_dataset(args.dataset_uri)
            logger.info(
                "Loaded prepared dataset ref=%s rows=%s path=%s",
                dataset_meta.get("dataset_ref", f"research:{to_date}"),
                len(training_df),
                args.dataset_uri,
            )
        else:
            builder = AlphaDatasetBuilder(project_root=project_root, data_domain="research")
            dataset_name = f"{args.model_name}_{args.model_version}_h{args.horizon}"
            prepared = builder.prepare(
                engine=engine,
                dataset_name=dataset_name,
                from_date=from_date,
                to_date=to_date,
                horizon=args.horizon,
                validation_fraction=0.2,
                register_dataset=True,
            )
            training_df, dataset_meta = AlphaDatasetBuilder.load_prepared_dataset(prepared.dataset_path)
            logger.info(
                "Prepared dataset for training ref=%s rows=%s path=%s",
                dataset_meta["dataset_ref"],
                len(training_df),
                prepared.dataset_path,
            )

        trained = train_and_register_model(
            engine=engine,
            registry=RegistryStore(project_root),
            training_df=training_df,
            dataset_meta=dataset_meta,
            horizon=args.horizon,
            model_name=args.model_name,
            model_version=args.model_version,
            progress_interval=args.progress_interval,
            min_train_years=args.min_train_years,
        )
        logger.info(
            "Research training complete model_id=%s engine=%s rows=%s artifact=%s eval=%s walkforward=%s",
            trained["model_id"],
            args.engine,
            len(training_df),
            trained["artifact_uri"],
            trained["evaluation"],
            trained["walkforward"].get("summary", {}),
        )


if __name__ == "__main__":
    main()
