"""Prepare reproducible research training datasets from OHLCV plus feature data."""

from __future__ import annotations

import argparse
from pathlib import Path

from analytics.alpha.dataset_builder import AlphaDatasetBuilder
from analytics.lightgbm_engine import LightGBMAlphaEngine
from analytics.ml_engine import AlphaEngine
from core.paths import ensure_domain_layout, research_static_end_date
from core.logging import log_context, logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare research training dataset")
    parser.add_argument("--engine", choices=["xgboost", "lightgbm"], default="lightgbm")
    parser.add_argument("--dataset-name", default="lightgbm_training")
    parser.add_argument("--from-date", help="Inclusive training start date")
    parser.add_argument("--to-date", help="Inclusive training end date. Defaults to prior year end.")
    parser.add_argument("--horizon", type=int, default=5)
    parser.add_argument("--validation-fraction", type=float, default=0.2)
    return parser


def _build_engine(engine_name: str, paths) -> AlphaEngine:
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

    with log_context(run_id="research-prepare-dataset", stage_name="prepare_dataset"):
        engine = _build_engine(args.engine, paths)
        builder = AlphaDatasetBuilder(project_root=project_root, data_domain="research")
        prepared = builder.prepare(
            engine=engine,
            dataset_name=args.dataset_name,
            from_date=from_date,
            to_date=to_date,
            horizon=args.horizon,
            validation_fraction=args.validation_fraction,
            register_dataset=True,
        )
        logger.info(
            "Prepared dataset complete ref=%s rows=%s symbols=%s",
            prepared.dataset_ref,
            prepared.row_count,
            prepared.symbol_count,
        )


if __name__ == "__main__":
    main()
