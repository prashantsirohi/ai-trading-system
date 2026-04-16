"""End-to-end LightGBM research workflow: prepare, train, walk-forward, compare, blend."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

from analytics.alpha.dataset_builder import AlphaDatasetBuilder
from analytics.alpha.training import train_and_register_model
from analytics.lightgbm_engine import LightGBMAlphaEngine
from analytics.registry import RegistryStore
from core.paths import ensure_domain_layout, research_static_end_date
from core.logging import log_context, logger


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


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[1]
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    horizons = _parse_horizons(args.horizons)
    to_date = args.to_date or research_static_end_date()

    registry = RegistryStore(project_root)
    builder = AlphaDatasetBuilder(project_root=project_root, data_domain="research")
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
                register_dataset=True,
            )
            training_df, dataset_meta = AlphaDatasetBuilder.load_prepared_dataset(prepared.dataset_path)
            model_name = f"{args.model_prefix}_{horizon}d"
            model_version = f"{args.from_date}_{to_date}"
            trained = train_and_register_model(
                engine=engine,
                registry=registry,
                training_df=training_df,
                dataset_meta=dataset_meta,
                horizon=horizon,
                model_name=model_name,
                model_version=model_version,
                progress_interval=args.progress_interval,
                min_train_years=args.min_train_years,
            )
            walkforward = trained["walkforward"]
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
