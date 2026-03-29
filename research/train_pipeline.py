"""Research training entrypoint using static historical snapshots."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

from analytics.ml_engine import AlphaEngine
from analytics.registry import RegistryStore
from utils.data_domains import ensure_domain_layout, research_static_end_date
from utils.logger import log_context, logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research training pipeline")
    parser.add_argument("--from-date", help="Inclusive training start date")
    parser.add_argument("--to-date", help="Inclusive training end date. Defaults to prior year end.")
    parser.add_argument("--horizon", type=int, default=5)
    parser.add_argument("--model-name", default="alpha_engine")
    parser.add_argument("--model-version", default="dev")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[1]
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    to_date = args.to_date or research_static_end_date()
    from_date = args.from_date or "2015-01-01"

    with log_context(run_id="research-train", stage_name="train"):
        engine = AlphaEngine(
            ohlcv_db_path=str(paths.ohlcv_db_path),
            feature_store_dir=str(paths.feature_store_dir),
            model_dir=str(paths.model_dir),
            data_domain="research",
        )
        training_df = engine.prepare_training_data(from_date=from_date, to_date=to_date)
        model, metadata = engine.train(training_df, horizon=args.horizon)

        artifact_path = Path(paths.model_dir) / f"{args.model_name}_{args.model_version}.json"
        artifact_path.write_text(str(metadata), encoding="utf-8")
        feature_schema_hash = hashlib.sha256(
            ",".join(sorted(training_df.columns)).encode("utf-8")
        ).hexdigest()

        registry = RegistryStore(project_root)
        model_id = registry.register_model(
            model_name=args.model_name,
            model_version=args.model_version,
            artifact_uri=str(artifact_path),
            feature_schema_hash=feature_schema_hash,
            train_snapshot_ref=f"research:{to_date}",
            approval_status="pending",
        )
        logger.info("Research training complete model_id=%s rows=%s", model_id, len(training_df))


if __name__ == "__main__":
    main()
