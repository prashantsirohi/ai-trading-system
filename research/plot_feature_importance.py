"""Render a Matplotlib feature-importance chart for a trained model metadata file."""

from __future__ import annotations

import argparse
from pathlib import Path

from analytics.visualizations import Visualizer
from utils.data_domains import ensure_domain_layout
from utils.logger import log_context, logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Plot model feature importance")
    parser.add_argument("--metadata-path", required=True, help="Path to model metadata JSON")
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--output-path", help="Optional PNG output path")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[1]
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")

    with log_context(run_id="research-feature-importance", stage_name="visualize"):
        visualizer = Visualizer(
            ohlcv_db_path=str(paths.ohlcv_db_path),
            feature_store_dir=str(paths.feature_store_dir),
            output_dir=str(paths.reports_dir),
        )
        output_path = visualizer.plot_feature_importance(
            metadata_path=args.metadata_path,
            top_n=args.top_n,
            output_path=args.output_path,
        )
        logger.info("Feature importance chart ready: %s", output_path)


if __name__ == "__main__":
    main()
