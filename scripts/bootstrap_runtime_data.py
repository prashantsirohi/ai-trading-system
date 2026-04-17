"""Bootstrap runtime data directories and optional seed masterdata refresh."""

from __future__ import annotations

import argparse
from pathlib import Path

from collectors.masterdata import MasterDataCollector
from core.bootstrap import ensure_project_root_on_path
from core.logging import logger
from core.paths import ensure_domain_layout


PROJECT_ROOT = Path(ensure_project_root_on_path(__file__))


def _ensure_runtime_paths(*, data_domain: str) -> dict[str, str]:
    paths = ensure_domain_layout(project_root=str(PROJECT_ROOT), data_domain=data_domain)
    dirs = {
        "root": paths.root_dir,
        "raw": paths.root_dir / "raw",
        "raw_nse_eq": paths.root_dir / "raw" / "NSE_EQ",
        "raw_nse_mto": paths.root_dir / "raw" / "NSE_MTO",
        "feature_store": paths.feature_store_dir,
        "pipeline_runs": paths.pipeline_runs_dir,
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return {name: str(path) for name, path in dirs.items()}


def bootstrap_runtime_data(*, refresh_masterdata: bool, data_domain: str) -> int:
    resolved = _ensure_runtime_paths(data_domain=data_domain)
    logger.info("Ensured runtime directories: %s", resolved)

    paths = ensure_domain_layout(project_root=str(PROJECT_ROOT), data_domain=data_domain)
    logger.info("Seed master database path: %s", paths.master_db_path)
    if refresh_masterdata:
        logger.info("Refreshing seed masterdata via collectors.masterdata...")
        collector = MasterDataCollector(db_path=str(paths.master_db_path))
        ok = collector.update()
        if not ok:
            logger.error("Masterdata refresh failed.")
            return 2
        logger.info("Masterdata refresh complete.")

    print("Next steps:")
    print("  1) Fetch OHLCV rows:")
    print("     python -m run.orchestrator --skip-preflight --stages ingest")
    print("  2) Build features + rank:")
    print("     python -m run.orchestrator --skip-preflight --stages features,rank")
    print("  3) Optional publish dry-run:")
    print("     python -m run.orchestrator --skip-preflight --stages publish --local-publish")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bootstrap runtime data directories and optional seed masterdata refresh.",
    )
    parser.add_argument(
        "--refresh-masterdata",
        action="store_true",
        help="Refresh data/masterdata.db from configured upstream source.",
    )
    parser.add_argument(
        "--data-domain",
        default="operational",
        choices=["operational", "research"],
        help="Runtime data domain to bootstrap directories for.",
    )
    args = parser.parse_args()
    return bootstrap_runtime_data(
        refresh_masterdata=bool(args.refresh_masterdata),
        data_domain=str(args.data_domain),
    )


if __name__ == "__main__":
    raise SystemExit(main())
