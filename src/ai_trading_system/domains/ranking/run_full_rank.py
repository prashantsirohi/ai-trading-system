"""Run full ranking over NSE symbols and write a local CSV snapshot."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from ai_trading_system.domains.ranking.ranker import StockRanker
from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.platform.utils.env import load_project_env


def run_full_rank(
    *,
    project_root: Path,
    data_domain: str = "operational",
    rank_date: str | None = None,
    output_csv: str = "rankings_latest.csv",
) -> dict[str, object]:
    paths = ensure_domain_layout(project_root=project_root, data_domain=data_domain)
    ranker = StockRanker(
        ohlcv_db_path=str(paths.ohlcv_db_path),
        feature_store_dir=str(paths.feature_store_dir),
    )
    t0 = time.time()
    result = ranker.rank_all(
        date=rank_date,
        exchanges=["NSE"],
        top_n=None,
    )
    elapsed = time.time() - t0
    if result is not None and not result.empty:
        result.to_csv(output_csv, index=False)
        return {
            "status": "success",
            "rows": int(len(result)),
            "elapsed_sec": round(float(elapsed), 2),
            "output_csv": output_csv,
        }
    return {"status": "failed", "rows": 0, "elapsed_sec": round(float(elapsed), 2), "output_csv": output_csv}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run full stock ranking and export CSV.")
    parser.add_argument("--data-domain", choices=["operational", "research"], default="operational")
    parser.add_argument("--date", dest="rank_date", default=None, help="Optional ranking date (YYYY-MM-DD)")
    parser.add_argument("--output", default="rankings_latest.csv", help="Output CSV path")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[4]
    load_project_env(project_root)
    outcome = run_full_rank(
        project_root=project_root,
        data_domain=args.data_domain,
        rank_date=args.rank_date,
        output_csv=str(args.output),
    )
    print(outcome)


if __name__ == "__main__":
    main()

