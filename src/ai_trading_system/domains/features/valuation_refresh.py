"""CLI and orchestration entrypoint for valuation feature refresh."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path
from typing import Any

from ai_trading_system.domains.features.valuation_cycle import refresh_valuation_cycle_features
from ai_trading_system.domains.features.valuation_index import DEFAULT_UNIVERSES, refresh_valuation_index
from ai_trading_system.domains.features.valuation_ttm import refresh_fundamental_ttm
from ai_trading_system.domains.fundamentals.screener_store import default_screener_db_path
from ai_trading_system.platform.db.paths import get_domain_paths


def refresh_valuation_features(
    *,
    ohlcv_db_path: str | Path | None = None,
    screener_db_path: str | Path | None = None,
    master_db_path: str | Path | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    universes: list[str] | tuple[str, ...] | None = None,
    min_history_days: int = 756,
) -> dict[str, Any]:
    paths = get_domain_paths()
    resolved_ohlcv = Path(ohlcv_db_path) if ohlcv_db_path is not None else paths.ohlcv_db_path
    resolved_screener = Path(screener_db_path) if screener_db_path is not None else default_screener_db_path()
    resolved_master = Path(master_db_path) if master_db_path is not None else paths.master_db_path
    universe_ids = list(universes or DEFAULT_UNIVERSES)
    if not resolved_screener.exists():
        return {
            "status": "skipped_missing_screener_db",
            "screener_db_path": str(resolved_screener),
            "universes": universe_ids,
        }
    ttm = refresh_fundamental_ttm(
        ohlcv_db_path=resolved_ohlcv,
        screener_db_path=resolved_screener,
        from_date=from_date,
        to_date=to_date,
    )
    valuation = refresh_valuation_index(
        ohlcv_db_path=resolved_ohlcv,
        master_db_path=resolved_master,
        universes=universe_ids,
        from_date=from_date,
        to_date=to_date,
    )
    cycle = refresh_valuation_cycle_features(
        ohlcv_db_path=resolved_ohlcv,
        from_date=from_date,
        to_date=to_date,
        min_history_days=min_history_days,
    )
    return {
        "status": "completed",
        "screener_db_path": str(resolved_screener),
        "ohlcv_db_path": str(resolved_ohlcv),
        "universes": universe_ids,
        "ttm": asdict(ttm),
        "valuation": asdict(valuation),
        "cycle": asdict(cycle),
    }


def build_parser() -> argparse.ArgumentParser:
    paths = get_domain_paths()
    parser = argparse.ArgumentParser(description="Refresh point-in-time valuation features.")
    parser.add_argument("--from-date", default=None)
    parser.add_argument("--to-date", default=None)
    parser.add_argument("--universe-id", action="append", default=None, help="Universe id. Can be passed multiple times.")
    parser.add_argument("--full-rebuild", action="store_true", help="Preserved for operator intent; omit dates to rebuild all.")
    parser.add_argument("--screener-db-path", default=str(default_screener_db_path()))
    parser.add_argument("--ohlcv-db-path", default=str(paths.ohlcv_db_path))
    parser.add_argument("--master-db-path", default=str(paths.master_db_path))
    parser.add_argument("--valuation-min-history-days", type=int, default=756)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = refresh_valuation_features(
        ohlcv_db_path=args.ohlcv_db_path,
        screener_db_path=args.screener_db_path,
        master_db_path=args.master_db_path,
        from_date=None if args.full_rebuild else args.from_date,
        to_date=args.to_date,
        universes=args.universe_id or list(DEFAULT_UNIVERSES),
        min_history_days=args.valuation_min_history_days,
    )
    print(result)


if __name__ == "__main__":
    main()


__all__ = ["refresh_valuation_features"]
