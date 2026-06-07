"""CLI and orchestration entrypoint for valuation feature refresh."""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from ai_trading_system.domains.features.valuation_cycle import refresh_valuation_cycle_features
from ai_trading_system.domains.features.valuation_index import DEFAULT_UNIVERSES, refresh_valuation_index
from ai_trading_system.domains.features.stock_valuation_bands import refresh_stock_valuation_bands
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
    enable_stock_valuation_bands: bool = True,
    stock_valuation_band_universe_id: str = "UNIV_TOP1000_MCAP",
    valuation_band_min_history_days_3y: int = 504,
    valuation_band_min_history_days_5y: int = 756,
    stock_valuation_bands_output_csv: str | Path | None = None,
    progress: bool = False,
) -> dict[str, Any]:
    started_at = time.perf_counter()

    def emit(message: str) -> None:
        if progress:
            elapsed = time.perf_counter() - started_at
            print(f"[valuation_refresh +{elapsed:,.1f}s] {message}", file=sys.stderr, flush=True)

    paths = get_domain_paths()
    resolved_ohlcv = Path(ohlcv_db_path) if ohlcv_db_path is not None else paths.ohlcv_db_path
    resolved_screener = Path(screener_db_path) if screener_db_path is not None else default_screener_db_path()
    resolved_master = Path(master_db_path) if master_db_path is not None else paths.master_db_path
    universe_ids = list(universes or DEFAULT_UNIVERSES)
    date_window = f"{from_date or 'earliest'}..{to_date or 'latest'}"
    emit(
        "starting "
        f"dates={date_window} universes={','.join(universe_ids)} "
        f"stock_bands={'on' if enable_stock_valuation_bands else 'off'}"
    )
    if not resolved_screener.exists():
        emit(f"skipped: missing Screener DB {resolved_screener}")
        return {
            "status": "skipped_missing_screener_db",
            "screener_db_path": str(resolved_screener),
            "universes": universe_ids,
        }
    emit("phase 1/4: refreshing point-in-time TTM fundamentals")
    phase_started = time.perf_counter()
    ttm = refresh_fundamental_ttm(
        ohlcv_db_path=resolved_ohlcv,
        screener_db_path=resolved_screener,
        from_date=from_date,
        to_date=to_date,
    )
    emit(
        "phase 1/4 complete "
        f"rows={ttm.rows:,} symbols={ttm.symbols:,} dates={ttm.dates:,} "
        f"quarterly={ttm.quarterly_rows:,} annual_fallback={ttm.annual_fallback_rows:,} "
        f"missing={ttm.missing_rows:,} elapsed={time.perf_counter() - phase_started:,.1f}s"
    )
    emit("phase 2/4: refreshing stock/universe/sector valuation")
    phase_started = time.perf_counter()
    valuation = refresh_valuation_index(
        ohlcv_db_path=resolved_ohlcv,
        master_db_path=resolved_master,
        universes=universe_ids,
        from_date=from_date,
        to_date=to_date,
    )
    emit(
        "phase 2/4 complete "
        f"stock_rows={valuation.stock_rows:,} membership_rows={valuation.membership_rows:,} "
        f"universe_index_rows={valuation.universe_index_rows:,} sector_rows={valuation.sector_rows:,} "
        f"elapsed={time.perf_counter() - phase_started:,.1f}s"
    )
    emit("phase 3/4: refreshing valuation cycle features")
    phase_started = time.perf_counter()
    cycle = refresh_valuation_cycle_features(
        ohlcv_db_path=resolved_ohlcv,
        from_date=from_date,
        to_date=to_date,
        min_history_days=min_history_days,
    )
    emit(
        "phase 3/4 complete "
        f"rows={cycle.rows:,} universe_rows={cycle.universe_rows:,} sector_rows={cycle.sector_rows:,} "
        f"elapsed={time.perf_counter() - phase_started:,.1f}s"
    )
    bands = None
    if enable_stock_valuation_bands:
        emit("phase 4/4: refreshing stock valuation bands")
        phase_started = time.perf_counter()
        bands = refresh_stock_valuation_bands(
            ohlcv_db_path=resolved_ohlcv,
            from_date=from_date,
            to_date=to_date,
            universe_id=stock_valuation_band_universe_id,
            min_history_days_3y=valuation_band_min_history_days_3y,
            min_history_days_5y=valuation_band_min_history_days_5y,
            output_csv=stock_valuation_bands_output_csv,
        )
        emit(
            "phase 4/4 complete "
            f"rows={bands.rows:,} symbols={bands.symbols:,} latest_rows={bands.latest_rows:,} "
            f"latest_date={bands.latest_date} elapsed={time.perf_counter() - phase_started:,.1f}s"
        )
    else:
        emit("phase 4/4 skipped: stock valuation bands disabled")
    emit(f"completed elapsed={time.perf_counter() - started_at:,.1f}s")
    return {
        "status": "completed",
        "screener_db_path": str(resolved_screener),
        "ohlcv_db_path": str(resolved_ohlcv),
        "universes": universe_ids,
        "ttm": asdict(ttm),
        "valuation": asdict(valuation),
        "cycle": asdict(cycle),
        "stock_valuation_bands": asdict(bands) if bands is not None else {"status": "disabled"},
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
    parser.add_argument("--enable-stock-valuation-bands", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--stock-valuation-band-universe-id", default="UNIV_TOP1000_MCAP")
    parser.add_argument("--valuation-band-min-history-days-3y", type=int, default=504)
    parser.add_argument("--valuation-band-min-history-days-5y", type=int, default=756)
    parser.add_argument("--stock-valuation-bands-output-csv", default=None)
    parser.add_argument("--quiet", action="store_true", help="Suppress phase progress messages.")
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
        enable_stock_valuation_bands=bool(args.enable_stock_valuation_bands),
        stock_valuation_band_universe_id=args.stock_valuation_band_universe_id,
        valuation_band_min_history_days_3y=int(args.valuation_band_min_history_days_3y),
        valuation_band_min_history_days_5y=int(args.valuation_band_min_history_days_5y),
        stock_valuation_bands_output_csv=args.stock_valuation_bands_output_csv,
        progress=not bool(args.quiet),
    )
    print(result)


if __name__ == "__main__":
    main()


__all__ = ["refresh_valuation_features"]
