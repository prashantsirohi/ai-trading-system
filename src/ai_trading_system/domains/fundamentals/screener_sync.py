"""Sync Screener Excel exports into the canonical SQLite fundamentals DB."""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
import uuid
from datetime import date
from pathlib import Path
from typing import Callable

from ai_trading_system.domains.fundamentals.screener_client import ScreenerClient
from ai_trading_system.domains.fundamentals.screener_readmodels import refresh_fundamental_readmodels
from ai_trading_system.domains.fundamentals.screener_store import ScreenerFinancialsStore
from ai_trading_system.platform.db.paths import get_domain_paths


DEFAULT_SYMBOL_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF_SEC = 5.0


class MissingExpectedQuarterError(ValueError):
    """Raised when a Screener export is valid but not updated to the expected quarter."""


def run_sync(
    *,
    limit: int | None = None,
    force: bool = False,
    db_path: str | Path | None = None,
    master_db_path: str | Path | None = None,
    exports_dir: str | Path | None = None,
    allow_download: bool = False,
    throttle_sec: float = 2.0,
    refresh_readmodels: bool = True,
    missing_current_results: bool = False,
    as_of_date: str | None = None,
    expected_report_date: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, int | str]:
    paths = get_domain_paths()
    resolved_db_path = Path(db_path) if db_path is not None else paths.fundamentals_dir / "screener_financials.db"
    resolved_exports_dir = Path(exports_dir) if exports_dir is not None else paths.fundamentals_dir / "exports"
    resolved_master_db_path = Path(master_db_path) if master_db_path is not None else paths.master_db_path
    store = ScreenerFinancialsStore(resolved_db_path)
    client = ScreenerClient(exports_dir=resolved_exports_dir)
    all_symbols = _load_symbols(
        resolved_master_db_path,
        exports_dir=resolved_exports_dir,
    )
    resolved_expected_report_date = None
    if missing_current_results:
        resolved_expected_report_date = expected_report_date or expected_quarterly_report_date(as_of_date)
        symbols = _symbols_missing_quarterly_report_date(
            resolved_db_path,
            all_symbols,
            report_date=resolved_expected_report_date,
        )
    else:
        synced = set() if force else store.get_synced_symbols()
        symbols = [symbol for symbol in all_symbols if symbol not in synced]
    if limit is not None:
        symbols = symbols[: int(limit)]
    batch_id = f"screener-{uuid.uuid4().hex[:10]}"
    store.begin_batch(batch_id, symbols_total=len(symbols), exports_dir=resolved_exports_dir, force=force)
    _emit(
        progress,
        "Starting Screener sync "
        f"sync_batch_id={batch_id} total={len(symbols)} "
        f"db_path={resolved_db_path} master_db_path={resolved_master_db_path} "
        f"exports_dir={resolved_exports_dir} allow_download={allow_download} force={force}"
        f"{f' missing_current_results=True expected_report_date={resolved_expected_report_date}' if missing_current_results else ''}",
    )
    if not symbols:
        if missing_current_results:
            _emit(
                progress,
                f"No symbols to sync; all available symbols already have quarterly report_date={resolved_expected_report_date}.",
            )
        else:
            _emit(progress, "No symbols to sync; all available symbols are already synced.")
    succeeded = 0
    failed = 0
    skipped = 0
    for index, symbol in enumerate(symbols):
        item_start = time.monotonic()
        action = "download+parse" if allow_download else "parse export"
        _emit(progress, f"[{index + 1}/{len(symbols)}] {symbol}: {action} started")
        try:
            if index > 0 and throttle_sec > 0 and allow_download:
                time.sleep(float(throttle_sec))
            _sync_symbol_with_retries(
                client=client,
                store=store,
                symbol=symbol,
                force_download=force or bool(missing_current_results and allow_download),
                allow_download=allow_download,
                expected_report_date=resolved_expected_report_date,
                sync_batch_id=batch_id,
                progress=progress,
                label=f"[{index + 1}/{len(symbols)}] {symbol}",
            )
            succeeded += 1
            _emit(
                progress,
                f"[{index + 1}/{len(symbols)}] {symbol}: ok "
                f"elapsed={time.monotonic() - item_start:.1f}s succeeded={succeeded} skipped={skipped} failed={failed}",
            )
        except MissingExpectedQuarterError as exc:
            skipped += 1
            _emit(
                progress,
                f"[{index + 1}/{len(symbols)}] {symbol}: skipped "
                f"reason={exc} elapsed={time.monotonic() - item_start:.1f}s "
                f"succeeded={succeeded} skipped={skipped} failed={failed}",
            )
        except Exception as exc:  # noqa: BLE001
            failed += 1
            store.record_error(batch_id, symbol, str(exc))
            _emit(
                progress,
                f"[{index + 1}/{len(symbols)}] {symbol}: failed "
                f"error={type(exc).__name__}: {exc} succeeded={succeeded} skipped={skipped} failed={failed}",
            )
    store.finish_batch(batch_id, succeeded=succeeded, failed=failed)
    if refresh_readmodels and succeeded:
        _emit(progress, f"Refreshing fundamentals readmodels from {resolved_db_path}")
        refresh_fundamental_readmodels(db_path=resolved_db_path)
        _emit(progress, "Readmodel refresh completed")
    elif refresh_readmodels:
        _emit(progress, "Readmodel refresh skipped because no symbols succeeded")
    _emit(
        progress,
        f"Finished Screener sync sync_batch_id={batch_id} total={len(symbols)} "
        f"succeeded={succeeded} skipped={skipped} failed={failed}",
    )
    if failed:
        _emit(
            progress,
            "Inspect failures with: "
            f"./.venv/bin/python -m sqlite3 {resolved_db_path} "
            f"\"SELECT symbol, error FROM screener_sync_error WHERE sync_batch_id = '{batch_id}' ORDER BY symbol;\"",
        )
    return {
        "sync_batch_id": batch_id,
        "total": len(symbols),
        "succeeded": succeeded,
        "skipped": skipped,
        "failed": failed,
        "expected_report_date": resolved_expected_report_date or "",
    }


def _sync_symbol_with_retries(
    *,
    client: ScreenerClient,
    store: ScreenerFinancialsStore,
    symbol: str,
    force_download: bool,
    allow_download: bool,
    expected_report_date: str | None,
    sync_batch_id: str,
    progress: Callable[[str], None] | None,
    label: str,
) -> None:
    last_exc: Exception | None = None
    for attempt in range(1, DEFAULT_SYMBOL_ATTEMPTS + 1):
        try:
            if attempt > 1:
                _emit(progress, f"{label}: retry attempt {attempt}/{DEFAULT_SYMBOL_ATTEMPTS}")
            data = client.fetch_company_data(
                symbol,
                force_download=force_download,
                allow_download=allow_download,
            )
            if expected_report_date is not None and not _has_quarterly_report_date(data, expected_report_date):
                raise MissingExpectedQuarterError(
                    f"expected quarterly report_date={expected_report_date} not found in Screener export"
                )
            store.save_company_financials(symbol, data, sync_batch_id=sync_batch_id)
            return
        except MissingExpectedQuarterError as exc:
            last_exc = exc
            break
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= DEFAULT_SYMBOL_ATTEMPTS:
                break
            _emit(
                progress,
                f"{label}: attempt {attempt}/{DEFAULT_SYMBOL_ATTEMPTS} failed "
                f"error={type(exc).__name__}: {exc}; retrying",
            )
            if allow_download and DEFAULT_RETRY_BACKOFF_SEC > 0:
                time.sleep(DEFAULT_RETRY_BACKOFF_SEC)
    if last_exc is not None:
        raise last_exc


def build_parser() -> argparse.ArgumentParser:
    paths = get_domain_paths()
    parser = argparse.ArgumentParser(
        description="Sync Screener Excel exports into screener_financials.db.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--db-path",
        default=str(paths.fundamentals_dir / "screener_financials.db"),
        help="Canonical Screener SQLite DB path.",
    )
    parser.add_argument(
        "--master-db-path",
        default=str(paths.master_db_path),
        help="Master symbol database used to choose sync symbols.",
    )
    parser.add_argument(
        "--exports-dir",
        default=str(paths.fundamentals_dir / "exports"),
        help="Directory containing Screener Excel exports.",
    )
    parser.add_argument("--allow-download", action="store_true", help="Download missing/stale Excel files from Screener.in")
    parser.add_argument(
        "--missing-current-results",
        action="store_true",
        help="Only sync symbols missing the latest expected quarterly report date.",
    )
    parser.add_argument(
        "--as-of-date",
        default=None,
        help="Date used to infer the latest expected quarterly report date.",
    )
    parser.add_argument(
        "--expected-report-date",
        default=None,
        help="Manual quarterly report_date override for --missing-current-results.",
    )
    parser.add_argument("--throttle-sec", type=float, default=2.0)
    parser.add_argument("--no-refresh-readmodels", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        result = run_sync(
            limit=args.limit,
            force=args.force,
            db_path=args.db_path,
            master_db_path=args.master_db_path,
            exports_dir=args.exports_dir,
            allow_download=args.allow_download,
            throttle_sec=args.throttle_sec,
            refresh_readmodels=not args.no_refresh_readmodels,
            missing_current_results=args.missing_current_results,
            as_of_date=args.as_of_date,
            expected_report_date=args.expected_report_date,
            progress=lambda message: print(message, flush=True),
        )
    except Exception as exc:  # noqa: BLE001
        print(f"FATAL Screener sync failed: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(1) from exc
    print(
        f"sync_batch_id={result['sync_batch_id']} total={result['total']} "
        f"succeeded={result['succeeded']} skipped={result['skipped']} failed={result['failed']}"
    )


def _load_symbols(master_db_path: Path, *, exports_dir: Path) -> list[str]:
    if not master_db_path.exists():
        return sorted({path.name.removesuffix("_screener.xlsx").upper() for path in exports_dir.glob("*_screener.xlsx")})
    conn = sqlite3.connect(master_db_path)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT COALESCE(nse_symbol, symbol_id) AS ticker
            FROM symbols
            WHERE (exchange = 'NSE' OR nse_symbol IS NOT NULL)
              AND COALESCE(nse_symbol, symbol_id) IS NOT NULL
              AND COALESCE(nse_symbol, symbol_id) != ''
            ORDER BY mcap DESC
            """
        ).fetchall()
    finally:
        conn.close()
    return [str(row[0]).upper().strip() for row in rows if str(row[0]).strip()]


def expected_quarterly_report_date(as_of_date: str | date | None = None) -> str:
    as_of = _parse_date(as_of_date) if as_of_date is not None else date.today()
    if as_of.month <= 3:
        return date(as_of.year - 1, 12, 31).isoformat()
    if as_of.month <= 6:
        return date(as_of.year, 3, 31).isoformat()
    if as_of.month <= 9:
        return date(as_of.year, 6, 30).isoformat()
    return date(as_of.year, 9, 30).isoformat()


def _symbols_missing_quarterly_report_date(
    db_path: Path,
    symbols: list[str],
    *,
    report_date: str,
) -> list[str]:
    if not symbols:
        return []
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT upper(trim(symbol)) AS symbol
            FROM screener_financials
            WHERE lower(trim(period_type)) = 'quarterly'
              AND date(report_date) = date(?)
            """,
            (report_date,),
        ).fetchall()
    finally:
        conn.close()
    present = {str(row[0]).upper().strip() for row in rows if str(row[0]).strip()}
    return [symbol for symbol in symbols if symbol.upper().strip() not in present]


def _has_quarterly_report_date(data: dict, report_date: str) -> bool:
    target = str(report_date)[:10]
    quarters = data.get("quarters", {})
    if not isinstance(quarters, dict):
        return False
    for values_by_date in quarters.values():
        if not isinstance(values_by_date, dict):
            continue
        if any(str(key)[:10] == target for key in values_by_date):
            return True
    return False


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _emit(progress: Callable[[str], None] | None, message: str) -> None:
    if progress is not None:
        progress(message)


__all__ = ["expected_quarterly_report_date", "run_sync"]


if __name__ == "__main__":
    main()
