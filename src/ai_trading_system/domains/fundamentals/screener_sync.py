"""Sync Screener Excel exports into the canonical SQLite fundamentals DB."""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from ai_trading_system.domains.fundamentals.contracts import SUPPORTED_STATEMENT_BASES, normalize_statement_basis
from ai_trading_system.domains.fundamentals.screener_client import ScreenerClient, ScreenerRateLimitError
from ai_trading_system.domains.fundamentals.screener_readmodels import refresh_fundamental_readmodels
from ai_trading_system.domains.fundamentals.screener_store import ScreenerFinancialsStore
from ai_trading_system.platform.db.paths import get_domain_paths


DEFAULT_SYMBOL_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF_SEC = 5.0
DEFAULT_MISSING_RESULTS_RETRY_COOLDOWN_HOURS = 72.0
BOTH_STATEMENT_BASES = "both"


class MissingExpectedQuarterError(ValueError):
    """Raised when a Screener export is valid but not updated to the expected quarter."""


@dataclass(frozen=True)
class MissingQuarterSelection:
    symbols: list[str]
    already_present: int
    terminal_standalone_fallback: int
    retry_cooldown: int


def run_unified_sync(
    *,
    statement_basis: str = BOTH_STATEMENT_BASES,
    **kwargs,
) -> dict[str, object]:
    """Run one or both physical statement-basis syncs and refresh readmodels once."""

    requested = str(statement_basis or BOTH_STATEMENT_BASES).strip().lower()
    if requested != BOTH_STATEMENT_BASES:
        return run_sync(statement_basis=normalize_statement_basis(requested), **kwargs)
    refresh_readmodels = bool(kwargs.pop("refresh_readmodels", True))
    progress = kwargs.get("progress")
    results: list[dict[str, int | str]] = []
    for index, basis in enumerate(SUPPORTED_STATEMENT_BASES):
        if index and bool(kwargs.get("allow_download")) and float(kwargs.get("throttle_sec", 2.0)) > 0:
            time.sleep(float(kwargs.get("throttle_sec", 2.0)))
        _emit(progress, f"Starting unified Screener basis {index + 1}/{len(SUPPORTED_STATEMENT_BASES)}: {basis}")
        results.append(
            run_sync(
                statement_basis=basis,
                refresh_readmodels=False,
                **kwargs,
            )
        )
    succeeded = sum(int(result["succeeded"]) for result in results)
    if refresh_readmodels:
        paths = get_domain_paths()
        resolved_db_path = Path(kwargs["db_path"]) if kwargs.get("db_path") is not None else paths.fundamentals_dir / "screener_financials.db"
        if succeeded or resolved_db_path.exists():
            _emit(progress, f"Refreshing resolved fundamentals readmodels from {resolved_db_path}")
            refresh_fundamental_readmodels(db_path=resolved_db_path)
            _emit(progress, "Resolved readmodel refresh completed")
    return {
        "sync_batch_id": ",".join(str(result["sync_batch_id"]) for result in results),
        "sync_batch_ids": [str(result["sync_batch_id"]) for result in results],
        "statement_basis": BOTH_STATEMENT_BASES,
        "total": sum(int(result["total"]) for result in results),
        "succeeded": succeeded,
        "skipped": sum(int(result["skipped"]) for result in results),
        "failed": sum(int(result["failed"]) for result in results),
        "detected_standalone": sum(int(result["detected_standalone"]) for result in results),
        "detected_consolidated": sum(int(result["detected_consolidated"]) for result in results),
        "expected_report_date": next(
            (str(result["expected_report_date"]) for result in results if result["expected_report_date"]),
            "",
        ),
        "basis_results": results,
    }


def run_sync(
    *,
    statement_basis: str,
    limit: int | None = None,
    force: bool = False,
    db_path: str | Path | None = None,
    master_db_path: str | Path | None = None,
    exports_dir: str | Path | None = None,
    allow_download: bool = False,
    throttle_sec: float = 2.0,
    refresh_readmodels: bool = True,
    missing_current_results: bool = False,
    missing_results_retry_cooldown_hours: float = DEFAULT_MISSING_RESULTS_RETRY_COOLDOWN_HOURS,
    as_of_date: str | None = None,
    expected_report_date: str | None = None,
    symbols: list[str] | None = None,
    progress: Callable[[str], None] | None = None,
    valuation_migration_backup_dir: str | Path | None = None,
) -> dict[str, int | str]:
    requested_basis = normalize_statement_basis(statement_basis)
    if float(missing_results_retry_cooldown_hours) < 0:
        raise ValueError("missing_results_retry_cooldown_hours must be non-negative")
    paths = get_domain_paths()
    resolved_db_path = Path(db_path) if db_path is not None else paths.fundamentals_dir / "screener_financials.db"
    resolved_exports_dir = Path(exports_dir) if exports_dir is not None else paths.fundamentals_dir / "exports"
    resolved_master_db_path = Path(master_db_path) if master_db_path is not None else paths.master_db_path
    store = ScreenerFinancialsStore(
        resolved_db_path,
        valuation_migration_backup_dir=valuation_migration_backup_dir,
    )
    client = ScreenerClient(exports_dir=resolved_exports_dir)
    all_symbols = _load_symbols(
        resolved_master_db_path,
        exports_dir=resolved_exports_dir,
    )
    if symbols:
        requested_symbols = _normalize_symbols(symbols)
        available_symbols = set(all_symbols) | _load_explicit_master_tickers(resolved_master_db_path)
        all_symbols = [
            symbol
            for symbol in requested_symbols
            if symbol in available_symbols or client.excel_path(symbol, statement_basis=requested_basis).exists()
        ]
    resolved_expected_report_date = None
    missing_selection: MissingQuarterSelection | None = None
    if missing_current_results:
        resolved_expected_report_date = expected_report_date or expected_quarterly_report_date(as_of_date)
        missing_selection = _select_symbols_missing_quarterly_report_date(
            resolved_db_path,
            all_symbols,
            report_date=resolved_expected_report_date,
            statement_basis=requested_basis,
            retry_cooldown_hours=missing_results_retry_cooldown_hours,
        )
        symbols = missing_selection.symbols
    else:
        synced = set() if force else store.get_synced_symbols(requested_basis=requested_basis)
        symbols = [symbol for symbol in all_symbols if symbol not in synced]
    if limit is not None:
        symbols = symbols[: int(limit)]
    batch_id = f"screener-{uuid.uuid4().hex[:10]}"
    store.begin_batch(
        batch_id,
        symbols_total=len(symbols),
        exports_dir=resolved_exports_dir,
        force=force,
        missing_current_results=missing_current_results,
        expected_report_date=resolved_expected_report_date,
        retry_cooldown_hours=missing_results_retry_cooldown_hours if missing_current_results else None,
    )
    _emit(
        progress,
        "Starting Screener sync "
        f"sync_batch_id={batch_id} total={len(symbols)} "
        f"db_path={resolved_db_path} master_db_path={resolved_master_db_path} "
        f"exports_dir={resolved_exports_dir} allow_download={allow_download} force={force}"
        f" statement_basis={requested_basis}"
        f"{f' missing_current_results=True expected_report_date={resolved_expected_report_date}' if missing_current_results else ''}",
    )
    if missing_selection is not None:
        _emit(
            progress,
            "Missing-results selection "
            f"eligible_before_limit={len(missing_selection.symbols)} "
            f"already_present={missing_selection.already_present} "
            f"terminal_standalone_fallback={missing_selection.terminal_standalone_fallback} "
            f"retry_cooldown={missing_selection.retry_cooldown} "
            f"retry_cooldown_hours={float(missing_results_retry_cooldown_hours):g}",
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
    detected_standalone = 0
    detected_consolidated = 0
    for index, symbol in enumerate(symbols):
        item_start = time.monotonic()
        action = "download+parse" if allow_download else "parse export"
        _emit(progress, f"[{index + 1}/{len(symbols)}] {symbol}: {action} started")
        try:
            if index > 0 and throttle_sec > 0 and allow_download:
                time.sleep(float(throttle_sec))
            detected_basis = _sync_symbol_with_retries(
                client=client,
                store=store,
                symbol=symbol,
                statement_basis=requested_basis,
                force_download=force or bool(missing_current_results and allow_download),
                allow_download=allow_download,
                expected_report_date=resolved_expected_report_date,
                sync_batch_id=batch_id,
                progress=progress,
                label=f"[{index + 1}/{len(symbols)}] {symbol}",
            )
            if detected_basis == "consolidated":
                detected_consolidated += 1
            else:
                detected_standalone += 1
            if detected_basis != requested_basis:
                _emit(
                    progress,
                    f"[{index + 1}/{len(symbols)}] {symbol}: statement basis fallback "
                    f"requested={requested_basis} detected={detected_basis}",
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
            store.record_sync_result(
                batch_id,
                symbol,
                requested_basis=requested_basis,
                detected_basis=None,
                export_path=client.excel_path(symbol, statement_basis=requested_basis),
                status="failed",
            )
            store.record_error(batch_id, symbol, str(exc))
            _emit(
                progress,
                f"[{index + 1}/{len(symbols)}] {symbol}: failed "
                f"error={type(exc).__name__}: {exc} succeeded={succeeded} skipped={skipped} failed={failed}",
            )
    store.finish_batch(batch_id, succeeded=succeeded, skipped=skipped, failed=failed)
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
        "detected_standalone": detected_standalone,
        "detected_consolidated": detected_consolidated,
        "expected_report_date": resolved_expected_report_date or "",
    }


def _sync_symbol_with_retries(
    *,
    client: ScreenerClient,
    store: ScreenerFinancialsStore,
    symbol: str,
    statement_basis: str,
    force_download: bool,
    allow_download: bool,
    expected_report_date: str | None,
    sync_batch_id: str,
    progress: Callable[[str], None] | None,
    label: str,
) -> str:
    last_exc: Exception | None = None
    for attempt in range(1, DEFAULT_SYMBOL_ATTEMPTS + 1):
        try:
            if attempt > 1:
                _emit(progress, f"{label}: retry attempt {attempt}/{DEFAULT_SYMBOL_ATTEMPTS}")
            fetched = client.fetch_company_data(
                symbol,
                statement_basis=statement_basis,
                force_download=force_download,
                allow_download=allow_download,
            )
            if expected_report_date is not None and not _has_quarterly_report_date(fetched.data, expected_report_date):
                store.record_sync_result(
                    sync_batch_id,
                    symbol,
                    requested_basis=fetched.requested_basis,
                    detected_basis=fetched.detected_basis,
                    export_path=fetched.export_path,
                    status="skipped",
                )
                raise MissingExpectedQuarterError(
                    f"expected quarterly report_date={expected_report_date} not found in Screener export"
                )
            store.save_company_financials(
                symbol,
                fetched.data,
                statement_basis=fetched.detected_basis,
                sync_batch_id=sync_batch_id,
            )
            store.record_sync_result(
                sync_batch_id,
                symbol,
                requested_basis=fetched.requested_basis,
                detected_basis=fetched.detected_basis,
                export_path=fetched.export_path,
                status="succeeded",
            )
            return fetched.detected_basis
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
            if allow_download:
                if isinstance(exc, ScreenerRateLimitError) and exc.retry_after is not None:
                    delay = min(60.0, float(exc.retry_after))
                else:
                    delay = min(60.0, DEFAULT_RETRY_BACKOFF_SEC * (2 ** (attempt - 1)))
                if delay > 0:
                    time.sleep(delay)
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
        "--statement-basis",
        default=BOTH_STATEMENT_BASES,
        choices=(*SUPPORTED_STATEMENT_BASES, BOTH_STATEMENT_BASES),
        help="Statement basis to request; 'both' runs standalone then consolidated with one final readmodel refresh.",
    )
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
    parser.add_argument(
        "--missing-results-retry-cooldown-hours",
        type=float,
        default=DEFAULT_MISSING_RESULTS_RETRY_COOLDOWN_HOURS,
        help=(
            "Do not redownload a symbol whose same-basis export was recently checked and still lacked "
            "the expected quarter; set to 0 to disable the cooldown."
        ),
    )
    parser.add_argument("--symbol", action="append", dest="symbols", help="Limit sync to one symbol; repeatable.")
    parser.add_argument("--symbols-file", default=None, help="Text file of symbols to sync, one per line.")
    parser.add_argument("--throttle-sec", type=float, default=2.0)
    parser.add_argument("--no-refresh-readmodels", action="store_true")
    parser.add_argument(
        "--statement-basis-migration-backup-dir",
        "--valuation-migration-backup-dir",
        dest="valuation_migration_backup_dir",
        default=None,
        help="Required backup directory when upgrading legacy financial or valuation tables to basis-aware keys.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        result = run_unified_sync(
            statement_basis=args.statement_basis,
            limit=args.limit,
            force=args.force,
            db_path=args.db_path,
            master_db_path=args.master_db_path,
            exports_dir=args.exports_dir,
            allow_download=args.allow_download,
            throttle_sec=args.throttle_sec,
            refresh_readmodels=not args.no_refresh_readmodels,
            missing_current_results=args.missing_current_results,
            missing_results_retry_cooldown_hours=args.missing_results_retry_cooldown_hours,
            as_of_date=args.as_of_date,
            expected_report_date=args.expected_report_date,
            symbols=_requested_symbols(args.symbols, args.symbols_file),
            progress=lambda message: print(message, flush=True),
            valuation_migration_backup_dir=args.valuation_migration_backup_dir,
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
        return sorted({_symbol_from_export_path(path) for path in exports_dir.glob("*_screener.xlsx")})
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


def _load_explicit_master_tickers(master_db_path: Path) -> set[str]:
    """Allow explicitly requested mastered BSE-only tickers without expanding default sync scope."""
    if not master_db_path.exists():
        return set()
    conn = sqlite3.connect(f"file:{master_db_path}?mode=ro", uri=True)
    try:
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(symbols)").fetchall()
        }
        identifiers = [column for column in ("symbol_id", "nse_symbol", "bse_symbol") if column in columns]
        if not identifiers:
            return set()
        select_list = ", ".join(identifiers)
        rows = conn.execute(f"SELECT {select_list} FROM symbols").fetchall()
    finally:
        conn.close()
    return {
        str(value).strip().upper()
        for row in rows
        for value in row
        if value is not None and str(value).strip()
    }


def _requested_symbols(symbols: list[str] | None, symbols_file: str | Path | None) -> list[str] | None:
    values: list[str] = []
    if symbols:
        values.extend(symbols)
    if symbols_file:
        values.extend(Path(symbols_file).read_text(encoding="utf-8").splitlines())
    normalized = _normalize_symbols(values)
    return normalized or None


def _normalize_symbols(symbols: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        clean = str(symbol).strip().upper()
        if not clean or clean.startswith("#") or clean in seen:
            continue
        normalized.append(clean)
        seen.add(clean)
    return normalized


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
    statement_basis: str,
    retry_cooldown_hours: float = 0.0,
) -> list[str]:
    return _select_symbols_missing_quarterly_report_date(
        db_path,
        symbols,
        report_date=report_date,
        statement_basis=statement_basis,
        retry_cooldown_hours=retry_cooldown_hours,
    ).symbols


def _select_symbols_missing_quarterly_report_date(
    db_path: Path,
    symbols: list[str],
    *,
    report_date: str,
    statement_basis: str,
    retry_cooldown_hours: float,
) -> MissingQuarterSelection:
    if not symbols:
        return MissingQuarterSelection([], 0, 0, 0)
    basis = normalize_statement_basis(statement_basis)
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT upper(trim(symbol)) AS symbol
            FROM screener_financials
            WHERE lower(trim(period_type)) = 'quarterly'
              AND date(report_date) = date(?)
              AND lower(trim(statement_basis)) = ?
            """,
            (report_date, basis),
        ).fetchall()
        terminal_fallback_rows = []
        if basis == "consolidated":
            terminal_fallback_rows = conn.execute(
                """
                SELECT DISTINCT upper(trim(symbol)) AS symbol
                FROM screener_sync_result
                WHERE requested_basis = 'consolidated'
                  AND detected_basis = 'standalone'
                  AND status = 'succeeded'
                """
            ).fetchall()
        cooldown_rows = []
        if float(retry_cooldown_hours) > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=float(retry_cooldown_hours))
            cooldown_rows = conn.execute(
                """
                SELECT DISTINCT upper(trim(r.symbol)) AS symbol
                FROM screener_sync_result r
                JOIN screener_sync_batch b ON b.sync_batch_id = r.sync_batch_id
                WHERE r.requested_basis = ?
                  AND r.status = 'skipped'
                  AND b.missing_current_results = 1
                  AND date(b.expected_report_date) = date(?)
                  AND datetime(r.created_at) >= datetime(?)
                """,
                (basis, report_date, cutoff.isoformat()),
            ).fetchall()
    finally:
        conn.close()
    present = {str(row[0]).upper().strip() for row in rows if str(row[0]).strip()}
    terminal_fallback = {
        str(row[0]).upper().strip() for row in terminal_fallback_rows if str(row[0]).strip()
    }
    retry_cooldown = {str(row[0]).upper().strip() for row in cooldown_rows if str(row[0]).strip()}
    universe = {symbol.upper().strip() for symbol in symbols}
    return MissingQuarterSelection(
        symbols=[
            symbol
            for symbol in symbols
            if symbol.upper().strip() not in present
            and symbol.upper().strip() not in terminal_fallback
            and symbol.upper().strip() not in retry_cooldown
        ],
        already_present=len(universe & present),
        terminal_standalone_fallback=len((universe - present) & terminal_fallback),
        retry_cooldown=len((universe - present - terminal_fallback) & retry_cooldown),
    )


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


def _symbol_from_export_path(path: Path) -> str:
    stem = path.name.removesuffix("_screener.xlsx")
    return stem.removesuffix("_consolidated").upper()


def _emit(progress: Callable[[str], None] | None, message: str) -> None:
    if progress is not None:
        progress(message)


__all__ = ["expected_quarterly_report_date", "run_sync", "run_unified_sync"]


if __name__ == "__main__":
    main()
