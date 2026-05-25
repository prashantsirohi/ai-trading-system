"""Sync Screener Excel exports into the canonical SQLite fundamentals DB."""

from __future__ import annotations

import argparse
import sqlite3
import time
import uuid
from pathlib import Path

from ai_trading_system.domains.fundamentals.screener_client import ScreenerClient
from ai_trading_system.domains.fundamentals.screener_readmodels import refresh_fundamental_readmodels
from ai_trading_system.domains.fundamentals.screener_store import ScreenerFinancialsStore, default_screener_db_path
from ai_trading_system.platform.db.paths import get_domain_paths


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
) -> dict[str, int | str]:
    paths = get_domain_paths()
    resolved_db_path = Path(db_path) if db_path is not None else default_screener_db_path()
    resolved_exports_dir = Path(exports_dir) if exports_dir is not None else paths.fundamentals_dir / "exports"
    store = ScreenerFinancialsStore(resolved_db_path)
    client = ScreenerClient(exports_dir=resolved_exports_dir)
    all_symbols = _load_symbols(
        Path(master_db_path) if master_db_path is not None else paths.master_db_path,
        exports_dir=resolved_exports_dir,
    )
    synced = set() if force else store.get_synced_symbols()
    symbols = [symbol for symbol in all_symbols if symbol not in synced]
    if limit is not None:
        symbols = symbols[: int(limit)]
    batch_id = f"screener-{uuid.uuid4().hex[:10]}"
    store.begin_batch(batch_id, symbols_total=len(symbols), exports_dir=resolved_exports_dir, force=force)
    succeeded = 0
    failed = 0
    for index, symbol in enumerate(symbols):
        try:
            if index > 0 and throttle_sec > 0 and allow_download:
                time.sleep(float(throttle_sec))
            data = client.fetch_company_data(symbol, force_download=force, allow_download=allow_download)
            store.save_company_financials(symbol, data, sync_batch_id=batch_id)
            succeeded += 1
        except Exception as exc:  # noqa: BLE001
            failed += 1
            store.record_error(batch_id, symbol, str(exc))
    store.finish_batch(batch_id, succeeded=succeeded, failed=failed)
    if refresh_readmodels and succeeded:
        refresh_fundamental_readmodels(db_path=resolved_db_path)
    return {"sync_batch_id": batch_id, "total": len(symbols), "succeeded": succeeded, "failed": failed}


def build_parser() -> argparse.ArgumentParser:
    paths = get_domain_paths()
    parser = argparse.ArgumentParser(description="Sync Screener Excel exports into screener_financials.db.")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--db-path", default=str(default_screener_db_path()))
    parser.add_argument("--master-db-path", default=str(paths.master_db_path))
    parser.add_argument("--exports-dir", default=str(paths.fundamentals_dir / "exports"))
    parser.add_argument("--allow-download", action="store_true", help="Download missing/stale Excel files from Screener.in")
    parser.add_argument("--throttle-sec", type=float, default=2.0)
    parser.add_argument("--no-refresh-readmodels", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = run_sync(
        limit=args.limit,
        force=args.force,
        db_path=args.db_path,
        master_db_path=args.master_db_path,
        exports_dir=args.exports_dir,
        allow_download=args.allow_download,
        throttle_sec=args.throttle_sec,
        refresh_readmodels=not args.no_refresh_readmodels,
    )
    print(
        f"sync_batch_id={result['sync_batch_id']} total={result['total']} "
        f"succeeded={result['succeeded']} failed={result['failed']}"
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


__all__ = ["run_sync"]


if __name__ == "__main__":
    main()
