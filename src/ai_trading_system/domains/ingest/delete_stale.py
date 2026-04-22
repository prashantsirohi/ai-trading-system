"""Delete stale symbols from catalog for controlled re-ingestion."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from ai_trading_system.platform.db.paths import ensure_domain_layout


def find_stale_symbols(conn: duckdb.DuckDBPyConnection, *, cutoff_date: str, min_rows: int = 300) -> list[tuple[str, int, str]]:
    return conn.execute(
        """
        SELECT
            symbol_id,
            COUNT(*) AS rows,
            MIN(CAST(timestamp AS DATE))::TEXT AS first_date
        FROM _catalog
        GROUP BY symbol_id
        HAVING MAX(CAST(timestamp AS DATE))::TEXT <= ?
            OR COUNT(*) < ?
        ORDER BY symbol_id
        """,
        [cutoff_date, int(min_rows)],
    ).fetchall()


def run_delete_stale(
    *,
    project_root: Path,
    data_domain: str = "operational",
    stale_days: int = 400,
    min_rows: int = 300,
    apply: bool = False,
) -> dict[str, Any]:
    paths = ensure_domain_layout(project_root=project_root, data_domain=data_domain)
    cutoff = (datetime.now() - timedelta(days=max(1, int(stale_days)))).strftime("%Y-%m-%d")
    conn = duckdb.connect(str(paths.ohlcv_db_path))
    try:
        stale = find_stale_symbols(conn, cutoff_date=cutoff, min_rows=min_rows)
        stale_symbols = [row[0] for row in stale]
        deleted_rows = 0
        if apply and stale_symbols:
            placeholders = ",".join(["?"] * len(stale_symbols))
            deleted = conn.execute(
                f"DELETE FROM _catalog WHERE symbol_id IN ({placeholders}) RETURNING 1",
                stale_symbols,
            ).fetchall()
            deleted_rows = len(deleted)
    finally:
        conn.close()
    return {
        "cutoff_date": cutoff,
        "min_rows": int(min_rows),
        "stale_symbols": stale_symbols,
        "stale_count": len(stale_symbols),
        "deleted_rows": deleted_rows,
        "apply": bool(apply),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Delete stale symbols from _catalog for re-ingestion.")
    parser.add_argument("--data-domain", choices=["operational", "research"], default="operational")
    parser.add_argument("--stale-days", type=int, default=400, help="Symbols with max timestamp older than this are stale.")
    parser.add_argument("--min-rows", type=int, default=300, help="Symbols below this row count are stale.")
    parser.add_argument("--apply", action="store_true", help="Apply delete; default is dry-run.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[4]
    result = run_delete_stale(
        project_root=project_root,
        data_domain=args.data_domain,
        stale_days=int(args.stale_days),
        min_rows=int(args.min_rows),
        apply=bool(args.apply),
    )
    print(f"Stale symbols: {result['stale_count']}")
    for symbol in result["stale_symbols"][:200]:
        print(f"  {symbol}")
    if result["apply"]:
        print(f"Deleted rows: {result['deleted_rows']}")
    else:
        print("Dry run only. Re-run with --apply to delete.")


if __name__ == "__main__":
    main()
