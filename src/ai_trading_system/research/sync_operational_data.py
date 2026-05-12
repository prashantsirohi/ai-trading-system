"""Sync operational OHLCV rows into the research data domain.

The operational store is the cleaned production truth. The research store is a
separate sandbox used by research backtests and training workflows.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from ai_trading_system.platform.db.paths import ensure_domain_layout


CATALOG_COLUMNS = (
    "symbol_id",
    "security_id",
    "exchange",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "parquet_file",
    "ingestion_version",
    "ingestion_ts",
)


def sync_operational_to_research(
    *,
    project_root: Path | str | None = None,
    exchange: str = "NSE",
    apply: bool = False,
) -> dict[str, Any]:
    root = Path(project_root).resolve() if project_root else None
    operational = ensure_domain_layout(project_root=root, data_domain="operational")
    research = ensure_domain_layout(project_root=root, data_domain="research")

    conn = duckdb.connect(str(research.ohlcv_db_path))
    try:
        op_path = str(operational.ohlcv_db_path).replace("'", "''")
        conn.execute(f"ATTACH '{op_path}' AS op (READ_ONLY)")
        where = ["exchange = ?"]
        params: list[Any] = [exchange]
        where_sql = " AND ".join(where)

        source_stats = conn.execute(
            f"""
            SELECT
                COUNT(*),
                MIN(CAST(timestamp AS DATE)),
                MAX(CAST(timestamp AS DATE))
            FROM op._catalog
            WHERE {where_sql}
            """,
            params,
        ).fetchone()
        source_count = int(source_stats[0] or 0)
        source_from = source_stats[1]
        source_to = source_stats[2]

        if source_from is None or source_to is None:
            return {
                "status": "dry_run" if not apply else "no_source_data",
                "source": str(operational.ohlcv_db_path),
                "target": str(research.ohlcv_db_path),
                "exchange": exchange,
                "source_from_date": None,
                "source_to_date": None,
                "rows_to_copy": 0,
            }

        refresh_where = [
            "exchange = ?",
            "CAST(timestamp AS DATE) >= CAST(? AS DATE)",
            "CAST(timestamp AS DATE) <= CAST(? AS DATE)",
        ]
        refresh_params: list[Any] = [exchange, source_from, source_to]
        refresh_where_sql = " AND ".join(refresh_where)

        if not apply:
            return {
                "status": "dry_run",
                "source": str(operational.ohlcv_db_path),
                "target": str(research.ohlcv_db_path),
                "exchange": exchange,
                "source_from_date": str(source_from) if source_from else None,
                "source_to_date": str(source_to) if source_to else None,
                "rows_to_copy": source_count,
                "refresh_mode": "replace_research_rows_inside_operational_date_range",
            }

        conn.execute(f"DELETE FROM _catalog WHERE {refresh_where_sql}", refresh_params)
        column_csv = ", ".join(CATALOG_COLUMNS)
        select_csv = ", ".join(
            [
                "symbol_id",
                "security_id",
                "exchange",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "parquet_file",
                "COALESCE(ingestion_version, 0)",
                "COALESCE(ingestion_ts, CURRENT_TIMESTAMP)",
            ]
        )
        inserted = conn.execute(
            f"""
            INSERT INTO _catalog ({column_csv})
            SELECT {select_csv}
            FROM op._catalog
            WHERE {where_sql}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY symbol_id, exchange, timestamp
                ORDER BY ingestion_ts DESC NULLS LAST
            ) = 1
            """,
            params,
        ).rowcount

        target_count = conn.execute(
            f"SELECT COUNT(*) FROM _catalog WHERE {refresh_where_sql}",
            refresh_params,
        ).fetchone()[0]
        total_target_count = conn.execute(
            "SELECT COUNT(*) FROM _catalog WHERE exchange = ?",
            [exchange],
        ).fetchone()[0]
        return {
            "status": "applied",
            "source": str(operational.ohlcv_db_path),
            "target": str(research.ohlcv_db_path),
            "exchange": exchange,
            "source_from_date": str(source_from) if source_from else None,
            "source_to_date": str(source_to) if source_to else None,
            "refresh_mode": "replace_research_rows_inside_operational_date_range",
            "source_rows": source_count,
            "target_rows_in_source_range": int(target_count or 0),
            "total_target_rows": int(total_target_count or 0),
            "inserted_rows": int(inserted if inserted is not None and inserted >= 0 else target_count or 0),
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }
    finally:
        conn.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync operational OHLCV into the research DB")
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--apply", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = sync_operational_to_research(
        exchange=args.exchange,
        apply=args.apply,
    )
    import json

    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
