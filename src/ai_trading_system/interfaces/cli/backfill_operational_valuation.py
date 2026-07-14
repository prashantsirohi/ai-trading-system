"""Backfill operational OHLCV and valuation features from research history."""

from __future__ import annotations

import argparse
import json
import shutil
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable

import duckdb

from ai_trading_system.domains.features.valuation_cycle import refresh_valuation_cycle_features
from ai_trading_system.domains.features.valuation_index import DEFAULT_UNIVERSES, refresh_valuation_index
from ai_trading_system.domains.features.valuation_ttm import refresh_fundamental_ttm
from ai_trading_system.domains.fundamentals.screener_store import default_screener_db_path
from ai_trading_system.domains.ingest.price_continuity import (
    DEFAULT_BULK_RAW_GAP_PCT,
    DEFAULT_BULK_RAW_GAP_SYMBOL_COUNT,
    BulkRawPriceBasisShift,
    BulkRawPriceBasisShiftError,
)
from ai_trading_system.platform.db.paths import get_domain_paths


DEFAULT_COPY_FROM_DATE = "2006-01-01"
DEFAULT_VALUATION_FROM_DATE = "2016-01-01"


@dataclass(frozen=True)
class CoverageSummary:
    source_min_date: str | None
    source_max_date: str | None
    source_rows: int
    source_symbols: int
    target_min_date: str | None
    target_max_date: str | None
    target_rows: int
    missing_rows: int
    source_rows_by_year: dict[int, int]
    missing_rows_by_year: dict[int, int]


@dataclass(frozen=True)
class CopyChunkResult:
    from_date: str
    to_date: str
    source_rows: int
    missing_rows: int
    inserted_rows: int
    dry_run: bool


@dataclass(frozen=True)
class ValidationTableSummary:
    table_name: str
    date_column: str
    min_date: str | None
    max_date: str | None
    rows: int


def inspect_ohlcv_coverage(
    *,
    source_db_path: str | Path,
    target_db_path: str | Path,
    from_date: str = DEFAULT_COPY_FROM_DATE,
    to_date: str | None = None,
) -> CoverageSummary:
    """Return source/target coverage and missing target rows for NSE OHLCV."""

    conn = duckdb.connect(str(target_db_path), read_only=True)
    try:
        _attach_source(conn, source_db_path)
        params = _range_params(from_date=from_date, to_date=to_date)
        source_stats = conn.execute(
            f"""
            SELECT
                MIN(trade_date),
                MAX(trade_date),
                COUNT(*),
                COUNT(DISTINCT symbol_id)
            FROM ({_source_daily_sql()})
            WHERE trade_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            """,
            params,
        ).fetchone()
        target_stats = conn.execute(
            """
            SELECT
                MIN(CAST(timestamp AS DATE)),
                MAX(CAST(timestamp AS DATE)),
                COUNT(*)
            FROM _catalog
            WHERE exchange = 'NSE'
            """
        ).fetchone()
        missing_rows = int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM ({_missing_source_daily_sql()})
                WHERE trade_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                """,
                params,
            ).fetchone()[0]
            or 0
        )
        source_by_year = _rows_by_year(
            conn,
            f"""
            SELECT trade_date
            FROM ({_source_daily_sql()})
            WHERE trade_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            """,
            params,
        )
        missing_by_year = _rows_by_year(
            conn,
            f"""
            SELECT trade_date
            FROM ({_missing_source_daily_sql()})
            WHERE trade_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            """,
            params,
        )
    finally:
        conn.close()

    return CoverageSummary(
        source_min_date=_date_or_none(source_stats[0]),
        source_max_date=_date_or_none(source_stats[1]),
        source_rows=int(source_stats[2] or 0),
        source_symbols=int(source_stats[3] or 0),
        target_min_date=_date_or_none(target_stats[0]),
        target_max_date=_date_or_none(target_stats[1]),
        target_rows=int(target_stats[2] or 0),
        missing_rows=missing_rows,
        source_rows_by_year=source_by_year,
        missing_rows_by_year=missing_by_year,
    )


def copy_ohlcv_chunk(
    *,
    source_db_path: str | Path,
    target_db_path: str | Path,
    from_date: str,
    to_date: str,
    dry_run: bool = False,
    run_id: str | None = None,
) -> CopyChunkResult:
    """Copy missing NSE OHLCV rows for one date chunk from research to operational."""

    conn = duckdb.connect(str(target_db_path), read_only=dry_run)
    try:
        _attach_source(conn, source_db_path)
        params = [from_date, to_date]
        source_rows = int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM ({_source_daily_sql()})
                WHERE trade_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                """,
                params,
            ).fetchone()[0]
            or 0
        )
        missing_rows = int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM ({_missing_source_daily_sql()})
                WHERE trade_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                """,
                params,
            ).fetchone()[0]
            or 0
        )
        if dry_run or missing_rows == 0:
            return CopyChunkResult(from_date, to_date, source_rows, missing_rows, 0, dry_run)

        shifts = _candidate_bulk_raw_price_basis_shifts(
            conn,
            from_date=from_date,
            to_date=to_date,
        )
        if shifts:
            raise BulkRawPriceBasisShiftError(
                f"Operational OHLCV backfill {from_date}..{to_date}",
                shifts,
            )

        target_columns = _table_columns(conn, "_catalog")
        select_list = _catalog_select_list(target_columns, run_id=run_id or _default_run_id())
        before = int(conn.execute("SELECT COUNT(*) FROM _catalog").fetchone()[0] or 0)
        conn.execute(
            f"""
            INSERT INTO _catalog ({", ".join(target_columns)})
            SELECT {select_list}
            FROM ({_missing_source_daily_sql()}) AS s
            WHERE s.trade_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            """,
            params,
        )
        after = int(conn.execute("SELECT COUNT(*) FROM _catalog").fetchone()[0] or 0)
    finally:
        conn.close()

    return CopyChunkResult(from_date, to_date, source_rows, missing_rows, after - before, dry_run)


def copy_ohlcv_backfill(
    *,
    source_db_path: str | Path,
    target_db_path: str | Path,
    from_date: str = DEFAULT_COPY_FROM_DATE,
    to_date: str | None = None,
    dry_run: bool = False,
    run_id: str | None = None,
) -> list[CopyChunkResult]:
    """Copy missing OHLCV rows in yearly chunks."""

    end = to_date or _source_max_date(source_db_path)
    return [
        copy_ohlcv_chunk(
            source_db_path=source_db_path,
            target_db_path=target_db_path,
            from_date=start,
            to_date=stop,
            dry_run=dry_run,
            run_id=run_id,
        )
        for start, stop in _year_chunks(from_date, end)
    ]


def run_valuation_backfill(
    *,
    ohlcv_db_path: str | Path,
    screener_db_path: str | Path,
    master_db_path: str | Path,
    from_date: str = DEFAULT_VALUATION_FROM_DATE,
    to_date: str | None = None,
    universes: Iterable[str] = DEFAULT_UNIVERSES,
    min_history_days: int = 756,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Refresh point-in-time valuation tables by year, then cycle features once."""

    end = to_date or _target_max_date(ohlcv_db_path)
    universe_ids = [str(item).strip().upper() for item in universes if str(item).strip()]
    if dry_run:
        return {
            "status": "dry_run",
            "from_date": from_date,
            "to_date": end,
            "chunks": [{"from_date": start, "to_date": stop} for start, stop in _year_chunks(from_date, end)],
            "universes": universe_ids,
        }

    chunks: list[dict[str, Any]] = []
    for start, stop in _year_chunks(from_date, end):
        ttm = refresh_fundamental_ttm(
            ohlcv_db_path=ohlcv_db_path,
            screener_db_path=screener_db_path,
            from_date=start,
            to_date=stop,
        )
        valuation = refresh_valuation_index(
            ohlcv_db_path=ohlcv_db_path,
            master_db_path=master_db_path,
            universes=universe_ids,
            from_date=start,
            to_date=stop,
        )
        chunks.append(
            {
                "from_date": start,
                "to_date": stop,
                "ttm": asdict(ttm),
                "valuation": asdict(valuation),
            }
        )
    cycle = refresh_valuation_cycle_features(
        ohlcv_db_path=ohlcv_db_path,
        from_date=from_date,
        to_date=end,
        min_history_days=min_history_days,
    )
    return {
        "status": "completed",
        "from_date": from_date,
        "to_date": end,
        "universes": universe_ids,
        "chunks": chunks,
        "cycle": asdict(cycle),
    }


def backup_operational_files(
    *,
    ohlcv_db_path: str | Path,
    master_db_path: str | Path,
    screener_db_path: str | Path,
    backup_dir: str | Path,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Copy the operational DB files to a timestamped backup directory."""

    resolved_backup_dir = Path(backup_dir)
    sources = [Path(ohlcv_db_path), Path(master_db_path), Path(screener_db_path)]
    planned = [{"source": str(path), "target": str(resolved_backup_dir / path.name)} for path in sources]
    if dry_run:
        return {"status": "dry_run", "backup_dir": str(resolved_backup_dir), "files": planned}
    resolved_backup_dir.mkdir(parents=True, exist_ok=True)
    copied = []
    for path in sources:
        if not path.exists():
            copied.append({"source": str(path), "status": "missing"})
            continue
        target = resolved_backup_dir / path.name
        shutil.copy2(path, target)
        copied.append({"source": str(path), "target": str(target), "status": "copied"})
    return {"status": "completed", "backup_dir": str(resolved_backup_dir), "files": copied}


def validation_summary(ohlcv_db_path: str | Path) -> list[ValidationTableSummary]:
    """Return min/max/count summaries for OHLCV and valuation tables."""

    targets = [
        ("_catalog", "timestamp"),
        ("fundamental_ttm", "as_of_date"),
        ("stock_valuation_daily", "date"),
        ("universe_index_daily", "date"),
        ("sector_valuation_daily", "date"),
        ("valuation_cycle_features", "date"),
    ]
    conn = duckdb.connect(str(ohlcv_db_path), read_only=True)
    try:
        rows = []
        for table_name, date_column in targets:
            if not _table_exists(conn, table_name):
                rows.append(ValidationTableSummary(table_name, date_column, None, None, 0))
                continue
            min_date, max_date, count = conn.execute(
                f"""
                SELECT MIN(CAST({date_column} AS DATE)), MAX(CAST({date_column} AS DATE)), COUNT(*)
                FROM {table_name}
                """
            ).fetchone()
            rows.append(
                ValidationTableSummary(
                    table_name=table_name,
                    date_column=date_column,
                    min_date=_date_or_none(min_date),
                    max_date=_date_or_none(max_date),
                    rows=int(count or 0),
                )
            )
        return rows
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    paths = get_domain_paths(data_domain="operational")
    research_paths = get_domain_paths(data_domain="research")
    parser = argparse.ArgumentParser(description="Backfill operational valuation history from research OHLCV.")
    parser.add_argument("--source-db-path", default=str(research_paths.ohlcv_db_path))
    parser.add_argument("--target-db-path", default=str(paths.ohlcv_db_path))
    parser.add_argument("--master-db-path", default=str(paths.master_db_path))
    parser.add_argument("--screener-db-path", default=str(default_screener_db_path()))
    parser.add_argument("--copy-from-date", default=DEFAULT_COPY_FROM_DATE)
    parser.add_argument("--valuation-from-date", default=DEFAULT_VALUATION_FROM_DATE)
    parser.add_argument("--to-date", default=None)
    parser.add_argument("--universe-id", action="append", default=None)
    parser.add_argument("--valuation-min-history-days", type=int, default=756)
    parser.add_argument("--skip-copy", action="store_true")
    parser.add_argument("--skip-valuation", action="store_true")
    parser.add_argument("--skip-backup", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--report-json", default=None)
    parser.add_argument("--backup-dir", default=None)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_id = _default_run_id()
    backup_dir = Path(args.backup_dir) if args.backup_dir else _default_backup_dir(Path(args.target_db_path), run_id)
    universes = args.universe_id or list(DEFAULT_UNIVERSES)

    report: dict[str, Any] = {
        "run_id": run_id,
        "dry_run": bool(args.dry_run),
        "source_db_path": args.source_db_path,
        "target_db_path": args.target_db_path,
        "coverage_before": asdict(
            inspect_ohlcv_coverage(
                source_db_path=args.source_db_path,
                target_db_path=args.target_db_path,
                from_date=args.copy_from_date,
                to_date=args.to_date,
            )
        ),
    }
    if not args.skip_backup:
        report["backup"] = backup_operational_files(
            ohlcv_db_path=args.target_db_path,
            master_db_path=args.master_db_path,
            screener_db_path=args.screener_db_path,
            backup_dir=backup_dir,
            dry_run=args.dry_run,
        )
    if not args.skip_copy:
        report["copy_chunks"] = [
            asdict(item)
            for item in copy_ohlcv_backfill(
                source_db_path=args.source_db_path,
                target_db_path=args.target_db_path,
                from_date=args.copy_from_date,
                to_date=args.to_date,
                dry_run=args.dry_run,
                run_id=run_id,
            )
        ]
    if not args.skip_valuation:
        report["valuation_backfill"] = run_valuation_backfill(
            ohlcv_db_path=args.target_db_path,
            screener_db_path=args.screener_db_path,
            master_db_path=args.master_db_path,
            from_date=args.valuation_from_date,
            to_date=args.to_date,
            universes=universes,
            min_history_days=args.valuation_min_history_days,
            dry_run=args.dry_run,
        )
    report["validation"] = [asdict(item) for item in validation_summary(args.target_db_path)]

    payload = json.dumps(report, indent=2, sort_keys=True)
    if args.report_json:
        Path(args.report_json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report_json).write_text(payload + "\n", encoding="utf-8")
    print(payload)


def _attach_source(conn: duckdb.DuckDBPyConnection, source_db_path: str | Path) -> None:
    escaped = str(Path(source_db_path)).replace("'", "''")
    conn.execute(f"ATTACH '{escaped}' AS src (READ_ONLY)")


def _source_daily_sql() -> str:
    return """
        WITH normalized_source AS (
            SELECT
                symbol_id,
                security_id,
                exchange,
                CASE
                    WHEN CAST(timestamp AS TIME) = TIME '18:30:00'
                    THEN timestamp + INTERVAL '5 hours 30 minutes'
                    ELSE timestamp
                END AS timestamp,
                open,
                high,
                low,
                close,
                volume,
                parquet_file,
                ingestion_version,
                ingestion_ts
            FROM src._catalog
            WHERE exchange = 'NSE'
              AND close IS NOT NULL
        )
        SELECT *
        FROM (
            SELECT
                symbol_id,
                security_id,
                exchange,
                timestamp,
                CAST(timestamp AS DATE) AS trade_date,
                open,
                high,
                low,
                close,
                volume,
                parquet_file,
                ingestion_version,
                ingestion_ts,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol_id, exchange, CAST(timestamp AS DATE)
                    ORDER BY timestamp DESC, ingestion_ts DESC NULLS LAST
                ) AS rn
            FROM normalized_source
        )
        WHERE rn = 1
    """


def _missing_source_daily_sql() -> str:
    return f"""
        SELECT s.*
        FROM ({_source_daily_sql()}) AS s
        WHERE NOT EXISTS (
            SELECT 1
            FROM _catalog AS t
            WHERE t.symbol_id = s.symbol_id
              AND t.exchange = s.exchange
              AND CAST(t.timestamp AS DATE) = s.trade_date
        )
    """


def _candidate_bulk_raw_price_basis_shifts(
    conn: duckdb.DuckDBPyConnection,
    *,
    from_date: str,
    to_date: str,
    gap_pct: float = DEFAULT_BULK_RAW_GAP_PCT,
    symbol_count: int = DEFAULT_BULK_RAW_GAP_SYMBOL_COUNT,
) -> list[BulkRawPriceBasisShift]:
    """Find broad gaps involving rows proposed by a research backfill."""

    rows = conn.execute(
        f"""
        WITH candidate_rows AS (
            SELECT symbol_id, exchange, timestamp, trade_date, close, TRUE AS is_candidate
            FROM ({_missing_source_daily_sql()})
            WHERE trade_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
        ),
        affected_symbols AS (
            SELECT DISTINCT symbol_id, exchange
            FROM candidate_rows
        ),
        target_window AS (
            SELECT
                t.symbol_id,
                t.exchange,
                t.timestamp,
                CAST(t.timestamp AS DATE) AS trade_date,
                t.close,
                FALSE AS is_candidate
            FROM _catalog t
            INNER JOIN affected_symbols a
                    ON a.symbol_id = t.symbol_id
                   AND a.exchange = t.exchange
            WHERE CAST(t.timestamp AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
        ),
        target_before AS (
            SELECT symbol_id, exchange, timestamp, trade_date, close, is_candidate
            FROM (
                SELECT
                    t.symbol_id,
                    t.exchange,
                    t.timestamp,
                    CAST(t.timestamp AS DATE) AS trade_date,
                    t.close,
                    FALSE AS is_candidate,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.symbol_id, t.exchange
                        ORDER BY t.timestamp DESC
                    ) AS row_number
                FROM _catalog t
                INNER JOIN affected_symbols a
                        ON a.symbol_id = t.symbol_id
                       AND a.exchange = t.exchange
                WHERE CAST(t.timestamp AS DATE) < CAST(? AS DATE)
            )
            WHERE row_number = 1
        ),
        target_after AS (
            SELECT symbol_id, exchange, timestamp, trade_date, close, is_candidate
            FROM (
                SELECT
                    t.symbol_id,
                    t.exchange,
                    t.timestamp,
                    CAST(t.timestamp AS DATE) AS trade_date,
                    t.close,
                    FALSE AS is_candidate,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.symbol_id, t.exchange
                        ORDER BY t.timestamp ASC
                    ) AS row_number
                FROM _catalog t
                INNER JOIN affected_symbols a
                        ON a.symbol_id = t.symbol_id
                       AND a.exchange = t.exchange
                WHERE CAST(t.timestamp AS DATE) > CAST(? AS DATE)
            )
            WHERE row_number = 1
        ),
        projected AS (
            SELECT * FROM candidate_rows
            UNION ALL
            SELECT * FROM target_window
            UNION ALL
            SELECT * FROM target_before
            UNION ALL
            SELECT * FROM target_after
        ),
        ordered AS (
            SELECT
                symbol_id,
                trade_date,
                close,
                is_candidate,
                LAG(close) OVER (
                    PARTITION BY symbol_id, exchange
                    ORDER BY timestamp
                ) AS prev_close,
                LAG(is_candidate) OVER (
                    PARTITION BY symbol_id, exchange
                    ORDER BY timestamp
                ) AS prev_is_candidate
            FROM projected
        )
        SELECT
            trade_date,
            symbol_id,
            ABS(((close / NULLIF(prev_close, 0)) - 1) * 100.0) AS abs_pct_change
        FROM ordered
        WHERE prev_close IS NOT NULL
          AND close IS NOT NULL
          AND (is_candidate OR COALESCE(prev_is_candidate, FALSE))
          AND ABS(((close / NULLIF(prev_close, 0)) - 1) * 100.0) >= ?
        ORDER BY trade_date, symbol_id
        """,
        [from_date, to_date, from_date, to_date, from_date, to_date, gap_pct],
    ).fetchall()

    by_date: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for trade_date, symbol_id, abs_pct_change in rows:
        by_date[str(trade_date)].append((str(symbol_id), float(abs_pct_change)))

    shifts: list[BulkRawPriceBasisShift] = []
    for trade_date, date_rows in sorted(by_date.items()):
        symbol_changes = {symbol: change for symbol, change in date_rows}
        if len(symbol_changes) < int(symbol_count):
            continue
        changes = list(symbol_changes.values())
        shifts.append(
            BulkRawPriceBasisShift(
                trade_date=trade_date,
                symbols=tuple(sorted(symbol_changes)),
                median_abs_pct_change=round(float(median(changes)), 4),
                max_abs_pct_change=round(float(max(changes)), 4),
            )
        )
    return shifts


def _catalog_select_list(target_columns: list[str], *, run_id: str) -> str:
    expressions = {
        "symbol_id": "s.symbol_id",
        "security_id": "s.security_id",
        "exchange": "s.exchange",
        "timestamp": "s.timestamp",
        "open": "s.open",
        "high": "s.high",
        "low": "s.low",
        "close": "s.close",
        "volume": "s.volume",
        "parquet_file": "s.parquet_file",
        "ingestion_version": "COALESCE(s.ingestion_version, 0)",
        "ingestion_ts": "CURRENT_TIMESTAMP",
        "provider": "'research_ohlcv_backfill'",
        "provider_priority": "90",
        "validation_status": "'research_backfill'",
        "validated_against": "'research_ohlcv'",
        "ingest_run_id": f"'{run_id}'",
        "repair_batch_id": f"'{run_id}'",
        "provider_confidence": "1.0",
        "provider_discrepancy_flag": "FALSE",
        "provider_discrepancy_note": "NULL",
        "adjusted_open": "NULL",
        "adjusted_high": "NULL",
        "adjusted_low": "NULL",
        "adjusted_close": "NULL",
        "adjustment_factor": "1.0",
        "adjustment_source": "'research_unadjusted'",
        "instrument_type": "'equity'",
        "is_benchmark": "FALSE",
        "benchmark_label": "NULL",
        "isin": "NULL",
        "series": "NULL",
        "trading_segment": "'EQ'",
    }
    return ", ".join(f"{expressions.get(column, 'NULL')} AS {column}" for column in target_columns)


def _rows_by_year(conn: duckdb.DuckDBPyConnection, sql: str, params: list[str]) -> dict[int, int]:
    rows = conn.execute(
        f"""
        SELECT EXTRACT(year FROM trade_date)::INTEGER AS year, COUNT(*) AS rows
        FROM ({sql})
        GROUP BY year
        ORDER BY year
        """,
        params,
    ).fetchall()
    return {int(year): int(count) for year, count in rows}


def _range_params(*, from_date: str, to_date: str | None) -> list[str]:
    return [from_date, to_date or "9999-12-31"]


def _year_chunks(from_date: str, to_date: str) -> list[tuple[str, str]]:
    start = date.fromisoformat(from_date[:10])
    end = date.fromisoformat(to_date[:10])
    chunks = []
    current = start
    while current <= end:
        stop = min(date(current.year, 12, 31), end)
        chunks.append((current.isoformat(), stop.isoformat()))
        current = date(current.year + 1, 1, 1)
    return chunks


def _source_max_date(source_db_path: str | Path) -> str:
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        value = conn.execute("SELECT MAX(CAST(timestamp AS DATE)) FROM _catalog WHERE exchange = 'NSE'").fetchone()[0]
    finally:
        conn.close()
    if value is None:
        raise RuntimeError(f"No NSE source rows found in {source_db_path}")
    return str(value)[:10]


def _target_max_date(target_db_path: str | Path) -> str:
    conn = duckdb.connect(str(target_db_path), read_only=True)
    try:
        value = conn.execute("SELECT MAX(CAST(timestamp AS DATE)) FROM _catalog WHERE exchange = 'NSE'").fetchone()[0]
    finally:
        conn.close()
    if value is None:
        raise RuntimeError(f"No NSE target rows found in {target_db_path}")
    return str(value)[:10]


def _table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    columns = [row[1] for row in conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()]
    if not columns:
        raise RuntimeError(f"Table not found or has no columns: {table_name}")
    return columns


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return bool(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = ?
            """,
            [table_name],
        ).fetchone()[0]
    )


def _date_or_none(value: Any) -> str | None:
    return None if value is None else str(value)[:10]


def _default_run_id() -> str:
    return f"operational_valuation_backfill_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def _default_backup_dir(target_db_path: Path, run_id: str) -> Path:
    return target_db_path.parent / "backups" / run_id


if __name__ == "__main__":
    main()
