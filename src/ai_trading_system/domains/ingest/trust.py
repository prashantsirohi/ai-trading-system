"""Shared helpers for OHLC provenance, quarantine, and trust summaries."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

from ai_trading_system.pipeline.contracts import TrustConfidenceEnvelope


CATALOG_TRUST_COLUMNS: dict[str, str] = {
    "provider": "VARCHAR",
    "provider_priority": "INTEGER",
    "validation_status": "VARCHAR",
    "validated_against": "VARCHAR",
    "ingest_run_id": "VARCHAR",
    "repair_batch_id": "VARCHAR",
    "provider_confidence": "DOUBLE",
    "provider_discrepancy_flag": "BOOLEAN",
    "provider_discrepancy_note": "VARCHAR",
    "adjusted_open": "DOUBLE",
    "adjusted_high": "DOUBLE",
    "adjusted_low": "DOUBLE",
    "adjusted_close": "DOUBLE",
    "adjustment_factor": "DOUBLE",
    "adjustment_source": "VARCHAR",
    "instrument_type": "VARCHAR",
    "is_benchmark": "BOOLEAN",
    "benchmark_label": "VARCHAR",
    "isin": "VARCHAR",
}

# Index catalog schemas for sectoral indices (NIFTY BANK, NIFTY AUTO, etc.)
INDEX_METADATA_COLUMNS: dict[str, str] = {
    "index_code": "VARCHAR PRIMARY KEY",
    "display_name": "VARCHAR NOT NULL",
    "family": "VARCHAR",
    "is_sectoral": "BOOLEAN DEFAULT TRUE",
    "benchmark_for": "VARCHAR",
    "source": "VARCHAR DEFAULT 'nseindia'",
    "active": "BOOLEAN DEFAULT TRUE",
    "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
}

INDEX_CATALOG_COLUMNS: dict[str, str] = {
    "index_code": "VARCHAR NOT NULL",
    "date": "DATE NOT NULL",
    "open": "DOUBLE",
    "high": "DOUBLE",
    "low": "DOUBLE",
    "close": "DOUBLE NOT NULL",
    "volume": "BIGINT",
    "value": "DOUBLE",
    "provider": "VARCHAR DEFAULT 'nseindia'",
    "ingest_run_id": "VARCHAR",
    "validated_at": "TIMESTAMP",
}

SECTOR_TO_INDEX_COLUMNS: dict[str, str] = {
    "system_sector": "VARCHAR PRIMARY KEY",
    "index_code": "VARCHAR NOT NULL",
    "index_name": "VARCHAR NOT NULL",
    "is_primary": "BOOLEAN DEFAULT TRUE",
    "fallback_index": "VARCHAR DEFAULT 'NIFTY_50'",
    "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
}


def _connect(db_path_or_conn: duckdb.DuckDBPyConnection | str | Path) -> tuple[duckdb.DuckDBPyConnection, bool]:
    if isinstance(db_path_or_conn, duckdb.DuckDBPyConnection):
        return db_path_or_conn, False
    conn = duckdb.connect(str(db_path_or_conn))
    return conn, True


def _table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchall()
    return {str(row[0]) for row in rows}


def ensure_data_trust_schema(db_path_or_conn: duckdb.DuckDBPyConnection | str | Path) -> None:
    conn, should_close = _connect(db_path_or_conn)
    try:
        for table_name in ("_catalog", "_catalog_history"):
            if not _table_columns(conn, table_name):
                continue
            existing = _table_columns(conn, table_name)
            for column_name, column_type in CATALOG_TRUST_COLUMNS.items():
                if column_name not in existing:
                    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _catalog_provenance (
                symbol_id VARCHAR,
                security_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                provider VARCHAR,
                provider_priority INTEGER,
                validation_status VARCHAR,
                validated_against VARCHAR,
                provider_confidence DOUBLE,
                provider_discrepancy_flag BOOLEAN,
                provider_discrepancy_note VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                adjusted_open DOUBLE,
                adjusted_high DOUBLE,
                adjusted_low DOUBLE,
                adjusted_close DOUBLE,
                adjustment_factor DOUBLE,
                adjustment_source VARCHAR,
                instrument_type VARCHAR,
                is_benchmark BOOLEAN,
                benchmark_label VARCHAR,
                isin VARCHAR,
                ingest_run_id VARCHAR,
                repair_batch_id VARCHAR,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        provenance_expected = {
            "provider_confidence": "DOUBLE",
            "provider_discrepancy_flag": "BOOLEAN",
            "provider_discrepancy_note": "VARCHAR",
            "adjusted_open": "DOUBLE",
            "adjusted_high": "DOUBLE",
            "adjusted_low": "DOUBLE",
            "adjusted_close": "DOUBLE",
            "adjustment_factor": "DOUBLE",
            "adjustment_source": "VARCHAR",
            "instrument_type": "VARCHAR",
            "is_benchmark": "BOOLEAN",
            "benchmark_label": "VARCHAR",
            "isin": "VARCHAR",
        }
        provenance_existing = _table_columns(conn, "_catalog_provenance")
        for column_name, column_type in provenance_expected.items():
            if column_name not in provenance_existing:
                conn.execute(f"ALTER TABLE _catalog_provenance ADD COLUMN {column_name} {column_type}")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _catalog_quarantine (
                symbol_id VARCHAR,
                security_id VARCHAR,
                exchange VARCHAR,
                trade_date DATE,
                reason VARCHAR,
                status VARCHAR DEFAULT 'active',
                source_run_id VARCHAR,
                repair_batch_id VARCHAR,
                note VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                resolved_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_quarantine_scope
            ON _catalog_quarantine (exchange, trade_date, status, symbol_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_provenance_scope
            ON _catalog_provenance (exchange, timestamp, provider, symbol_id)
            """
        )
    finally:
        if should_close:
            conn.close()


def ensure_index_schema(db_path_or_conn: duckdb.DuckDBPyConnection | str | Path) -> None:
    """Ensure index catalog tables exist with proper schema."""
    conn, should_close = _connect(db_path_or_conn)
    try:
        # Create _index_metadata table
        existing_meta = _table_columns(conn, "_index_metadata")
        if not existing_meta:
            conn.execute("""
                CREATE TABLE _index_metadata (
                    index_code VARCHAR PRIMARY KEY,
                    display_name VARCHAR NOT NULL,
                    family VARCHAR,
                    is_sectoral BOOLEAN DEFAULT TRUE,
                    benchmark_for VARCHAR,
                    source VARCHAR DEFAULT 'nseindia',
                    active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Add columns if table existed but missing columns
        else:
            for col, dtype in INDEX_METADATA_COLUMNS.items():
                if col not in existing_meta:
                    conn.execute(f"ALTER TABLE _index_metadata ADD COLUMN {col} {dtype}")

        # Create _index_catalog table
        existing_cat = _table_columns(conn, "_index_catalog")
        if not existing_cat:
            conn.execute("""
                CREATE TABLE _index_catalog (
                    index_code VARCHAR NOT NULL,
                    date DATE NOT NULL,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE NOT NULL,
                    volume BIGINT,
                    value DOUBLE,
                    provider VARCHAR DEFAULT 'nseindia',
                    ingest_run_id VARCHAR,
                    validated_at TIMESTAMP,
                    PRIMARY KEY (index_code, date)
                )
            """)
        else:
            for col, dtype in INDEX_CATALOG_COLUMNS.items():
                if col not in existing_cat:
                    conn.execute(f"ALTER TABLE _index_catalog ADD COLUMN {col} {dtype}")

        # Create sector_to_index mapping table
        existing_map = _table_columns(conn, "sector_to_index")
        if not existing_map:
            conn.execute("""
                CREATE TABLE sector_to_index (
                    system_sector VARCHAR PRIMARY KEY,
                    index_code VARCHAR NOT NULL,
                    index_name VARCHAR NOT NULL,
                    is_primary BOOLEAN DEFAULT TRUE,
                    fallback_index VARCHAR DEFAULT 'NIFTY_50',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        else:
            for col, dtype in SECTOR_TO_INDEX_COLUMNS.items():
                if col not in existing_map:
                    conn.execute(f"ALTER TABLE sector_to_index ADD COLUMN {col} {dtype}")

    finally:
        if should_close:
            conn.close()


def record_provenance_rows(
    db_path_or_conn: duckdb.DuckDBPyConnection | str | Path,
    rows: pd.DataFrame,
) -> int:
    if rows is None or rows.empty:
        return 0
    conn, should_close = _connect(db_path_or_conn)
    try:
        ensure_data_trust_schema(conn)
        frame = rows.copy()
        for column in [
            "symbol_id",
            "security_id",
            "exchange",
            "timestamp",
            "provider",
            "provider_priority",
            "validation_status",
            "validated_against",
            "provider_confidence",
            "provider_discrepancy_flag",
            "provider_discrepancy_note",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "adjusted_open",
            "adjusted_high",
            "adjusted_low",
            "adjusted_close",
            "adjustment_factor",
            "adjustment_source",
            "instrument_type",
            "is_benchmark",
            "benchmark_label",
            "isin",
            "ingest_run_id",
            "repair_batch_id",
        ]:
            if column not in frame.columns:
                frame[column] = None
        conn.register("trust_rows", frame)
        conn.execute(
            """
            INSERT INTO _catalog_provenance
            (symbol_id, security_id, exchange, timestamp, provider, provider_priority,
             validation_status, validated_against, provider_confidence, provider_discrepancy_flag, provider_discrepancy_note,
             open, high, low, close, volume,
             adjusted_open, adjusted_high, adjusted_low, adjusted_close, adjustment_factor, adjustment_source,
             instrument_type, is_benchmark, benchmark_label, isin,
             ingest_run_id, repair_batch_id)
            SELECT
                symbol_id,
                security_id,
                exchange,
                timestamp,
                provider,
                provider_priority,
                validation_status,
                validated_against,
                provider_confidence,
                provider_discrepancy_flag,
                provider_discrepancy_note,
                open,
                high,
                low,
                close,
                volume,
                adjusted_open,
                adjusted_high,
                adjusted_low,
                adjusted_close,
                adjustment_factor,
                adjustment_source,
                instrument_type,
                is_benchmark,
                benchmark_label,
                isin,
                ingest_run_id,
                repair_batch_id
            FROM trust_rows
            """
        )
        return int(len(frame))
    finally:
        if should_close:
            conn.close()


def reconcile_provider_row(primary_row: dict, fallback_row: dict | None = None) -> dict:
    """Keep primary row authoritative while surfacing discrepancy metadata."""
    chosen = dict(primary_row or {})
    chosen["provider_confidence"] = float(chosen.get("provider_confidence", 1.0) or 1.0)
    chosen["provider_discrepancy_flag"] = bool(chosen.get("provider_discrepancy_flag", False))
    chosen["provider_discrepancy_note"] = chosen.get("provider_discrepancy_note")

    if fallback_row is None:
        return chosen

    primary_close = primary_row.get("close")
    fallback_close = fallback_row.get("close")
    if primary_close is None or fallback_close is None:
        return chosen

    try:
        diff = abs(float(primary_close) - float(fallback_close))
    except (TypeError, ValueError):
        return chosen
    if diff > 0:
        chosen["provider_discrepancy_flag"] = True
        chosen["provider_discrepancy_note"] = f"primary_vs_fallback_close_diff={diff}"
        chosen["provider_confidence"] = min(float(chosen["provider_confidence"]), 0.8)
    return chosen


def annotate_provider_reconciliation(
    frame: pd.DataFrame,
    *,
    primary_provider: str = "nse_bhavcopy",
    fallback_provider: str = "yfinance",
) -> pd.DataFrame:
    """Add provider confidence and discrepancy markers to ingest rows."""
    if frame is None or frame.empty:
        return pd.DataFrame(columns=frame.columns if isinstance(frame, pd.DataFrame) else None)
    output = frame.copy(deep=True)
    if "provider_confidence" in output.columns:
        provider_confidence = pd.to_numeric(output["provider_confidence"], errors="coerce")
        output.loc[:, "provider_confidence"] = provider_confidence.where(provider_confidence.notna(), 1.0)
    else:
        output.loc[:, "provider_confidence"] = 1.0
    output.loc[:, "provider_discrepancy_flag"] = False
    output.loc[:, "provider_discrepancy_note"] = None

    required = {"symbol_id", "exchange", "timestamp", "provider", "close"}
    if not required.issubset(set(output.columns)):
        return output

    keys = ["symbol_id", "exchange", "timestamp"]
    primary = output[output["provider"] == primary_provider]
    fallback = output[output["provider"] == fallback_provider]
    if primary.empty or fallback.empty:
        return output

    merged = primary[keys + ["close"]].merge(
        fallback[keys + ["close"]],
        on=keys,
        how="inner",
        suffixes=("_primary", "_fallback"),
    )
    if merged.empty:
        return output

    for row in merged.itertuples(index=False):
        primary_row = {"close": row.close_primary}
        fallback_row = {"close": row.close_fallback}
        reconciled = reconcile_provider_row(primary_row, fallback_row)
        mask = (
            (output["provider"] == primary_provider)
            & (output["symbol_id"] == row.symbol_id)
            & (output["exchange"] == row.exchange)
            & (output["timestamp"] == row.timestamp)
        )
        output.loc[mask, "provider_confidence"] = float(reconciled["provider_confidence"])
        output.loc[mask, "provider_discrepancy_flag"] = bool(reconciled["provider_discrepancy_flag"])
        output.loc[mask, "provider_discrepancy_note"] = reconciled["provider_discrepancy_note"]
    return output


def quarantine_symbol_dates(
    db_path_or_conn: duckdb.DuckDBPyConnection | str | Path,
    *,
    symbol_rows: Iterable[dict[str, Any]],
    trade_dates: Iterable[str],
    reason: str,
    status: str = "active",
    source_run_id: str | None = None,
    repair_batch_id: str | None = None,
    note: str | None = None,
) -> int:
    trade_date_list = sorted({str(item) for item in trade_dates if item})
    symbol_row_list = [row for row in symbol_rows if row]
    if not trade_date_list or not symbol_row_list:
        return 0

    rows: list[dict[str, Any]] = []
    for trade_date in trade_date_list:
        for row in symbol_row_list:
            rows.append(
                {
                    "symbol_id": str(row.get("symbol_id", "")),
                    "security_id": str(row.get("security_id", "")),
                    "exchange": str(row.get("exchange", "NSE") or "NSE"),
                    "trade_date": trade_date,
                    "reason": reason,
                    "status": str(status or "active"),
                    "source_run_id": source_run_id,
                    "repair_batch_id": repair_batch_id,
                    "note": note,
                }
            )

    if not rows:
        return 0

    conn, should_close = _connect(db_path_or_conn)
    try:
        ensure_data_trust_schema(conn)
        frame = pd.DataFrame(rows)
        conn.register("quarantine_rows", frame)
        conn.execute(
            """
            DELETE FROM _catalog_quarantine
            USING quarantine_rows
            WHERE _catalog_quarantine.symbol_id = quarantine_rows.symbol_id
              AND _catalog_quarantine.exchange = quarantine_rows.exchange
              AND _catalog_quarantine.trade_date = CAST(quarantine_rows.trade_date AS DATE)
              AND _catalog_quarantine.status = 'active'
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog_quarantine
            (symbol_id, security_id, exchange, trade_date, reason, status, source_run_id, repair_batch_id, note)
            SELECT
                symbol_id,
                security_id,
                exchange,
                CAST(trade_date AS DATE),
                reason,
                status,
                source_run_id,
                repair_batch_id,
                note
            FROM quarantine_rows
            """
        )
        return int(len(frame))
    finally:
        if should_close:
            conn.close()


def resolve_quarantine_for_rows(
    db_path_or_conn: duckdb.DuckDBPyConnection | str | Path,
    rows: pd.DataFrame,
    *,
    note: str | None = None,
) -> int:
    if rows is None or rows.empty:
        return 0
    frame = rows.copy(deep=True)
    if "timestamp" not in frame.columns:
        return 0
    frame.loc[:, "trade_date"] = pd.to_datetime(frame["timestamp"]).dt.date.astype(str)
    for column in ("symbol_id", "exchange"):
        if column not in frame.columns:
            return 0
    frame = frame[["symbol_id", "exchange", "trade_date"]].drop_duplicates()
    if frame.empty:
        return 0

    conn, should_close = _connect(db_path_or_conn)
    try:
        ensure_data_trust_schema(conn)
        conn.register("resolved_rows", frame)
        updated = conn.execute(
            """
            UPDATE _catalog_quarantine
            SET status = 'resolved',
                resolved_at = CURRENT_TIMESTAMP,
                note = COALESCE(?, note)
            FROM resolved_rows
            WHERE _catalog_quarantine.symbol_id = resolved_rows.symbol_id
              AND _catalog_quarantine.exchange = resolved_rows.exchange
              AND _catalog_quarantine.trade_date = CAST(resolved_rows.trade_date AS DATE)
              AND _catalog_quarantine.status = 'active'
            RETURNING 1
            """,
            [note],
        ).fetchall()
        return int(len(updated))
    finally:
        if should_close:
            conn.close()


def load_data_trust_summary(
    db_path: str | Path,
    *,
    run_date: str | None = None,
    fallback_warn_threshold: float = 0.25,
    quarantine_lookback_days: int = 30,
    blocked_quarantine_symbol_threshold: int = 10,
    blocked_quarantine_ratio_threshold: float = 0.01,
) -> dict[str, Any]:
    path = Path(db_path)
    if not path.exists():
        envelope = TrustConfidenceEnvelope.from_trust_summary({"status": "missing"})
        return {
            "status": "missing",
            "db_path": str(path),
            "latest_trade_date": None,
            "latest_validated_date": None,
            "provider_counts_by_date": {},
            "active_quarantined_dates": [],
            "active_quarantined_symbols": 0,
            "fallback_ratio_latest": 0.0,
            "latest_provider_stats": {},
            "latest_repair_batch": {},
            "latest_quarantined_symbols": 0,
            "latest_quarantined_symbol_ratio": 0.0,
            "trust_confidence": envelope.to_dict(),
        }

    conn = duckdb.connect(str(path), read_only=True)
    try:
        catalog_columns = _table_columns(conn, "_catalog")
        if not catalog_columns:
            envelope = TrustConfidenceEnvelope.from_trust_summary({"status": "missing"})
            return {
                "status": "missing",
                "db_path": str(path),
                "latest_trade_date": None,
                "latest_validated_date": None,
                "provider_counts_by_date": {},
                "active_quarantined_dates": [],
                "active_quarantined_symbols": 0,
                "fallback_ratio_latest": 0.0,
                "latest_provider_stats": {},
                "latest_repair_batch": {},
                "latest_quarantined_symbols": 0,
                "latest_quarantined_symbol_ratio": 0.0,
                "trust_confidence": envelope.to_dict(),
            }
        latest_trade_date = conn.execute(
            "SELECT MAX(CAST(timestamp AS DATE)) FROM _catalog WHERE exchange = 'NSE'"
        ).fetchone()[0]
        latest_validated_date = None
        if "validation_status" in catalog_columns:
            latest_validated_date = conn.execute(
                """
                SELECT MAX(CAST(timestamp AS DATE))
                FROM _catalog
                WHERE exchange = 'NSE'
                  AND COALESCE(validation_status, 'legacy_unverified') != 'legacy_unverified'
                """
            ).fetchone()[0]
        effective_run_date = str(run_date or latest_trade_date or "")
        quarantine_window_start = None
        if effective_run_date:
            quarantine_window_start = (
                pd.Timestamp(effective_run_date) - pd.Timedelta(days=int(quarantine_lookback_days))
            ).date().isoformat()

        provider_expr = "COALESCE(provider, 'unknown')" if "provider" in catalog_columns else "'unknown'"
        provider_rows = conn.execute(
            f"""
            SELECT
                CAST(timestamp AS DATE) AS trade_date,
                {provider_expr} AS provider,
                COUNT(*) AS row_count
            FROM _catalog
            WHERE exchange = 'NSE'
              AND CAST(timestamp AS DATE) >= COALESCE(CAST(? AS DATE), CURRENT_DATE) - INTERVAL 7 DAY
            GROUP BY 1, 2
            ORDER BY 1, 2
            """,
            [run_date],
        ).fetchall()
        quarantine_rows = []
        if _table_columns(conn, "_catalog_quarantine"):
            quarantine_rows = conn.execute(
                """
                SELECT trade_date, COUNT(*) AS row_count, COUNT(DISTINCT symbol_id) AS symbol_count
                FROM _catalog_quarantine
                WHERE exchange = 'NSE'
                  AND status = 'active'
                  AND trade_date >= COALESCE(CAST(? AS DATE), CURRENT_DATE - INTERVAL 30 DAY)
                GROUP BY trade_date
                ORDER BY trade_date
                """,
                [quarantine_window_start],
            ).fetchall()
        latest_repair_row = None
        if "repair_batch_id" in catalog_columns:
            latest_repair_row = conn.execute(
                """
                SELECT repair_batch_id, MAX(timestamp) AS latest_timestamp, COUNT(*) AS repaired_rows
                FROM _catalog
                WHERE repair_batch_id IS NOT NULL
                GROUP BY repair_batch_id
                ORDER BY latest_timestamp DESC
                LIMIT 1
                """
            ).fetchone()
    finally:
        conn.close()

    provider_counts_by_date: dict[str, dict[str, int]] = {}
    for trade_date, provider, row_count in provider_rows:
        key = str(trade_date)
        provider_counts_by_date.setdefault(key, {})
        provider_counts_by_date[key][str(provider)] = int(row_count)

    latest_trade_key = str(latest_trade_date) if latest_trade_date is not None else None
    latest_provider_stats = provider_counts_by_date.get(latest_trade_key or "", {})
    latest_total = sum(latest_provider_stats.values())
    latest_primary = int(latest_provider_stats.get("nse_bhavcopy", 0))
    latest_fallback = int(latest_provider_stats.get("yfinance", 0))
    latest_unknown = int(latest_provider_stats.get("unknown", 0))
    fallback_ratio_latest = (latest_fallback / latest_total) if latest_total else 0.0
    primary_ratio_latest = (latest_primary / latest_total) if latest_total else 0.0
    unknown_ratio_latest = (latest_unknown / latest_total) if latest_total else 0.0

    active_quarantined_dates = [str(row[0]) for row in quarantine_rows]
    active_quarantined_symbols = int(sum(int(row[2]) for row in quarantine_rows))
    quarantine_symbol_count_by_date = {str(row[0]): int(row[2] or 0) for row in quarantine_rows}
    latest_quarantined_symbols = int(quarantine_symbol_count_by_date.get(latest_trade_key or "", 0))
    latest_quarantined_symbol_ratio = (
        (latest_quarantined_symbols / latest_total) if latest_total else 0.0
    )
    latest_repair_batch = (
        {
            "repair_batch_id": str(latest_repair_row[0]),
            "latest_timestamp": str(latest_repair_row[1]) if latest_repair_row[1] is not None else None,
            "repaired_rows": int(latest_repair_row[2] or 0),
            "status": "incomplete" if active_quarantined_dates else "completed",
        }
        if latest_repair_row
        else {}
    )

    status = "legacy" if "validation_status" not in catalog_columns else "trusted"
    if status != "legacy":
        if latest_trade_key and latest_trade_key in active_quarantined_dates:
            exceeds_symbol_threshold = latest_quarantined_symbols > int(blocked_quarantine_symbol_threshold)
            exceeds_ratio_threshold = latest_quarantined_symbol_ratio > float(blocked_quarantine_ratio_threshold)
            status = "blocked" if (exceeds_symbol_threshold or exceeds_ratio_threshold) else "degraded"
        elif active_quarantined_dates:
            status = "degraded"
        elif latest_unknown > 0:
            status = "degraded"
        elif fallback_ratio_latest > fallback_warn_threshold:
            status = "degraded"

    provider_confidence = max(0.0, min(1.0, 1.0 - float(fallback_ratio_latest) - float(unknown_ratio_latest)))
    envelope = TrustConfidenceEnvelope.from_trust_summary(
        {
            "status": status,
            "active_quarantined_dates": active_quarantined_dates,
            "active_quarantined_symbols": active_quarantined_symbols,
            "fallback_ratio_latest": round(fallback_ratio_latest, 4),
            "primary_ratio_latest": round(primary_ratio_latest, 4),
            "unknown_ratio_latest": round(unknown_ratio_latest, 4),
            "latest_provider_stats": {
                "trade_date": latest_trade_key,
                "counts": latest_provider_stats,
                "total_rows": latest_total,
                "primary_rows": latest_primary,
                "fallback_rows": latest_fallback,
                "unknown_rows": latest_unknown,
            },
            "latest_trade_date": latest_trade_key,
            "latest_validated_date": str(latest_validated_date) if latest_validated_date is not None else None,
            "trust_confidence": {"provider_confidence": round(provider_confidence, 4)},
        }
    )

    return {
        "status": status,
        "db_path": str(path),
        "latest_trade_date": latest_trade_key,
        "latest_validated_date": str(latest_validated_date) if latest_validated_date is not None else None,
        "provider_counts_by_date": provider_counts_by_date,
        "active_quarantined_dates": active_quarantined_dates,
        "active_quarantined_symbols": active_quarantined_symbols,
        "latest_quarantined_symbols": latest_quarantined_symbols,
        "latest_quarantined_symbol_ratio": round(latest_quarantined_symbol_ratio, 4),
        "fallback_ratio_latest": round(fallback_ratio_latest, 4),
        "primary_ratio_latest": round(primary_ratio_latest, 4),
        "unknown_ratio_latest": round(unknown_ratio_latest, 4),
        "latest_provider_stats": {
            "trade_date": latest_trade_key,
            "counts": latest_provider_stats,
            "total_rows": latest_total,
            "primary_rows": latest_primary,
            "fallback_rows": latest_fallback,
            "unknown_rows": latest_unknown,
        },
        "latest_repair_batch": latest_repair_batch,
        "trust_confidence": envelope.to_dict(),
    }


def load_symbol_trust_state(
    db_path: str | Path,
    symbols: Iterable[str],
) -> pd.DataFrame:
    symbol_list = sorted({str(symbol).upper() for symbol in symbols if symbol})
    if not symbol_list:
        return pd.DataFrame(
            columns=[
                "symbol_id",
                "provider",
                "validation_status",
                "validated_against",
                "repair_batch_id",
                "latest_trade_date",
                "is_quarantined",
            ]
        )
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        placeholders = ", ".join(["?"] * len(symbol_list))
        frame = conn.execute(
            f"""
            WITH latest_catalog AS (
                SELECT
                    symbol_id,
                    provider,
                    validation_status,
                    validated_against,
                    repair_batch_id,
                    CAST(timestamp AS DATE) AS latest_trade_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp DESC
                    ) AS rn
                FROM _catalog
                WHERE exchange = 'NSE'
                  AND UPPER(symbol_id) IN ({placeholders})
            ),
            active_quarantine AS (
                SELECT DISTINCT symbol_id
                FROM _catalog_quarantine
                WHERE exchange = 'NSE'
                  AND status = 'active'
                  AND UPPER(symbol_id) IN ({placeholders})
            )
            SELECT
                latest_catalog.symbol_id,
                latest_catalog.provider,
                latest_catalog.validation_status,
                latest_catalog.validated_against,
                latest_catalog.repair_batch_id,
                latest_catalog.latest_trade_date,
                CASE WHEN active_quarantine.symbol_id IS NULL THEN FALSE ELSE TRUE END AS is_quarantined
            FROM latest_catalog
            LEFT JOIN active_quarantine
                ON latest_catalog.symbol_id = active_quarantine.symbol_id
            WHERE latest_catalog.rn = 1
            """,
            [*symbol_list, *symbol_list],
        ).fetchdf()
    finally:
        conn.close()
    return frame
