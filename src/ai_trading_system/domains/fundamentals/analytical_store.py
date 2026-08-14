"""Canonical analytical fundamentals DuckDB helpers."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths


def default_fundamentals_duckdb_path(project_root: Path | str | None = None) -> Path:
    return get_domain_paths(project_root=project_root, data_domain="operational").root_dir / "fundamentals.duckdb"


def connect_fundamentals_duckdb(
    db_path: str | Path | None = None,
    *,
    project_root: Path | str | None = None,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    path = Path(db_path) if db_path is not None else default_fundamentals_duckdb_path(project_root)
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


def ensure_fundamentals_analytical_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS screener_financials (
            symbol VARCHAR,
            period_type VARCHAR,
            report_date DATE,
            statement_basis VARCHAR DEFAULT 'standalone',
            metric_id VARCHAR,
            value DOUBLE,
            available_at DATE,
            source VARCHAR,
            sync_batch_id VARCHAR,
            synced_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamental_period_facts (
            symbol VARCHAR NOT NULL,
            period_type VARCHAR NOT NULL,
            report_date DATE NOT NULL,
            statement_basis VARCHAR NOT NULL DEFAULT 'standalone',
            available_at DATE,
            sales_cr DOUBLE,
            net_profit_cr DOUBLE,
            operating_profit_cr DOUBLE,
            expenses_cr DOUBLE,
            opm_pct DOUBLE,
            npm_pct DOUBLE,
            PRIMARY KEY (symbol, period_type, report_date, statement_basis)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS company_growth_features (
            symbol VARCHAR NOT NULL,
            report_date DATE NOT NULL,
            statement_basis VARCHAR NOT NULL DEFAULT 'standalone',
            available_at DATE NOT NULL,
            sales_cr DOUBLE,
            net_profit_cr DOUBLE,
            operating_profit_cr DOUBLE,
            opm_pct DOUBLE,
            npm_pct DOUBLE,
            sales_qoq_growth DOUBLE,
            sales_yoy_growth DOUBLE,
            profit_qoq_growth DOUBLE,
            profit_yoy_growth DOUBLE,
            operating_profit_qoq_growth DOUBLE,
            operating_profit_yoy_growth DOUBLE,
            opm_qoq_change DOUBLE,
            opm_yoy_change DOUBLE,
            npm_qoq_change DOUBLE,
            npm_yoy_change DOUBLE,
            sales_4q_cagr DOUBLE,
            profit_4q_cagr DOUBLE,
            sales_8q_cagr DOUBLE,
            profit_8q_cagr DOUBLE,
            positive_profit_quarters_4q INTEGER,
            sales_growth_positive_quarters_4q INTEGER,
            profit_growth_positive_quarters_4q INTEGER,
            margin_expansion_quarters_4q INTEGER,
            created_at TIMESTAMP,
            PRIMARY KEY (symbol, report_date, statement_basis)
        )
        """
    )
    _ensure_duckdb_column(conn, "screener_financials", "statement_basis", "VARCHAR DEFAULT 'standalone'")
    _ensure_duckdb_column(conn, "fundamental_period_facts", "statement_basis", "VARCHAR DEFAULT 'standalone'")
    _ensure_duckdb_column(conn, "company_growth_features", "statement_basis", "VARCHAR DEFAULT 'standalone'")
    _require_basis_aware_derived_keys(conn)
    conn.execute(
        """
        CREATE OR REPLACE VIEW screener_statement_basis_resolution AS
        WITH coverage AS (
            SELECT
                symbol,
                max(report_date) FILTER (
                    WHERE lower(trim(period_type)) = 'quarterly'
                      AND coalesce(nullif(lower(trim(statement_basis)), ''), 'standalone') = 'consolidated'
                ) AS consolidated_latest_quarter,
                max(report_date) FILTER (
                    WHERE lower(trim(period_type)) = 'quarterly'
                      AND coalesce(nullif(lower(trim(statement_basis)), ''), 'standalone') = 'standalone'
                ) AS standalone_latest_quarter,
                count(*) FILTER (
                    WHERE coalesce(nullif(lower(trim(statement_basis)), ''), 'standalone') = 'consolidated'
                ) AS consolidated_rows,
                count(*) FILTER (
                    WHERE coalesce(nullif(lower(trim(statement_basis)), ''), 'standalone') = 'standalone'
                ) AS standalone_rows
                ,count(DISTINCT report_date) FILTER (
                    WHERE lower(trim(period_type)) = 'quarterly'
                      AND coalesce(nullif(lower(trim(statement_basis)), ''), 'standalone') = 'consolidated'
                ) AS consolidated_quarter_count
                ,count(DISTINCT report_date) FILTER (
                    WHERE lower(trim(period_type)) = 'quarterly'
                      AND coalesce(nullif(lower(trim(statement_basis)), ''), 'standalone') = 'standalone'
                ) AS standalone_quarter_count
                ,count(DISTINCT report_date) FILTER (
                    WHERE lower(trim(period_type)) = 'annual'
                      AND coalesce(nullif(lower(trim(statement_basis)), ''), 'standalone') = 'consolidated'
                ) AS consolidated_annual_count
                ,count(DISTINCT report_date) FILTER (
                    WHERE lower(trim(period_type)) = 'annual'
                      AND coalesce(nullif(lower(trim(statement_basis)), ''), 'standalone') = 'standalone'
                ) AS standalone_annual_count
            FROM screener_financials
            GROUP BY symbol
        )
        SELECT
            symbol,
            CASE
                WHEN consolidated_latest_quarter IS NOT NULL
                 AND (standalone_latest_quarter IS NULL OR consolidated_latest_quarter >= standalone_latest_quarter)
                 AND consolidated_quarter_count >= least(standalone_quarter_count, 5)
                 AND consolidated_annual_count >= least(standalone_annual_count, 2)
                    THEN 'consolidated'
                WHEN standalone_rows > 0 THEN 'standalone'
                WHEN consolidated_rows > 0 THEN 'consolidated'
                ELSE 'standalone'
            END AS statement_basis,
            CASE
                WHEN consolidated_latest_quarter IS NOT NULL
                 AND standalone_latest_quarter IS NULL
                 AND consolidated_quarter_count >= least(standalone_quarter_count, 5)
                 AND consolidated_annual_count >= least(standalone_annual_count, 2)
                    THEN 'consolidated_only_quarterly'
                WHEN consolidated_latest_quarter IS NOT NULL
                 AND consolidated_latest_quarter >= standalone_latest_quarter
                 AND consolidated_quarter_count >= least(standalone_quarter_count, 5)
                 AND consolidated_annual_count >= least(standalone_annual_count, 2)
                    THEN 'consolidated_current'
                WHEN standalone_rows > 0 AND consolidated_latest_quarter IS NULL
                    THEN 'standalone_fallback_no_consolidated_quarterly'
                WHEN standalone_rows > 0
                 AND (
                    consolidated_quarter_count < least(standalone_quarter_count, 5)
                    OR consolidated_annual_count < least(standalone_annual_count, 2)
                 ) THEN 'standalone_fallback_insufficient_consolidated_history'
                WHEN standalone_rows > 0 THEN 'standalone_fallback_newer_quarter'
                WHEN consolidated_rows > 0 THEN 'consolidated_only_no_quarterly'
                ELSE 'standalone_fallback'
            END AS basis_resolution_reason,
            consolidated_latest_quarter,
            standalone_latest_quarter
        FROM coverage
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW screener_financials_resolved AS
        SELECT
            f.*,
            r.basis_resolution_reason,
            r.consolidated_latest_quarter,
            r.standalone_latest_quarter
        FROM screener_financials f
        JOIN screener_statement_basis_resolution r
          ON r.symbol = f.symbol
         AND r.statement_basis = coalesce(nullif(lower(trim(f.statement_basis)), ''), 'standalone')
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW company_growth_features_resolved AS
        SELECT
            g.*,
            r.basis_resolution_reason,
            r.consolidated_latest_quarter,
            r.standalone_latest_quarter
        FROM company_growth_features g
        JOIN screener_statement_basis_resolution r
          ON r.symbol = g.symbol
         AND r.statement_basis = coalesce(nullif(lower(trim(g.statement_basis)), ''), 'standalone')
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS company_insight_tags (
            symbol VARCHAR NOT NULL,
            report_date DATE NOT NULL,
            insight_type VARCHAR NOT NULL,
            insight_score DOUBLE,
            evidence_json VARCHAR,
            created_at TIMESTAMP,
            PRIMARY KEY (symbol, report_date, insight_type)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sector_earnings_leadership (
            sector_name VARCHAR NOT NULL,
            report_date DATE NOT NULL,
            sector_sales_yoy_growth DOUBLE,
            sector_profit_yoy_growth DOUBLE,
            sector_sales_qoq_growth DOUBLE,
            sector_profit_qoq_growth DOUBLE,
            median_company_sales_yoy DOUBLE,
            median_company_profit_yoy DOUBLE,
            sales_positive_pct DOUBLE,
            profit_positive_pct DOUBLE,
            margin_expansion_pct DOUBLE,
            great_result_count INTEGER,
            turnaround_count INTEGER,
            compounder_count INTEGER,
            aggregate_sales_growth_rank DOUBLE,
            aggregate_profit_growth_rank DOUBLE,
            sales_breadth_rank DOUBLE,
            profit_breadth_rank DOUBLE,
            margin_expansion_rank DOUBLE,
            great_result_count_rank DOUBLE,
            turnaround_count_rank DOUBLE,
            sector_fundamental_score DOUBLE,
            earnings_trend_label VARCHAR,
            created_at TIMESTAMP,
            PRIMARY KEY (sector_name, report_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS universe_valuation_daily (
            universe_id VARCHAR NOT NULL,
            date DATE NOT NULL,
            index_level_equal_weight DOUBLE,
            index_level_mcap_weight DOUBLE,
            total_market_cap_cr DOUBLE,
            total_ttm_profit_cr DOUBLE,
            positive_profit_market_cap_cr DOUBLE,
            loss_making_market_cap_cr DOUBLE,
            pe_ttm DOUBLE,
            earnings_yield DOUBLE,
            loss_mcap_pct DOUBLE,
            pe_200dma DOUBLE,
            pe_1y_median DOUBLE,
            pe_3y_median DOUBLE,
            pe_5y_median DOUBLE,
            pe_zscore_3y DOUBLE,
            pe_zscore_5y DOUBLE,
            pe_percentile_3y DOUBLE,
            pe_percentile_5y DOUBLE,
            valuation_zone VARCHAR,
            created_at TIMESTAMP,
            PRIMARY KEY (universe_id, date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS valuation_cycle_features (
            entity_type VARCHAR NOT NULL,
            entity_id VARCHAR NOT NULL,
            date DATE NOT NULL,
            pe_ttm DOUBLE,
            pe_200dma DOUBLE,
            pe_distance_from_200dma DOUBLE,
            pe_5y_median DOUBLE,
            pe_percentile_5y DOUBLE,
            pe_zscore_5y DOUBLE,
            valuation_zone VARCHAR,
            cycle_signal VARCHAR,
            created_at TIMESTAMP,
            PRIMARY KEY (entity_type, entity_id, date)
        )
        """
    )
    for statement in (
        "CREATE INDEX IF NOT EXISTS idx_company_growth_symbol_date ON company_growth_features(symbol, report_date)",
        "CREATE INDEX IF NOT EXISTS idx_company_tags_type_date ON company_insight_tags(insight_type, report_date)",
        "CREATE INDEX IF NOT EXISTS idx_sector_leadership_date ON sector_earnings_leadership(report_date)",
        "CREATE INDEX IF NOT EXISTS idx_universe_valuation_id_date ON universe_valuation_daily(universe_id, date)",
        "CREATE INDEX IF NOT EXISTS idx_fund_valuation_cycle_entity_date ON valuation_cycle_features(entity_type, entity_id, date)",
    ):
        conn.execute(statement)


def migrate_fundamentals_analytical_basis_keys(
    conn: duckdb.DuckDBPyConnection,
    *,
    verified_backup_path: str | Path,
) -> dict[str, int]:
    """Rebuild legacy derived tables with basis-aware keys after a verified backup."""

    targets = {
        "fundamental_period_facts": ["symbol", "period_type", "report_date", "statement_basis"],
        "company_growth_features": ["symbol", "report_date", "statement_basis"],
    }
    legacy = {
        table: key
        for table, key in targets.items()
        if _relation_exists(conn, table) and _primary_key_columns(conn, table) != key
    }
    if not legacy:
        return {}
    backup = Path(verified_backup_path)
    if not backup.is_file() or backup.stat().st_size <= 0:
        raise RuntimeError(
            "Legacy fundamentals analytical keys require a verified DuckDB backup before migration"
        )

    counts: dict[str, int] = {}
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DROP VIEW IF EXISTS company_growth_features_resolved")
        for table in legacy:
            temp_table = f"{table}__basis_key_migration"
            conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            conn.execute(_derived_table_ddl(table=temp_table, kind=table))
            columns = _derived_table_columns(table)
            basis_select = (
                "coalesce(nullif(lower(trim(statement_basis)), ''), 'standalone') AS statement_basis"
                if _has_column(conn, table, "statement_basis")
                else "'standalone' AS statement_basis"
            )
            select_columns = [basis_select if column == "statement_basis" else column for column in columns]
            conn.execute(
                f"INSERT INTO {temp_table} ({', '.join(columns)}) "
                f"SELECT {', '.join(select_columns)} FROM {table}"
            )
            old_count = int(conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            new_count = int(conn.execute(f"SELECT count(*) FROM {temp_table}").fetchone()[0])
            if old_count != new_count:
                raise RuntimeError(
                    f"{table} basis-key migration row-count mismatch: {old_count} != {new_count}"
                )
            conn.execute(f"DROP TABLE {table}")
            conn.execute(f"ALTER TABLE {temp_table} RENAME TO {table}")
            counts[table] = new_count
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    ensure_fundamentals_analytical_schema(conn)
    return counts


def _ensure_duckdb_column(conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str, column_type: str) -> None:
    exists = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name = ? AND column_name = ?
        """,
        [table_name, column_name],
    ).fetchone()[0]
    if not exists:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def _require_basis_aware_derived_keys(conn: duckdb.DuckDBPyConnection) -> None:
    expected = {
        "fundamental_period_facts": ["symbol", "period_type", "report_date", "statement_basis"],
        "company_growth_features": ["symbol", "report_date", "statement_basis"],
    }
    legacy = [table for table, key in expected.items() if _primary_key_columns(conn, table) != key]
    if legacy:
        raise RuntimeError(
            "Legacy fundamentals analytical primary key omits statement_basis for "
            f"{', '.join(legacy)}; create and verify a DuckDB backup, then run "
            "migrate_fundamentals_analytical_basis_keys"
        )


def _primary_key_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    row = conn.execute(
        """
        SELECT constraint_column_names
        FROM duckdb_constraints()
        WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'
        """,
        [table_name],
    ).fetchone()
    return [str(column) for column in (row[0] if row else [])]


def _relation_exists(conn: duckdb.DuckDBPyConnection, relation_name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [relation_name],
        ).fetchone()[0]
    )


def _has_column(conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = ? AND column_name = ?",
            [table_name, column_name],
        ).fetchone()[0]
    )


def _derived_table_columns(table: str) -> list[str]:
    if table == "fundamental_period_facts":
        return [
            "symbol", "period_type", "report_date", "statement_basis", "available_at",
            "sales_cr", "net_profit_cr", "operating_profit_cr", "expenses_cr", "opm_pct", "npm_pct",
        ]
    if table == "company_growth_features":
        return [
            "symbol", "report_date", "statement_basis", "available_at", "sales_cr", "net_profit_cr",
            "operating_profit_cr", "opm_pct", "npm_pct", "sales_qoq_growth", "sales_yoy_growth",
            "profit_qoq_growth", "profit_yoy_growth", "operating_profit_qoq_growth",
            "operating_profit_yoy_growth", "opm_qoq_change", "opm_yoy_change", "npm_qoq_change",
            "npm_yoy_change", "sales_4q_cagr", "profit_4q_cagr", "sales_8q_cagr", "profit_8q_cagr",
            "positive_profit_quarters_4q", "sales_growth_positive_quarters_4q",
            "profit_growth_positive_quarters_4q", "margin_expansion_quarters_4q", "created_at",
        ]
    raise ValueError(f"Unsupported derived fundamentals table: {table}")


def _derived_table_ddl(*, table: str, kind: str) -> str:
    if kind == "fundamental_period_facts":
        return f"""
            CREATE TABLE {table} (
                symbol VARCHAR NOT NULL, period_type VARCHAR NOT NULL, report_date DATE NOT NULL,
                statement_basis VARCHAR NOT NULL DEFAULT 'standalone', available_at DATE,
                sales_cr DOUBLE, net_profit_cr DOUBLE, operating_profit_cr DOUBLE, expenses_cr DOUBLE,
                opm_pct DOUBLE, npm_pct DOUBLE,
                PRIMARY KEY (symbol, period_type, report_date, statement_basis)
            )
        """
    if kind == "company_growth_features":
        return f"""
            CREATE TABLE {table} (
                symbol VARCHAR NOT NULL, report_date DATE NOT NULL,
                statement_basis VARCHAR NOT NULL DEFAULT 'standalone', available_at DATE NOT NULL,
                sales_cr DOUBLE, net_profit_cr DOUBLE, operating_profit_cr DOUBLE, opm_pct DOUBLE, npm_pct DOUBLE,
                sales_qoq_growth DOUBLE, sales_yoy_growth DOUBLE, profit_qoq_growth DOUBLE, profit_yoy_growth DOUBLE,
                operating_profit_qoq_growth DOUBLE, operating_profit_yoy_growth DOUBLE,
                opm_qoq_change DOUBLE, opm_yoy_change DOUBLE, npm_qoq_change DOUBLE, npm_yoy_change DOUBLE,
                sales_4q_cagr DOUBLE, profit_4q_cagr DOUBLE, sales_8q_cagr DOUBLE, profit_8q_cagr DOUBLE,
                positive_profit_quarters_4q INTEGER, sales_growth_positive_quarters_4q INTEGER,
                profit_growth_positive_quarters_4q INTEGER, margin_expansion_quarters_4q INTEGER,
                created_at TIMESTAMP,
                PRIMARY KEY (symbol, report_date, statement_basis)
            )
        """
    raise ValueError(f"Unsupported derived fundamentals table: {kind}")


def mirror_screener_financials(
    *,
    screener_db_path: str | Path,
    fundamentals_db_path: str | Path | None = None,
    project_root: Path | str | None = None,
) -> int:
    sqlite_path = Path(screener_db_path)
    if not sqlite_path.exists():
        return 0
    source = sqlite3.connect(str(sqlite_path))
    try:
        columns = {row[1] for row in source.execute("PRAGMA table_info(screener_financials)").fetchall()}
        basis_expr = (
            "coalesce(nullif(lower(trim(statement_basis)), ''), 'standalone')"
            if "statement_basis" in columns
            else "'standalone'"
        )
        frame = pd.read_sql_query(
            """
            SELECT
                upper(trim(symbol)) AS symbol,
                lower(trim(period_type)) AS period_type,
                date(report_date) AS report_date,
                {basis_expr} AS statement_basis,
                lower(trim(metric_id)) AS metric_id,
                value,
                date(available_at) AS available_at,
                source,
                sync_batch_id,
                synced_at
            FROM screener_financials
            """.format(basis_expr=basis_expr),
            source,
        )
    finally:
        source.close()
    conn = connect_fundamentals_duckdb(fundamentals_db_path, project_root=project_root)
    try:
        ensure_fundamentals_analytical_schema(conn)
        conn.execute("DELETE FROM screener_financials")
        if not frame.empty:
            conn.register("_screener_financials_frame", frame)
            try:
                conn.execute(
                    """
                    INSERT INTO screener_financials (
                        symbol, period_type, report_date, statement_basis, metric_id, value,
                        available_at, source, sync_batch_id, synced_at
                    )
                    SELECT
                        symbol, period_type, report_date, statement_basis, metric_id, value,
                        available_at, source, sync_batch_id, synced_at
                    FROM _screener_financials_frame
                    """
                )
            finally:
                conn.unregister("_screener_financials_frame")
    finally:
        conn.close()
    return int(len(frame))


__all__ = [
    "connect_fundamentals_duckdb",
    "default_fundamentals_duckdb_path",
    "ensure_fundamentals_analytical_schema",
    "migrate_fundamentals_analytical_basis_keys",
    "mirror_screener_financials",
]
