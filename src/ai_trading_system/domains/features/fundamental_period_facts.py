"""Wide period facts derived from Screener EAV financials."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

FACT_METRICS = ("sales", "net_profit", "operating_profit", "expenses")


@dataclass(frozen=True)
class PeriodFactsResult:
    symbols: int
    facts_rows: int
    enriched_rows: int
    start_date: str | None
    end_date: str | None


def ensure_sector_earnings_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS symbols_dim (
            symbol VARCHAR PRIMARY KEY,
            name VARCHAR,
            sector_name VARCHAR,
            industry_group VARCHAR,
            industry VARCHAR,
            mcap DOUBLE,
            source VARCHAR,
            updated_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamental_period_facts (
            symbol VARCHAR NOT NULL,
            period_type VARCHAR NOT NULL,
            report_date DATE NOT NULL,
            available_at DATE,
            sales_cr DOUBLE,
            net_profit_cr DOUBLE,
            operating_profit_cr DOUBLE,
            expenses_cr DOUBLE,
            opm_pct DOUBLE,
            npm_pct DOUBLE,
            PRIMARY KEY (symbol, period_type, report_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamental_period_facts_enriched (
            symbol VARCHAR NOT NULL,
            period_type VARCHAR NOT NULL,
            report_date DATE NOT NULL,
            available_at DATE,
            sales_cr DOUBLE,
            net_profit_cr DOUBLE,
            operating_profit_cr DOUBLE,
            expenses_cr DOUBLE,
            opm_pct DOUBLE,
            npm_pct DOUBLE,
            sector_name VARCHAR,
            industry_group VARCHAR,
            industry VARCHAR,
            PRIMARY KEY (symbol, period_type, report_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS company_fundamental_growth (
            symbol VARCHAR NOT NULL,
            sector_name VARCHAR,
            industry_group VARCHAR,
            report_date DATE NOT NULL,
            available_at DATE,
            sales_cr DOUBLE,
            net_profit_cr DOUBLE,
            operating_profit_cr DOUBLE,
            opm_pct DOUBLE,
            npm_pct DOUBLE,
            sales_qoq_growth DOUBLE,
            sales_yoy_growth DOUBLE,
            profit_qoq_growth DOUBLE,
            profit_yoy_growth DOUBLE,
            opm_yoy_change DOUBLE,
            PRIMARY KEY (symbol, report_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sector_fundamental_growth (
            sector_name VARCHAR NOT NULL,
            report_date DATE NOT NULL,
            company_count INTEGER,
            sector_sales_cr DOUBLE,
            sector_profit_cr DOUBLE,
            sector_operating_profit_cr DOUBLE,
            median_sales_yoy_growth DOUBLE,
            median_profit_yoy_growth DOUBLE,
            median_sales_qoq_growth DOUBLE,
            median_profit_qoq_growth DOUBLE,
            avg_opm_pct DOUBLE,
            median_opm_yoy_change DOUBLE,
            sales_yoy_positive_pct DOUBLE,
            profit_yoy_positive_pct DOUBLE,
            margin_expansion_pct DOUBLE,
            sector_sales_qoq_growth DOUBLE,
            sector_sales_yoy_growth DOUBLE,
            sector_profit_qoq_growth DOUBLE,
            sector_profit_yoy_growth DOUBLE,
            PRIMARY KEY (sector_name, report_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sector_earnings_leadership (
            sector_name VARCHAR NOT NULL,
            report_date DATE NOT NULL,
            sector_sales_yoy_growth DOUBLE,
            sector_sales_qoq_growth DOUBLE,
            sector_profit_yoy_growth DOUBLE,
            sector_profit_qoq_growth DOUBLE,
            median_sales_yoy_growth DOUBLE,
            median_profit_yoy_growth DOUBLE,
            sales_yoy_positive_pct DOUBLE,
            profit_yoy_positive_pct DOUBLE,
            margin_expansion_pct DOUBLE,
            sales_yoy_rank DOUBLE,
            sales_qoq_rank DOUBLE,
            profit_yoy_rank DOUBLE,
            median_profit_rank DOUBLE,
            sales_breadth_rank DOUBLE,
            margin_rank DOUBLE,
            sector_earnings_growth_score DOUBLE,
            earnings_trend_label VARCHAR,
            PRIMARY KEY (sector_name, report_date)
        )
        """
    )
    for statement in (
        "CREATE INDEX IF NOT EXISTS idx_period_facts_symbol_date ON fundamental_period_facts(symbol, report_date)",
        "CREATE INDEX IF NOT EXISTS idx_company_growth_symbol_date ON company_fundamental_growth(symbol, report_date)",
        "CREATE INDEX IF NOT EXISTS idx_sector_growth_date ON sector_fundamental_growth(report_date)",
        "CREATE INDEX IF NOT EXISTS idx_sector_earnings_date ON sector_earnings_leadership(report_date)",
    ):
        conn.execute(statement)


def refresh_fundamental_period_facts(
    *,
    ohlcv_db_path: str | Path,
    screener_db_path: str | Path,
    master_db_path: str | Path,
    from_date: str | None = None,
    to_date: str | None = None,
) -> PeriodFactsResult:
    facts = _load_period_facts(screener_db_path, from_date=from_date, to_date=to_date)
    symbols = _load_symbols_dim(master_db_path)
    enriched = _enrich_facts(facts, symbols)

    conn = duckdb.connect(str(ohlcv_db_path))
    try:
        ensure_sector_earnings_schema(conn)
        _replace_symbols_dim(conn, symbols)
        if not facts.empty:
            start, end = str(facts["report_date"].min())[:10], str(facts["report_date"].max())[:10]
            _replace_range(conn, "fundamental_period_facts", facts, "report_date", start, end)
            _replace_range(conn, "fundamental_period_facts_enriched", enriched, "report_date", start, end)
        else:
            start = end = None
    finally:
        conn.close()

    return PeriodFactsResult(
        symbols=int(len(symbols)),
        facts_rows=int(len(facts)),
        enriched_rows=int(len(enriched)),
        start_date=start,
        end_date=end,
    )


def _load_period_facts(
    screener_db_path: str | Path,
    *,
    from_date: str | None,
    to_date: str | None,
) -> pd.DataFrame:
    conn = sqlite3.connect(str(screener_db_path))
    try:
        filters = [
            "lower(trim(period_type)) IN ('quarterly', 'annual')",
            f"lower(trim(metric_id)) IN ({','.join(['?'] * len(FACT_METRICS))})",
        ]
        params: list[Any] = list(FACT_METRICS)
        if from_date:
            filters.append("date(report_date) >= date(?)")
            params.append(str(from_date)[:10])
        if to_date:
            filters.append("date(report_date) <= date(?)")
            params.append(str(to_date)[:10])
        raw = pd.read_sql_query(
            f"""
            SELECT
                upper(trim(symbol)) AS symbol,
                lower(trim(period_type)) AS period_type,
                date(report_date) AS report_date,
                max(date(available_at)) AS available_at,
                max(CASE WHEN lower(trim(metric_id)) = 'sales' THEN value END) AS sales_cr,
                max(CASE WHEN lower(trim(metric_id)) = 'net_profit' THEN value END) AS net_profit_cr,
                max(CASE WHEN lower(trim(metric_id)) = 'operating_profit' THEN value END) AS operating_profit_cr,
                max(CASE WHEN lower(trim(metric_id)) = 'expenses' THEN value END) AS expenses_cr
            FROM screener_financials
            WHERE {' AND '.join(filters)}
            GROUP BY upper(trim(symbol)), lower(trim(period_type)), date(report_date)
            """,
            conn,
            params=params,
        )
    finally:
        conn.close()
    if raw.empty:
        return pd.DataFrame(columns=_fact_columns())
    facts = raw.copy()
    facts.loc[:, "report_date"] = pd.to_datetime(facts["report_date"]).dt.date
    facts.loc[:, "available_at"] = pd.to_datetime(facts["available_at"], errors="coerce").dt.date
    for column in ("sales_cr", "net_profit_cr", "operating_profit_cr", "expenses_cr"):
        facts.loc[:, column] = pd.to_numeric(facts[column], errors="coerce")
    sales = pd.to_numeric(facts["sales_cr"], errors="coerce")
    facts.loc[:, "opm_pct"] = pd.to_numeric(facts["operating_profit_cr"], errors="coerce") / sales.where(sales.gt(0)) * 100.0
    facts.loc[:, "npm_pct"] = pd.to_numeric(facts["net_profit_cr"], errors="coerce") / sales.where(sales.gt(0)) * 100.0
    return facts[_fact_columns()].sort_values(["symbol", "period_type", "report_date"]).reset_index(drop=True)


def _load_symbols_dim(master_db_path: str | Path) -> pd.DataFrame:
    if not Path(master_db_path).exists():
        return pd.DataFrame(columns=_symbol_columns())
    conn = sqlite3.connect(str(master_db_path))
    try:
        frames = []
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "stock_details" in tables:
            stock = pd.read_sql_query(
                """
                SELECT
                    Symbol AS symbol,
                    Name AS name,
                    Sector AS sector_name,
                    [Industry Group] AS industry_group,
                    Industry AS industry,
                    MCAP AS mcap,
                    'stock_details' AS source
                FROM stock_details
                WHERE exchange = 'NSE'
                """,
                conn,
            )
            frames.append(stock)
        if "symbols" in tables:
            if "sector_mapping" in tables:
                symbols = pd.read_sql_query(
                    """
                    SELECT
                        s.symbol_id AS symbol,
                        s.symbol_name AS name,
                        COALESCE(sm.system_sector, s.sector) AS sector_name,
                        s.sector AS industry_group,
                        s.industry AS industry,
                        s.mcap AS mcap,
                        'symbols' AS source
                    FROM symbols s
                    LEFT JOIN sector_mapping sm ON s.sector = sm.industry
                    WHERE s.exchange = 'NSE'
                    """,
                    conn,
                )
            else:
                symbols = pd.read_sql_query(
                    """
                    SELECT
                        symbol_id AS symbol,
                        symbol_name AS name,
                        sector AS sector_name,
                        sector AS industry_group,
                        industry,
                        mcap,
                        'symbols' AS source
                    FROM symbols
                    WHERE exchange = 'NSE'
                    """,
                    conn,
                )
            frames.append(symbols)
    finally:
        conn.close()
    if not frames:
        return pd.DataFrame(columns=_symbol_columns())
    combined = pd.concat(frames, ignore_index=True)
    combined.loc[:, "symbol"] = combined["symbol"].astype(str).str.upper().str.strip()
    combined = combined.loc[combined["symbol"].ne("")]
    combined.loc[:, "_priority"] = combined["source"].map({"stock_details": 0, "symbols": 1}).fillna(9)
    combined = combined.sort_values(["symbol", "_priority"], kind="stable").drop_duplicates("symbol", keep="first")
    combined.loc[:, "mcap"] = pd.to_numeric(combined.get("mcap"), errors="coerce")
    return combined[_symbol_columns()].reset_index(drop=True)


def _enrich_facts(facts: pd.DataFrame, symbols: pd.DataFrame) -> pd.DataFrame:
    if facts.empty:
        return pd.DataFrame(columns=_enriched_columns())
    symbol_cols = ["symbol", "sector_name", "industry_group", "industry"]
    enriched = facts.merge(symbols[symbol_cols], on="symbol", how="left") if not symbols.empty else facts.copy()
    for column in ("sector_name", "industry_group", "industry"):
        if column not in enriched.columns:
            enriched.loc[:, column] = pd.NA
    return enriched[_enriched_columns()]


def _replace_symbols_dim(conn: duckdb.DuckDBPyConnection, frame: pd.DataFrame) -> None:
    conn.execute("DELETE FROM symbols_dim")
    if frame.empty:
        return
    payload = frame.copy()
    payload.loc[:, "updated_at"] = pd.Timestamp.now(tz='UTC').tz_localize(None)
    columns = [*_symbol_columns(), "updated_at"]
    conn.register("_symbols_dim_frame", payload[columns])
    try:
        conn.execute(f"INSERT INTO symbols_dim ({', '.join(columns)}) SELECT {', '.join(columns)} FROM _symbols_dim_frame")
    finally:
        conn.unregister("_symbols_dim_frame")


def _replace_range(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    frame: pd.DataFrame,
    date_col: str,
    start: str,
    end: str,
) -> None:
    conn.execute(
        f"DELETE FROM {table_name} WHERE {date_col} BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)",
        [start, end],
    )
    if frame.empty:
        return
    columns = list(frame.columns)
    conn.register("_period_facts_frame", frame[columns])
    try:
        conn.execute(f"INSERT INTO {table_name} ({', '.join(columns)}) SELECT {', '.join(columns)} FROM _period_facts_frame")
    finally:
        conn.unregister("_period_facts_frame")


def _fact_columns() -> list[str]:
    return [
        "symbol",
        "period_type",
        "report_date",
        "available_at",
        "sales_cr",
        "net_profit_cr",
        "operating_profit_cr",
        "expenses_cr",
        "opm_pct",
        "npm_pct",
    ]


def _enriched_columns() -> list[str]:
    return [*_fact_columns(), "sector_name", "industry_group", "industry"]


def _symbol_columns() -> list[str]:
    return ["symbol", "name", "sector_name", "industry_group", "industry", "mcap", "source"]


__all__ = [
    "PeriodFactsResult",
    "ensure_sector_earnings_schema",
    "refresh_fundamental_period_facts",
]
