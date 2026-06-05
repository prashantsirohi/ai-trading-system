"""DuckDB schema for point-in-time valuation features."""

from __future__ import annotations

import duckdb


def ensure_valuation_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create valuation feature tables and useful indexes."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS universe_definition (
            universe_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            method VARCHAR,
            base_date DATE,
            base_level DOUBLE DEFAULT 1000,
            created_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS universe_membership (
            universe_id VARCHAR NOT NULL,
            as_of_date DATE NOT NULL,
            symbol VARCHAR NOT NULL,
            sector_name VARCHAR,
            industry_group VARCHAR,
            market_cap_rank INTEGER,
            included BOOLEAN DEFAULT TRUE,
            reason VARCHAR,
            PRIMARY KEY (universe_id, as_of_date, symbol)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamental_ttm (
            symbol VARCHAR NOT NULL,
            as_of_date DATE NOT NULL,
            ttm_sales_cr DOUBLE,
            ttm_net_profit_cr DOUBLE,
            ttm_operating_profit_cr DOUBLE,
            adjusted_equity_shares_cr DOUBLE,
            book_value_cr DOUBLE,
            earnings_source VARCHAR,
            source_batch_id VARCHAR,
            created_at TIMESTAMP,
            PRIMARY KEY (symbol, as_of_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_valuation_daily (
            universe_id VARCHAR NOT NULL,
            date DATE NOT NULL,
            symbol VARCHAR NOT NULL,
            sector_name VARCHAR,
            close DOUBLE,
            adjusted_equity_shares_cr DOUBLE,
            market_cap_cr DOUBLE,
            ttm_sales_cr DOUBLE,
            ttm_net_profit_cr DOUBLE,
            book_value_cr DOUBLE,
            pe_ttm DOUBLE,
            ps_ttm DOUBLE,
            pb DOUBLE,
            earnings_yield DOUBLE,
            valuation_warning VARCHAR,
            earnings_source VARCHAR,
            PRIMARY KEY (universe_id, date, symbol)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS universe_index_daily (
            universe_id VARCHAR NOT NULL,
            index_type VARCHAR NOT NULL,
            date DATE NOT NULL,
            level DOUBLE,
            return_1d DOUBLE,
            constituent_count INTEGER,
            total_market_cap_cr DOUBLE,
            total_ttm_profit_cr DOUBLE,
            pe_ttm DOUBLE,
            earnings_yield DOUBLE,
            PRIMARY KEY (universe_id, index_type, date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sector_valuation_daily (
            universe_id VARCHAR NOT NULL,
            date DATE NOT NULL,
            sector_name VARCHAR NOT NULL,
            constituent_count INTEGER,
            positive_earnings_count INTEGER,
            loss_making_count INTEGER,
            total_market_cap_cr DOUBLE,
            total_ttm_profit_cr DOUBLE,
            pe_ttm DOUBLE,
            pe_median DOUBLE,
            pe_trimmed_avg DOUBLE,
            earnings_yield DOUBLE,
            loss_mcap_pct DOUBLE,
            PRIMARY KEY (universe_id, date, sector_name)
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
            earnings_yield DOUBLE,
            pe_pctile_3y DOUBLE,
            pe_pctile_5y DOUBLE,
            pe_pctile_10y DOUBLE,
            pe_median_5y DOUBLE,
            pe_avg_5y DOUBLE,
            pe_zscore_3y DOUBLE,
            pe_zscore_5y DOUBLE,
            pe_zscore_10y DOUBLE,
            valuation_zone VARCHAR,
            cycle_signal VARCHAR,
            PRIMARY KEY (entity_type, entity_id, date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_valuation_bands (
            universe_id VARCHAR NOT NULL,
            date DATE NOT NULL,
            symbol VARCHAR NOT NULL,
            sector_name VARCHAR,
            pe_ttm DOUBLE,
            ps_ttm DOUBLE,
            pb DOUBLE,
            pe_median_3y DOUBLE,
            pe_median_5y DOUBLE,
            ps_median_3y DOUBLE,
            ps_median_5y DOUBLE,
            pb_median_3y DOUBLE,
            pb_median_5y DOUBLE,
            pe_pctile_3y DOUBLE,
            pe_pctile_5y DOUBLE,
            ps_pctile_3y DOUBLE,
            ps_pctile_5y DOUBLE,
            pb_pctile_3y DOUBLE,
            pb_pctile_5y DOUBLE,
            pe_vs_3y_median_pct DOUBLE,
            pe_vs_5y_median_pct DOUBLE,
            ps_vs_3y_median_pct DOUBLE,
            ps_vs_5y_median_pct DOUBLE,
            pb_vs_3y_median_pct DOUBLE,
            pb_vs_5y_median_pct DOUBLE,
            valuation_history_score DOUBLE,
            valuation_history_bucket VARCHAR,
            valuation_reason VARCHAR,
            observations_3y INTEGER,
            observations_5y INTEGER,
            created_at TIMESTAMP,
            PRIMARY KEY (universe_id, date, symbol)
        )
        """
    )
    for statement in (
        "ALTER TABLE fundamental_ttm ADD COLUMN IF NOT EXISTS ttm_sales_cr DOUBLE",
        "ALTER TABLE fundamental_ttm ADD COLUMN IF NOT EXISTS ttm_net_profit_cr DOUBLE",
        "ALTER TABLE fundamental_ttm ADD COLUMN IF NOT EXISTS ttm_operating_profit_cr DOUBLE",
        "ALTER TABLE fundamental_ttm ADD COLUMN IF NOT EXISTS adjusted_equity_shares_cr DOUBLE",
        "ALTER TABLE fundamental_ttm ADD COLUMN IF NOT EXISTS book_value_cr DOUBLE",
        "ALTER TABLE stock_valuation_daily ADD COLUMN IF NOT EXISTS ttm_sales_cr DOUBLE",
        "ALTER TABLE stock_valuation_daily ADD COLUMN IF NOT EXISTS book_value_cr DOUBLE",
        "ALTER TABLE stock_valuation_daily ADD COLUMN IF NOT EXISTS ps_ttm DOUBLE",
        "ALTER TABLE stock_valuation_daily ADD COLUMN IF NOT EXISTS pb DOUBLE",
        "ALTER TABLE stock_valuation_daily ADD COLUMN IF NOT EXISTS valuation_warning VARCHAR",
        "ALTER TABLE valuation_cycle_features ADD COLUMN IF NOT EXISTS pe_median_5y DOUBLE",
        "ALTER TABLE valuation_cycle_features ADD COLUMN IF NOT EXISTS pe_avg_5y DOUBLE",
        "CREATE INDEX IF NOT EXISTS idx_fundamental_ttm_symbol_date ON fundamental_ttm(symbol, as_of_date)",
        "CREATE INDEX IF NOT EXISTS idx_stock_valuation_universe_date ON stock_valuation_daily(universe_id, date)",
        "CREATE INDEX IF NOT EXISTS idx_stock_valuation_symbol_date ON stock_valuation_daily(symbol, date)",
        "CREATE INDEX IF NOT EXISTS idx_stock_valuation_bands_symbol_date ON stock_valuation_bands(symbol, date)",
        "CREATE INDEX IF NOT EXISTS idx_sector_valuation_universe_date ON sector_valuation_daily(universe_id, date)",
        "CREATE INDEX IF NOT EXISTS idx_universe_index_id_type_date ON universe_index_daily(universe_id, index_type, date)",
        "CREATE INDEX IF NOT EXISTS idx_valuation_cycle_entity_date ON valuation_cycle_features(entity_type, entity_id, date)",
    ):
        conn.execute(statement)


__all__ = ["ensure_valuation_schema"]
