"""Shared data-loading helpers for ranking output generators."""

from __future__ import annotations

import sqlite3

import pandas as pd
import pyarrow.parquet as pq


def load_sector_rs() -> pd.DataFrame:
    """Load sector RS from parquet, normalize dates."""
    df = pq.read_table("data/feature_store/all_symbols/sector_rs.parquet").to_pandas()
    df.index = pd.to_datetime(df.index).normalize()
    df = df[~df.index.duplicated(keep="last")]
    return df


def load_stock_vs_sector() -> pd.DataFrame:
    """Load stock vs sector RS from parquet, normalize dates."""
    df = pq.read_table(
        "data/feature_store/all_symbols/stock_vs_sector.parquet"
    ).to_pandas()
    df.index = pd.to_datetime(df.index).normalize()
    df = df[~df.index.duplicated(keep="last")]
    return df


def load_sector_mapping() -> pd.DataFrame:
    """Load symbol to sector mapping from SQLite using sector_mapping table."""
    conn = sqlite3.connect("data/masterdata.db")
    df = pd.read_sql("""
        SELECT s.symbol_id as symbol, COALESCE(sm.system_sector, 'Other') as sector
        FROM symbols s
        LEFT JOIN sector_mapping sm ON s.sector = sm.industry
        WHERE s.exchange = 'NSE'
    """, conn)
    conn.close()
    return df


def load_sector_map() -> dict:
    """Load symbol to sector mapping as dictionary using sector_mapping table."""
    conn = sqlite3.connect("data/masterdata.db")
    rows = conn.execute("""
        SELECT s.symbol_id, COALESCE(sm.system_sector, 'Other')
        FROM symbols s
        LEFT JOIN sector_mapping sm ON s.sector = sm.industry
        WHERE s.exchange = 'NSE'
    """).fetchall()
    conn.close()
    return {symbol: sector for symbol, sector in rows}


def compute_stock_rs_full(
    stock_vs_sector: pd.DataFrame, sector_rs: pd.DataFrame, sector_mapping: pd.DataFrame
) -> pd.DataFrame:
    """Compute full stock RS = sector RS (for each stock's sector) + stock vs sector."""
    sector_map = dict(zip(sector_mapping["symbol"], sector_mapping["sector"]))

    stock_rs_full = stock_vs_sector.copy()

    for stock in stock_vs_sector.columns:
        sector = sector_map.get(stock)
        if sector and sector in sector_rs.columns:
            sector_rs_vals = sector_rs[sector].ffill()
            stock_vs_vals = stock_vs_sector[stock].ffill()
            stock_rs_full[stock] = sector_rs_vals + stock_vs_vals

    return stock_rs_full

