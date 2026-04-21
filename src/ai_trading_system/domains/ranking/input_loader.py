"""Input loading helpers for the ranking domain."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.platform.logging.logger import logger


class RankerInputLoader:
    """Load ranking inputs from DuckDB, parquet features, and master data."""

    def __init__(self, *, ohlcv_db_path: str, feature_store_dir: str, master_db_path: str):
        self.ohlcv_db_path = ohlcv_db_path
        self.feature_store_dir = feature_store_dir
        self.master_db_path = master_db_path
        self._sector_rs_cache: pd.DataFrame | None = None
        self._stock_vs_sector_cache: pd.DataFrame | None = None
        self._sector_map_cache: dict[str, str] | None = None

    def get_conn(self):
        return duckdb.connect(self.ohlcv_db_path, read_only=True)

    def latest_available_date(self, *, exchange: str = "NSE") -> str | None:
        conn = self.get_conn()
        try:
            latest = conn.execute(
                "SELECT MAX(timestamp) FROM _catalog WHERE exchange = ?",
                [exchange],
            ).fetchone()[0]
        finally:
            conn.close()
        if latest is None:
            return None
        return str(latest.date()) if hasattr(latest, "date") else str(latest)[:10]

    def normalize_symbol_exchange_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """Repair rows where symbol_id/exchange were swapped in legacy loads."""
        if data.empty or "symbol_id" not in data.columns or "exchange" not in data.columns:
            return data

        normalized = data.copy()
        valid_exchanges = {"NSE", "BSE"}
        swap_mask = normalized["symbol_id"].isin(valid_exchanges) & ~normalized["exchange"].isin(valid_exchanges)
        if swap_mask.any():
            logger.warning(
                "Detected %s rows with swapped symbol_id/exchange columns; normalizing in ranker",
                int(swap_mask.sum()),
            )
            original_symbol = normalized.loc[swap_mask, "symbol_id"].copy()
            normalized.loc[swap_mask, "symbol_id"] = normalized.loc[swap_mask, "exchange"].astype(str)
            normalized.loc[swap_mask, "exchange"] = original_symbol.astype(str)
        return normalized

    def load_latest_market_data(self, *, exchanges: list[str]) -> pd.DataFrame:
        conn = self.get_conn()
        try:
            frame = conn.execute(
                f"""
                SELECT
                    symbol_id,
                    exchange,
                    MAX(timestamp) AS timestamp,
                    arg_max(close, timestamp) AS close,
                    arg_max(volume, timestamp) AS volume,
                    arg_max(high, timestamp) AS high,
                    arg_max(low, timestamp) AS low,
                    arg_max(open, timestamp) AS open
                FROM _catalog
                WHERE exchange IN ({",".join(f"'{exchange}'" for exchange in exchanges)})
                  AND timestamp IS NOT NULL
                GROUP BY symbol_id, exchange
                """
            ).fetchdf()
        finally:
            conn.close()

        if frame.empty:
            return frame
        frame.loc[:, "timestamp"] = pd.to_datetime(frame["timestamp"])
        return self.normalize_symbol_exchange_columns(frame)

    def load_return_frame(self, *, period: int) -> pd.DataFrame:
        conn = self.get_conn()
        try:
            ret_data = conn.execute(
                f"""
                SELECT
                    symbol_id, exchange, timestamp,
                    close,
                    LAG(close, {period}) OVER w AS close_{period}_ago
                FROM _catalog
                WHERE exchange = 'NSE'
                WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
                """
            ).fetchdf()
        finally:
            conn.close()

        if ret_data.empty:
            return pd.DataFrame(columns=["symbol_id", "exchange", "return_pct"])

        ret_data.loc[:, "return_pct"] = (
            (ret_data["close"] - ret_data[f"close_{period}_ago"])
            / ret_data[f"close_{period}_ago"].replace(0, float("nan"))
            * 100
        )
        ret_data = ret_data.dropna(subset=["return_pct"])
        ret_data.loc[:, "timestamp"] = pd.to_datetime(ret_data["timestamp"], errors="coerce")
        ret_data = ret_data.sort_values(["symbol_id", "exchange", "timestamp"])
        ret_data = ret_data.drop_duplicates(["symbol_id", "exchange"], keep="last")
        return ret_data[["symbol_id", "exchange", "return_pct"]]

    def load_return_frame_multi(self, *, periods: list[int] = None) -> pd.DataFrame:
        periods = periods or [20, 60, 120]
        conn = self.get_conn()
        try:
            lag_clauses = ", ".join([f"LAG(close, {p}) OVER w AS close_{p}_ago" for p in periods])
            ret_data = conn.execute(
                f"""
                SELECT
                    symbol_id, exchange, timestamp, close,
                    {lag_clauses}
                FROM _catalog
                WHERE exchange = 'NSE'
                WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
                """
            ).fetchdf()
        finally:
            conn.close()

        if ret_data.empty:
            cols = ["symbol_id", "exchange"] + [f"return_{p}" for p in periods]
            return pd.DataFrame(columns=cols)

        for p in periods:
            col_name = f"close_{p}_ago"
            if col_name in ret_data.columns:
                ret_data.loc[:, f"return_{p}"] = (
                    (ret_data["close"] - ret_data[col_name])
                    / ret_data[col_name].replace(0, float("nan"))
                    * 100
                )

        ret_data = ret_data.dropna(subset=[f"return_{p}" for p in periods if f"return_{p}" in ret_data.columns])
        ret_data.loc[:, "timestamp"] = pd.to_datetime(ret_data["timestamp"], errors="coerce")
        ret_data = ret_data.sort_values(["symbol_id", "exchange", "timestamp"])
        ret_data = ret_data.drop_duplicates(["symbol_id", "exchange"], keep="last")

        return_cols = ["symbol_id", "exchange"] + [f"return_{p}" for p in periods]
        existing_cols = [c for c in return_cols if c in ret_data.columns]
        return ret_data[existing_cols]

    def load_volume_frame(self) -> pd.DataFrame:
        conn = self.get_conn()
        try:
            volume = conn.execute(
                """
                SELECT
                    symbol_id,
                    exchange,
                    volume,
                    AVG(volume) OVER w AS vol_20_avg,
                    MAX(volume) OVER w AS vol_20_max
                FROM _catalog
                WHERE exchange = 'NSE'
                WINDOW w AS (
                    PARTITION BY symbol_id
                    ORDER BY timestamp
                    ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
                )
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id ORDER BY timestamp DESC
                ) = 1
                """
            ).fetchdf()
        finally:
            conn.close()
        return volume

    def load_latest_adx(self, *, date: str) -> pd.DataFrame:
        adx_path = Path(self.feature_store_dir) / "adx" / "NSE"
        if not adx_path.exists():
            return pd.DataFrame(columns=["symbol_id", "exchange", "adx_14"])

        conn = self.get_conn()
        try:
            cutoff_ts = pd.to_datetime(date).strftime("%Y-%m-%d")
            adx_latest = conn.execute(
                f"""
                SELECT *
                FROM read_parquet('{adx_path}/*.parquet')
                WHERE timestamp <= '{cutoff_ts}'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id ORDER BY timestamp DESC
                ) = 1
                """
            ).fetchdf()
        finally:
            conn.close()

        if adx_latest.empty:
            return pd.DataFrame(columns=["symbol_id", "exchange", "adx_14"])

        adx_latest = self.normalize_symbol_exchange_columns(adx_latest)
        if "adx_14" not in adx_latest.columns and "adx_value" in adx_latest.columns:
            adx_latest.loc[:, "adx_14"] = adx_latest["adx_value"]
        return adx_latest

    def load_latest_sma(self, *, date: str) -> pd.DataFrame:
        conn = self.get_conn()
        try:
            cutoff_ts = pd.to_datetime(date).strftime("%Y-%m-%d")
            sma_latest = conn.execute(
                f"""
                SELECT symbol_id, exchange, close, sma_20, sma_50, timestamp
                FROM (
                    SELECT
                        symbol_id, exchange, close, timestamp,
                        AVG(close) OVER (
                            PARTITION BY symbol_id
                            ORDER BY timestamp
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ) AS sma_20,
                        AVG(close) OVER (
                            PARTITION BY symbol_id
                            ORDER BY timestamp
                            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                        ) AS sma_50
                    FROM _catalog
                    WHERE exchange = 'NSE'
                      AND timestamp IS NOT NULL
                      AND timestamp <= '{cutoff_ts}'
                ) sub
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id ORDER BY timestamp DESC
                ) = 1
                """
            ).fetchdf()
        finally:
            conn.close()
        return sma_latest

    def load_latest_highs(self, *, date: str, window: int) -> pd.DataFrame:
        conn = self.get_conn()
        try:
            cutoff_ts = pd.to_datetime(date).strftime("%Y-%m-%d")
            highs = conn.execute(
                f"""
                SELECT symbol_id, exchange, close, high_52w, timestamp
                FROM (
                    SELECT
                        symbol_id, exchange, close, timestamp,
                        MAX(high) OVER (
                            PARTITION BY symbol_id
                            ORDER BY timestamp
                            ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW
                        ) AS high_52w
                    FROM _catalog
                    WHERE exchange = 'NSE'
                      AND timestamp IS NOT NULL
                      AND timestamp <= '{cutoff_ts}'
                ) sub
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id ORDER BY timestamp DESC
                ) = 1
                """
            ).fetchdf()
        finally:
            conn.close()
        return highs

    def load_latest_delivery(self, *, date: str) -> pd.DataFrame:
        conn = self.get_conn()
        try:
            cutoff_ts = pd.to_datetime(date).strftime("%Y-%m-%d")
            delivery = conn.execute(
                f"""
                SELECT symbol_id, exchange, delivery_pct
                FROM _delivery
                WHERE timestamp <= '{cutoff_ts}'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id, exchange
                    ORDER BY timestamp DESC
                ) = 1
                """
            ).fetchdf()
        finally:
            conn.close()
        return self.normalize_symbol_exchange_columns(delivery)

    def load_sector_inputs(self) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str]]:
        if (
            self._sector_rs_cache is not None
            and self._stock_vs_sector_cache is not None
            and self._sector_map_cache is not None
        ):
            return self._sector_rs_cache, self._stock_vs_sector_cache, self._sector_map_cache

        all_symbols_dir = Path(self.feature_store_dir) / "all_symbols"
        sector_rs_path = all_symbols_dir / "sector_rs.parquet"
        stock_vs_sector_path = all_symbols_dir / "stock_vs_sector.parquet"

        if sector_rs_path.exists():
            sector_rs = pd.read_parquet(sector_rs_path)
            sector_rs.index = pd.to_datetime(sector_rs.index).normalize()
            sector_rs = sector_rs[~sector_rs.index.duplicated(keep="last")]
        else:
            sector_rs = pd.DataFrame()

        if stock_vs_sector_path.exists():
            stock_vs_sector = pd.read_parquet(stock_vs_sector_path)
            stock_vs_sector.index = pd.to_datetime(stock_vs_sector.index).normalize()
            stock_vs_sector = stock_vs_sector[~stock_vs_sector.index.duplicated(keep="last")]
        else:
            stock_vs_sector = pd.DataFrame()

        sector_map: dict[str, str] = {}
        master_db_path = Path(self.master_db_path)
        if master_db_path.exists():
            conn = sqlite3.connect(master_db_path)
            try:
                rows = conn.execute("""
                    SELECT s.symbol_id, COALESCE(sm.system_sector, 'Other')
                    FROM symbols s
                    LEFT JOIN sector_mapping sm ON s.sector = sm.industry
                    WHERE s.exchange = 'NSE'
                """).fetchall()
                sector_map = {symbol: sector for symbol, sector in rows}
            finally:
                conn.close()

        self._sector_rs_cache = sector_rs
        self._stock_vs_sector_cache = stock_vs_sector
        self._sector_map_cache = sector_map
        return sector_rs, stock_vs_sector, sector_map
