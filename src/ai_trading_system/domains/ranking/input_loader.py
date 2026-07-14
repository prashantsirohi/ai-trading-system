"""Input loading helpers for the ranking domain."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.features.indicators import add_stage2_features
from ai_trading_system.domains.features.phase1 import PHASE1_BREADTH_COLUMNS, PHASE1_SYMBOL_COLUMNS
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

    @staticmethod
    def _query_exchanges(exchanges: list[str]) -> tuple[list[str], str]:
        normalized = [str(exchange).strip().upper() for exchange in exchanges if str(exchange).strip()]
        if not normalized:
            raise ValueError("At least one exchange is required for ranking inputs")
        return normalized, ",".join("?" for _ in normalized)

    def load_latest_market_data(
        self,
        *,
        as_of: str,
        exchanges: list[str],
    ) -> pd.DataFrame:
        normalized_exchanges, exchange_placeholders = self._query_exchanges(exchanges)
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
                WHERE exchange IN ({exchange_placeholders})
                  AND timestamp IS NOT NULL
                  AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
                GROUP BY symbol_id, exchange
                """,
                [*normalized_exchanges, as_of],
            ).fetchdf()
        finally:
            conn.close()

        if frame.empty:
            return frame
        frame.loc[:, "timestamp"] = pd.to_datetime(frame["timestamp"])
        cutoff_date = pd.Timestamp(as_of).date()
        if frame["timestamp"].dt.date.gt(cutoff_date).any():
            raise RuntimeError(f"Rank market input contains rows after as_of={cutoff_date}")
        return self.normalize_symbol_exchange_columns(frame)

    def load_latest_stage2(
        self,
        *,
        date: str,
        exchanges: list[str],
        rel_strength_frame: pd.DataFrame | None = None,
        history_bars: int = 300,
    ) -> pd.DataFrame:
        normalized_exchanges, exchange_placeholders = self._query_exchanges(exchanges)
        history_bars = int(history_bars)
        if history_bars < 1 or history_bars > 5_000:
            raise ValueError("history_bars must be between 1 and 5000")
        conn = self.get_conn()
        try:
            history = conn.execute(
                f"""
                SELECT
                    symbol_id,
                    exchange,
                    timestamp,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM (
                    SELECT
                        symbol_id,
                        exchange,
                        timestamp,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        ROW_NUMBER() OVER (
                            PARTITION BY symbol_id, exchange
                            ORDER BY timestamp DESC
                        ) AS rn_desc
                    FROM _catalog
                    WHERE exchange IN ({exchange_placeholders})
                      AND timestamp IS NOT NULL
                      AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
                ) latest
                WHERE rn_desc <= ?
                ORDER BY symbol_id, exchange, timestamp
                """,
                [*normalized_exchanges, date, history_bars],
            ).fetchdf()
        finally:
            conn.close()

        if history.empty:
            return pd.DataFrame(
                columns=[
                    "symbol_id",
                    "exchange",
                    "timestamp",
                    "close",
                    "sma_20",
                    "sma_50",
                    "sma_200",
                    "sma_150",
                    "sma50_slope_20d_pct",
                    "sma200_slope_20d_pct",
                    "sma150_slope_20d_pct",
                    "sma50_sma200_gap_pct",
                    "sma50_sma200_gap_delta_5d",
                    "sma50_sma200_gap_delta_20d",
                    "sma50_sma200_gap_delta_60d",
                    "sma20_sma50_gap_pct",
                    "sma20_sma50_gap_delta_10d",
                    "golden_cross_days_since",
                    "golden_cross_failed",
                    "stage2_score",
                    "is_stage2_structural",
                    "is_stage2_candidate",
                    "is_stage2_uptrend",
                    "stage2_label",
                    "stage2_hard_fail_reason",
                    "stage2_fail_reason",
                ]
            )

        history = self.normalize_symbol_exchange_columns(history)
        history.loc[:, "timestamp"] = pd.to_datetime(history["timestamp"], errors="coerce")
        history = history.sort_values(["symbol_id", "exchange", "timestamp"], kind="stable").reset_index(drop=True)

        rs_frame = None
        if rel_strength_frame is not None and not rel_strength_frame.empty:
            rs_cols = [column for column in ["symbol_id", "exchange", "rel_strength_score"] if column in rel_strength_frame.columns]
            if len(rs_cols) == 3:
                rs_frame = (
                    rel_strength_frame[rs_cols]
                    .drop_duplicates(["symbol_id", "exchange"], keep="last")
                    .reset_index(drop=True)
                )

        latest_rows: list[pd.DataFrame] = []
        for (_, _), group in history.groupby(["symbol_id", "exchange"], sort=False):
            enriched_input = group.copy()

            high_252 = pd.to_numeric(
                enriched_input["high"].rolling(252, min_periods=1).max(),
                errors="coerce",
            ).replace(0, pd.NA)
            close = pd.to_numeric(enriched_input["close"], errors="coerce")
            volume = pd.to_numeric(enriched_input["volume"], errors="coerce")
            enriched_input.loc[:, "near_52w_high_pct"] = ((1.0 - close / high_252) * 100.0).clip(0.0, 100.0)
            vol_avg = volume.rolling(20, min_periods=10).mean().replace(0, pd.NA)
            enriched_input.loc[:, "volume_ratio_20"] = volume / vol_avg

            if rs_frame is not None:
                rs_match = rs_frame.loc[
                    (rs_frame["symbol_id"] == enriched_input["symbol_id"].iloc[-1])
                    & (rs_frame["exchange"] == enriched_input["exchange"].iloc[-1]),
                    "rel_strength_score",
                ]
                if not rs_match.empty:
                    enriched_input.loc[:, "rel_strength_score"] = float(pd.to_numeric(rs_match.iloc[-1], errors="coerce"))

            enriched = add_stage2_features(enriched_input)
            latest_rows.append(enriched.tail(1))

        if not latest_rows:
            return pd.DataFrame()

        latest = pd.concat(latest_rows, ignore_index=True)
        keep_cols = [
            "symbol_id",
            "exchange",
            "timestamp",
            "close",
            "sma_20",
            "sma_50",
            "sma_200",
            "sma_150",
            "sma50_slope_20d_pct",
            "sma200_slope_20d_pct",
            "sma150_slope_20d_pct",
            "sma50_sma200_gap_pct",
            "sma50_sma200_gap_delta_5d",
            "sma50_sma200_gap_delta_20d",
            "sma50_sma200_gap_delta_60d",
            "sma20_sma50_gap_pct",
            "sma20_sma50_gap_delta_10d",
            "golden_cross_days_since",
            "golden_cross_failed",
            "stage2_score",
            "is_stage2_structural",
            "is_stage2_candidate",
            "is_stage2_uptrend",
            "stage2_label",
            "stage2_hard_fail_reason",
            "stage2_fail_reason",
        ]
        return latest[[column for column in keep_cols if column in latest.columns]].reset_index(drop=True)

    def load_return_frame(
        self,
        *,
        period: int,
        as_of: str,
        exchanges: list[str] | None = None,
    ) -> pd.DataFrame:
        period = int(period)
        if period < 1:
            raise ValueError("period must be positive")
        normalized_exchanges, exchange_placeholders = self._query_exchanges(exchanges or ["NSE"])
        conn = self.get_conn()
        try:
            ret_data = conn.execute(
                f"""
                SELECT
                    symbol_id, exchange, timestamp,
                    close,
                    LAG(close, {period}) OVER w AS close_{period}_ago
                FROM _catalog
                WHERE exchange IN ({exchange_placeholders})
                  AND timestamp IS NOT NULL
                  AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
                WINDOW w AS (PARTITION BY symbol_id, exchange ORDER BY timestamp)
                """,
                [*normalized_exchanges, as_of],
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

    def load_return_frame_multi(
        self,
        *,
        as_of: str,
        periods: list[int] | None = None,
        exchanges: list[str] | None = None,
    ) -> pd.DataFrame:
        periods = [int(period) for period in (periods or [20, 60, 120])]
        if any(period < 1 for period in periods):
            raise ValueError("periods must contain only positive values")
        normalized_exchanges, exchange_placeholders = self._query_exchanges(exchanges or ["NSE"])
        conn = self.get_conn()
        try:
            lag_clauses = ", ".join([f"LAG(close, {p}) OVER w AS close_{p}_ago" for p in periods])
            ret_data = conn.execute(
                f"""
                SELECT
                    symbol_id, exchange, timestamp, close,
                    {lag_clauses}
                FROM _catalog
                WHERE exchange IN ({exchange_placeholders})
                  AND timestamp IS NOT NULL
                  AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
                WINDOW w AS (PARTITION BY symbol_id, exchange ORDER BY timestamp)
                """,
                [*normalized_exchanges, as_of],
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

    def load_volume_frame(
        self,
        *,
        as_of: str,
        exchanges: list[str] | None = None,
    ) -> pd.DataFrame:
        normalized_exchanges, exchange_placeholders = self._query_exchanges(exchanges or ["NSE"])
        conn = self.get_conn()
        try:
            volume = conn.execute(
                f"""
                WITH volume_features AS (
                    SELECT
                        symbol_id,
                        exchange,
                        timestamp,
                        volume,
                        AVG(volume) OVER w_current AS vol_20_avg,
                        MAX(volume) OVER w_current AS vol_20_max,
                        AVG(volume) OVER w_prior AS vol_20_avg_prior,
                        STDDEV_SAMP(volume) OVER w_prior AS vol_20_std_prior
                    FROM _catalog
                    WHERE exchange IN ({exchange_placeholders})
                      AND timestamp IS NOT NULL
                      AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
                    WINDOW
                        w_current AS (
                            PARTITION BY symbol_id, exchange
                            ORDER BY timestamp
                            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
                        ),
                        w_prior AS (
                            PARTITION BY symbol_id, exchange
                            ORDER BY timestamp
                            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                        )
                )
                SELECT
                    symbol_id,
                    exchange,
                    volume,
                    vol_20_avg,
                    vol_20_max,
                    CASE
                        WHEN vol_20_std_prior IS NULL OR vol_20_std_prior = 0 THEN NULL
                        ELSE (volume - vol_20_avg_prior) / vol_20_std_prior
                    END AS volume_zscore_20
                FROM volume_features
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id, exchange ORDER BY timestamp DESC
                ) = 1
                """,
                [*normalized_exchanges, as_of],
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
            adx_latest = conn.execute(
                """
                SELECT *
                FROM read_parquet(?, union_by_name = true)
                WHERE CAST(timestamp AS DATE) <= CAST(? AS DATE)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id ORDER BY timestamp DESC
                ) = 1
                """,
                [str(adx_path / "*.parquet"), date],
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
            sma_latest = conn.execute(
                """
                SELECT symbol_id, exchange, close, sma_20, sma_50, sma_200, sma_200_bars, timestamp
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
                        ) AS sma_50,
                        AVG(close) OVER (
                            PARTITION BY symbol_id
                            ORDER BY timestamp
                            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                        ) AS sma_200,
                        COUNT(close) OVER (
                            PARTITION BY symbol_id
                            ORDER BY timestamp
                            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                        ) AS sma_200_bars
                    FROM _catalog
                    WHERE exchange = 'NSE'
                      AND timestamp IS NOT NULL
                      AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
                ) sub
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id ORDER BY timestamp DESC
                ) = 1
                """,
                [date],
            ).fetchdf()
        finally:
            conn.close()
        return sma_latest

    def load_latest_highs(self, *, date: str, window: int) -> pd.DataFrame:
        window = int(window)
        if window < 1 or window > 5_000:
            raise ValueError("window must be between 1 and 5000")
        conn = self.get_conn()
        try:
            highs = conn.execute(
                f"""
                SELECT symbol_id, exchange, close, high_52w, prox_lookback_days, timestamp
                FROM (
                    SELECT
                        symbol_id, exchange, close, timestamp,
                        MAX(high) OVER w AS high_52w,
                        COUNT(*) OVER w AS prox_lookback_days
                    FROM _catalog
                    WHERE exchange = 'NSE'
                      AND timestamp IS NOT NULL
                      AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
                    WINDOW w AS (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW
                    )
                ) sub
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id ORDER BY timestamp DESC
                ) = 1
                """,
                [date],
            ).fetchdf()
        finally:
            conn.close()
        return highs

    def load_latest_delivery(self, *, date: str) -> pd.DataFrame:
        conn = self.get_conn()
        try:
            delivery = conn.execute(
                """
                SELECT symbol_id, exchange, delivery_pct
                FROM _delivery
                WHERE CAST(timestamp AS DATE) <= CAST(? AS DATE)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id, exchange
                    ORDER BY timestamp DESC
                ) = 1
                """,
                [date],
            ).fetchdf()
        finally:
            conn.close()
        return self.normalize_symbol_exchange_columns(delivery)

    def load_latest_phase1_symbol_features(self, *, date: str, exchange: str = "NSE") -> pd.DataFrame:
        """Load latest persisted Phase 1 symbol features as of date."""
        columns = ["symbol_id", "exchange", "timestamp", *PHASE1_SYMBOL_COLUMNS]
        conn = self.get_conn()
        try:
            exists = bool(
                conn.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'feat_phase1_symbol_features'"
                ).fetchone()[0]
            )
            if not exists:
                return pd.DataFrame(columns=columns)
            frame = conn.execute(
                f"""
                SELECT {", ".join(columns)}
                FROM feat_phase1_symbol_features
                WHERE exchange = ?
                  AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id, exchange
                    ORDER BY timestamp DESC
                ) = 1
                """,
                [exchange, date],
            ).fetchdf()
        finally:
            conn.close()
        return self.normalize_symbol_exchange_columns(frame)

    def load_latest_market_breadth(self, date: str, exchange: str = "NSE") -> pd.DataFrame:
        """Load latest persisted Phase 1 market breadth as of date."""
        columns = ["symbol_id", "exchange", "timestamp", *PHASE1_BREADTH_COLUMNS]
        conn = self.get_conn()
        try:
            exists = bool(
                conn.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'feat_phase1_market_breadth'"
                ).fetchone()[0]
            )
            if not exists:
                return pd.DataFrame(columns=columns)
            return conn.execute(
                f"""
                SELECT {", ".join(columns)}
                FROM feat_phase1_market_breadth
                WHERE exchange = ?
                  AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                [exchange, date],
            ).fetchdf()
        finally:
            conn.close()

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
