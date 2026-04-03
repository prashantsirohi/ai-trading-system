import os
import sqlite3
import duckdb
import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from utils.data_domains import ensure_domain_layout
from utils.logger import logger


class StockRanker:
    """
    Multi-Factor Weighted Ranking Engine.

    Factors & weights:
      - Relative Strength  (35%): Return vs Nifty 50 benchmark over N periods
      - Volume Intensity   (25%): Volume spike relative to 20-day avg
      - Trend Persistence  (15%): ADX score + price above key MAs
      - Proximity to Highs (30%): (52w_high - close) / 52w_high

    Penalties:
      - Stocks down >30% over 1 year receive a -30 point penalty on composite score.

    All scores are percentile-ranked [0-100] per factor, then
    weighted-summed to produce a composite score.
    """

    WEIGHTS = {
        "relative_strength": 0.25,
        "volume_intensity": 0.18,
        "trend_persistence": 0.15,
        "proximity_highs": 0.17,
        "delivery_pct": 0.10,
        "sector_strength": 0.15,
    }

    def __init__(
        self,
        ohlcv_db_path: str = None,
        feature_store_dir: str = None,
        data_domain: str = "operational",
    ):
        paths = ensure_domain_layout(
            project_root=os.path.dirname(os.path.dirname(__file__)),
            data_domain=data_domain,
        )
        if ohlcv_db_path is None:
            ohlcv_db_path = str(paths.ohlcv_db_path)
        if feature_store_dir is None:
            feature_store_dir = str(paths.feature_store_dir)
        self.ohlcv_db_path = ohlcv_db_path
        self.feature_store_dir = feature_store_dir
        self.data_domain = data_domain
        self.master_db_path = str(paths.master_db_path)
        self._sector_rs_cache: pd.DataFrame | None = None
        self._stock_vs_sector_cache: pd.DataFrame | None = None
        self._sector_map_cache: dict[str, str] | None = None
        os.makedirs(self.feature_store_dir, exist_ok=True)

    def _get_conn(self):
        return duckdb.connect(self.ohlcv_db_path, read_only=True)

    def _normalize_symbol_exchange_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """Repair rows where symbol_id/exchange were swapped in legacy operational loads."""
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

    def _load_sector_inputs(self) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str]]:
        """Lazily load sector leadership inputs for ranking."""
        if (
            self._sector_rs_cache is not None
            and self._stock_vs_sector_cache is not None
            and self._sector_map_cache is not None
        ):
            return (
                self._sector_rs_cache,
                self._stock_vs_sector_cache,
                self._sector_map_cache,
            )

        all_symbols_dir = os.path.join(self.feature_store_dir, "all_symbols")
        sector_rs_path = os.path.join(all_symbols_dir, "sector_rs.parquet")
        stock_vs_sector_path = os.path.join(all_symbols_dir, "stock_vs_sector.parquet")

        if os.path.exists(sector_rs_path):
            sector_rs = pd.read_parquet(sector_rs_path)
            sector_rs.index = pd.to_datetime(sector_rs.index).normalize()
            sector_rs = sector_rs[~sector_rs.index.duplicated(keep="last")]
        else:
            sector_rs = pd.DataFrame()

        if os.path.exists(stock_vs_sector_path):
            stock_vs_sector = pd.read_parquet(stock_vs_sector_path)
            stock_vs_sector.index = pd.to_datetime(stock_vs_sector.index).normalize()
            stock_vs_sector = stock_vs_sector[
                ~stock_vs_sector.index.duplicated(keep="last")
            ]
        else:
            stock_vs_sector = pd.DataFrame()

        sector_map: dict[str, str] = {}
        if os.path.exists(self.master_db_path):
            conn = sqlite3.connect(self.master_db_path)
            try:
                rows = conn.execute(
                    "SELECT Symbol, Sector FROM stock_details WHERE Symbol IS NOT NULL"
                ).fetchall()
                sector_map = {symbol: sector for symbol, sector in rows if sector}
            finally:
                conn.close()

        self._sector_rs_cache = sector_rs
        self._stock_vs_sector_cache = stock_vs_sector
        self._sector_map_cache = sector_map
        return sector_rs, stock_vs_sector, sector_map

    def rank_all(
        self,
        date: str = None,
        exchanges: List[str] = None,
        min_score: float = 50.0,
        top_n: int = None,
        benchmark_symbol: str = "NIFTY50",
        weights: Dict[str, float] = None,
    ) -> pd.DataFrame:
        """
        Rank all symbols for a given date.

        Args:
            date: YYYY-MM-DD. Defaults to latest available.
            exchanges: List of exchanges. Defaults to ['NSE'].
            min_score: Minimum composite score to include.
            top_n: Return only top N stocks.
            benchmark_symbol: Symbol to use as benchmark for relative strength.

        Returns:
            DataFrame with columns: symbol_id, exchange, composite_score,
            relative_strength_score, volume_intensity_score,
            trend_persistence_score, proximity_highs_score, regime
        """
        if weights is None:
            weights = self.WEIGHTS

        if exchanges is None:
            exchanges = ["NSE"]

        if date is None:
            conn = self._get_conn()
            latest = conn.execute(
                "SELECT MAX(timestamp) FROM _catalog WHERE exchange = 'NSE'"
            ).fetchone()[0]
            conn.close()
            date = str(latest.date()) if hasattr(latest, "date") else str(latest)[:10]

        logger.info(f"Ranking stocks for date={date}, exchanges={exchanges}")

        conn = self._get_conn()
        try:
            all_data = conn.execute(f"""
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
                WHERE exchange IN ({",".join(f"'{e}'" for e in exchanges)})
                  AND timestamp IS NOT NULL
                GROUP BY symbol_id, exchange
            """).fetchdf()
        finally:
            conn.close()

        if all_data.empty:
            logger.warning("No data available for ranking")
            return pd.DataFrame()

        all_data["timestamp"] = pd.to_datetime(all_data["timestamp"])
        all_data = self._normalize_symbol_exchange_columns(all_data)

        scores = self._compute_relative_strength(all_data, date, benchmark_symbol)
        scores = self._compute_volume_intensity(scores)
        scores = self._compute_trend_persistence(scores, date)
        scores = self._compute_proximity_highs(scores, date)
        scores = self._compute_delivery(scores, date)
        scores = self._compute_sector_strength(scores, date)

        for factor, col in [
            ("relative_strength", "rel_strength"),
            ("volume_intensity", "vol_intensity"),
            ("trend_persistence", "trend_score"),
            ("proximity_highs", "prox_high"),
            ("delivery_pct", "delivery_pct"),
        ]:
            scores[f"{col}_score"] = scores[col].rank(pct=True) * 100

        scores["sector_rs_score"] = scores["sector_rs_value"].rank(pct=True) * 100
        scores["stock_vs_sector_score"] = (
            scores["stock_vs_sector_value"].rank(pct=True) * 100
        )
        scores["sector_strength_score"] = (
            scores["sector_rs_score"] * 0.6 + scores["stock_vs_sector_score"] * 0.4
        )

        scores["composite_score"] = sum(
            scores[f"{col}_score"] * w
            for col, w in [
                ("rel_strength", weights["relative_strength"]),
                ("vol_intensity", weights["volume_intensity"]),
                ("trend_score", weights["trend_persistence"]),
                ("prox_high", weights["proximity_highs"]),
                ("delivery_pct", weights["delivery_pct"]),
            ]
        ) + scores["sector_strength_score"] * weights["sector_strength"]

        # scores = self._apply_1yr_penalty(scores, weights)

        scores = scores.sort_values("composite_score", ascending=False)
        scores = scores[scores["composite_score"] >= min_score]

        if top_n:
            scores = scores.head(top_n)

        cols = [
            "symbol_id",
            "exchange",
            "close",
            "composite_score",
            # Normalized factor scores.
            "rel_strength_score",
            "vol_intensity_score",
            "trend_score_score",
            "prox_high_score",
            "delivery_pct_score",
            "sector_strength_score",
            # Raw factor metrics for attribution/explainability.
            "rel_strength",
            "vol_intensity",
            "trend_score",
            "prox_high",
            "delivery_pct",
            "sector_rs_value",
            "stock_vs_sector_value",
            "sector_name",
            # Supporting context commonly needed in downstream UIs.
            "high_52w",
            "vol_20_avg",
            "adx_14",
            "sma_20",
            "sma_50",
            "volume",
            "timestamp",
        ]
        available = [c for c in cols if c in scores.columns]
        return scores[available].reset_index(drop=True)

    def _apply_1yr_penalty(
        self,
        scores: pd.DataFrame,
        weights: Dict[str, float] = None,
    ) -> pd.DataFrame:
        """
        Apply a penalty to stocks down >30% over 1 year.
        Penalty = proximity_highs_weight * 30 points.
        Reduces false signals from stocks in long-term downtrends.
        """
        if weights is None:
            weights = self.WEIGHTS

        penalty = weights["proximity_highs"] * 30
        try:
            conn = self._get_conn()
            try:
                yr_return = conn.execute("""
                    SELECT
                        symbol_id, exchange, close,
                        LAG(close, 252) OVER (
                            PARTITION BY symbol_id ORDER BY timestamp
                        ) AS close_1yr_ago
                    FROM _catalog
                    WHERE exchange = 'NSE'
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY symbol_id ORDER BY timestamp DESC
                    ) = 1
                """).fetchdf()
            finally:
                conn.close()

            if "close_1yr_ago" not in yr_return.columns:
                return scores

            yr_return["ret_1yr"] = (
                (yr_return["close"] - yr_return["close_1yr_ago"])
                / yr_return["close_1yr_ago"].replace(0, np.nan)
            ).fillna(0) * 100

            scores = scores.merge(
                yr_return[["symbol_id", "exchange", "ret_1yr"]],
                on=["symbol_id", "exchange"],
                how="left",
            )

            penalty_mask = scores["ret_1yr"].fillna(0) < -30
            scores.loc[penalty_mask, "composite_score"] -= 30
            scores.loc[penalty_mask, "prox_high_score"] = (
                scores.loc[penalty_mask, "prox_high_score"] * 0.5
            )

            scores.drop(columns=["ret_1yr"], inplace=True, errors="ignore")
            logger.info(
                f"1-year penalty applied to {penalty_mask.sum()} stocks "
                f"(down >30% over 1 year)"
            )
        except Exception as e:
            logger.warning(f"Could not apply 1-year penalty: {e}")

        return scores

    def _compute_relative_strength(
        self,
        data: pd.DataFrame,
        date: str,
        benchmark_symbol: str,
        periods: List[int] = None,
    ) -> pd.DataFrame:
        """
        Relative Strength = stock_return(period) - benchmark_return(period).
        Uses 20-day (1-month) return as primary signal.
        """
        if periods is None:
            periods = [20]

        conn = self._get_conn()
        try:
            p = periods[0]
            ret_data = conn.execute(f"""
                SELECT
                    symbol_id, exchange,
                    close,
                    LAG(close, {p}) OVER w AS close_{p}_ago
                FROM _catalog
                WHERE exchange = 'NSE'
                WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
            """).fetchdf()

            ret_data["return_pct"] = (
                (ret_data["close"] - ret_data[f"close_{p}_ago"])
                / ret_data[f"close_{p}_ago"].replace(0, float("nan"))
                * 100
            )
            conn.close()

            ret_data = self._normalize_symbol_exchange_columns(ret_data)
            ret_data = ret_data.dropna(subset=["return_pct"])
            ret_data = ret_data.drop_duplicates(["symbol_id", "exchange"])
            data = data.merge(
                ret_data[["symbol_id", "exchange", "return_pct"]],
                on=["symbol_id", "exchange"],
                how="left",
            )
        except Exception as e:
            logger.warning(f"Could not compute relative strength: {e}")

        if "return_pct" not in data.columns:
            data["return_pct"] = 0.0
        data["rel_strength"] = data["return_pct"].fillna(0)
        return data

    def _compute_volume_intensity(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Volume Intensity = today's volume / 20-day average volume.
        High ratio indicates institutional conviction.
        """
        conn = self._get_conn()
        try:
            vol_data = conn.execute("""
                SELECT
                    symbol_id,
                    exchange,
                    volume,
                    AVG(volume) OVER w AS vol_20_avg,
                    MAX(volume) OVER w AS vol_20_max
                FROM _catalog
                WHERE exchange = 'NSE'
                WINDOW w AS (PARTITION BY symbol_id
                             ORDER BY timestamp
                             ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id ORDER BY timestamp DESC
                ) = 1
            """).fetchdf()
        finally:
            conn.close()

        vol_data = self._normalize_symbol_exchange_columns(vol_data)
        data = data.merge(
            vol_data[["symbol_id", "exchange", "vol_20_avg", "vol_20_max"]],
            on=["symbol_id", "exchange"],
            how="left",
        )
        data["vol_intensity"] = (
            data["volume"] / data["vol_20_avg"].replace(0, np.nan)
        ).fillna(1)
        return data

    def _compute_trend_persistence(
        self,
        data: pd.DataFrame,
        date: str,
    ) -> pd.DataFrame:
        """
        Trend Persistence score:
          - ADX score (normalized 0-100) from feature store
          - Price above SMA(20) and SMA(50) -> alignment bonus
        """
        adx_path = os.path.join(self.feature_store_dir, "adx", "NSE")
        if os.path.exists(adx_path):
            try:
                conn = self._get_conn()
                cutoff_ts = pd.to_datetime(date).strftime("%Y-%m-%d")
                adx_latest = conn.execute(
                    f"""
                    SELECT *
                    FROM read_parquet('{adx_path}/*.parquet')
                    WHERE timestamp <= '{cutoff_ts}'
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp DESC) = 1
                    """
                ).fetchdf()
                conn.close()
                if not adx_latest.empty:
                    adx_latest = self._normalize_symbol_exchange_columns(adx_latest)
                    if "adx_14" not in adx_latest.columns and "adx_value" in adx_latest.columns:
                        adx_latest["adx_14"] = adx_latest["adx_value"]
                    data = data.merge(
                        adx_latest[["symbol_id", "exchange", "adx_14"]],
                        on=["symbol_id", "exchange"],
                        how="left",
                    )
            except Exception as e:
                logger.warning(f"ADX load failed: {e}")

        if "adx_14" not in data.columns:
            data["adx_14"] = 50.0

        try:
            conn = self._get_conn()
            cutoff_ts = pd.to_datetime(date).strftime("%Y-%m-%d")
            sma_latest = conn.execute(f"""
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
            """).fetchdf()
            conn.close()

            sma_latest = self._normalize_symbol_exchange_columns(sma_latest)
            data = data.merge(
                sma_latest[["symbol_id", "exchange", "sma_20", "sma_50"]],
                on=["symbol_id", "exchange"],
                how="left",
            )
        except Exception as e:
            logger.warning(f"Could not compute SMA: {e}")

        if "sma_20" not in data.columns:
            data["sma_20"] = data["close"]
        if "sma_50" not in data.columns:
            data["sma_50"] = data["close"]

        data["adx_score"] = data.get("adx_14", 50).fillna(50) * 2
        data["sma20_aligned"] = (
            (data["close"] > data["sma_20"].replace(0, np.nan))
            .fillna(False)
            .astype(int)
        )
        data["sma50_aligned"] = (
            (data["close"] > data["sma_50"].replace(0, np.nan))
            .fillna(False)
            .astype(int)
        )
        above_sma20 = data["close"] > data["sma_20"].replace(0, np.nan)
        above_sma50 = data["close"] > data["sma_50"].replace(0, np.nan)
        dir_mult = pd.Series(1.0, index=data.index)
        dir_mult[~above_sma50] = 0.0
        dir_mult[~above_sma20 & above_sma50] = 0.5
        data["trend_score"] = (
            data["adx_score"].fillna(0) * 0.6 * dir_mult
            + data["sma20_aligned"] * 25
            + data["sma50_aligned"] * 15
        )
        return data

    def _compute_proximity_highs(
        self,
        data: pd.DataFrame,
        date: str,
        window: int = 252,
    ) -> pd.DataFrame:
        """
        Proximity to 52-Week High: (high_52w - close) / high_52w.
        Lower = closer to highs = better launch zone.
        """
        try:
            conn = self._get_conn()
            cutoff_ts = pd.to_datetime(date).strftime("%Y-%m-%d")
            highs = conn.execute(f"""
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
            """).fetchdf()
            conn.close()

            highs = self._normalize_symbol_exchange_columns(highs)
            data = data.merge(
                highs[["symbol_id", "exchange", "high_52w"]],
                on=["symbol_id", "exchange"],
                how="left",
            )
        except Exception as e:
            logger.warning(f"Could not compute proximity highs: {e}")

        if "high_52w" not in data.columns:
            data["high_52w"] = data["close"]

        data["prox_high"] = (
            1 - (data["close"] / data["high_52w"].replace(0, np.nan))
        ).fillna(0.5) * 100

        return data

    def _compute_delivery(
        self,
        data: pd.DataFrame,
        date: str,
    ) -> pd.DataFrame:
        """
        Delivery Quality score: higher delivery % indicates institutional buying.
        Uses raw delivery_pct; NaN defaults to 20 (typical market average).
        """
        try:
            conn = self._get_conn()
            cutoff_ts = pd.to_datetime(date).strftime("%Y-%m-%d")
            del_data = conn.execute(f"""
                SELECT symbol_id, exchange, delivery_pct
                FROM _delivery
                WHERE timestamp <= '{cutoff_ts}'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id, exchange
                    ORDER BY timestamp DESC
                ) = 1
            """).fetchdf()
            conn.close()

            if not del_data.empty:
                del_data = self._normalize_symbol_exchange_columns(del_data)
                data = data.merge(
                    del_data[["symbol_id", "exchange", "delivery_pct"]],
                    on=["symbol_id", "exchange"],
                    how="left",
                )
        except Exception as e:
            logger.warning(f"Could not compute delivery factor: {e}")

        if "delivery_pct" not in data.columns:
            data["delivery_pct"] = np.nan
        data["delivery_pct"] = data["delivery_pct"].fillna(20.0)
        return data

    def _compute_sector_strength(
        self,
        data: pd.DataFrame,
        date: str,
    ) -> pd.DataFrame:
        """Add sector leadership and stock-vs-sector relative strength inputs."""
        sector_rs, stock_vs_sector, sector_map = self._load_sector_inputs()
        if sector_rs.empty or stock_vs_sector.empty or not sector_map:
            data["sector_rs_value"] = 0.5
            data["stock_vs_sector_value"] = 0.0
            return data

        cutoff = pd.to_datetime(date).normalize()
        sector_slice = sector_rs.loc[sector_rs.index <= cutoff]
        stock_vs_slice = stock_vs_sector.loc[stock_vs_sector.index <= cutoff]
        if sector_slice.empty or stock_vs_slice.empty:
            data["sector_rs_value"] = 0.5
            data["stock_vs_sector_value"] = 0.0
            return data

        latest_sector = sector_slice.ffill().iloc[-1]
        latest_stock_vs = stock_vs_slice.ffill().iloc[-1]

        data["sector_name"] = data["symbol_id"].map(sector_map)
        data["sector_rs_value"] = data["sector_name"].map(latest_sector.to_dict())
        data["stock_vs_sector_value"] = data["symbol_id"].map(latest_stock_vs.to_dict())
        data["sector_rs_value"] = data["sector_rs_value"].fillna(0.5)
        data["stock_vs_sector_value"] = data["stock_vs_sector_value"].fillna(0.0)
        return data

    def rank_with_fundamentals(
        self,
        date: str = None,
        exchanges: List[str] = None,
        industry_filter: str = None,
        mcap_filter: str = None,
        min_score: float = 60.0,
        top_n: int = 50,
    ) -> pd.DataFrame:
        """
        Rank with fundamental filters applied first.
        """
        scores = self.rank_all(date, exchanges, min_score=0, top_n=None)

        fund_path = os.path.join(self.feature_store_dir, "fundamental", "NSE")
        if os.path.exists(fund_path):
            import glob

            fund_files = glob.glob(os.path.join(fund_path, "*.parquet"))
            if fund_files:
                fund_df = pd.concat(pd.read_parquet(f) for f in fund_files)
                scores = scores.merge(
                    fund_df[
                        ["symbol_id", "industry_group", "industry", "mcap_category"]
                    ],
                    on="symbol_id",
                    how="left",
                )

                if industry_filter:
                    scores = scores[
                        scores["industry"].str.contains(industry_filter, na=False)
                    ]
                if mcap_filter:
                    scores = scores[scores["mcap_category"] == mcap_filter]

        scores = scores.sort_values("composite_score", ascending=False)
        if top_n:
            scores = scores.head(top_n)
        return scores.reset_index(drop=True)
