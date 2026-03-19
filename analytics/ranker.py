import os
import logging
import duckdb
import pandas as pd
import numpy as np
from typing import Optional, List, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        "relative_strength": 0.35,
        "volume_intensity": 0.25,
        "trend_persistence": 0.15,
        "proximity_highs": 0.30,
    }

    def __init__(
        self,
        ohlcv_db_path: str = None,
        feature_store_dir: str = None,
    ):
        if ohlcv_db_path is None:
            ohlcv_db_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "data",
                "ohlcv.duckdb",
            )
        if feature_store_dir is None:
            feature_store_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "data",
                "feature_store",
            )
        self.ohlcv_db_path = ohlcv_db_path
        self.feature_store_dir = feature_store_dir
        os.makedirs(self.feature_store_dir, exist_ok=True)

    def _get_conn(self):
        return duckdb.connect(self.ohlcv_db_path)

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
                    timestamp,
                    close,
                    volume,
                    high,
                    low,
                    open
                FROM _catalog
                WHERE exchange IN ({",".join(f"'{e}'" for e in exchanges)})
                  AND timestamp IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id, exchange
                    ORDER BY timestamp DESC
                ) = 1
            """).fetchdf()
        finally:
            conn.close()

        if all_data.empty:
            logger.warning("No data available for ranking")
            return pd.DataFrame()

        all_data["timestamp"] = pd.to_datetime(all_data["timestamp"])

        scores = self._compute_relative_strength(all_data, date, benchmark_symbol)
        scores = self._compute_volume_intensity(scores)
        scores = self._compute_trend_persistence(scores, date)
        scores = self._compute_proximity_highs(scores, date)

        for factor, col in [
            ("relative_strength", "rel_strength"),
            ("volume_intensity", "vol_intensity"),
            ("trend_persistence", "trend_score"),
            ("proximity_highs", "prox_high"),
        ]:
            scores[f"{col}_score"] = scores[col].rank(pct=True) * 100

        scores["composite_score"] = sum(
            scores[f"{col}_score"] * w
            for col, w in [
                ("rel_strength", weights["relative_strength"]),
                ("vol_intensity", weights["volume_intensity"]),
                ("trend_score", weights["trend_persistence"]),
                ("prox_high", weights["proximity_highs"]),
            ]
        )

        scores = self._apply_1yr_penalty(scores, weights)

        scores = scores.sort_values("composite_score", ascending=False)
        scores = scores[scores["composite_score"] >= min_score]

        if top_n:
            scores = scores.head(top_n)

        cols = [
            "symbol_id",
            "exchange",
            "close",
            "composite_score",
            "rel_strength_score",
            "vol_intensity_score",
            "trend_score_score",
            "prox_high_score",
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
        import glob

        adx_path = os.path.join(self.feature_store_dir, "adx", "NSE")
        if os.path.exists(adx_path):
            try:
                files = glob.glob(os.path.join(adx_path, "*.parquet"))
                if files:
                    adx_df = pd.concat(pd.read_parquet(f) for f in files[:500])
                    adx_df["timestamp"] = pd.to_datetime(adx_df["timestamp"])
                    cutoff = pd.to_datetime(date)
                    adx_df = adx_df[adx_df["timestamp"] <= cutoff]
                    adx_latest = (
                        adx_df.groupby(["symbol_id", "exchange"])["adx_14"]
                        .last()
                        .reset_index()
                    )
                    data = data.merge(
                        adx_latest, on=["symbol_id", "exchange"], how="left"
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
        data["trend_score"] = (
            data["adx_score"].fillna(0) * 0.6
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
