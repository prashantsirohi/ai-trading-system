import os
import sqlite3
import sys
import warnings
from datetime import datetime, timedelta
from typing import Tuple, Dict, List, Optional

import numpy as np
import pandas as pd
import duckdb
from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.platform.logging.logger import logger

warnings.filterwarnings("ignore")


class RankBacktester:
    """
    Backtest framework for finding optimal ranking factor weights.

    Pipeline:
      Step 1  Equal Weight Baseline      â€” 25/25/25/25 weights
      Step 2  Cross-Sectional Backtest   â€” long top-N each rebalance period
      Step 3  Grid Search Optimization   â€” search weight combos on train set
      Step 4  Train/Test Split           â€” tune on train, validate on test
      Step 5  Equal Weighted Portfolio    â€” run on held-out test period
      Step 6  Performance Analysis       â€” Sharpe, Sortino, max DD, win rate
      Step 7  Weight Optimization        â€” best weights from grid search
    """

    DEFAULT_WEIGHTS = {
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
        top_n: int = 20,
        rebalance_days: int = 21,
        data_domain: str = "research",
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
        self.top_n = top_n
        self.rebalance_days = rebalance_days
        self.data_domain = data_domain
        self.master_db_path = str(paths.master_db_path)
        self._sector_rs_cache: Optional[pd.DataFrame] = None
        self._stock_vs_sector_cache: Optional[pd.DataFrame] = None
        self._sector_map_cache: Optional[dict[str, str]] = None

    def _get_conn(self):
        return duckdb.connect(self.ohlcv_db_path)

    def _load_sector_inputs(self) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str]]:
        """Lazily load sector leadership artifacts once per backtest process."""
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
            stock_vs_sector = stock_vs_sector[~stock_vs_sector.index.duplicated(keep="last")]
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

    def _attach_sector_strength(
        self,
        latest: pd.DataFrame,
        date: str,
    ) -> pd.DataFrame:
        """Merge sector leadership metrics for the requested ranking date."""
        sector_rs, stock_vs_sector, sector_map = self._load_sector_inputs()
        if sector_rs.empty or stock_vs_sector.empty or not sector_map:
            latest["sector_rs_value"] = 0.5
            latest["stock_vs_sector_value"] = 0.0
            return latest

        cutoff = pd.to_datetime(date).normalize()
        sector_slice = sector_rs.loc[sector_rs.index <= cutoff]
        stock_vs_slice = stock_vs_sector.loc[stock_vs_sector.index <= cutoff]
        if sector_slice.empty or stock_vs_slice.empty:
            latest["sector_rs_value"] = 0.5
            latest["stock_vs_sector_value"] = 0.0
            return latest

        latest_sector = sector_slice.ffill().iloc[-1]
        latest_stock_vs_sector = stock_vs_slice.ffill().iloc[-1]

        latest["sector_name"] = latest["symbol_id"].map(sector_map)
        latest["sector_rs_value"] = latest["sector_name"].map(latest_sector.to_dict())
        latest["stock_vs_sector_value"] = latest["symbol_id"].map(
            latest_stock_vs_sector.to_dict()
        )
        latest["sector_rs_value"] = latest["sector_rs_value"].fillna(0.5)
        latest["stock_vs_sector_value"] = latest["stock_vs_sector_value"].fillna(0.0)
        return latest

    # â”€â”€â”€ Step 1: Load data â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def load_ohlcv(self, from_date: str, to_date: str) -> pd.DataFrame:
        """
        Load OHLCV data for all NSE symbols as a wide DataFrame (date × symbol).
        Always loads from 400 days before from_date to ensure ranking features
        can be computed (features look up to 1 year of history).
        """
        lookback_date = (pd.to_datetime(from_date) - pd.Timedelta(days=400)).strftime(
            "%Y-%m-%d"
        )
        conn = self._get_conn()
        try:
            df = conn.execute(f"""
                SELECT
                    timestamp::DATE AS date,
                    symbol_id,
                    open, high, low, close, volume
                FROM _catalog
                WHERE exchange = 'NSE'
                  AND timestamp >= '{lookback_date}'
                  AND timestamp <= '{to_date}'
                ORDER BY timestamp, symbol_id
            """).fetchdf()
        finally:
            conn.close()

        df["date"] = pd.to_datetime(df["date"])
        prices = df.pivot(index="date", columns="symbol_id", values="close")
        prices = prices.dropna(thresh=int(len(prices) * 0.8), axis=1)
        prices = prices.ffill().dropna()
        return prices

    def load_features_for_ranking(
        self,
        date: str,
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        """
        Compute raw factor values for all symbols as of a given date.
        Returns DataFrame with columns: symbol_id, rel_strength, vol_intensity,
        trend_score, prox_high.
        """
        conn = self._get_conn()
        try:
            cutoff_ts = pd.to_datetime(date).strftime("%Y-%m-%d")

            latest = conn.execute(f"""
                SELECT
                    symbol_id, exchange, close, volume,
                    LAG(close, 20) OVER (PARTITION BY symbol_id ORDER BY timestamp) AS close_20d_ago,
                    MAX(high) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                    ) AS high_52w
                FROM _catalog
                WHERE exchange = '{exchange}'
                  AND timestamp <= '{cutoff_ts}'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id ORDER BY timestamp DESC
                ) = 1
            """).fetchdf()

            adx_path = os.path.join(
                self.feature_store_dir, "adx", exchange, "*.parquet"
            ).replace("\\", "/")
            if os.path.exists(os.path.join(self.feature_store_dir, "adx", exchange)):
                adx_df = conn.execute(f"""
                    SELECT symbol_id, exchange, adx_14 AS adx_value
                    FROM read_parquet('{adx_path}')
                    WHERE timestamp <= '{cutoff_ts}'
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY symbol_id ORDER BY timestamp DESC
                    ) = 1
                """).fetchdf()
                latest = latest.merge(
                    adx_df[["symbol_id", "exchange", "adx_value"]],
                    on=["symbol_id", "exchange"],
                    how="left",
                )
            latest["adx_value"] = latest["adx_value"].fillna(50)

            vol_df = conn.execute(f"""
                SELECT symbol_id, exchange, volume,
                       AVG(volume) OVER w AS vol_20_avg
                FROM _catalog
                WHERE exchange = '{exchange}'
                  AND timestamp <= '{cutoff_ts}'
                WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id ORDER BY timestamp DESC
                ) = 1
            """).fetchdf()
            latest = latest.merge(
                vol_df[["symbol_id", "exchange", "vol_20_avg"]],
                on=["symbol_id", "exchange"],
                how="left",
            )
            latest["vol_20_avg"] = latest["vol_20_avg"].fillna(latest["volume"])

            sma_df = conn.execute(f"""
                SELECT symbol_id, exchange, close,
                       AVG(close) OVER w20 AS sma_20,
                       AVG(close) OVER w50 AS sma_50
                FROM _catalog
                WHERE exchange = '{exchange}'
                  AND timestamp <= '{cutoff_ts}'
                WINDOW w20 AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                       w50 AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id ORDER BY timestamp DESC
                ) = 1
            """).fetchdf()
            latest = latest.merge(
                sma_df[["symbol_id", "exchange", "sma_20", "sma_50"]],
                on=["symbol_id", "exchange"],
                how="left",
            )

            # Research data may not include delivery snapshots yet. Use a neutral
            # fallback so the factor stays present without crashing the backtest.
            try:
                del_df = conn.execute(f"""
                    SELECT symbol_id, exchange, delivery_pct
                    FROM _delivery
                    WHERE timestamp <= '{cutoff_ts}'
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY symbol_id, exchange
                        ORDER BY timestamp DESC
                    ) = 1
                """).fetchdf()
                latest = latest.merge(
                    del_df[["symbol_id", "exchange", "delivery_pct"]],
                    on=["symbol_id", "exchange"],
                    how="left",
                )
            except duckdb.Error:
                latest["delivery_pct"] = np.nan
            latest["delivery_pct"] = latest["delivery_pct"].fillna(20.0)
        finally:
            conn.close()

        latest = self._attach_sector_strength(latest, date)

        latest["rel_strength"] = (
            (latest["close"] - latest["close_20d_ago"])
            / latest["close_20d_ago"].replace(0, np.nan)
        ).fillna(0) * 100

        latest["vol_intensity"] = (
            latest["volume"] / latest["vol_20_avg"].replace(0, np.nan)
        ).fillna(1)

        latest["adx_score"] = latest["adx_value"].fillna(50) * 2
        above_sma20 = latest["close"] > latest["sma_20"].replace(0, np.nan)
        above_sma50 = latest["close"] > latest["sma_50"].replace(0, np.nan)
        dir_mult = pd.Series(1.0, index=latest.index)
        dir_mult[~above_sma50] = 0.0
        dir_mult[~above_sma20 & above_sma50] = 0.5
        latest["trend_score"] = (
            latest["adx_score"].fillna(0) * 0.6 * dir_mult
            + above_sma20.astype(float) * 25
            + above_sma50.astype(float) * 15
        )

        latest["prox_high"] = (
            1 - (latest["close"] / latest["high_52w"].replace(0, np.nan))
        ).fillna(0.5) * 100

        return latest[
            [
                "symbol_id",
                "rel_strength",
                "vol_intensity",
                "trend_score",
                "prox_high",
                "delivery_pct",
                "sector_rs_value",
                "stock_vs_sector_value",
                "close",
            ]
        ]

    def rank_stocks(
        self,
        date: str,
        weights: Dict[str, float] = None,
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        """
        Rank all stocks for a given date using factor percentile scores.
        Returns sorted DataFrame with composite_score.
        """
        if weights is None:
            weights = self.DEFAULT_WEIGHTS

        df = self.load_features_for_ranking(date, exchange)

        for col in [
            "rel_strength",
            "vol_intensity",
            "trend_score",
            "prox_high",
            "delivery_pct",
        ]:
            df[f"{col}_pct"] = df[col].rank(pct=True) * 100

        df["sector_rs_pct"] = df["sector_rs_value"].rank(pct=True) * 100
        df["stock_vs_sector_pct"] = df["stock_vs_sector_value"].rank(pct=True) * 100
        # Favor stocks in strong sectors, while still rewarding names outperforming
        # their own sector peers.
        df["sector_strength_pct"] = (
            df["sector_rs_pct"] * 0.6 + df["stock_vs_sector_pct"] * 0.4
        )

        df["composite_score"] = (
            df["rel_strength_pct"] * weights["relative_strength"]
            + df["vol_intensity_pct"] * weights["volume_intensity"]
            + df["trend_score_pct"] * weights["trend_persistence"]
            + df["prox_high_pct"] * weights["proximity_highs"]
            + df["delivery_pct_pct"] * weights["delivery_pct"]
            + df["sector_strength_pct"] * weights["sector_strength"]
        )
        df = df.sort_values("composite_score", ascending=False)
        return df.reset_index(drop=True)

    # â”€â”€â”€ Step 2: Cross-sectional backtest signals â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def generate_signals(
        self,
        prices: pd.DataFrame,
        weights: Dict[str, float] = None,
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        """
        Generate daily entry/exit signals using top-N cross-sectional ranking.
        Rebalances every `rebalance_days` trading days.

        Returns a boolean DataFrame (same shape as prices) where True = long.
        """
        if weights is None:
            weights = self.DEFAULT_WEIGHTS

        dates = sorted(prices.index)
        n_dates = len(dates)

        signals = pd.DataFrame(False, index=dates, columns=prices.columns, dtype=bool)

        rank_cache = {}

        for i in range(0, n_dates, self.rebalance_days):
            rebal_date = dates[min(i, n_dates - 1)]
            rebal_date_str = rebal_date.strftime("%Y-%m-%d")

            logger.info(f"  Ranking for {rebal_date.date()}...")
            ranked = self.rank_stocks(rebal_date_str, weights, exchange)
            cutoff = ranked["composite_score"].quantile(0.75)
            eligible = ranked[ranked["composite_score"] >= cutoff]
            top_symbols = eligible.head(self.top_n)["symbol_id"].tolist()

            for sym in top_symbols:
                if sym in signals.columns:
                    signals.loc[rebal_date:, sym] = True

        return signals

    # â”€â”€â”€ Step 3 & 5: Run backtest via internal portfolio simulation â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def run_backtest(
        self,
        prices: pd.DataFrame,
        signals: pd.DataFrame,
        initial_cash: float = 1_000_000,
        fees: float = 0.001,
    ) -> Tuple[dict, pd.DataFrame, pd.DataFrame]:
        """
        Run equal-weight long-only basket backtest.

        For each rebalance period, computes the equal-weight return of all
        active stocks and compounds equity forward.
        Returns (metrics, equity_curve, trades).
        """
        entries = signals.reindex(prices.index).fillna(False)
        exits = entries.shift(self.rebalance_days).fillna(False)

        common_cols = prices.columns.intersection(entries.columns)
        prices_sub = prices[common_cols]
        entries_sub = entries[common_cols]
        exits_sub = exits[common_cols]

        equity_dates = []
        equity_values = []
        trades = []
        wins = 0

        n_rows = len(prices_sub)
        current_equity = initial_cash

        for period_start_idx in range(0, n_rows, self.rebalance_days):
            period_end_idx = period_start_idx + self.rebalance_days
            if period_end_idx >= n_rows:
                break

            entry_row = entries_sub.iloc[period_start_idx]
            exit_row = exits_sub.iloc[period_start_idx]
            active = entry_row & ~exit_row
            n_active = int(active.sum())

            if n_active == 0:
                continue

            entry_prices = prices_sub.iloc[period_start_idx]
            exit_prices = prices_sub.iloc[period_end_idx]

            stock_returns = (exit_prices - entry_prices) / entry_prices
            period_active = (
                active
                & entry_prices.notna()
                & exit_prices.notna()
                & (entry_prices > 0)
                & (exit_prices > 0)
            )
            valid_stocks = stock_returns[period_active]

            if len(valid_stocks) == 0:
                continue

            equal_wt_return = float(valid_stocks.mean())
            n_traded = len(valid_stocks)
            trade_cash = current_equity / n_traded
            fees_paid = trade_cash * fees * 2 * n_traded
            pnl = trade_cash * equal_wt_return * n_traded - fees_paid
            current_equity = max(0, current_equity + pnl)

            equity_dates.append(prices_sub.index[period_end_idx])
            equity_values.append(current_equity)

            if equal_wt_return > 0:
                wins += 1
            trades.append(
                {
                    "entry_date": prices_sub.index[period_start_idx],
                    "exit_date": prices_sub.index[period_end_idx],
                    "n_stocks": n_traded,
                    "avg_return": equal_wt_return,
                    "fees_paid": fees_paid,
                    "pnl": pnl,
                    "equity": current_equity,
                }
            )

        total_trades = len(trades)

        equity_series = pd.Series(
            [initial_cash] + equity_values,
            index=[prices_sub.index[0]] + equity_dates,
        )
        equity_series.index = pd.to_datetime(equity_series.index)

        final_equity = equity_series.iloc[-1]
        total_return = (final_equity / initial_cash - 1) * 100
        n_days = (equity_series.index[-1] - equity_series.index[0]).days
        annual_return = (
            ((final_equity / initial_cash) ** (365.0 / max(n_days, 1)) - 1) * 100
            if n_days > 0
            else 0
        )

        period_returns = (
            pd.Series(equity_values) / (pd.Series([initial_cash] + equity_values[:-1]))
            - 1
        )
        period_returns = period_returns.replace([np.inf, -np.inf], np.nan).dropna()

        running_max = equity_series.cummax()
        drawdown = (equity_series - running_max) / running_max.clip(lower=1e-9)
        max_drawdown = float(drawdown.min() * 100)

        if len(period_returns) > 1:
            ann_vol = (
                period_returns.std(ddof=0) * np.sqrt(252.0 / self.rebalance_days) * 100
            )
            annual_vol = ann_vol if np.isfinite(ann_vol) else 0
            sharpe = (annual_return / annual_vol) if annual_vol > 0 else 0
            downside = period_returns[period_returns < 0]
            dvol = downside.std(ddof=0) * np.sqrt(252.0 / self.rebalance_days) * 100
            sortino = (annual_return / dvol) if (len(downside) > 0 and dvol > 0) else 0
        else:
            annual_vol = 0
            sharpe = 0
            sortino = 0

        win_rate = (wins / total_trades * 100) if total_trades > 0 else 0

        metrics = {
            "total_return": round(total_return, 2),
            "annual_return": round(annual_return, 2),
            "annual_vol": round(annual_vol, 2),
            "sharpe_ratio": round(sharpe, 2),
            "sortino_ratio": round(sortino, 2),
            "max_drawdown": round(max_drawdown, 2),
            "n_trades": total_trades,
            "win_rate": round(win_rate, 1),
            "final_value": round(final_equity, 2),
        }

        equity_df = equity_series.reset_index()
        equity_df.columns = ["date", "equity"]
        trades_df = pd.DataFrame(trades) if trades else pd.DataFrame()

        return metrics, equity_df, trades_df

    # â”€â”€â”€ Step 3: Grid search on train set â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def grid_search_weights(
        self,
        prices: pd.DataFrame,
        grid: List[Dict[str, float]] = None,
    ) -> Tuple[Dict[str, float], pd.DataFrame]:
        """
        Step 3: Grid search over weight combinations.
        Evaluates each weight combo using Sharpe ratio on the provided prices.
        """
        if grid is None:
            grid = self._default_weight_grid()

        logger.info(f"Grid search over {len(grid)} weight combinations...")
        results = []

        for w in grid:
            try:
                signals = self.generate_signals(prices, w)
                metrics, _, _ = self.run_backtest(prices, signals)
                results.append(
                    {
                        **w,
                        **metrics,
                    }
                )
            except Exception as e:
                logger.warning(f"  Weight combo {w} failed: {e}")

        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values("sharpe_ratio", ascending=False)
        best = results_df.iloc[0]
        best_weights = {
            "relative_strength": float(best["relative_strength"]),
            "volume_intensity": float(best["volume_intensity"]),
            "trend_persistence": float(best["trend_persistence"]),
            "proximity_highs": float(best["proximity_highs"]),
            "delivery_pct": float(best["delivery_pct"]),
        }
        logger.info(f"Best weights: {best_weights}")
        logger.info(f"Best Sharpe: {best['sharpe_ratio']:.2f}")
        return best_weights, results_df

    def _default_weight_grid(self) -> List[Dict[str, float]]:
        """Generate a grid of weight combinations (must sum to 1)."""
        grid = []
        for rs in [0.1, 0.2, 0.25, 0.3, 0.35]:
            for vol in [0.1, 0.15, 0.2, 0.25]:
                for trend in [0.05, 0.1, 0.15, 0.2]:
                    for prox in [0.1, 0.15, 0.2, 0.25]:
                        deliv = round(1.0 - rs - vol - trend - prox, 2)
                        if deliv >= 0.05:
                            grid.append(
                                {
                                    "relative_strength": rs,
                                    "volume_intensity": vol,
                                    "trend_persistence": trend,
                                    "proximity_highs": prox,
                                    "delivery_pct": deliv,
                                }
                            )
        unique = []
        seen = set()
        for g in grid:
            key = tuple(round(v, 2) for v in g.values())
            if key not in seen:
                seen.add(key)
                unique.append(g)
        return unique

    # â”€â”€â”€ Steps 4-7: Full pipeline â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def run_full_pipeline(
        self,
        train_years: int = 2,
        test_years: int = 1,
        initial_cash: float = 1_000_000,
        fees: float = 0.001,
    ) -> Dict:
        """
        Steps 4-7: Full train/test pipeline.

        1. Split data into train and test periods
        2. Step 4: Run equal-weight baseline on train
        3. Step 3: Grid search for optimal weights on train
        4. Step 5: Run equal-weight on test (out-of-sample)
        5. Step 6: Analyze performance
        6. Step 7: Run optimized weights on test
        """
        conn = self._get_conn()
        try:
            latest_date = conn.execute(
                "SELECT MAX(timestamp) FROM _catalog WHERE exchange = 'NSE'"
            ).fetchone()[0]
            earliest_date = conn.execute(
                "SELECT MIN(timestamp) FROM _catalog WHERE exchange = 'NSE'"
            ).fetchone()[0]
        finally:
            conn.close()

        latest_str = str(latest_date)[:10]
        earliest_str = str(earliest_date)[:10]
        test_end = pd.to_datetime(latest_str)
        test_start = test_end - pd.Timedelta(days=test_years * 365)
        train_end = test_start - pd.Timedelta(days=1)
        train_start = train_end - pd.Timedelta(days=train_years * 365)

        logger.info("=" * 60)
        logger.info(f"Train: {train_start.date()} â†’ {train_end.date()}")
        logger.info(f"Test:  {test_start.date()} â†’ {test_end.date()}")
        logger.info("=" * 60)

        logger.info("Step 4: Loading price data for train period...")
        train_prices = self.load_ohlcv(
            train_start.strftime("%Y-%m-%d"),
            train_end.strftime("%Y-%m-%d"),
        )
        logger.info(
            f"  Train: {train_prices.shape[0]} days, {train_prices.shape[1]} symbols"
        )

        logger.info("Step 4: Equal-weight baseline on train set...")
        eq_signals_train = self.generate_signals(train_prices, self.DEFAULT_WEIGHTS)
        eq_metrics_train, eq_equity_train, _ = self.run_backtest(
            train_prices, eq_signals_train, initial_cash, fees
        )
        self._print_metrics("  Equal-weight (train)", eq_metrics_train)

        logger.info("Step 3: Grid search on train set...")
        best_weights, grid_results = self.grid_search_weights(train_prices)
        logger.info(f"  Grid evaluated {len(grid_results)} combinations")
        self._print_metrics("  Optimized (train)", grid_results.iloc[0].to_dict())

        logger.info("Step 5: Loading price data for test period...")
        test_prices = self.load_ohlcv(
            test_start.strftime("%Y-%m-%d"),
            test_end.strftime("%Y-%m-%d"),
        )
        logger.info(
            f"  Test: {test_prices.shape[0]} days, {test_prices.shape[1]} symbols"
        )

        logger.info("Step 5: Equal-weight on test (out-of-sample)...")
        eq_signals_test = self.generate_signals(test_prices, self.DEFAULT_WEIGHTS)
        eq_metrics_test, eq_equity_test, _ = self.run_backtest(
            test_prices, eq_signals_test, initial_cash, fees
        )
        self._print_metrics("  Equal-weight (test)", eq_metrics_test)

        logger.info("Step 7: Optimized weights on test set...")
        opt_signals_test = self.generate_signals(test_prices, best_weights)
        opt_metrics_test, opt_equity_test, opt_trades_test = self.run_backtest(
            test_prices, opt_signals_test, initial_cash, fees
        )
        self._print_metrics("  Optimized (test)", opt_metrics_test)

        logger.info("=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)
        summary = {
            "train_period": f"{train_start.date()} to {train_end.date()}",
            "test_period": f"{test_start.date()} to {test_end.date()}",
            "best_weights": best_weights,
            "train_equal_weight": eq_metrics_train,
            "train_optimized": grid_results.iloc[0].to_dict(),
            "test_equal_weight": eq_metrics_test,
            "test_optimized": opt_metrics_test,
            "grid_results": grid_results,
            "test_optimized_equity": opt_equity_test,
            "test_optimized_trades": opt_trades_test,
            "test_equal_weight_equity": eq_equity_test,
        }
        logger.info(f"Best weights: {best_weights}")
        logger.info(f"Equal-weight test Sharpe: {eq_metrics_test['sharpe_ratio']:.2f}")
        logger.info(f"Optimized test Sharpe: {opt_metrics_test['sharpe_ratio']:.2f}")
        logger.info(
            f"Improvement: {opt_metrics_test['sharpe_ratio'] - eq_metrics_test['sharpe_ratio']:+.2f}"
        )
        return summary

    def _print_metrics(self, label: str, metrics: Dict):
        if isinstance(metrics, dict):
            logger.info(f"  {label}:")
            logger.info(
                f"    Ret={metrics.get('total_return', 0):.1f}%, "
                f"Sharpe={metrics.get('sharpe_ratio', 0):.2f}, "
                f"MaxDD={metrics.get('max_drawdown', 0):.1f}%, "
                f"Trades={metrics.get('n_trades', 0)}"
            )

    def quick_backtest(
        self,
        weights: Dict[str, float] = None,
        from_date: str = None,
        to_date: str = None,
        train_frac: float = 0.7,
        initial_cash: float = 1_000_000,
    ) -> Dict:
        """
        Single backtest on a date range with optional train/test split.
        Convenience method â€” calls run_full_pipeline with simplified params.
        """
        if weights is None:
            weights = self.DEFAULT_WEIGHTS

        if from_date is None:
            conn = self._get_conn()
            try:
                latest = conn.execute(
                    "SELECT MAX(timestamp) FROM _catalog WHERE exchange = 'NSE'"
                ).fetchone()[0]
            finally:
                conn.close()
            to_date = str(latest)[:10]

        if to_date is None:
            to_date = from_date
            from_date = (pd.to_datetime(to_date) - pd.Timedelta(days=730)).strftime(
                "%Y-%m-%d"
            )

        prices = self.load_ohlcv(from_date, to_date)
        signals = self.generate_signals(prices, weights)
        metrics, equity, trades = self.run_backtest(prices, signals, initial_cash)
        return {
            "metrics": metrics,
            "equity_curve": equity,
            "trades": trades,
            "prices": prices,
            "signals": signals,
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Rank backtester")
    parser.add_argument("--train-years", type=int, default=2)
    parser.add_argument("--test-years", type=int, default=1)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--rebalance-days", type=int, default=21)
    parser.add_argument("--cash", type=float, default=1_000_000)
    args = parser.parse_args()

    bt = RankBacktester(top_n=args.top_n, rebalance_days=args.rebalance_days)
    result = bt.run_full_pipeline(
        train_years=args.train_years,
        test_years=args.test_years,
        initial_cash=args.cash,
    )
    return result


if __name__ == "__main__":
    main()
